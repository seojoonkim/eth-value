// Lightweight replacement for Dune query 6386591 ("L2 Total Volume"), which now fails with
// "too many stages" because it rescans every chain from 2022 on each run.
// Same method/filters as the original, limited to the last N days, so the series stays continuous.
const CHAINS = [
    // [display name, dune chain/table prefix, native price source]
    ['Arbitrum', 'arbitrum', 'eth'],
    ['Base', 'base', 'eth'],
    ['Optimism', 'optimism', 'eth'],
    ['Blast', 'blast', 'eth'],
    ['zkSync Era', 'zksync', 'eth'],
    ['Scroll', 'scroll', 'eth'],
    ['Linea', 'linea', 'eth'],
    ['Mantle', 'mantle', 'mnt'],
];

// One chain, one date window → small enough for Dune's planner (the 8-chain UNION fails with "too many stages").
function l2ChainSQL(chain, fromDate, toDate) {
    const [name, t, px] = chain;
    const token = px === 'mnt'
        ? "blockchain = 'mantle' AND contract_address = 0x78c1b0c915c4faa5fffa6cabf0219da63d7f4cb8"
        : "blockchain = 'ethereum' AND contract_address = 0xc02aaa39b223fe8d0a0e5c4f27ead9083c756cc2";
    const win = (col) => `${col} >= DATE '${fromDate}' AND ${col} < DATE '${toDate}'`;
    return `WITH p AS (
  SELECT DATE_TRUNC('day', minute) AS date, AVG(price) AS price FROM prices.usd WHERE ${token} AND ${win('minute')} GROUP BY 1
), n AS (
  SELECT DATE_TRUNC('day', block_time) AS date, SUM(value / 1e18) AS native_amount
  FROM ${t}.transactions WHERE ${win('block_time')} AND value > 0 AND success = true GROUP BY 1
), k AS (
  SELECT DATE_TRUNC('day', block_time) AS date, SUM(amount_usd) AS token_volume_usd
  FROM tokens.transfers WHERE blockchain = '${t}' AND ${win('block_time')} AND amount_usd > 0 AND amount_usd < 1e12 GROUP BY 1
)
SELECT n.date, '${name}' AS chain, n.native_amount * p.price AS native_volume_usd,
       n.native_amount * p.price + COALESCE(k.token_volume_usd, 0) AS total_volume_usd
FROM n JOIN p ON p.date = n.date LEFT JOIN k ON k.date = n.date ORDER BY 1`;
}

// Backward-compatible name used in tests: SQL for one chain over the last `days` days.
function l2RecentSQL(days = 10, chain = CHAINS[0]) {
    const d = (ms) => new Date(ms).toISOString().slice(0, 10);
    return l2ChainSQL(chain, d(Date.now() - days * 864e5), d(Date.now() + 864e5));
}

const sleep = (ms) => new Promise(r => setTimeout(r, ms));

// Run ad-hoc SQL on Dune. Tries the direct SQL endpoint, then create-query+execute.
async function runDuneSQL(apiKey, sql, { log = console.log, maxWaitMs = 600000 } = {}) {
    const H = { 'X-Dune-API-Key': apiKey, 'Content-Type': 'application/json' };
    let executionId = null;
    let r = await fetch('https://api.dune.com/api/v1/sql/execute', { method: 'POST', headers: H, body: JSON.stringify({ sql, performance: 'medium' }) });
    if (r.ok) executionId = (await r.json()).execution_id;
    else {
        log(`  ↪ dune sql/execute HTTP ${r.status}; trying create-query`);
        r = await fetch('https://api.dune.com/api/v1/query', { method: 'POST', headers: H, body: JSON.stringify({ name: 'ETHval L2 volume (recent, auto)', query_sql: sql, is_private: false }) });
        if (!r.ok) throw new Error(`dune create query HTTP ${r.status} ${(await r.text()).slice(0, 160)}`);
        const qid = (await r.json()).query_id;
        r = await fetch(`https://api.dune.com/api/v1/query/${qid}/execute`, { method: 'POST', headers: H, body: JSON.stringify({ performance: 'medium' }) });
        if (!r.ok) throw new Error(`dune execute HTTP ${r.status} ${(await r.text()).slice(0, 160)}`);
        executionId = (await r.json()).execution_id;
    }
    log(`  ⏳ dune execution ${executionId}`);
    const t0 = Date.now();
    while (Date.now() - t0 < maxWaitMs) {
        await sleep(8000);
        const s = await fetch(`https://api.dune.com/api/v1/execution/${executionId}/status`, { headers: H }).then(x => x.json()).catch(() => ({}));
        if (s.state === 'QUERY_STATE_COMPLETED') break;
        if (/FAILED|CANCELLED|EXPIRED/.test(s.state || '')) throw new Error(`dune ${s.state}: ${JSON.stringify(s.error || '').slice(0, 200)}`);
    }
    const res = await fetch(`https://api.dune.com/api/v1/execution/${executionId}/results?limit=5000`, { headers: H }).then(x => x.json());
    if (!res.result) throw new Error(`dune results missing: ${JSON.stringify(res.error || res).slice(0, 200)}`);
    return res.result.rows || [];
}

// Collect [fromDate, toDate) for all chains in chunks of `chunkDays`. Failures are per chunk, not all-or-nothing.
async function collectL2Range(apiKey, fromDate, toDate, { chunkDays = 31, log = console.log } = {}) {
    const rows = [], failures = [];
    for (const chain of CHAINS) {
        for (let a = Date.parse(fromDate); a < Date.parse(toDate); a += chunkDays * 864e5) {
            const f = new Date(a).toISOString().slice(0, 10);
            const t = new Date(Math.min(a + chunkDays * 864e5, Date.parse(toDate))).toISOString().slice(0, 10);
            try { rows.push(...await runDuneSQL(apiKey, l2ChainSQL(chain, f, t), { log: () => {} })); }
            catch (e) { failures.push(`${chain[0]} ${f}: ${e.message.slice(0, 80)}`); }
        }
        log(`  ↪ L2 ${chain[0]}: rows so far ${rows.length}`);
    }
    return { rows, failures };
}

module.exports = { l2RecentSQL, l2ChainSQL, collectL2Range, runDuneSQL, CHAINS };
