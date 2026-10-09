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

function l2RecentSQL(days = 10) {
    const since = `CURRENT_DATE - INTERVAL '${days}' DAY`;
    const parts = CHAINS.map(([name, t, px]) => `
  SELECT n.date, '${name}' AS chain,
         n.native_amount * p.price AS native_volume_usd,
         n.native_amount * p.price + COALESCE(k.token_volume_usd, 0) AS total_volume_usd
  FROM (SELECT DATE_TRUNC('day', block_time) AS date, SUM(value / 1e18) AS native_amount
        FROM ${t}.transactions WHERE block_time >= ${since} AND value > 0 AND success = true GROUP BY 1) n
  JOIN ${px}_prices p ON p.date = n.date
  LEFT JOIN (SELECT DATE_TRUNC('day', block_time) AS date, SUM(amount_usd) AS token_volume_usd
             FROM tokens.transfers WHERE blockchain = '${t}' AND block_time >= ${since}
               AND amount_usd > 0 AND amount_usd < 1e12 GROUP BY 1) k ON k.date = n.date`).join('\n  UNION ALL');
    return `WITH eth_prices AS (
  SELECT DATE_TRUNC('day', minute) AS date, AVG(price) AS price FROM prices.usd
  WHERE blockchain = 'ethereum' AND contract_address = 0xc02aaa39b223fe8d0a0e5c4f27ead9083c756cc2 AND minute >= ${since} GROUP BY 1
), mnt_prices AS (
  SELECT DATE_TRUNC('day', minute) AS date, AVG(price) AS price FROM prices.usd
  WHERE blockchain = 'mantle' AND contract_address = 0x78c1b0c915c4faa5fffa6cabf0219da63d7f4cb8 AND minute >= ${since} GROUP BY 1
)
SELECT * FROM (${parts}
) ORDER BY date DESC, chain`;
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

module.exports = { l2RecentSQL, runDuneSQL, CHAINS };
