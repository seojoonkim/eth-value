// Probe (CI only): why L2 stablecoin volume for polygon/arbitrum stopped advancing.
// Runs the collector's own SQL for a recent window + a symbol census. Prints row counts only.
const { runDuneSQL } = require('../scripts/dune-l2-recent.js');
const extra = require('../scripts/extra-sources.js');
const KEY = process.env.DUNE_API_KEY;
(async () => {
    for (const [chain, from, to] of [['polygon', '2026-03-19', '2026-03-29'], ['polygon', '2026-09-28', '2026-10-08'], ['arbitrum', '2026-08-06', '2026-08-16'], ['arbitrum', '2026-09-28', '2026-10-08'], ['base', '2026-09-28', '2026-10-08']]) {
        const t0 = Date.now();
        try {
            const rows = await runDuneSQL(KEY, extra.l2StablecoinVolSQL(chain, from, to), { log: () => {}, maxWaitMs: 300000 });
            console.log(`SQL ${chain} ${from}..${to} rows=${rows.length} sum=${rows.reduce((a, r) => a + (+r.total_volume || 0), 0).toExponential(2)} ${((Date.now() - t0) / 1000).toFixed(0)}s`);
        } catch (e) { console.log(`SQL ${chain} ${from}..${to} ERR ${e.message.slice(0, 200)} ${((Date.now() - t0) / 1000).toFixed(0)}s`); }
    }
    for (const chain of ['polygon', 'arbitrum']) {
        const sql = `SELECT symbol, COUNT(*) AS n, SUM(amount_usd) AS usd, MAX(block_time) AS last_t
FROM tokens.transfers WHERE blockchain = '${chain}' AND block_time >= DATE '2026-10-01' AND block_time < DATE '2026-10-03'
  AND (symbol LIKE 'USD%' OR symbol = 'DAI') GROUP BY 1 ORDER BY 3 DESC NULLS LAST LIMIT 12`;
        try {
            const rows = await runDuneSQL(KEY, sql, { log: () => {}, maxWaitMs: 300000 });
            console.log(`CENSUS ${chain}: ${rows.map(r => `${r.symbol}:${r.n}:${r.usd ? (+r.usd).toExponential(1) : 'null'}`).join(' ')}`);
        } catch (e) { console.log(`CENSUS ${chain} ERR ${e.message.slice(0, 200)}`); }
    }
})();
