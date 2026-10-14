// Probe (CI only): per-transaction concentration of L1 token-transfer USD on spike days. Aggregates only.
const { runDuneSQL } = require('../../scripts/dune-l2-recent.js');
const KEY = process.env.DUNE_API_KEY;
const days = ['2026-09-06', '2026-10-06', '2026-09-09', '2026-09-25', '2026-10-02', '2026-09-05'];
const inList = days.map(d => `DATE '${d}'`).join(',');
const Q = {
  top_tx: `WITH x AS (SELECT CAST(block_date AS DATE) d, tx_hash, tx_to, COUNT(*) n, SUM(amount_usd) usd,
      SUM(CASE WHEN "from" = 0x0000000000000000000000000000000000000000 OR "to" = 0x0000000000000000000000000000000000000000 THEN amount_usd ELSE 0 END) mintburn_usd,
      ARRAY_JOIN(ARRAY_AGG(DISTINCT symbol), ',') syms
    FROM tokens.transfers WHERE blockchain='ethereum' AND block_date IN (${inList}) AND amount_usd > 0 AND amount_usd < 1e12 GROUP BY 1,2,3)
    SELECT * FROM (SELECT x.*, ROW_NUMBER() OVER (PARTITION BY d ORDER BY usd DESC) rk FROM x) WHERE rk <= 5 ORDER BY d, rk`,
  concentration: `WITH x AS (SELECT CAST(block_date AS DATE) d, tx_hash, tx_to, SUM(amount_usd) usd FROM tokens.transfers
      WHERE blockchain='ethereum' AND block_date >= DATE '2026-08-25' AND amount_usd > 0 AND amount_usd < 1e12 GROUP BY 1,2,3)
    SELECT d, SUM(usd) total, SUM(CASE WHEN usd > 1e10 THEN usd ELSE 0 END) usd_in_tx_gt_10b, COUNT_IF(usd > 1e10) n_tx_gt_10b,
      MAX_BY(tx_to, usd) top_to, MAX(usd) top_tx_usd FROM x GROUP BY 1 ORDER BY 1`,
};
(async () => {
  for (const [k, sql] of Object.entries(Q)) {
    try { const rows = await runDuneSQL(KEY, sql, { log: () => {}, maxWaitMs: 600000 });
      console.log(`### ${k} (${rows.length})`); for (const r of rows) console.log(JSON.stringify(r)); }
    catch (e) { console.log(`${k}: ERR ${e.message.slice(0, 300)}`); }
  }
  process.exit(0);
})();
