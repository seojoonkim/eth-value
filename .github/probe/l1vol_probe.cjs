// Probe (CI only): which tokens drive L1 total-volume spike days? Prints aggregates only.
const { runDuneSQL } = require('../../scripts/dune-l2-recent.js');
const KEY = process.env.DUNE_API_KEY;
const days = ['2026-09-06', '2026-10-06', '2026-09-09', '2026-09-25', '2026-10-02', '2026-09-05'];
const inList = days.map(d => `DATE '${d}'`).join(',');
const Q = {
  top_tokens: `WITH t AS (SELECT CAST(block_date AS DATE) AS d, contract_address, symbol, SUM(amount_usd) AS usd, COUNT(*) AS n, MAX(amount_usd) AS max_tx, approx_percentile(price_usd, 0.5) AS p50
    FROM tokens.transfers WHERE blockchain='ethereum' AND block_date IN (${inList}) AND amount_usd > 0 AND amount_usd < 1e12 GROUP BY 1,2,3)
    SELECT * FROM (SELECT t.*, ROW_NUMBER() OVER (PARTITION BY d ORDER BY usd DESC) rk FROM t) WHERE rk <= 6 ORDER BY d, rk`,
  big_tx: `SELECT CAST(block_date AS DATE) d, symbol, contract_address, amount, price_usd, amount_usd, tx_hash FROM tokens.transfers
    WHERE blockchain='ethereum' AND block_date IN (${inList}) AND amount_usd > 1e10 ORDER BY amount_usd DESC LIMIT 25`,
  daily_bigtx: `SELECT CAST(block_date AS DATE) d, COUNT(*) n_gt_1e10, SUM(amount_usd) usd_gt_1e10, SUM(CASE WHEN amount_usd > 1e9 THEN amount_usd END) usd_gt_1e9
    FROM tokens.transfers WHERE blockchain='ethereum' AND block_date >= DATE '2026-08-25' AND amount_usd > 1e9 AND amount_usd < 1e12 GROUP BY 1 ORDER BY 1`,
};
(async () => {
  for (const [k, sql] of Object.entries(Q)) {
    try { const rows = await runDuneSQL(KEY, sql, { log: () => {}, maxWaitMs: 600000 });
      console.log(`### ${k} (${rows.length})`); for (const r of rows) console.log(JSON.stringify(r)); }
    catch (e) { console.log(`${k}: ERR ${e.message.slice(0, 300)}`); }
  }
})();
