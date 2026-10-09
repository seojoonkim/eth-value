// Probe (CI only): is Dune's Blast data still updating? Prints max timestamps / row counts only.
const { runDuneSQL } = require('../../scripts/dune-l2-recent.js');
const KEY = process.env.DUNE_API_KEY;
const Q = {
  tx_last: "SELECT MAX(block_time) AS last_t, COUNT(*) AS n FROM blast.transactions WHERE block_time >= DATE '2025-09-01'",
  tx_by_month: "SELECT DATE_TRUNC('month', block_time) AS m, COUNT(*) AS n FROM blast.transactions WHERE block_time >= DATE '2025-06-01' GROUP BY 1 ORDER BY 1",
  blocks_last: "SELECT MAX(time) AS last_t, MAX(number) AS last_block FROM blast.blocks WHERE time >= DATE '2025-09-01'",
  transfers_last: "SELECT MAX(block_time) AS last_t, COUNT(*) AS n FROM tokens.transfers WHERE blockchain = 'blast' AND block_time >= DATE '2025-09-01'",
};
(async () => {
  for (const [k, sql] of Object.entries(Q)) {
    try { const rows = await runDuneSQL(KEY, sql, { log: () => {}, maxWaitMs: 300000 }); console.log(`${k}: ${JSON.stringify(rows)}`); }
    catch (e) { console.log(`${k}: ERR ${e.message.slice(0, 200)}`); }
  }
})();
