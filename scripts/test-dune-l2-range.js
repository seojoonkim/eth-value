// Repro for 2026-10-14 collect-data timeout: Blast (not indexed after 2025-09-26) must not be queried
// for windows after that day, and every chunk must carry a bounded wait. Run: node scripts/test-dune-l2-range.js
const assert = require('assert');
const { collectL2Range, CHAINS } = require('./dune-l2-recent.js');
(async () => {
  const calls = [];
  const runner = async (key, sql, opt) => { calls.push({ chain: (sql.match(/'([^']+)' AS chain/) || [])[1], from: (sql.match(/>= DATE '([\d-]+)'/) || [])[1], wait: opt.maxWaitMs }); return []; };
  await collectL2Range('k', '2026-10-10', '2026-10-14', { runner, log: () => {} });
  assert(!calls.some(c => c.chain === 'Blast'), 'Blast queried after its last indexed day');
  assert.strictEqual(calls.length, CHAINS.length - 1, 'other chains still queried once each');
  assert(calls.every(c => c.wait > 0 && c.wait <= 300000), 'per-chunk wait must be bounded to 5 min');
  calls.length = 0;
  await collectL2Range('k', '2025-09-01', '2025-10-15', { runner, log: () => {} });
  const b = calls.filter(c => c.chain === 'Blast');
  assert(b.length >= 1 && b.every(c => c.from <= '2025-09-26'), 'Blast history before the cutoff is still collected');
  console.log('DUNE L2 RANGE PASS', calls.length, 'calls');
})().catch(e => { console.error('DUNE L2 RANGE FAIL', e.message); process.exit(1); });
