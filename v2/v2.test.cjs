// Unit test: v2.js headline math must match the decided method (Python .qa/headline.py → ≈$3,168 on 2026-10-08 dump).
// Usage: node v2/v2.test.cjs [/tmp/ethval-data.json]
const assert = require('assert');
const fs = require('fs');
const V2 = require('./v2.js');

// 1. pure helpers
assert.strictEqual(V2.median([3, 1, 2]), 2);
assert.strictEqual(V2.median([4, 1, 2, 3]), 2.5);
const sm = V2.smooth([{ date: '2026-01-01', value: 10 }, { date: '2026-01-02', value: 20 }, { date: '2026-01-03', value: -1 }], 30);
assert.strictEqual(sm.get('2026-01-02'), 15);
assert.ok(!sm.has('2026-01-03'), 'non-positive values are skipped');
assert.ok(V2.ANCHORED.every(m => !V2.INDEPENDENT.includes(m)), 'anchored and independent sets are disjoint');
assert.strictEqual(V2.ANCHORED.length + V2.INDEPENDENT.length, 12);

// 2. real-data regression (optional when dump exists)
const p = process.argv[2] || '/tmp/ethval-data.json';
if (fs.existsSync(p)) {
  const D = JSON.parse(fs.readFileSync(p, 'utf8'));
  const hist = {};
  for (const [id, s] of Object.entries(D.hf)) hist[id] = s.map(([date, value]) => ({ date, value }));
  const ph = D.price.map(([date, value]) => ({ date, value }));
  const last = V2.dayKey(ph[ph.length - 1].date);
  const r = V2.valueOn(hist, V2.INDEPENDENT, last);
  console.log('headline', last, Math.round(r.value), 'range', Math.round(r.lo), Math.round(r.hi), 'n', r.n);
  assert.ok(Math.abs(r.value - 3168) / 3168 < 0.02, `headline ${r.value} should be ≈3168`);
  const tr = V2.trackRecord(hist, ph, V2.INDEPENDENT, 180);
  console.log('track', tr);
  assert.ok(tr.abovePct >= 55 && tr.abovePct <= 65, 'above-price share ≈60%');
  const series = V2.compositeSeries(hist, ph);
  let jumps = 0;
  for (let i = 1; i < series.length; i++) if (Math.abs(series[i].value / series[i - 1].value - 1) > 0.15) jumps++;
  console.log('composite points', series.length, 'jumps>15%', jumps);
  assert.strictEqual(jumps, 0);
}
console.log('v2 tests OK');
