// 2026-11-15: self-test for float_consistency_check judge() — scenario inputs are built from the card formula itself
// (not site data): (1) live price ticks between loads -> PASS, (2) stale first render (original bug) -> FAIL,
// (3) same price but different inputs -> FAIL. usage: node .qa/float_judge_selftest.cjs
const { judge } = require('./float_consistency_check.cjs');
const supply = 120.7e6, staked = 36.1e6, tvl = 60e9, l2Tvl = 30e9, oldest = 52.0;
const f = px => Math.max(0, 100 - staked / supply * 100 - tvl / px / supply * 100 - l2Tvl / px / supply * 100 - 3);
const run = (mode, px, cardPx = px, inp = {}) => {
  const st = inp.staked ?? staked;
  const v = Math.max(0, 100 - st / supply * 100 - tvl / cardPx / supply * 100 - l2Tvl / cardPx / supply * 100 - 3);
  const calc = Math.max(0, 100 - st / supply * 100 - tvl / px / supply * 100 - l2Tvl / px / supply * 100 - 3);
  const ch = v - oldest;
  return { mode, px, price: px, supply, tvl, l2Tvl, staked: st, value: v.toFixed(1), change: (ch >= 0 ? '+' : '') + ch.toFixed(1) + 'pp 1Y', calc: calc.toFixed(1), chartLast: v.toFixed(1) };
};
let bad = 0;
const t = (name, runs, wantPass) => {
  const fails = judge(runs); const ok = (fails.length === 0) === wantPass;
  if (!ok) bad++;
  console.log((ok ? 'ok  ' : 'BAD ') + name + ' -> ' + (fails.length ? 'FAIL: ' + fails.join(' | ') : 'PASS'));
};
console.log('float at 1825.06 / 1810.65:', f(1825.06).toFixed(3), f(1810.65).toFixed(3));
t('2026-11-15 tick 1825.06 -> 1810.65', [run('normal', 1825.06), run('normal', 1825.06), run('price-late', 1810.65), run('supabase-late', 1810.65)], true);
t('same price everywhere', [run('normal', 2000), run('normal', 2000), run('price-late', 2000), run('supabase-late', 2000)], true);
t('original bug: stale first render (card at 2507.01, state 2357.58)', [run('normal', 2357.58), run('normal', 2357.58), run('price-late', 2357.58, 2507.01), run('supabase-late', 2357.58)], false);
t('price tick + one load with different staked input', [run('normal', 1825.06), run('normal', 1825.06), run('price-late', 1810.65, 1810.65, { staked: 37.5e6 }), run('supabase-late', 1810.65)], false);
if (bad) { console.log('FLOAT JUDGE SELFTEST FAIL (' + bad + ')'); process.exit(1); }
console.log('FLOAT JUDGE SELFTEST PASS');
