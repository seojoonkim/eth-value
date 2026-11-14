// 2026-10-15: Effective Float card (#effective-float-display / #float-change) must be the same on every load
// of the same build, regardless of whether the live price or the Supabase close arrives first.
// Bug seen live: 49.6% -2.3pp vs 50.8% -1.1pp (first render used yesterday's close 2507.01 vs live 2357.58
// and was never recomputed). Loads: normal x2, live price delayed 6s, Supabase delayed 6s.
// PASS when all loads show the same value/change AND the value equals the formula recomputed from final state
// with the price the card claims to use.
// usage: node .qa/with_server.cjs .qa/float_consistency_check.cjs   (or: node .qa/float_consistency_check.cjs https://ethval.com/)
function judge(runs) {
  const fails = [];
  const vals = new Set(runs.map(r => r.value + ' | ' + r.change));
  // 2026-10-15: live price is real (not mocked) so it ticks between loads; at a 0.05 rounding boundary 2371.74 vs
  // 2373.22 legitimately gives 49.7 vs 49.8.
  // 2026-11-15: a bigger tick (1,825.06 -> 1,810.65, -0.8%) moved the card by more than 0.1pp and the old ≤0.1pp
  // tolerance flagged it as the stale-render bug (full QA failed once, rerun PASS). When loads used different prices,
  // compare like with like instead: recompute every load's float at ONE reference price from that load's own inputs
  // (staked/TVL/L2 TVL/supply) — those must agree (same data), each card must equal its own formula + chart, and
  // the period change must move by the same amount as the value (oldest point is price-independent history).
  // The original bug (card rendered with a stale price, never recomputed) still fails: card != own formula.
  const selfOk = runs.every(r => r.value === r.calc && r.chartLast === r.calc);
  const prices = new Set(runs.map(r => r.px));
  const refPx = runs[0].px;
  const floatAt = (r, px) => (px > 0 && r.supply > 0) ? Math.max(0, 100 - (r.staked / r.supply) * 100 - (r.tvl / px / r.supply) * 100 - (r.l2Tvl / px / r.supply) * 100 - 3) : NaN;
  const atRef = runs.map(r => floatAt(r, refPx));
  const base = runs.map(r => parseFloat(r.change) - (+r.value)); // change - value = -(oldest point), price-independent
  const spread = a => Math.max(...a) - Math.min(...a);
  const sameInputs = atRef.every(Number.isFinite) && spread(atRef) <= 0.05;
  const sameBase = base.every(Number.isFinite) && spread(base) <= 0.2001; // two 0.1 roundings
  const tick = selfOk && prices.size > 1 && sameInputs && sameBase;
  if (prices.size > 1) console.log('price per load: ' + runs.map(r => r.mode + '=' + r.px).join(', ') + ' | float at ref price ' + refPx + ': ' + atRef.map(x => x.toFixed(3)).join(', '));
  if (vals.size !== 1 && !tick) {
    fails.push('card differs between loads: ' + [...vals].join(' / '));
    if (prices.size > 1 && !sameInputs) fails.push('same-price recompute differs (inputs differ, not only price): ' + atRef.map(x => x.toFixed(3)).join(', '));
    if (prices.size > 1 && !sameBase) fails.push('period change does not move with value (oldest point differs): ' + base.map(x => x.toFixed(2)).join(', '));
  }
  if (vals.size !== 1 && tick) console.log('note: live price differed between loads; each load = own formula + chart, same inputs at one price: ' + [...vals].join(' / '));
  runs.forEach(r => {
    if (!/^\d+\.\d$/.test(r.value)) fails.push(`${r.mode}: value not rendered (${r.value})`);
    if (r.calc && r.value !== r.calc) fails.push(`${r.mode}: card ${r.value} != recomputed from final state ${r.calc}`);
    if (r.chartLast && r.value !== r.chartLast) fails.push(`${r.mode}: card ${r.value} != chart last point ${r.chartLast}`);
  });
  return fails;
}
module.exports = { judge };
if (require.main !== module) return;
const { launch } = require('./browser.cjs');
(async () => {
  const url = process.env.QA_URL || process.argv[2] || 'http://127.0.0.1:8765/';
  const fails = [];
  const b = await launch();
  const runs = [];
  const modes = (process.env.FLOAT_MODES || 'normal,normal,price-late,supabase-late').split(',');
  for (const mode of modes) {
    const p = await b.newPage({ viewport: { width: 1280, height: 900 } });
    if (mode === 'price-late') {
      await p.route(/api\.binance\.com|api\.coingecko\.com|api\.coinbase\.com|api\.exchange\.coinbase\.com|api\.kraken\.com/, async r => {
        await new Promise(res => setTimeout(res, 6000)); r.continue().catch(() => {});
      });
    } else if (mode === 'supabase-late') {
      await p.route(/supabase\.co/, async r => {
        await new Promise(res => setTimeout(res, 6000)); r.continue().catch(() => {});
      });
    }
    await p.goto(url, { waitUntil: 'domcontentloaded' });
    await p.waitForTimeout(mode === 'normal' ? 16000 : 24000);
    await p.evaluate(() => document.getElementById('supply-chart')?.scrollIntoView());
    await p.waitForTimeout(1500);
    const r = await p.evaluate(() => {
      const t = id => (document.getElementById(id)?.textContent || '').trim();
      const S = state;
      const price = S.price, supply = S.circulatingSupply;
      // recompute card value from final state (same formula as updateSupplyDistributionChart)
      const px = typeof supplyFloatPrice === 'function' ? supplyFloatPrice() : price;
      let calc = null;
      if (px > 0 && supply > 0) {
        const staked = (S.stakedEth / supply) * 100, defi = (S.tvl / px / supply) * 100, oth = (S.l2Tvl / px / supply) * 100 + 3;
        calc = Math.max(0, 100 - staked - defi - oth);
      }
      const chartLast = (typeof supplyChart !== 'undefined' && supplyChart?.data?.datasets?.[0]?.data) ? supplyChart.data.datasets[0].data.slice(-1)[0] : null;
      return { value: t('float-pct').replace('%',''), change: t('float-change'), price, livePrice: S.livePrice, px, supply, tvl: S.tvl, l2Tvl: S.l2Tvl, staked: S.stakedEth, calc: calc == null ? null : calc.toFixed(1), chartLast: chartLast == null ? null : (+chartLast).toFixed(1) };
    });
    r.mode = mode;
    runs.push(r);
    console.log(JSON.stringify(r));
    await p.close();
  }
  await b.close();
  fails.push(...judge(runs));
  if (fails.length) { console.log('FLOAT FAIL\n- ' + fails.join('\n- ')); process.exit(1); }
  console.log('FLOAT ALL PASS (' + runs.length + ' loads: ' + [...new Set(runs.map(r => r.value + ' | ' + r.change))].join(' / ') + ')');
})().catch(e => { console.error(e); process.exit(1); });
