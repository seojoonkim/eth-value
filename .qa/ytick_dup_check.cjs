// 2026-10-16 guard: no chart may show the same y-axis tick label twice (e.g. Net Supply 90D showed
// 0.9%·0.8%·0.8%·0.8% because the ticks were rounded to one decimal on a narrow range).
// Scans every Chart.js instance on the page (all tabs rendered) for every period button of the
// net-supply card plus the default period of every other chart, at 390 and 1280.
// usage: node .qa/with_server.cjs .qa/ytick_dup_check.cjs   |   node .qa/ytick_dup_check.cjs https://ethval.com/
const { launch } = require('./browser.cjs');
(async () => {
  const url = process.env.QA_URL || process.argv[2] || 'http://127.0.0.1:8765/';
  const fails = [];
  const b = await launch();
  const scan = () => {
    const out = [];
    const inst = (window.Chart && Chart.instances) ? Object.values(Chart.instances) : [];
    for (const c of inst) {
      const id = c.canvas && c.canvas.id || '?';
      for (const [k, s] of Object.entries(c.scales || {})) {
        if (k === 'x' || s.axis === 'x' || s.options.display === false) continue;
        if (s.type === 'category') continue;
        const labels = (s.ticks || []).map(t => Array.isArray(t.label) ? t.label.join(' ') : String(t.label ?? '')).filter(l => l !== '');
        const seen = new Set(), dup = [];
        for (const l of labels) { if (seen.has(l)) dup.push(l); seen.add(l); }
        if (dup.length) out.push({ id, axis: k, labels });
      }
    }
    return { n: inst.length, out };
  };
  for (const w of [390, 1280]) {
    const p = await b.newPage({ viewport: { width: w, height: 900 } });
    const errs = []; p.on('pageerror', e => errs.push(e.message));
    await p.goto(url, { waitUntil: 'domcontentloaded' });
    await p.waitForFunction(() => /%\/yr/.test(document.getElementById('net-supply-change')?.textContent || ''), null, { timeout: 60000 }).catch(() => {});
    await p.waitForFunction(() => window.netSupplyChart || (typeof netSupplyChart !== 'undefined' && netSupplyChart), null, { timeout: 30000 }).catch(() => {});
    await p.waitForTimeout(4000);
    const base = await p.evaluate(scan);
    console.log(`${w}: charts ${base.n}, dup-axis ${base.out.length}`);
    for (const o of base.out) fails.push(`${w} default ${o.id}.${o.axis}: ${o.labels.join(' · ')}`);
    // net supply card: every period
    for (const d of [7, 30, 90, 180, 365, 1095]) {
      const r = await p.evaluate(days => {
        if (typeof updateNetSupplyChart !== 'function') return { err: 'no updateNetSupplyChart' };
        updateNetSupplyChart(days);
        const c = Object.values(Chart.instances).find(x => x.canvas && x.canvas.id === 'net-supply-chart');
        if (!c) return { err: 'no chart (history too short?)', len: (state.netSupplyHistory || []).length };
        return { labels: (c.scales.y.ticks || []).map(t => String(t.label)) };
      }, d);
      if (r.err) { console.log(`${w} net-supply ${d}D: ${r.err} ${r.len ?? ''}`); continue; }
      console.log(`${w} net-supply ${d}D: ${r.labels.join(' · ')}`);
      if (new Set(r.labels).size !== r.labels.length) fails.push(`${w} net-supply ${d}D duplicate y ticks: ${r.labels.join(' · ')}`);
    }
    if (errs.length) fails.push(`${w} page errors: ${errs.slice(0, 3).join(' | ')}`);
    await p.close();
  }
  await b.close();
  if (fails.length) { console.log('FAIL\n' + fails.join('\n')); process.exit(1); }
  console.log('YTICK DUP PASS');
})().catch(e => { console.error(e); process.exit(1); });
