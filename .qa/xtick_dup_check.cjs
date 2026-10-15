// 2026-10-16 guard: no chart may show the same x-axis tick label twice (e.g. Net Supply 90D at 1280 showed
// "Jul '26 · Aug '26 · Aug '26 · Sep '26" because fmt.chartLabel is month-precision and 90D ticks fall in one month).
// Scans every Chart.js instance (all tabs rendered) at the default period, plus every period of the
// net-supply card, at 390 and 1280. Only labels actually drawn (non-empty) count.
// usage: node .qa/with_server.cjs .qa/xtick_dup_check.cjs   |   node .qa/xtick_dup_check.cjs https://ethval.com/
const { launch } = require('./browser.cjs');
(async () => {
  const url = process.env.QA_URL || process.argv[2] || 'http://127.0.0.1:8765/';
  const verbose = process.argv.includes('-v');
  const fails = [];
  const b = await launch();
  const scan = () => {
    const out = [], all = [];
    const inst = (window.Chart && Chart.instances) ? Object.values(Chart.instances) : [];
    for (const c of inst) {
      const id = (c.canvas && c.canvas.id) || '?';
      for (const [k, s] of Object.entries(c.scales || {})) {
        if (!(k === 'x' || s.axis === 'x') || s.options.display === false || (s.options.ticks && s.options.ticks.display === false)) continue;
        if (!s.width || !s.height) continue; // hidden (inactive tab / collapsed)
        const labels = (s.ticks || []).map(t => Array.isArray(t.label) ? t.label.join(' ') : String(t.label ?? '')).filter(l => l !== '');
        const seen = new Set(), dup = [];
        for (const l of labels) { if (seen.has(l)) dup.push(l); seen.add(l); }
        all.push({ id, axis: k, labels });
        if (dup.length) out.push({ id, axis: k, labels });
      }
    }
    return { n: inst.length, out, all };
  };
  for (const w of [390, 1280]) {
    const p = await b.newPage({ viewport: { width: w, height: 900 } });
    const errs = []; p.on('pageerror', e => errs.push(e.message));
    await p.goto(url, { waitUntil: 'domcontentloaded' });
    await p.waitForFunction(() => /%\/yr/.test(document.getElementById('net-supply-change')?.textContent || ''), null, { timeout: 60000 }).catch(() => {});
    await p.waitForTimeout(5000);
    // scroll through so lazy/deferred cards render
    await p.evaluate(async () => { for (let y = 0; y < document.body.scrollHeight; y += 800) { window.scrollTo(0, y); await new Promise(r => setTimeout(r, 60)); } window.scrollTo(0, 0); });
    await p.waitForTimeout(4000);
    const base = await p.evaluate(scan);
    console.log(`${w}: charts ${base.n}, x-axes ${base.all.length}, dup-axis ${base.out.length}`);
    if (verbose) for (const o of base.all) console.log(`  ${o.id}.${o.axis}: ${o.labels.join(' · ')}`);
    for (const o of base.out) fails.push(`${w} default ${o.id}.${o.axis}: ${o.labels.join(' · ')}`);
    for (const d of [7, 30, 90, 180, 365, 1095]) {
      const r = await p.evaluate(days => {
        if (typeof updateNetSupplyChart !== 'function') return { err: 'no updateNetSupplyChart' };
        updateNetSupplyChart(days);
        const c = Object.values(Chart.instances).find(x => x.canvas && x.canvas.id === 'net-supply-chart');
        if (!c) return { err: 'no chart' };
        return { labels: (c.scales.x.ticks || []).map(t => String(t.label ?? '')).filter(Boolean) };
      }, d);
      if (r.err) { console.log(`${w} net-supply ${d}D: ${r.err}`); continue; }
      console.log(`${w} net-supply ${d}D: ${r.labels.join(' · ')}`);
      if (new Set(r.labels).size !== r.labels.length) fails.push(`${w} net-supply ${d}D duplicate x ticks: ${r.labels.join(' · ')}`);
    }
    if (errs.length) fails.push(`${w} page errors: ${errs.slice(0, 3).join(' | ')}`);
    await p.close();
  }
  await b.close();
  if (fails.length) { console.log('FAIL (' + fails.length + ')\n' + fails.join('\n')); process.exit(1); }
  console.log('XTICK DUP PASS');
})().catch(e => { console.error(e); process.exit(1); });
