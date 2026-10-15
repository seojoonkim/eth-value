// 2026-10-16 guard: no chart's x-axis labels may be drawn rotated (MVRV card at 1280 showed
// "Jul 18 · Aug 2 · Aug 17 · Sep 1 …" tilted because its ticks had no maxRotation and maxTicksLimit 6 at font 10).
// Scans every visible Chart.js instance (Chart.js scale.labelRotation, degrees) at 390 and 1280, plus every
// MVRV period tab. Also fails on > 6 drawn x labels on the MVRV card.
// usage: node .qa/with_server.cjs .qa/xtick_rotation_check.cjs   |   node .qa/xtick_rotation_check.cjs https://ethval.com/
const { launch } = require('./browser.cjs');
(async () => {
  const url = process.env.QA_URL || process.argv[2] || 'http://127.0.0.1:8765/';
  const verbose = process.argv.includes('-v');
  const fails = [];
  const b = await launch();
  const scan = () => {
    const rows = [];
    const inst = (window.Chart && Chart.instances) ? Object.values(Chart.instances) : [];
    for (const c of inst) {
      const id = (c.canvas && c.canvas.id) || '?';
      for (const [k, s] of Object.entries(c.scales || {})) {
        if (!(k === 'x' || s.axis === 'x') || s.options.display === false || (s.options.ticks && s.options.ticks.display === false)) continue;
        if (!s.width || !s.height) continue;
        const labels = (s.ticks || []).map(t => Array.isArray(t.label) ? t.label.join(' ') : String(t.label ?? '')).filter(l => l !== '');
        if (!labels.length) continue;
        rows.push({ id, axis: k, rot: Math.round((s.labelRotation || 0) * 10) / 10, labels });
      }
    }
    return { n: inst.length, rows };
  };
  for (const w of [390, 1280]) {
    const p = await b.newPage({ viewport: { width: w, height: 900 } });
    const errs = []; p.on('pageerror', e => errs.push(e.message));
    await p.goto(url, { waitUntil: 'domcontentloaded' });
    await p.waitForFunction(() => Object.values(window.Chart?.instances || {}).some(c => c.canvas && c.canvas.id === 'mvrv-ratio-chart'), null, { timeout: 60000 }).catch(() => {});
    await p.waitForTimeout(4000);
    await p.evaluate(async () => { for (let y = 0; y < document.body.scrollHeight; y += 800) { window.scrollTo(0, y); await new Promise(r => setTimeout(r, 60)); } window.scrollTo(0, 0); });
    await p.waitForTimeout(3000);
    const base = await p.evaluate(scan);
    const rotated = base.rows.filter(r => r.rot > 0);
    console.log(`${w}: charts ${base.n}, visible x-axes ${base.rows.length}, rotated ${rotated.length}`);
    for (const r of (verbose ? base.rows : rotated)) console.log(`  ${r.id}.${r.axis} rot=${r.rot}: ${r.labels.join(' · ')}`);
    for (const r of rotated) fails.push(`${w} default ${r.id}.${r.axis} rot=${r.rot}: ${r.labels.join(' · ')}`);
    // MVRV card: every period tab
    const tabs = await p.evaluate(() => {
      const card = document.getElementById('mvrv-ratio-chart')?.closest('.metric-card, .card, [class*="card"]');
      return card ? [...card.querySelectorAll('button')].map((b, i) => ({ i, t: b.textContent.trim() })).filter(x => /^\d+[DWMY]$|^ALL$/i.test(x.t)) : [];
    });
    if (!tabs.length) fails.push(`${w} mvrv: no period tabs found`);
    for (const tb of tabs) {
      await p.evaluate(i => {
        const card = document.getElementById('mvrv-ratio-chart').closest('.metric-card, .card, [class*="card"]');
        card.querySelectorAll('button')[i].click();
      }, tb.i);
      await p.waitForTimeout(700);
      const r = await p.evaluate(() => {
        const c = Object.values(Chart.instances).find(x => x.canvas && x.canvas.id === 'mvrv-ratio-chart');
        if (!c) return { err: 'no chart' };
        const s = c.scales.x;
        return { rot: Math.round((s.labelRotation || 0) * 10) / 10, labels: (s.ticks || []).map(t => String(t.label ?? '')).filter(Boolean) };
      });
      if (r.err) { fails.push(`${w} mvrv ${tb.t}: ${r.err}`); continue; }
      console.log(`${w} mvrv ${tb.t}: rot=${r.rot} n=${r.labels.length} ${r.labels.join(' · ')}`);
      if (r.rot > 0) fails.push(`${w} mvrv ${tb.t} rotated ${r.rot}°`);
      if (r.labels.length > 6) fails.push(`${w} mvrv ${tb.t} ${r.labels.length} x labels (>6)`);
    }
    if (errs.length) fails.push(`${w} page errors: ${errs.slice(0, 3).join(' | ')}`);
    await p.close();
  }
  await b.close();
  if (fails.length) { console.log('FAIL (' + fails.length + ')\n' + fails.join('\n')); process.exit(1); }
  console.log('XTICK ROTATION PASS');
})().catch(e => { console.error(e); process.exit(1); });
