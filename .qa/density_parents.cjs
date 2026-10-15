// Read-only: parent grids of the repeated cards + their gap/columns (1440 and 390).
const { chromium } = require('playwright');
const URL = process.argv[2] || 'https://ethval.com/';
(async () => {
  const b = await chromium.launch();
  for (const w of [1440, 390]) {
    const p = await b.newPage({ viewport: { width: w, height: 900 } });
    await p.addInitScript(() => { localStorage.setItem('ethval-theme', 'light'); localStorage.setItem('ethval-lang', 'en'); });
    await p.goto(URL, { waitUntil: 'networkidle', timeout: 90000 }).catch(() => {});
    await p.waitForTimeout(2500);
    const r = await p.evaluate(() => ['ni-card', 'metric-card', 'valuation-model', 'method-card', 'category-card', 'ev-research-card', 'sim-slider-item'].map(c => {
      const e = document.querySelector('.' + c); if (!e) return c + ': none';
      const par = e.parentElement, s = getComputedStyle(par);
      const pk = (typeof par.className === 'string' ? par.className : par.tagName).trim().split(/\s+/).join('.');
      return `${c} in .${pk}: ${s.display} gap ${s.rowGap}/${s.columnGap} cols ${s.gridTemplateColumns.split(' ').length} w${Math.round(e.getBoundingClientRect().width)}`;
    }));
    console.log(w + '\n' + r.join('\n'));
    await p.close();
  }
  await b.close();
})();
