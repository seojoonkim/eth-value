// Read-only: which target cards already carry a background-image (so a gradient overlay must not replace it).
const { chromium } = require('playwright');
const URL = process.argv[2] || 'https://ethval.com/';
const C = ['ni-card','metric-card','valuation-model','method-card','category-card','ev-research-card','simulator-box','price-card','summary-valuation-box','conclusion-panel','community-stats-box','ni-chart-box','ni-matrix','ni-score','historical-main-chart','fvb','fbt','sim-presets','mc-box'];
(async () => {
  const b = await chromium.launch(); const p = await b.newPage({ viewport: { width: 1440, height: 900 } });
  for (const t of ['light', 'dark']) {
    await p.addInitScript((t) => { localStorage.setItem('ethval-theme', t); }, t);
    await p.goto(URL, { waitUntil: 'networkidle', timeout: 90000 }).catch(() => {}); await p.waitForTimeout(2000);
    const r = await p.evaluate((C) => C.map(c => { const all = [...document.querySelectorAll('.' + c)];
      const imgs = [...new Set(all.map(e => getComputedStyle(e).backgroundImage.slice(0, 60)))];
      const bgs = [...new Set(all.map(e => getComputedStyle(e).backgroundColor))];
      return `${c} n${all.length} img=${imgs.join(' | ')} bg=${bgs.join(' | ')}`; }), C);
    console.log(t + '\n' + r.join('\n'));
  }
  await b.close();
})();
