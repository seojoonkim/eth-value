// Read-only: which cards overflow horizontally at 390 (light).
const { chromium } = require('playwright');
const URL = process.argv[2] || 'https://ethval.com/';
(async () => {
  const b = await chromium.launch();
  const p = await b.newPage({ viewport: { width: 390, height: 900 } });
  await p.addInitScript(() => { localStorage.setItem('ethval-theme', 'light'); localStorage.setItem('ethval-lang', 'en'); });
  await p.goto(URL, { waitUntil: 'networkidle', timeout: 90000 }).catch(() => {});
  await p.waitForTimeout(2500);
  const r = await p.evaluate(() => [...document.querySelectorAll('.metric-card, .valuation-model, .ni-card')]
    .filter(e => e.scrollWidth > e.clientWidth + 1)
    .map(e => {
      const kid = [...e.querySelectorAll('*')].find(k => k.getBoundingClientRect().right > e.getBoundingClientRect().right + 1);
      const s = getComputedStyle(e);
      return `${e.className.split(' ')[0]} cw${e.clientWidth} sw${e.scrollWidth} ovf-x ${s.overflowX} pad ${s.paddingLeft} | kid ${kid ? (kid.className || kid.tagName) + ' "' + (kid.textContent || '').trim().slice(0, 30) + '"' : '-'}`;
    }));
  console.log(r.join('\n') || 'none');
  await b.close();
})();
