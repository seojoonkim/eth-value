// Read-only audit: count border/shadow/radius/bg styles on visible cards + section backgrounds (light, 1440).
const { chromium } = require('playwright');
const URL = process.argv[2] || 'https://ethval.com/';
(async () => {
  const b = await chromium.launch();
  const p = await b.newPage({ viewport: { width: 1440, height: 900 } });
  await p.addInitScript(() => { localStorage.setItem('ethval-theme', 'light'); localStorage.setItem('ethval-lang', 'en'); });
  await p.goto(URL, { waitUntil: 'networkidle', timeout: 90000 }).catch(() => {});
  await p.waitForTimeout(3000);
  const r = await p.evaluate(() => {
    const tally = {};
    const add = (k) => { tally[k] = (tally[k] || 0) + 1; };
    const els = [...document.querySelectorAll('.container *')].filter(e => {
      const b = e.getBoundingClientRect(); return b.width > 120 && b.height > 40;
    });
    for (const e of els) {
      const cs = getComputedStyle(e);
      const bw = parseFloat(cs.borderTopWidth) + parseFloat(cs.borderLeftWidth);
      const hasBg = cs.backgroundColor !== 'rgba(0, 0, 0, 0)';
      if (!(bw > 0 || cs.boxShadow !== 'none' || hasBg)) continue;
      const cls = (e.className && e.className.baseVal === undefined ? e.className : '').split(' ')[0] || e.tagName;
      add(`${cls} | b${bw ? 1 : 0} s${cs.boxShadow === 'none' ? 0 : 1} | ${cs.backgroundColor.replace(/rgba?\(|\)| /g,'')} | r${parseFloat(cs.borderTopLeftRadius)}`);
    }
    const secs = [...document.querySelectorAll('.section[id]')].map(s => s.id + ':' + getComputedStyle(s).backgroundColor);
    return { body: getComputedStyle(document.body).backgroundColor, font: getComputedStyle(document.body).fontFamily, secs,
      top: Object.entries(tally).sort((a, b) => b[1] - a[1]).slice(0, 80) };
  });
  console.log(r.body, r.secs.join(' ')); for (const [k,v] of r.top) console.log(v, k);
  await p.screenshot({ path: '/tmp/apple_before_1440.png' });
  await p.evaluate(() => window.scrollTo(0, 2600)); await p.waitForTimeout(800);
  await p.screenshot({ path: '/tmp/apple_before_1440_b.png' });
  await b.close();
})();
