// Live: footer of each AI commentary block shows the real model name (Claude Haiku 5.5).
const { chromium } = require('playwright');
(async () => {
  const b = await chromium.launch();
  const p = await b.newPage({ viewport: { width: 1440, height: 900 } });
  await p.goto('https://ethval.com/?cb=' + Date.now(), { waitUntil: 'networkidle', timeout: 90000 }).catch(() => {});
  await p.waitForTimeout(6000);
  const feet = await p.evaluate(() => [...document.querySelectorAll('*')]
    .filter(e => e.children.length === 0 && /Daily Analysis|일일 분석|by\s|분석 ·/.test(e.textContent) && /Haiku|Luna|Claude/.test(e.textContent))
    .map(e => e.textContent.trim().replace(/\s+/g, ' ')));
  console.log('footers:', feet.length); [...new Set(feet)].forEach(f => console.log('  ' + f));
  const el = await p.$('text=/Claude Haiku 5\\.5/');
  if (el) { await el.scrollIntoViewIfNeeded(); await p.evaluate(() => window.scrollBy(0, 150)); await p.waitForTimeout(800); await p.screenshot({ path: '/tmp/haiku_footer_live.png' }); console.log('shot /tmp/haiku_footer_live.png'); }
  const ok = feet.length > 0 && feet.every(f => /Claude Haiku 5\.5/.test(f));
  console.log(ok ? 'LIVE MODEL LABEL PASS' : 'LIVE MODEL LABEL FAIL');
  await b.close();
})();
