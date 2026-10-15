// Capture live ethval.com for design review: 1440 light/dark (top + 3 scrolls), 390 light (top + 2 scrolls).
const { chromium } = require('playwright');
(async () => {
  const b = await chromium.launch();
  const shots = [];
  for (const [w, h, theme, n] of [[1440, 900, 'light', 4], [1440, 900, 'dark', 2], [390, 844, 'light', 3]]) {
    const ctx = await b.newContext({ viewport: { width: w, height: h }, deviceScaleFactor: 1 });
    await ctx.addInitScript(t => { try { localStorage.setItem('ethval-theme', t); } catch (e) {} }, theme);
    const p = await ctx.newPage();
    await p.goto('https://ethval.com/?cb=' + Date.now(), { waitUntil: 'networkidle', timeout: 90000 }).catch(() => {});
    await p.waitForTimeout(4000);
    const H = await p.evaluate(() => document.documentElement.scrollHeight);
    const stops = [0, 0.18, 0.4, 0.65].slice(0, n);
    for (let i = 0; i < stops.length; i++) {
      await p.evaluate(y => window.scrollTo(0, y), Math.round(H * stops[i]));
      await p.waitForTimeout(1200);
      const f = `/tmp/review_${w}_${theme}_${i}.jpg`;
      await p.screenshot({ path: f, type: 'jpeg', quality: 70 });
      shots.push(f);
    }
    await ctx.close();
  }
  await b.close();
  console.log(shots.join('\n'));
})();
