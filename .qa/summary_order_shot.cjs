// Full-viewport screenshots of the Valuation summary area at 390 and 1280 (dark) to confirm visual order.
// Uses the shared .qa/browser.cjs launcher (raw playwright.chromium has no installed headless shell here).
const { launch } = require('./browser.cjs');
(async () => {
  const b = await launch();
  for (const w of [390, 1280]) {
    const p = await b.newPage({ viewport: { width: w, height: w === 390 ? 1400 : 1000 }, deviceScaleFactor: 2, colorScheme: 'dark' });
    await p.addInitScript(() => { try { localStorage.setItem('theme', 'dark'); localStorage.setItem('ethval-theme', 'dark'); localStorage.setItem('ethval-lang', 'en'); } catch (e) {} });
    await p.goto(process.argv[2], { waitUntil: 'domcontentloaded', timeout: 60000 });
    await p.waitForFunction(() => window.ETHvalNewsImpact && window.ETHvalNewsImpact.state.status === 'ok', null, { timeout: 45000 }).catch(() => {});
    await p.waitForTimeout(2500);
    await p.evaluate(() => document.getElementById('valuation').scrollIntoView());
    await p.waitForTimeout(500);
    await p.screenshot({ path: `${process.argv[3]}-${w}.png` });
    const r = await p.evaluate(() => { const t = s => Math.round(document.querySelector(s).getBoundingClientRect().top), l = s => Math.round(document.querySelector(s).getBoundingClientRect().left); const pad = s => getComputedStyle(document.querySelector(s)).paddingLeft; return { price: [l('.summary-valuation-box'), t('.summary-valuation-box')], strip: [l('#news-impact-strip'), t('#news-impact-strip')], desc: [l('.summary-description'), t('.summary-description')], padPrice: pad('.summary-valuation-box'), padStrip: pad('#news-impact-strip') }; });
    console.log(w, JSON.stringify(r));
  }
  await b.close();
})();
