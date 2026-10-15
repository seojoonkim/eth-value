// Price card: the change stat under the chart must follow the selected tab (30D/90D/1Y/3Y)
// and match the price N days ago in state.priceHistory (closest row, within tolerance).
// usage: node .qa/price_period_check.cjs <url> [outdir]   (prints PRICE PERIOD ALL PASS)
const { chromium } = require('playwright');
const fs = require('fs'), path = require('path'), os = require('os');
(async () => {
  const url = process.argv[2] || 'http://127.0.0.1:8765/', out = process.argv[3] || '/tmp/ethval-price-period';
  fs.mkdirSync(out, { recursive: true });
  const cache = path.join(os.homedir(), 'Library/Caches/ms-playwright');
  const exe = fs.readdirSync(cache).filter(d => /^chromium-\d+$/.test(d)).sort().reverse().map(d => path.join(cache, d, 'chrome-mac-arm64/Google Chrome for Testing.app/Contents/MacOS/Google Chrome for Testing')).find(p => fs.existsSync(p));
  const b = await chromium.launch({ executablePath: exe });
  let bad = 0;
  for (const [w, theme] of [[1280, 'dark'], [390, 'light']]) {
    const ctx = await b.newContext({ viewport: { width: w, height: 900 }, isMobile: w < 500, deviceScaleFactor: 2 });
    const p = await ctx.newPage();
    await p.addInitScript(t => { try { localStorage.setItem('ethval-theme', t); } catch (e) {} }, theme);
    await p.goto(url, { waitUntil: 'domcontentloaded' });
    await p.waitForFunction(() => typeof state !== 'undefined' && state.priceHistory && state.priceHistory.length > 300, null, { timeout: 60000 }).catch(() => {});
    await p.waitForTimeout(3000);
    for (const per of ['30', '90', '365', '1095']) {
      const tab = p.locator(`.price-card-right .mini-tab[data-period="${per}"]`);
      if (!(await tab.count())) { console.log(`${w} tab ${per}: MISSING`); bad++; continue; }
      const locked = await tab.evaluate(e => e.classList.contains('gated-tab-locked'));
      await tab.click({ force: true }).catch(() => {});
      await p.waitForTimeout(1200);
      const r = await p.evaluate(per => {
        const days = +per, label = { 30: '30D', 90: '90D', 365: '1Y', 1095: '3Y' }[days];
        const txt = document.getElementById('price-change-stat').textContent.trim();
        const active = (document.querySelector('.price-card-right .mini-tab.active') || {}).dataset?.period;
        const DAY = 864e5, now = Date.now(), target = now - days * DAY, tol = Math.min(30, Math.max(3, Math.round(days * .1))) * DAY;
        let best = null, bd = Infinity;
        for (const d of state.priceHistory) { const df = Math.abs(d.date.getTime() - target); if (df < bd) { bd = df; best = d; } }
        const cur = state.price;
        const exp = best && bd <= tol ? ((cur - best.value) / best.value) * 100 : null;
        return { txt, active, label, exp, pastDate: best && best.date.toISOString().slice(0, 10), pastVal: best && best.value, cur };
      }, per);
      const m = r.txt.match(/^([+-]?\d+(?:\.\d)?)% (\S+)$/);
      let ok = !!m && r.active === per;
      if (ok && r.exp != null) ok = m[2] === r.label && Math.abs(+m[1] - r.exp) <= 0.15;
      console.log(`${w} ${theme} tab ${per}${locked ? '(locked)' : ''}: active=${r.active} shown="${r.txt}" expected=${r.exp == null ? 'n/a' : (r.exp >= 0 ? '+' : '') + r.exp.toFixed(1) + '% ' + r.label} past=${r.pastDate} $${r.pastVal && r.pastVal.toFixed(1)} now $${r.cur && r.cur.toFixed(1)} ${ok ? 'OK' : 'FAIL'}`);
      if (!ok) bad++;
      if (per === '30') {
        const card = p.locator('#price-change-stat');
        await card.scrollIntoViewIfNeeded();
        await p.screenshot({ path: `${out}/price30_${w}_${theme}.png` });
      }
    }
    await ctx.close();
  }
  await b.close();
  console.log(bad ? `PRICE PERIOD FAIL ${bad}` : 'PRICE PERIOD ALL PASS');
  process.exit(bad ? 1 : 0);
})();
