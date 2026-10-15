// 2026-10-15 Facebook-style layout gate: rail sits 24px beside the body container (not the screen edge);
// rail + container centred as one group; top banner spans only the container. Usage: node .qa/layout_measure.cjs [url]
const { chromium } = require('playwright');
const URL = process.argv[2] || 'https://ethval.com/';
let fail = 0; const ok = (c, m) => { console.log((c ? 'PASS ' : 'FAIL ') + m); if (!c) fail++; };
(async () => {
  const b = await chromium.launch();
  for (const [w, theme] of [[1280, 'dark'], [1440, 'light'], [1920, 'dark'], [2560, 'light']]) {
    const ctx = await b.newContext({ viewport: { width: w, height: 900 } });
    await ctx.addInitScript((t) => { try { localStorage.setItem('ethval-theme', t); } catch (e) {} }, theme);
    const p = await ctx.newPage();
    await p.goto(URL + (URL.includes('?') ? '&' : '?') + 'lang=kr', { waitUntil: 'domcontentloaded', timeout: 90000 });
    await p.waitForTimeout(4000);
    const r = await p.evaluate(() => {
      const R = (s) => { const e = document.querySelector(s); if (!e || getComputedStyle(e).display === 'none') return null; const r = e.getBoundingClientRect(); return { l: r.left, r: r.right, w: r.width }; };
      return { vw: document.documentElement.clientWidth, sb: R('.sb'), cont: R('.container'), ban: R('.onboarding-banner'), sx: document.documentElement.scrollWidth };
    });
    const tag = `${w} ${theme}`;
    const leftGap = r.sb.l, rightGap = r.vw - r.cont.r;
    ok(Math.abs((r.cont.l - r.sb.r) - 24) <= 1, `${tag} rail→container gap 24 (${(r.cont.l - r.sb.r)|0}): rail ${r.sb.l|0}..${r.sb.r|0}, container ${r.cont.l|0}..${r.cont.r|0}`);
    ok(Math.abs(leftGap - rightGap) <= 10, `${tag} group centred: left gap ${leftGap|0} vs right gap ${rightGap|0}`);
    ok(r.cont.w <= 1401, `${tag} container width ${r.cont.w|0} <= 1400`);
    ok(r.sx <= r.vw + 1, `${tag} no horizontal scroll (${r.sx} <= ${r.vw})`);
    if (r.ban) ok(Math.abs(r.ban.l - r.cont.l) <= 1 && Math.abs(r.ban.r - r.cont.r) <= 1, `${tag} banner = container span: ${r.ban.l|0}..${r.ban.r|0}`);
    await p.screenshot({ path: `/tmp/layout_${w}_${theme}.png` });
    await ctx.close();
  }
  await b.close();
  console.log(fail ? 'LAYOUT FAIL ' + fail : 'LAYOUT ALL PASS');
  process.exit(fail ? 1 : 0);
})();
