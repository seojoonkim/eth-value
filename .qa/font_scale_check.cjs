// Font scale gate: every visible text renders at a size on the standard scale, never below 12px.
// Scale: 12 13 14 16 18 20 24 28 32 36 44 48 (px). Exempt: logo wordmark, decorative emoji icons.
// usage: node font_scale_check.cjs <url>
const { launch } = require('./browser.cjs');
const SCALE = [12, 13, 14, 16, 18, 20, 24, 28, 32, 36, 44, 48];
(async () => {
  const url = process.argv[2]; const b = await launch(); let fail = 0;
  for (const [w, lang, theme] of [[1280, 'ko', 'dark'], [1280, 'en', 'light'], [390, 'ko', 'dark'], [390, 'ja', 'light'], [768, 'zh', 'dark'], [1440, 'en', 'dark']]) {
    const p = await b.newPage({ viewport: { width: w, height: 900 } });
    await p.addInitScript(([l, t]) => { localStorage.setItem('ethval-lang', l); localStorage.setItem('ethval-theme', t); }, [lang, theme]);
    await p.goto(url, { waitUntil: 'load', timeout: 45000 }); await p.waitForTimeout(8000);
    const bad = await p.evaluate(SCALE => {
      const out = {}; const tw = document.createTreeWalker(document.body, NodeFilter.SHOW_TEXT); let n;
      while ((n = tw.nextNode())) {
        if (!n.textContent.trim()) continue; const e = n.parentElement; if (!e || e.closest('svg, .logo, .celebration-icon')) continue;
        const cs = getComputedStyle(e); if (cs.display === 'none' || cs.visibility === 'hidden') continue;
        const r = e.getBoundingClientRect(); if (!r.width || !r.height) continue;
        if (/^[\p{Extended_Pictographic}\uFE0F\s]+$/u.test(n.textContent.trim())) continue;
        const fs = Math.round(parseFloat(cs.fontSize) * 10) / 10;
        // Simon 2026-10-14: phone section titles 34px ("아주 약간만 더 작게" from 36) — the only off-scale size allowed
        if (fs === 34 && innerWidth <= 768 && e.closest('.section-title')) continue;
        // 2026-10-16 db0f094: Ratings titles intentionally enlarged by +1px (desktop 17px, mobile 19px).
        if (e.closest('#ratings .method-name') && ((innerWidth <= 768 && fs === 19) || (innerWidth > 768 && fs === 17))) continue;
        if (!SCALE.includes(fs)) { const k = fs + 'px ' + (typeof e.className === 'string' && e.className ? '.' + e.className.split(' ')[0] : e.tagName); out[k] = (out[k] || 0) + 1; }
      }
      return out;
    }, SCALE);
    const keys = Object.keys(bad); const label = `${w} ${lang} ${theme}`;
    if (keys.length) { fail++; console.log(`FAIL ${label}: ${keys.slice(0, 12).map(k => k + ' x' + bad[k]).join(', ')}`); } else console.log(`PASS ${label}`);
    await p.close();
  }
  await b.close(); console.log(fail ? 'FONT-SCALE FAIL' : 'FONT-SCALE ALL PASS'); process.exit(fail ? 1 : 0);
})();
