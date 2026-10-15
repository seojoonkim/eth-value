// Apple-tone gate: cards have no visible outline/shadow, radius 16; dark text tokens untouched; no horizontal scroll.
// usage: node .qa/apple_tone_check.cjs [url]
const { chromium } = require('playwright');
const URL = process.argv[2] || 'https://ethval.com/';
const CARDS = ['ni-card', 'metric-card', 'valuation-model', 'method-card', 'simulator-box', 'price-card', 'summary-valuation-box'];
let fail = 0;
const ok = (c, m) => { console.log((c ? 'PASS ' : 'FAIL ') + m); if (!c) fail++; };
(async () => {
  const b = await chromium.launch();
  for (const [w, theme] of [[1440, 'light'], [1440, 'dark'], [390, 'light'], [390, 'dark']]) {
    const p = await b.newPage({ viewport: { width: w, height: 900 } });
    await p.addInitScript((t) => { localStorage.setItem('ethval-theme', t); localStorage.setItem('ethval-lang', 'en'); }, theme);
    await p.goto(URL, { waitUntil: 'networkidle', timeout: 90000 }).catch(() => {});
    await p.waitForTimeout(2500);
    const r = await p.evaluate((CARDS) => {
      const out = {};
      for (const c of CARDS) {
        const e = document.querySelector('.' + c);
        if (!e) { out[c] = null; continue; }
        const cs = getComputedStyle(e);
        out[c] = { bc: cs.borderTopColor, sh: cs.boxShadow, r: parseFloat(cs.borderTopLeftRadius), img: cs.backgroundImage, pad: parseFloat(cs.paddingTop) };
      }
      const bs = getComputedStyle(document.body);
      // Count cards whose visible text pokes past the card edge (a 2px scrollWidth slack on .valuation-model is
      // pre-existing, hidden by overflow-x and not text, so measure text nodes instead of scrollWidth).
      const clip = [...document.querySelectorAll('.metric-card, .valuation-model, .ni-card')].filter(card => {
        const cr = card.getBoundingClientRect(); if (!cr.width) return false;
        const w = document.createTreeWalker(card, NodeFilter.SHOW_TEXT); let n;
        while ((n = w.nextNode())) { if (!n.textContent.trim()) continue; const rg = document.createRange(); rg.selectNodeContents(n);
          for (const r of rg.getClientRects()) if (r.width && (r.right > cr.right + 0.5 || r.left < cr.left - 0.5)) return true; }
        return false; }).length;
      return { out, clip, bodyImg: bs.backgroundImage, text: bs.color, bg: bs.backgroundColor, sw: document.documentElement.scrollWidth, dark: document.body.classList.contains('dark') };
    }, CARDS);
    const tag = `${w} ${theme}`;
    ok(r.dark === (theme === 'dark'), `${tag} theme applied`);
    for (const [c, v] of Object.entries(r.out)) {
      if (!v) { console.log(`skip ${tag} ${c} (not on page)`); continue; }
      ok(/rgba\(0, 0, 0, 0\)|transparent/.test(v.bc) && v.sh === 'none' && v.r === 16, `${tag} ${c} border ${v.bc} shadow ${v.sh === 'none' ? '-' : v.sh} r${v.r}`);
    }
    for (const [c, v] of Object.entries(r.out)) if (v) ok(/linear-gradient/.test(v.img), `${tag} ${c} has sheen gradient`);
    if (w >= 769) ok(r.out['metric-card'] && r.out['metric-card'].pad >= 20 && r.out['ni-card'].pad >= 18, `${tag} roomier card padding`);
    ok(/radial-gradient/.test(r.bodyImg), `${tag} page top glow`);
    ok(r.clip === 0, `${tag} no card content overflow (${r.clip})`);
    if (theme === 'dark') ok(r.text !== 'rgb(29, 29, 31)', `${tag} dark text stays light (${r.text})`);
    ok(r.sw <= w, `${tag} no horizontal scroll (${r.sw})`);
    await p.screenshot({ path: `/tmp/apple_${w}_${theme}.png` });
    await p.evaluate(() => window.scrollTo(0, 2600)); await p.waitForTimeout(700);
    await p.screenshot({ path: `/tmp/apple_${w}_${theme}_b.png` });
    await p.close();
  }
  await b.close();
  console.log(fail ? `APPLE TONE FAIL (${fail})` : 'APPLE TONE ALL PASS');
  process.exit(fail ? 1 : 0);
})();
