// Ratings model-card titles (.method-name) must be the largest text in the card header: ≥ formula & body size.
// Simon 2026-10-16 db0f094: keep the approved +1px title bump (phone 19px, desktop 17px).
// Fails if a cascade loss shrinks them again or if another off-scale value slips in.
const { launch } = require('./browser.cjs');
const WANT = { 360: 19, 390: 19, 1280: 17 };
(async () => {
  const url = process.argv[2] || 'http://127.0.0.1:8765/';
  const b = await launch(); const fails = [];
  for (const [w, want] of Object.entries(WANT)) {
    const p = await b.newPage({ viewport: { width: +w, height: 900 } });
    await p.goto(url, { waitUntil: 'domcontentloaded' }); await p.waitForTimeout(4000);
    const r = await p.evaluate(() => {
      const t = [...document.querySelectorAll('#ratings .method-name')].filter(e => e.offsetParent);
      const card = t[0] && t[0].closest('.method-card, [class*=card]');
      const body = card && [...card.querySelectorAll('p, .method-description, [class*=desc]')].find(e => e.offsetParent && e.textContent.trim().length > 40);
      const over = t.filter(e => e.scrollWidth > e.clientWidth + 1 || e.getBoundingClientRect().right > innerWidth).length;
      return { n: t.length, sizes: [...new Set(t.map(e => getComputedStyle(e).fontSize))], body: body ? getComputedStyle(body).fontSize : null, over, docW: document.documentElement.scrollWidth };
    });
    console.log(w, JSON.stringify(r));
    if (r.n < 12) fails.push(`${w}: only ${r.n} titles`);
    if (r.sizes.length !== 1 || parseFloat(r.sizes[0]) !== want) fails.push(`${w}: sizes ${r.sizes} want ${want}px`);
    if (r.body && parseFloat(r.sizes[0]) < parseFloat(r.body)) fails.push(`${w}: title ${r.sizes[0]} < body ${r.body}`);
    if (r.over) fails.push(`${w}: ${r.over} titles overflow`);
    if (r.docW > +w) fails.push(`${w}: horizontal scroll ${r.docW}`);
    await p.close();
  }
  await b.close();
  if (fails.length) { console.log('RATING-TITLE FAIL\n' + fails.join('\n')); process.exit(1); }
  console.log('RATING-TITLE ALL PASS');
})();
