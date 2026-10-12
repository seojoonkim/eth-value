// News Impact QA: renders strip + panel at 390/1280, light+dark, ko/en; checks overflow/overlap; screenshots.
const { launch } = require('./browser.cjs');
const url = process.argv[2] || 'http://127.0.0.1:8799/';
const out = process.argv[3] || '/tmp/ni';
(async () => {
  const b = await launch();
  let fail = 0;
  for (const [w, lang, dark] of [[1280, 'ko', false], [390, 'ko', false], [1280, 'en', true], [390, 'en', true]]) {
    const p = await b.newPage({ viewport: { width: w, height: 900 }, deviceScaleFactor: 2 });
    const errs = [];
    p.on('pageerror', e => errs.push(e.message));
    await p.addInitScript(([l, d]) => { localStorage.setItem('ethval-lang', l); localStorage.setItem('theme', d ? 'dark' : 'light'); localStorage.setItem('ethval-theme', d ? 'dark' : 'light'); }, [lang, dark]);
    // Always load the root: a static server has no /ko/ route (404 page has no strip). Language is set below.
    await p.goto(url, { waitUntil: 'domcontentloaded', timeout: 60000 });
    if (!(await p.$('#news-impact-strip'))) { console.log(`FAIL ${w} ${lang}: #news-impact-strip missing at ${p.url()}`); fail++; await p.close(); continue; }
    await p.waitForFunction(() => window.ETHvalNewsImpact && window.ETHvalNewsImpact.state.status !== 'loading', null, { timeout: 45000 }).catch(() => {});
    if (dark) await p.evaluate(() => document.body.classList.add('dark'));
    await p.evaluate(l => { if (typeof setLanguage === 'function') setLanguage(l, false); }, lang);
    await p.waitForTimeout(1500);
    const r = await p.evaluate(() => {
      const S = window.ETHvalNewsImpact && window.ETHvalNewsImpact.state;
      const box = id => { const e = document.getElementById(id); if (!e) return null; const b = e.getBoundingClientRect(); return { x: b.x, w: b.width, h: b.height, sw: e.scrollWidth, cw: e.clientWidth }; };
      const strip = box('news-impact-strip'), panel = box('news-impact-panel');
      const docOverflow = document.documentElement.scrollWidth - window.innerWidth;
      // overlap check: children of card-top / strip head
      const overlaps = [];
      document.querySelectorAll('#news-impact-strip .ni-v-main > *, #news-impact-strip .ni-drv > *, .ni-card-top > *').forEach((a, i, arr) => {
        const ra = a.getBoundingClientRect();
        for (const bb of [a.nextElementSibling].filter(Boolean)) { const rb = bb.getBoundingClientRect(); if (ra.right > rb.left + 1 && ra.bottom > rb.top + 1 && rb.right > ra.left + 1 && rb.bottom > ra.top + 1) overlaps.push(a.className + '|' + bb.className); }
      });
      const cut = [...document.querySelectorAll('#news-impact-panel .ni-card-title, .ni-top-t')].filter(e => e.scrollWidth > e.clientWidth + 1).length;
      return { status: S && S.status, events: S && S.events.length, cards: document.querySelectorAll('#news-impact-panel .ni-card').length,
        markers: S && S.chart ? S.chart.data.datasets[1].data.length : 0, strip, panel, docOverflow, overlaps: overlaps.slice(0, 5), cut,
        stripText: document.getElementById('news-impact-strip').innerText.slice(0, 160).replace(/\n/g, ' / '),
        // order guard (Simon 2026-10-13): price box must render ABOVE the news-impact strip
        priceFirst: (() => { const a = document.querySelector('.summary-valuation-box'), b = document.getElementById('news-impact-strip'); return !!(a && b && a.getBoundingClientRect().top < b.getBoundingClientRect().top); })() };
    });
    const bad = r.status !== 'ok' || !r.cards || !r.markers || r.docOverflow > 1 || r.overlaps.length || !r.priceFirst || errs.some(e => /news|ni-/i.test(e));
    if (bad) fail++;
    console.log(`${bad ? 'FAIL' : 'PASS'} ${w} ${lang} ${dark ? 'dark' : 'light'}`, JSON.stringify(r), errs.slice(0, 3));
    const strip = await p.$('#news-impact-strip');
    await strip.scrollIntoViewIfNeeded();
    await strip.screenshot({ path: `${out}-strip-${w}-${lang}${dark ? '-dark' : ''}.png` });
    const panel = await p.$('#news-impact-panel');
    await panel.scrollIntoViewIfNeeded();
    // open first card to verify expanded state
    await p.click('#news-impact-panel .ni-card').catch(() => {});
    await p.waitForTimeout(300);
    await panel.screenshot({ path: `${out}-panel-${w}-${lang}${dark ? '-dark' : ''}.png` });
    await p.close();
  }
  await b.close();
  console.log(fail ? `FAIL ${fail}` : 'ALL PASS');
  process.exit(fail ? 1 : 0);
})();
