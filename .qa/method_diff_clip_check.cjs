// 2026-10-16 guard: the valuation method cards' gap badge (.method-chart-diff, e.g. "+137.4% Undervalued")
// must stay inside its card. Live 1280 measured 9 cards whose badge ran 10-34px past the card's right edge
// (header row = value + nowrap badge + period tabs could not wrap the badge).
// Checks every .method-card that is rendered, at 390 / 768 / 1024 / 1280 × en / ko / ja / zh:
//   - badge right/left edge within the card (and within .method-right), 1px tolerance
//   - badge not overlapping the 90D/1Y/3Y tabs or the value
//   - badge text is a real value (not "--") once data has loaded
// usage: node .qa/with_server.cjs .qa/method_diff_clip_check.cjs   |   node .qa/method_diff_clip_check.cjs https://ethval.com/ [-v]
const { launch } = require('./browser.cjs');
(async () => {
  const url = process.env.QA_URL || process.argv[2] || 'http://127.0.0.1:8765/';
  const verbose = process.argv.includes('-v');
  const fails = [];
  const b = await launch();
  const widths = (process.env.WIDTHS || '390,768,1024,1280').split(',').map(Number);
  for (const w of widths) {
    const p = await b.newPage({ viewport: { width: w, height: 900 } });
    const errs = []; p.on('pageerror', e => errs.push(e.message));
    await p.goto(url, { waitUntil: 'domcontentloaded' });
    await p.waitForFunction(() => [...document.querySelectorAll('.method-chart-diff')].filter(e => /%/.test(e.textContent)).length >= 6, null, { timeout: 60000 }).catch(() => {});
    for (const lang of ['en', 'ko', 'ja', 'zh']) {
      const btn = await p.$(`.lang-btn[data-lang="${lang}"]`);
      if (btn) { await p.evaluate(el => el.click(), btn); await p.waitForTimeout(500); }
      const r = await p.evaluate(() => {
        const out = []; let n = 0;
        for (const card of document.querySelectorAll('.method-card')) {
          const d = card.querySelector('.method-chart-diff');
          if (!d) continue;
          const cr = card.getBoundingClientRect(); const dr = d.getBoundingClientRect();
          if (!cr.width || !dr.width) continue;
          n++;
          const model = card.dataset.model || '?';
          const right = card.querySelector('.method-right'); const rr = right ? right.getBoundingClientRect() : cr;
          const tabs = card.querySelector('.method-chart-tabs'); const tr = tabs ? tabs.getBoundingClientRect() : null;
          const v = card.querySelector('.method-chart-value'); const vr = v ? v.getBoundingClientRect() : null;
          const ov = (a, b2) => a && b2 && a.left < b2.right - 1 && b2.left < a.right - 1 && a.top < b2.bottom - 1 && b2.top < a.bottom - 1;
          const row = { model, text: d.textContent.trim(), over: Math.round(dr.right - cr.right), overR: Math.round(dr.right - rr.right), left: Math.round(cr.left - dr.left), tabsOv: !!ov(dr, tr), valOv: !!ov(dr, vr), h: Math.round(dr.height) };
          out.push(row);
        }
        return { n, out, docOverflow: document.documentElement.scrollWidth - innerWidth };
      });
      let bad = 0;
      for (const row of r.out) {
        const why = [];
        if (row.over > 1 || row.overR > 1) why.push(`right +${Math.max(row.over, row.overR)}px`);
        if (row.left > 1) why.push(`left +${row.left}px`);
        if (row.tabsOv) why.push('overlaps tabs');
        if (row.valOv) why.push('overlaps value');
        if (why.length) { bad++; fails.push(`${w} ${lang} ${row.model} "${row.text}": ${why.join(', ')}`); }
        else if (verbose) console.log(`  ok ${w} ${lang} ${row.model} "${row.text}" over=${row.over} h=${row.h}`);
      }
      if (r.n < 12) fails.push(`${w} ${lang}: only ${r.n} visible method badges (expected ≥12)`);
      const empty = r.out.filter(x => !/%/.test(x.text)).length;
      if (empty) fails.push(`${w} ${lang}: ${empty} badges without a value`);
      if (r.docOverflow > 0) fails.push(`${w} ${lang}: document horizontal overflow ${r.docOverflow}px`);
      console.log(`${w} ${lang}: badges ${r.n}, clipped/overlap ${bad}, max over ${Math.max(...r.out.map(x => x.over))}px`);
    }
    if (errs.length) fails.push(`${w}: page errors ${errs.slice(0, 3).join(' | ')}`);
    await p.close();
  }
  await b.close();
  if (fails.length) { console.log('METHOD DIFF CLIP FAIL\n' + fails.join('\n')); process.exit(1); }
  console.log('METHOD DIFF CLIP PASS');
})().catch(e => { console.error(e); process.exit(1); });
