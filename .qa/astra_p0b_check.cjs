// 2026-10-16 Astra audit P0 (F01·F05·F07) guard.
// F01: the Staking DCF formula shown must reproduce the shown value. The code caps the value at 3× price
//      (and (1+APR)/(9%−3%) ≥ 16.7, so the cap always binds) → the formula text must state the cap and
//      the shown model value must equal 3 × the price it was computed with.
// F05: every AI daily-analysis block says its numbers are as of generation time (cards above are live).
// F07: '% from ATH' equals (shown price − shown ATH) / shown ATH (same price object), within rounding.
// usage: node .qa/with_server.cjs .qa/astra_p0b_check.cjs   |   node .qa/astra_p0b_check.cjs https://ethval.com/
const { launch } = require('./browser.cjs');
(async () => {
  const url = process.env.QA_URL || process.argv[2] || 'http://127.0.0.1:8765/';
  const fails = [];
  const ASOF = { en: 'Numbers as of when it was written', ko: '생성 당시 기준', zh: '以生成时为准', ja: '生成時点のもの' };
  const b = await launch();
  for (const [w, lang] of [[1280, 'en'], [390, 'ko'], [1280, 'ja'], [390, 'zh']]) {
    const p = await b.newPage({ viewport: { width: w, height: 900 } });
    await p.goto(url + (lang === 'en' ? '' : `?lang=${lang}`), { waitUntil: 'domcontentloaded' });
    await p.waitForFunction(() => typeof setLanguage === 'function', null, { timeout: 30000 });
    await p.evaluate(l => setLanguage(l, false), lang);
    await p.waitForFunction(() => /\d/.test(document.getElementById('from-ath')?.textContent || '') &&
      /\d/.test(document.getElementById('current-price')?.textContent || ''), null, { timeout: 45000 }).catch(() => {});
    await p.waitForFunction(() => document.querySelectorAll('[id^="commentary-"] .ai-commentary-text').length >= 5, null, { timeout: 45000 }).catch(() => {});
    await p.waitForFunction(() => /\d/.test(document.getElementById('method-value-dcf')?.textContent || ''), null, { timeout: 45000 }).catch(() => {});
    await p.waitForTimeout(2000);
    const r = await p.evaluate(() => {
      const num = id => parseFloat((document.getElementById(id)?.textContent || '').replace(/[^\d.\-−]/g, '').replace('−', '-'));
      const price = num('current-price'), ath = num('ath'), shown = num('from-ath');
      const formulas = [...document.querySelectorAll('[data-model="dcf"] .model-formula, .method-card[data-model="dcf"] .method-formula')].map(e => e.textContent);
      let dcf = null;
      let dPrice = null; try { const d = getCurrentModelData(); dPrice = d.price; const res = calculateSingleModel('dcf', d); dcf = res && res.value; } catch (e) { dcf = 'ERR ' + e.message; }
      const cardText = document.querySelector('.method-card[data-model="dcf"] .method-value, #method-value-dcf')?.textContent || '';
      const foot = [...document.querySelectorAll('[id^="commentary-"] .ai-commentary-footer')].filter(e => e.offsetParent).map(e => e.innerText);
      return { price, ath, shown, statePrice: dPrice, cardText, formulas, dcf, note: document.querySelector('.method-card[data-model="dcf"] .method-note')?.textContent || '', foot };
    });
    const tag = `${w}/${lang}`;
    const calc = (r.price - r.ath) / r.ath * 100;
    if (!isFinite(r.shown) || !isFinite(calc)) fails.push(`${tag}: ATH change not rendered (${r.price} / ${r.ath} / ${r.shown})`);
    else if (Math.abs(calc - r.shown) > 0.15) fails.push(`${tag}: from ATH shows ${r.shown}% but price ${r.price} vs ATH ${r.ath} gives ${calc.toFixed(2)}%`);
    if (r.formulas.length < 2) fails.push(`${tag}: DCF formula elements missing (${r.formulas.length})`);
    r.formulas.forEach(f => { if (!/3 × Price/.test(f)) fails.push(`${tag}: DCF formula hides the 3× cap: ${f}`); });
    if (!/16\.7/.test(r.note)) fails.push(`${tag}: DCF note does not explain the cap: ${r.note.slice(0, 80)}`);
    if (typeof r.dcf !== 'number') fails.push(`${tag}: DCF not computed (${r.dcf})`);
    else if (Math.abs(r.dcf / r.statePrice - 3) > 1e-9) fails.push(`${tag}: DCF ${r.dcf} is not 3 × price ${r.statePrice} (formula text would not reproduce it)`);
    const cardV = parseFloat(r.cardText.replace(/[^\d.]/g, ''));
    if (!(cardV > 0)) fails.push(`${tag}: DCF card value missing (${r.cardText})`);
    else if (Math.abs(cardV / r.statePrice - 3) > 0.01) fails.push(`${tag}: DCF card ${r.cardText} is not 3 × price ${r.statePrice}`);
    if (r.foot.length < 5) fails.push(`${tag}: only ${r.foot.length} visible commentary footers`);
    r.foot.forEach((f, i) => { if (!f.includes(ASOF[lang])) fails.push(`${tag}: commentary footer ${i} lacks as-of note: ${f.replace(/\s+/g, ' ')}`); });
    console.log(`${tag}: price ${r.price} ATH ${r.ath} shown ${r.shown}% calc ${calc.toFixed(2)}% · DCF ${typeof r.dcf === 'number' ? (r.dcf / r.statePrice).toFixed(6) + '×' : r.dcf} · card ${r.cardText} · footers ${r.foot.length}`);
    await p.close();
  }
  await b.close();
  fails.forEach(f => console.log('FAIL ' + f));
  console.log(fails.length ? `FAIL astra_p0b (${fails.length})` : 'PASS astra_p0b');
  process.exit(fails.length ? 1 : 0);
})().catch(e => { console.log('FAIL ' + e.message); process.exit(1); });
