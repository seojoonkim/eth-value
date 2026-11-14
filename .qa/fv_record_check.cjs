// At-the-time record line (fv-backtest.js .fbt-rec): values = last fair_value_snapshots capture per UTC day,
// every calendar day from the first record listed (blank when not recorded), recomputed column = backtest days,
// horizon counts = recorded days whose exact +h stored price exists, 4 languages, no overflow/clipping.
// usage: node with_server.cjs fv_record_check.cjs  |  node fv_record_check.cjs https://ethval.com/ [shotdir]
const { launch } = require('./browser.cjs');
(async () => {
  const url = process.argv[2] || 'http://127.0.0.1:8765/', shots = process.argv[3];
  const fails = []; const b = await launch();
  const word = { en: 'At-the-time record', ko: '당시 기록 기준', zh: '当时记录', ja: '当時の記録' };
  for (const [w, theme, lang] of [[390, 'dark', 'ko'], [390, 'light', 'en'], [1280, 'dark', 'zh'], [1280, 'light', 'ja']]) {
    const p = await b.newPage({ viewport: { width: w, height: 900 } });
    await p.goto(url + ({ ko: '#kr', zh: '#zh', ja: '#ja' }[lang] || ''), { waitUntil: 'domcontentloaded' });
    await p.evaluate(t => { document.body.classList.toggle('dark', t === 'dark'); document.documentElement.classList.toggle('dark', t === 'dark'); }, theme);
    await p.waitForFunction(() => +document.getElementById('fv-backtest')?.dataset.recDays > 0, null, { timeout: 120000 }).catch(() => {});
    await p.waitForTimeout(600);
    const r = await p.evaluate(async () => {
      const box = document.getElementById('fv-backtest'); const el = box && box.querySelector('.fbt-rec');
      if (!el || el.hidden) return { missing: true };
      const { data, error } = await supabaseClient.from('fair_value_snapshots').select('captured_at,snapshot_date,composite_usd').order('captured_at');
      if (error) return { err: error.message };
      const last = {}; data.forEach(x => { last[x.snapshot_date] = +x.composite_usd; });
      const days = Object.keys(last).sort();
      const res = window.ethvalBacktest.compute(); const rc = new Map(res.days.map(d => [d.date, d.fv]));
      const P = new Map(); state.priceHistory.forEach(d => { if (d.value > 0) P.set(new Date(d.date).toISOString().slice(0, 10), d.value); });
      const add = (s, n) => new Date(Date.parse(s + 'T00:00:00Z') + n * 864e5).toISOString().slice(0, 10);
      const o = { line: el.querySelector('.fbt-rec-line').textContent, recDays: +box.dataset.recDays, recFirst: box.dataset.recFirst, recN: box.dataset.recN, bad: [] };
      o.expDays = days.length; o.expFirst = days[0];
      o.expN = [30, 90, 180].map(h => days.filter(d => P.get(d) && P.get(add(d, h))).length).join(',');
      const rows = [...el.querySelectorAll('tbody tr')]; o.rows = rows.length;
      let span = 0; for (let d = days[0]; d <= days[days.length - 1]; d = add(d, 1)) span++; o.expRows = span;
      rows.forEach(tr => { const d = tr.dataset.date, rec = tr.dataset.rec, re = tr.dataset.re;
        const er = last[d] ? last[d].toFixed(2) : ''; if (rec !== er) o.bad.push(d + ' rec ' + rec + '≠' + er);
        const ee = rc.has(d) ? rc.get(d).toFixed(2) : ''; if (re !== ee) o.bad.push(d + ' re ' + re + '≠' + ee);
        if ((rec === '' || re === '') && tr.children[3].textContent !== '') o.bad.push(d + ' diff not blank'); });
      o.blanksRe = rows.filter(tr => tr.dataset.re === '').map(tr => tr.dataset.date);
      // open details and check layout
      const det = el.querySelector('details'); det.open = true; await new Promise(r => setTimeout(r, 200));
      o.overflow = document.documentElement.scrollWidth > innerWidth + 1;
      const br = box.getBoundingClientRect(); o.tblOut = el.querySelector('table').getBoundingClientRect().right > br.right + 1;
      o.clipped = [...el.querySelectorAll('th, td, summary')].filter(e => e.scrollWidth > e.clientWidth + 1).map(e => e.textContent).slice(0, 5);
      det.open = false; o.top = el.getBoundingClientRect().top + scrollY;
      return o;
    });
    const tag = `${w} ${theme} ${lang}`;
    if (r.missing || r.err) { fails.push(tag + ' rec missing ' + (r.err || '')); await p.close(); continue; }
    console.log(tag, JSON.stringify({ recDays: r.recDays, first: r.recFirst, n: r.recN, rows: r.rows, blanksRe: r.blanksRe }));
    console.log('  ' + r.line);
    if (!r.line.includes(word[lang])) fails.push(tag + ' title');
    if (!r.line.includes('2026-10-15') || r.recFirst !== r.expFirst) fails.push(tag + ' start date ' + r.recFirst);
    if (r.recDays !== r.expDays) fails.push(tag + ` days ${r.recDays}≠${r.expDays}`);
    if (r.recN !== r.expN) fails.push(tag + ` horizon n ${r.recN}≠${r.expN}`);
    if (r.rows !== r.expRows) fails.push(tag + ` rows ${r.rows}≠${r.expRows}`);
    if (r.bad.length) fails.push(tag + ' ' + r.bad.slice(0, 4).join('; '));
    if (r.overflow || r.tblOut) fails.push(tag + ' overflow');
    if (r.clipped.length) fails.push(tag + ' clipped ' + r.clipped.join(','));
    if (shots) { await p.evaluate(() => { document.querySelector('#fv-backtest details.fbt-rec-days').open = true; });
      await p.evaluate(y => scrollTo(0, y - 80), r.top); await p.waitForTimeout(300);
      await p.locator('#fv-backtest').screenshot({ path: `${shots}/rec_${w}_${theme}_${lang}.png` }); }
    await p.close();
  }
  await b.close();
  if (fails.length) { console.log('FAIL\n' + fails.join('\n')); process.exit(1); }
  console.log('FV RECORD ALL PASS');
})();
