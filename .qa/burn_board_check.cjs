// Live burn counter + leaderboard (burn-board.js) check — section 02.3, right after #commentary-supply_dynamics.
//  - placement inside #fundamentals, full grid width, closed <details> by default
//  - independent recompute from ultrasound.money (same block only): KPI 1h/24h/7d == feesBurned (wei / 1e18),
//    leaderboard rows == source top 10 by fees for the active window, share == fees / feesBurned<win>, sorted, ranks 1..10
//  - honesty: no per-L2 rows (blob fees only as the single source line), unlabelled contracts show address (no invented name)
//  - 24h/7d tab switch re-renders with the 7d board
//  - 4 languages: title/lead/notes translated (no English leftover in ko/ja/zh), no overflow at 390/1280,
//    nothing leaves the box, no clipped numbers
// usage: node .qa/burn_board_check.cjs [url]
const { launch } = require('./browser.cjs');
const API = 'https://ultrasound.money/api/fees/grouped-analysis-1';
(async () => {
  let fail = 0; const bad = m => { fail = 1; console.log('FAIL ' + m); };
  const url = process.argv[2] || 'https://ethval.com/';
  const b = await launch();
  const runs = [[390, 'dark', 'en'], [390, 'light', 'ko'], [1280, 'dark', 'ja'], [1280, 'light', 'zh'], [1280, 'dark', 'en'], [390, 'dark', 'zh']];
  for (const [w, theme, lang] of runs) {
    const p = await b.newPage({ viewport: { width: w, height: 900 } });
    let raw = null;
    p.on('response', async r => { if (r.url().startsWith(API) && r.ok()) { try { raw = await r.json(); } catch (e) {} } });
    await p.addInitScript(([t, l]) => { try { localStorage.setItem('ethval-theme', t); localStorage.setItem('ethval-lang', l); } catch (e) {} }, [theme, lang]);
    // 2026-10-15: default 30s goto timed out under load (full QA runs 10+ browsers); 90s + one retry.
    const target = url + (lang === 'en' ? '' : `?lang=${lang}`);
    await p.goto(target, { waitUntil: 'domcontentloaded', timeout: 90000 })
      .catch(() => p.goto(target, { waitUntil: 'domcontentloaded', timeout: 90000 }));
    await p.waitForFunction(() => !!document.getElementById('burn-board'), null, { timeout: 60000 }).catch(() => {});
    const closed = await p.evaluate(() => { const d = document.querySelector('#burn-board details'); return d ? !d.open : null; });
    if (closed !== true) bad(`${w} ${lang} box not closed by default (${closed})`);
    await p.evaluate(() => document.getElementById('burn-board')?.scrollIntoView());
    await p.waitForFunction(() => document.getElementById('burn-board')?.dataset.ready === '1', null, { timeout: 60000 }).catch(() => {});
    await p.evaluate(l => { if (typeof setLanguage === 'function' && document.documentElement.lang !== l) setLanguage(l); }, lang);
    await p.evaluate(() => { const d = document.querySelector('#burn-board details'); if (d) d.open = true; document.getElementById('burn-board')?.scrollIntoView(); });
    await p.waitForTimeout(700);
    const tag = `${w} ${theme} ${lang}`;
    const grab = () => p.evaluate(() => {
      const root = document.getElementById('burn-board'); if (!root || root.dataset.ready !== '1') return { missing: true, status: root && root.dataset.status };
      const box = root.querySelector('details').getBoundingClientRect(), out = [];
      root.querySelectorAll('table, td, th, p, .bbx-kpi, .bbx-tabs, summary span').forEach(e => { const q = e.getBoundingClientRect(); if (q.width && (q.right > box.right + 0.5 || q.left < box.left - 0.5)) out.push(e.tagName + ':' + e.textContent.slice(0, 30)); });
      root.querySelectorAll('td b, .bbx-kpi b').forEach(e => { if (e.scrollWidth > e.clientWidth + 1) out.push('clip:' + e.textContent); });
      const grid = root.parentElement.getBoundingClientRect(), rr = root.getBoundingClientRect();
      return {
        block: +root.dataset.block, win: root.dataset.win, inFund: !!root.closest('#fundamentals'),
        after: document.getElementById('commentary-supply_dynamics')?.nextElementSibling === root,
        full: Math.abs(rr.width - grid.width) < 2,
        kpi: Object.fromEntries([...root.querySelectorAll('.bbx-kpi')].map(k => [k.dataset.k, k.querySelector('b').textContent])),
        rows: [...root.querySelectorAll('tbody tr')].map(tr => ({ id: tr.dataset.id, eth: +tr.dataset.eth, share: +tr.dataset.share, rank: tr.cells[0].textContent, who: tr.cells[1].textContent })),
        out, overflow: document.documentElement.scrollWidth > innerWidth + 1,
        title: root.querySelector('.mc-title').textContent, lead: root.querySelector('.mc-lead').textContent,
        notes: [...root.querySelectorAll('.csx-note')].map(n => n.textContent).join(' ')
      };
    });
    let r = await grab();
    if (r.missing) { bad(`${tag} panel missing/not ready status=${r.status}`); await p.close(); continue; }
    if (!raw) { bad(`${tag} could not capture the source payload`); await p.close(); continue; }
    if (raw.number !== r.block) bad(`${tag} block ${r.block} != payload ${raw.number}`);
    if (!r.inFund || !r.after || !r.full) bad(`${tag} placement inFund=${r.inFund} after=${r.after} full=${r.full}`);
    const fe = v => { v = Number(v) / 1e18; return v >= 1000 ? Math.round(v).toLocaleString('en-US') : v >= 100 ? v.toFixed(1) : v >= 1 ? v.toFixed(2) : v.toFixed(3); };
    for (const k of ['1h', '24h', '7d']) { const e = fe(raw.feesBurned['feesBurned' + k]) + ' ETH'; if (r.kpi[k] !== e) bad(`${tag} KPI ${k} '${r.kpi[k]}' != source '${e}'`); }
    const check = (rows, win) => {
      const tot = Number(raw.feesBurned['feesBurned' + win]);
      const src = raw.leaderboards['leaderboard' + win].slice().sort((a, b) => Number(b.fees) - Number(a.fees)).slice(0, 10);
      if (rows.length !== src.length) bad(`${tag} ${win} ${rows.length} rows != ${src.length}`);
      rows.forEach((x, i) => {
        const s = src[i]; if (!s) return;
        if (x.id !== s.id) bad(`${tag} ${win} row ${i + 1} ${x.id} != source ${s.id}`);
        if (Math.abs(x.eth / (Number(s.fees) / 1e18) - 1) > 1e-9) bad(`${tag} ${win} ${x.id} eth ${x.eth}`);
        if (Math.abs(x.share / (Number(s.fees) / tot) - 1) > 1e-9) bad(`${tag} ${win} ${x.id} share ${x.share}`);
        if (x.rank !== String(i + 1)) bad(`${tag} ${win} rank ${x.rank}`);
        if (s.type === 'contract' && !s.name && !/0x[0-9a-f]{4}…[0-9a-f]{4}/.test(x.who)) bad(`${tag} unlabelled ${s.id} shown as '${x.who}'`);
        if (s.type === 'contract' && s.name && !x.who.includes(s.name)) bad(`${tag} label '${s.name}' not shown ('${x.who}')`);
      });
      if (rows.some((x, i) => i && x.eth > rows[i - 1].eth + 1e-12)) bad(`${tag} ${win} not sorted`);
    };
    check(r.rows, '24h');
    await p.click('#burn-board .bbx-tab[data-win="7d"]'); await p.waitForTimeout(300);
    const r7 = await grab(); if (r7.win !== '7d') bad(`${tag} tab switch failed`); else check(r7.rows, '7d');
    const blobRows = r7.rows.filter(x => x.id === 'blob-fees').length; if (blobRows > 1) bad(`${tag} ${blobRows} blob rows (source has one combined line)`);
    if (lang !== 'en') {
      const en = ['Live burn', 'who is burning', 'refreshed every minute', 'Per-L2 ranking', 'Top burners', 'App / contract'];
      en.forEach(s => { if ((r.title + r.lead + r.notes + r.rows.map(x => x.who).join(' ')).includes(s)) bad(`${tag} English leftover '${s}'`); });
    }
    if (r.out.length || r7.out.length) bad(`${tag} outside box: ${[...r.out, ...r7.out].slice(0, 4).join(' | ')}`);
    if (r.overflow) bad(`${tag} page overflow`);
    console.log(`${tag}: block ${r.block} kpi ${JSON.stringify(r.kpi)} top ${r.rows[0] && r.rows[0].who.slice(0, 30)} ${(r.rows[0] || {}).share}`);
    await p.close();
  }
  await b.close();
  console.log(fail ? 'BURN BOARD FAIL' : 'BURN BOARD ALL PASS'); process.exit(fail);
})();
