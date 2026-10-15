// 2026-10-15 backlog 131/132/133/135/137 render check.
//  - simulator active addresses == 02.5 card value
//  - DEX desc says daily (not weekly) in en/kr
//  - validator method desc no longer claims the live 10Y is 4.5%
//  - kr: Open Interest card title/desc translated
//  - 390: supply (Effective Float) chart x labels do not overlap
// usage: node .qa/backlog_1015_check.cjs <url>
const { launch } = require('./browser.cjs');
const URL = process.argv[2] || 'https://ethval.com/';
let fail = 0; const ok = (c, m) => { console.log((c ? 'PASS ' : 'FAIL ') + m); if (!c) fail++; };
(async () => {
  const b = await launch();
  for (const [lang, w] of [['en', 1280], ['kr', 1280], ['kr', 390]]) {
    const p = await (await b.newContext({ viewport: { width: w, height: 900 } })).newPage();
    await p.goto(URL + '#' + lang, { waitUntil: 'networkidle', timeout: 90000 });
    await p.waitForFunction(() => /\d/.test(document.getElementById('l1-active-addr-value')?.textContent || '') && /\d/.test(document.getElementById('sim-activeAddresses-current')?.textContent || ''), null, { timeout: 60000 }).catch(() => {});
    await p.waitForTimeout(4000);
    const r = await p.evaluate(() => {
      const txt = (s) => document.querySelector(s)?.textContent.trim() || '';
      const oi = document.querySelector('[data-i18n="metric.openInterest"]'), oid = document.querySelector('[data-i18n="desc.openInterest"]');
      let labels = null;
      const c = window.Chart && Chart.getChart && Chart.getChart(document.getElementById('supply-chart'));
      if (c && c.scales.x) {
        const sx = c.scales.x, ctx = c.ctx; ctx.save(); ctx.font = '8px sans-serif';
        // use the rendered label items (respects callback nulls and align:'inner')
        labels = (sx._labelItems || sx.getLabelItems?.() || []).filter(it => it.label != null && it.label !== '').map(it => {
          const lab = String(it.label), wd = ctx.measureText(lab).width, x = it.options?.translation?.[0] ?? it.translation?.[0];
          const al = it.options?.textAlign || it.textAlign || 'center';
          const l = al === 'left' ? x : al === 'right' ? x - wd : x - wd / 2;
          return { lab, l, r: l + wd };
        });
        ctx.restore();
      }
      return { card: txt('#l1-active-addr-value'), sim: txt('#sim-activeAddresses-current'), dex: txt('[data-i18n="desc.dex"]'),
        val: txt('[data-i18n="method.validator.desc"]'), oi: oi?.textContent.trim(), oid: oid?.textContent.trim(), labels, cw: c?.width };
    });
    const tag = `${w} ${lang}`;
    const num = (s) => { const m = String(s).replace(/,/g, '').match(/([\d.]+)\s*([KMB])?/i); if (!m) return NaN; return +m[1] * ({ K: 1e3, M: 1e6, B: 1e9 }[(m[2] || '').toUpperCase()] || 1); };
    const cv = num(r.card), sv = num(r.sim);
    ok(cv > 0 && Math.abs(cv - sv) / cv < 0.01, `${tag} active addr card "${r.card}" == simulator "${r.sim}"`);
    ok(lang === 'en' ? /^Daily DEX/.test(r.dex) : /일일 DEX/.test(r.dex), `${tag} DEX desc daily: "${r.dex.slice(0, 40)}"`);
    ok(!/10Y Treasury \(~4\.5%\)|10년물 국채\(~4\.5%\)/.test(r.val) && /4\.5%/.test(r.val), `${tag} validator desc = assumption: "${(r.val.match(/[^.。]*4\.5%[^.。]*/) || [''])[0].slice(0, 80)}"`);
    if (lang === 'kr') ok(r.oi === '미결제약정' && /[가-힣]/.test(r.oid || ''), `${tag} OI title/desc: "${r.oi}" / "${(r.oid || '').slice(0, 30)}"`);
    if (w === 390) {
      const L = r.labels || [];
      const overlap = L.some((a, i) => i && a.l < L[i - 1].r + 2);
      ok(L.length >= 2 && !overlap, `${tag} supply x labels ${L.length} (${L.map(x => x.lab).join(' | ')}) canvas=${r.cw} overlap=${overlap}`);
      await p.locator('#supply-chart').scrollIntoViewIfNeeded().catch(() => {});
      await p.waitForTimeout(800);
      await p.locator('[data-metric="supply"]').screenshot({ path: '/tmp/supply_390.png' }).catch(() => {});
    }
    await p.context().close();
  }
  await b.close();
  console.log(fail ? 'BACKLOG-1015 FAIL' : 'BACKLOG-1015 ALL PASS'); process.exit(fail ? 1 : 0);
})();
