// Read-only density audit (1440 light): padding of cards, gap of grids, section spacing, line-height of body text.
const { chromium } = require('playwright');
const URL = process.argv[2] || 'https://ethval.com/';
(async () => {
  const b = await chromium.launch();
  const p = await b.newPage({ viewport: { width: 1440, height: 900 } });
  await p.addInitScript(() => { localStorage.setItem('ethval-theme', 'light'); localStorage.setItem('ethval-lang', 'en'); });
  await p.goto(URL, { waitUntil: 'networkidle', timeout: 90000 }).catch(() => {});
  await p.waitForTimeout(2500);
  const r = await p.evaluate(() => {
    const first = (sel) => document.querySelector(sel);
    const card = (c) => { const e = first('.' + c); if (!e) return null; const s = getComputedStyle(e);
      return `${c}: pad ${s.paddingTop}/${s.paddingLeft} r${s.borderTopLeftRadius}`; };
    const grids = [...document.querySelectorAll('.container *')].filter(e => { const s = getComputedStyle(e);
      return (s.display === 'grid' || s.display === 'flex') && e.children.length >= 3 && e.getBoundingClientRect().width > 500; })
      .map(e => { const s = getComputedStyle(e); const k = (typeof e.className === 'string' ? e.className.split(' ')[0] : '') || e.tagName;
        return `${k}: ${s.display} gap ${s.rowGap}/${s.columnGap} kids ${e.children.length}`; });
    const uniq = [...new Set(grids)].slice(0, 30);
    const secs = [...document.querySelectorAll('.section[id]')].map(s => { const cs = getComputedStyle(s);
      return `${s.id}: pad ${cs.paddingTop}/${cs.paddingBottom} mar ${cs.marginTop}/${cs.marginBottom}`; });
    const bodyText = first('.ai-commentary p, .summary-description p, .metric-desc');
    return { cards: ['ni-card','metric-card','valuation-model','method-card','simulator-box','price-card','summary-valuation-box','ev-research-card','category-card','ni-chart-box','conclusion-panel'].map(card).filter(Boolean),
      grids: uniq, secs, lh: bodyText ? getComputedStyle(bodyText).lineHeight + ' / ' + getComputedStyle(bodyText).fontSize : null,
      h: document.documentElement.scrollHeight };
  });
  console.log(r.cards.join('\n')); console.log('--'); console.log(r.grids.join('\n')); console.log('--'); console.log(r.secs.join('\n'));
  console.log('text lh', r.lh, 'pageH', r.h);
  await b.close();
})();
