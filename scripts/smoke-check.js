// Smoke check for the ETHval dashboard.
// Fails when the headline valuation is broken (NaN / missing model prices),
// which is what happened when stale Supabase data fell outside the 90-day
// chart window and zeroed state.tvl / state.l2Tvl.
//
// Usage: node scripts/smoke-check.js [url]   (default https://ethval.com)
// Requires: npm i playwright (dev only; not a site dependency)
const { chromium } = require('playwright');

const URL = process.argv[2] || 'https://ethval.com';
const EXPECTED_MODELS = 12;

(async () => {
  const browser = await chromium.launch({ headless: true });
  const page = await browser.newPage({ viewport: { width: 1440, height: 900 } });
  const pageErrors = [];
  page.on('pageerror', e => pageErrors.push(e.message));
  await page.goto(URL, { waitUntil: 'networkidle', timeout: 90000 });
  await page.waitForTimeout(6000);

  const r = await page.evaluate(() => {
    const txt = id => (document.getElementById(id) || {}).textContent || '';
    const cards = [...document.querySelectorAll('.valuation-model')].map(c => ({
      model: c.dataset.model,
      price: parseFloat(c.dataset.price),
    }));
    return {
      cards,
      headline: ['summary-fair-value', 'summary-opportunity', 'composite-price', 'composite-diff', 'summary-upside']
        .map(id => [id, txt(id)]),
      tvl: typeof state !== 'undefined' ? state.tvl : null,
      l2Tvl: typeof state !== 'undefined' ? state.l2Tvl : null,
    };
  });
  await browser.close();

  const failures = [];
  const bad = r.cards.filter(c => !Number.isFinite(c.price) || c.price <= 0).map(c => c.model);
  if (r.cards.length !== EXPECTED_MODELS) failures.push(`expected ${EXPECTED_MODELS} model cards, got ${r.cards.length}`);
  if (bad.length) failures.push(`models without a price: ${bad.join(', ')}`);
  for (const [id, t] of r.headline) if (/NaN|undefined/.test(t)) failures.push(`${id} shows "${t.trim()}"`);
  if (!(r.tvl > 0)) failures.push(`state.tvl is ${r.tvl}`);
  if (!(r.l2Tvl > 0)) failures.push(`state.l2Tvl is ${r.l2Tvl}`);
  if (pageErrors.length) failures.push(`page errors: ${pageErrors.slice(0, 3).join(' | ')}`);

  console.log(JSON.stringify({ url: URL, headline: Object.fromEntries(r.headline), tvl: r.tvl, l2Tvl: r.l2Tvl }, null, 1));
  if (failures.length) {
    console.error('SMOKE FAIL\n - ' + failures.join('\n - '));
    process.exit(1);
  }
  console.log('SMOKE PASS');
})().catch(e => { console.error('SMOKE ERROR', e); process.exit(2); });
