#!/usr/bin/env node
/* ETHval — daily fair-value snapshot (table public.fair_value_snapshots, sql/fair_value_snapshots.sql).
 *
 * Past fair values elsewhere on the site are recomputed with today's formulas / constants / supply, so the
 * value the site actually showed on a given day was never kept. This script keeps it from now on:
 * it opens this checkout in headless Chromium (same way scripts/build-model-pages.cjs does), waits until
 * all 12 model cards and the composite headline have a value, and stores exactly what the page shows.
 * No backfill, no estimate: if any model card has no value, nothing is stored (exit 2).
 *
 * usage: node scripts/fv-snapshot.cjs          (needs SUPABASE_URL + SUPABASE_SERVICE_KEY; inserts one row)
 *        node scripts/fv-snapshot.cjs --dry    (prints the row, writes nothing)
 * Runs in .github/workflows/model-pages.yml after every successful data collection.
 */
const http = require('http'), fs = require('fs'), path = require('path'), os = require('os');
const { execSync } = require('child_process');
const ROOT = path.resolve(__dirname, '..');
const { chromium } = require(require.resolve('playwright', { paths: [path.join(ROOT, '.qa'), ROOT] }));
const MODELS = ['tvlMultiple', 'stakingScarcity', 'metcalfe', 'dcf', 'l2Ecosystem', 'ps', 'revenueYield',
  'commitmentPremium', 'appCapital', 'validatorEcon', 'ethMonetary', 'ecosystemSettlement'];
const DRY = process.argv.includes('--dry');

function findChrome() {
  const cache = path.join(os.homedir(), 'Library/Caches/ms-playwright');
  if (!fs.existsSync(cache)) return null;
  return fs.readdirSync(cache).filter(d => /^chromium-\d+$/.test(d)).sort().reverse()
    .map(d => path.join(cache, d, 'chrome-mac-arm64/Google Chrome for Testing.app/Contents/MacOS/Google Chrome for Testing'))
    .find(p => fs.existsSync(p)) || null;
}
function serve() {
  const types = { '.html': 'text/html', '.js': 'text/javascript', '.css': 'text/css', '.json': 'application/json', '.svg': 'image/svg+xml' };
  const srv = http.createServer((req, res) => {
    let p = decodeURIComponent(req.url.split('?')[0]); if (p.endsWith('/')) p += 'index.html';
    const f = path.join(ROOT, p);
    if (!f.startsWith(ROOT)) { res.writeHead(403); return res.end(); }
    fs.readFile(f, (e, b) => { if (e) { res.writeHead(404); return res.end(); } res.writeHead(200, { 'content-type': types[path.extname(f)] || 'application/octet-stream' }); res.end(b); });
  });
  return new Promise(r => srv.listen(0, '127.0.0.1', () => r(srv)));
}

async function read() {
  const srv = await serve();
  const exe = findChrome();
  const b = await chromium.launch(exe ? { executablePath: exe } : {});
  try {
    const p = await b.newPage({ viewport: { width: 1280, height: 900 } });
    await p.goto(`http://127.0.0.1:${srv.address().port}/`, { waitUntil: 'domcontentloaded' });
    await p.waitForFunction(MODELS => typeof state !== 'undefined' && state.historicalDataLoaded && state.price > 0 &&
      MODELS.every(m => { const c = document.querySelector(`.valuation-model[data-model="${m}"]`); return c && parseFloat(c.dataset.price) > 0; }) &&
      parseFloat(document.getElementById('composite-price').dataset.value) > 0, MODELS, { timeout: 240000 });
    await p.waitForTimeout(3000); // let the last DB sync / price refresh settle, then read once
    return await p.evaluate(MODELS => {
      const models = {}, incl = [];
      MODELS.forEach(m => {
        const c = document.querySelector(`.valuation-model[data-model="${m}"]`);
        const v = parseFloat(c.dataset.price);
        models[m] = v;
        const t = c.querySelector('.model-toggle input');
        if (t && t.checked && v > 0) incl.push(v);
      });
      const shown = parseFloat(document.getElementById('composite-price').dataset.value);
      const mean = incl.reduce((a, v) => a + v, 0) / incl.length;
      const badge = document.querySelector('.version-badge');
      const cl = window.EthvalConstants && window.EthvalConstants.CHANGELOG;
      return { models, composite: shown, mean, n: incl.length, price: state.price,
        live: state.livePrice > 0 && state.price === state.livePrice,
        version: badge ? badge.textContent.trim() : null,
        constantsCommit: cl && cl.length ? cl[cl.length - 1].commit : null };
    }, MODELS);
  } finally { await b.close(); srv.close(); }
}

(async () => {
  const r = await read();
  const bad = MODELS.filter(m => !(r.models[m] > 0 && isFinite(r.models[m])));
  if (bad.length) { console.error('no snapshot: missing model value(s): ' + bad.join(', ')); process.exit(2); }
  // the headline must be the plain mean of the included cards (same rule as recalculateWeightedAverage)
  if (!(r.n > 0) || Math.abs(r.composite - r.mean) > 0.01) { console.error(`no snapshot: composite ${r.composite} != mean of ${r.n} cards ${r.mean}`); process.exit(2); }
  if (!r.version) { console.error('no snapshot: version badge not found'); process.exit(2); }
  const now = new Date();
  let sha = process.env.GITHUB_SHA || null;
  if (!sha) try { sha = execSync('git rev-parse HEAD', { cwd: ROOT }).toString().trim(); } catch (e) {}
  const row = {
    captured_at: now.toISOString(), snapshot_date: now.toISOString().slice(0, 10),
    composite_usd: +r.composite.toFixed(4), models_included: r.n,
    models: Object.fromEntries(MODELS.map(m => [m, +r.models[m].toFixed(4)])),
    eth_price_usd: r.price, price_is_live: r.live, site_version: r.version,
    constants_commit: r.constantsCommit, site_commit: sha ? sha.slice(0, 12) : null
  };
  console.log(JSON.stringify(row, null, 1));
  if (DRY) return console.log('dry run — nothing stored');
  const url = process.env.SUPABASE_URL, key = process.env.SUPABASE_SERVICE_KEY;
  if (!url || !key) { console.error('missing SUPABASE_URL / SUPABASE_SERVICE_KEY'); process.exit(1); }
  const res = await fetch(url.replace(/\/$/, '') + '/rest/v1/fair_value_snapshots', {
    method: 'POST', headers: { apikey: key, Authorization: 'Bearer ' + key, 'Content-Type': 'application/json', Prefer: 'return=minimal' },
    body: JSON.stringify(row) });
  if (!res.ok) { console.error('insert failed', res.status, await res.text()); process.exit(1); }
  console.log('stored snapshot', row.captured_at, 'composite', row.composite_usd);
})().catch(e => { console.error(e); process.exit(1); });
