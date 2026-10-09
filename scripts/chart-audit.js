#!/usr/bin/env node
// Chart audit: every rendered Chart.js chart on ethval.com must have real data.
// Fails (exit 1) when a visible chart has < 5 real points (e.g. one-day series) or only zeros.
// Why: in 2026-10 Open Interest showed a single dot and both L2 stablecoin charts were flat zero for
// months while the headline value still rendered — nothing flagged it.
// Usage: node scripts/chart-audit.js [url]
const { chromium } = require('playwright');

const URL = process.argv[2] || 'https://ethval.com/';
const MIN_POINTS = 5;

(async () => {
    // CI installs the matching headless shell; local machines may only have a cached full chromium-* build.
    let browser;
    try { browser = await chromium.launch(); }
    catch (e) {
        const fs = require('fs'), path = require('path'), os = require('os');
        const cache = path.join(os.homedir(), 'Library/Caches/ms-playwright');
        const exe = fs.existsSync(cache) && fs.readdirSync(cache).filter(d => /^chromium-\d+$/.test(d)).sort().reverse()
            .map(d => path.join(cache, d, 'chrome-mac-arm64/Google Chrome for Testing.app/Contents/MacOS/Google Chrome for Testing'))
            .find(p => fs.existsSync(p));
        if (!exe) throw e;
        browser = await chromium.launch({ executablePath: exe });
    }
    const page = await browser.newPage({ viewport: { width: 1280, height: 900 } });
    await page.goto(URL + (URL.includes('?') ? '&' : '?') + 'audit=' + Date.now(), { waitUntil: 'domcontentloaded', timeout: 60000 });
    await page.waitForTimeout(8000);
    // Scroll the full page so lazily-initialised charts render.
    const height = await page.evaluate(() => document.body.scrollHeight);
    for (let y = 0; y < height + 1200; y += 1200) { await page.evaluate(v => window.scrollTo(0, v), y); await page.waitForTimeout(120); }
    await page.waitForTimeout(3000);
    const rows = await page.evaluate(() => Object.values(window.Chart?.instances || {}).map(c => {
        const vals = (c.data?.datasets || []).flatMap(d => (d.data || []).map(v => (v && typeof v === 'object') ? v.y : v));
        const real = vals.filter(v => v !== null && v !== undefined && Number.isFinite(+v));
        const visible = !!(c.canvas && c.canvas.offsetParent && c.canvas.getBoundingClientRect().width > 0);
        return { id: c.canvas?.id || '(no-id)', points: real.length, nonzero: real.filter(v => +v !== 0).length, visible };
    }));
    await browser.close();
    const seen = rows.filter(r => r.visible);
    const bad = seen.filter(r => r.points < MIN_POINTS || r.nonzero === 0);
    for (const r of bad) console.log(`${r.points < MIN_POINTS ? 'FEW ' : 'ZERO'} ${r.id} points=${r.points} nonzero=${r.nonzero}`);
    if (seen.length < 20) { console.log(`FAIL: only ${seen.length} charts rendered (page broken?)`); process.exit(1); }
    console.log(bad.length ? `FAIL: ${bad.length}/${seen.length} charts without real data` : `PASS: ${seen.length} charts have real data`);
    process.exit(bad.length ? 1 : 0);
})().catch(e => { console.error('ERR', e.message); process.exit(2); });
