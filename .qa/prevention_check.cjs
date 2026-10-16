// Bounded regression QA for d-001d8cc649.
// Checks only four regressions:
// 1) real theme toggle at 390/1280 in both directions
// 2) locator re-query safety after rerenders
// 3) share-image comparison/signals bars occupy their intended space
// 4) mobile signoff separators stay non-visible (no orphaned divider)
//
// Usage:
//   node .qa/prevention_check.cjs <url> [outdir]
//   node .qa/prevention_check.cjs --fixtures-only [outdir]
const fs = require('fs');
const path = require('path');

const OUT_DEFAULT = '/tmp/ethval-prevention';

function mkdirp(dir) {
  fs.mkdirSync(dir, { recursive: true });
}

function slug(s) {
  return String(s).replace(/[^a-z0-9]+/gi, '-').replace(/(^-|-$)/g, '').toLowerCase() || 'run';
}

function writeJson(file, value) {
  fs.writeFileSync(file, JSON.stringify(value, null, 2) + '\n');
}

function parseUsd(text) {
  const n = parseFloat(String(text || '').replace(/[^0-9.\-]/g, ''));
  return Number.isFinite(n) ? n : NaN;
}

function approx(a, b, tol) {
  return Math.abs(a - b) <= tol;
}

function assertThemeTransition(sample) {
  const fails = [];
  const want = sample.toDark ? 'dark' : 'light';
  if (sample.after.bodyDark !== sample.toDark) fails.push('body class did not toggle');
  if (sample.after.htmlDark !== sample.toDark) fails.push('html class did not toggle');
  if (sample.after.storageTheme !== want) fails.push(`localStorage ethval-theme is ${sample.after.storageTheme || 'null'}`);
  const keys = ['bg', 'card', 'text'];
  const unchanged = keys.filter(k => !sample.after.tokens[k] || sample.before.tokens[k] === sample.after.tokens[k]);
  if (unchanged.length) fails.push('theme tokens unchanged: ' + unchanged.join(', '));
  for (const key of ['signoffBackground', 'shareButtonBackground']) {
    if (!sample.after[key] || sample.before[key] === sample.after[key]) fails.push(key + ' did not change');
  }
  if (sample.after.shareTheme !== want) fails.push(`share image rendered as ${sample.after.shareTheme || 'unknown'}`);
  if (sample.after.sharePixelBrightness == null) fails.push('share image theme sample missing');
  return fails;
}

function assertShareGeometry(sample) {
  const fails = [];
  const compare = sample.comparison || {};
  if (!compare.bg) fails.push('comparison background bar missing');
  if (!compare.fair) fails.push('comparison fair-value bar missing');
  if (!compare.current) fails.push('comparison current-price bar missing');
  if (compare.bg) {
    if (!approx(compare.bg.bbox.x, sample.expected.compare.x, 3)) fails.push('comparison bar x drift');
    if (!approx(compare.bg.bbox.w, sample.expected.compare.totalW, 3)) fails.push('comparison total width drift');
  }
  if (compare.fair && !approx(compare.fair.bbox.w, sample.expected.compare.fairW, 4)) fails.push('fair-value bar width mismatch');
  if (compare.current && !approx(compare.current.bbox.w, sample.expected.compare.currentW, 4)) fails.push('current-price bar width mismatch');

  const signals = sample.signals || {};
  const expectedSegments = sample.expected.signals.segmentCount;
  const drawnSegments = Array.isArray(signals.segments) ? signals.segments.length : 0;
  if (drawnSegments !== expectedSegments) fails.push(`signals segments ${drawnSegments}/${expectedSegments}`);
  const drawnWidth = Array.isArray(signals.segments) ? signals.segments.reduce((sum, seg) => sum + seg.bbox.w, 0) : 0;
  if (!approx(drawnWidth, sample.expected.signals.totalW, 3)) fails.push('signals total width mismatch');
  if (!signals.coverage || !approx(signals.coverage.w, sample.expected.signals.totalW, 3)) fails.push('signals bar leaves a gap');
  return fails;
}

function assertMobileSignoff(sample) {
  const fails = [];
  if (sample.viewportWidth > 600) return fails;
  if (sample.visibleDividerCount !== 0) fails.push(`visible mobile dividers ${sample.visibleDividerCount}`);
  if (sample.orphanedDividerCount !== 0) fails.push(`orphaned mobile dividers ${sample.orphanedDividerCount}`);
  return fails;
}

function runNegativeFixtures(outDir) {
  const fixtures = {
    theme: {
      toDark: true,
      before: { tokens: { bg: '#ffffff', card: '#f7f7f7', text: '#111111' } },
      after: {
        bodyDark: true,
        htmlDark: false,
        storageTheme: 'light',
        tokens: { bg: '#ffffff', card: '#f7f7f7', text: '#111111' },
        shareTheme: 'light',
        sharePixelBrightness: 242,
      }
    },
    share: {
      expected: {
        compare: { x: 126, totalW: 500, fairW: 360, currentW: 250 },
        signals: { totalW: 376, segmentCount: 3 }
      },
      comparison: {
        bg: { bbox: { x: 126, y: 426, w: 500, h: 10 } },
        fair: { bbox: { x: 126, y: 426, w: 360, h: 10 } }
      },
      signals: {
        segments: [
          { bbox: { x: 706, y: 416, w: 100, h: 12 } },
          { bbox: { x: 866, y: 416, w: 100, h: 12 } }
        ],
        coverage: { x: 706, y: 416, w: 260, h: 12 }
      }
    },
    signoff: {
      viewportWidth: 390,
      visibleDividerCount: 1,
      orphanedDividerCount: 1
    }
  };
  const results = {
    theme: assertThemeTransition(fixtures.theme),
    share: assertShareGeometry(fixtures.share),
    signoff: assertMobileSignoff(fixtures.signoff)
  };
  if (!results.theme.length || !results.share.length || !results.signoff.length) {
    throw new Error('negative fixtures did not fail as expected');
  }
  writeJson(path.join(outDir, 'fixtures.json'), { fixtures, results });
  return results;
}

async function runBrowser(url, outDir) {
  const { launch } = require('./browser.cjs');
  const browser = await launch();
  const report = { url, outputDir: outDir, cases: [], fixtures: null, failures: [] };

  async function saveDataUrlPng(file, dataUrl) {
    fs.writeFileSync(file, Buffer.from(String(dataUrl).split(',')[1] || '', 'base64'));
  }

  async function waitForDashboard(page) {
    await page.goto(url, { waitUntil: 'domcontentloaded', timeout: 45000 });
    await page.waitForFunction(() => typeof window.renderEthvalShareImage === 'function', null, { timeout: 45000 });
    await page.waitForFunction(() => {
      const t = (document.getElementById('composite-price') || {}).textContent || '';
      return /\d/.test(t);
    }, null, { timeout: 60000 });
    await page.waitForTimeout(3500);
  }

  async function captureThemeState(page, fileBase) {
    const snapshot = await page.evaluate(async () => {
      const css = getComputedStyle(document.body);
      const signoff = document.querySelector('.summary-signoff');
      const shareBtn = document.querySelector('.ev-share-btn');
      let shareTheme = null;
      let sharePixelBrightness = null;
      if (typeof window.renderEthvalShareImage === 'function') {
        const dataUrl = await window.renderEthvalShareImage();
        await new Promise((resolve, reject) => {
          const img = new Image();
          img.onload = () => {
            try {
              const c = document.createElement('canvas');
              c.width = img.width;
              c.height = img.height;
              const ctx = c.getContext('2d');
              ctx.drawImage(img, 0, 0);
              const px = ctx.getImageData(18, 18, 1, 1).data;
              sharePixelBrightness = Math.round((px[0] + px[1] + px[2]) / 3);
              shareTheme = sharePixelBrightness < 120 ? 'dark' : 'light';
              resolve();
            } catch (err) { reject(err); }
          };
          img.onerror = reject;
          img.src = dataUrl;
        });
      }
      return {
        bodyDark: document.body.classList.contains('dark'),
        htmlDark: document.documentElement.classList.contains('dark'),
        storageTheme: localStorage.getItem('ethval-theme'),
        tokens: {
          bg: css.getPropertyValue('--bg').trim(),
          card: css.getPropertyValue('--bg-card').trim(),
          text: css.getPropertyValue('--text').trim()
        },
        shareTheme,
        sharePixelBrightness,
        signoffBackground: signoff ? getComputedStyle(signoff).backgroundColor : null,
        shareButtonBackground: shareBtn ? (getComputedStyle(shareBtn).backgroundImage || getComputedStyle(shareBtn).backgroundColor) : null
      };
    });
    const signoff = page.locator('.summary-signoff').first();
    await signoff.scrollIntoViewIfNeeded();
    await page.waitForTimeout(500);
    await signoff.screenshot({ path: `${fileBase}-signoff.png`, timeout: 8000 });
    await page.screenshot({ path: `${fileBase}-page.png`, fullPage: false });
    return snapshot;
  }

  async function captureShareGeometry(page, fileBase) {
    const payload = await page.evaluate(async () => {
      function wrapContext(ctx, ops) {
        if (ctx.__ethvalWrapped) return ctx;
        ctx.__ethvalWrapped = true;
        let pathBox = null;
        const mark = (x, y) => {
          if (!Number.isFinite(x) || !Number.isFinite(y)) return;
          if (!pathBox) pathBox = { minX: x, minY: y, maxX: x, maxY: y };
          pathBox.minX = Math.min(pathBox.minX, x);
          pathBox.minY = Math.min(pathBox.minY, y);
          pathBox.maxX = Math.max(pathBox.maxX, x);
          pathBox.maxY = Math.max(pathBox.maxY, y);
        };
        const capturePathFill = () => {
          if (!pathBox) return;
          ops.push({
            type: 'pathFill',
            fillStyle: String(ctx.fillStyle),
            bbox: { x: pathBox.minX, y: pathBox.minY, w: pathBox.maxX - pathBox.minX, h: pathBox.maxY - pathBox.minY }
          });
        };
        const bind = name => ctx[name].bind(ctx);
        const beginPath = bind('beginPath');
        ctx.beginPath = (...args) => { pathBox = null; return beginPath(...args); };
        const moveTo = bind('moveTo');
        ctx.moveTo = (x, y, ...args) => { mark(x, y); return moveTo(x, y, ...args); };
        const lineTo = bind('lineTo');
        ctx.lineTo = (x, y, ...args) => { mark(x, y); return lineTo(x, y, ...args); };
        const rect = bind('rect');
        ctx.rect = (x, y, w, h, ...args) => { mark(x, y); mark(x + w, y + h); return rect(x, y, w, h, ...args); };
        const arc = bind('arc');
        ctx.arc = (x, y, r, sa, ea, ccw, ...args) => { mark(x - r, y - r); mark(x + r, y + r); return arc(x, y, r, sa, ea, ccw, ...args); };
        const arcTo = bind('arcTo');
        // rr() uses axis-aligned corner tangents; their endpoints bound the rounded rect.
        ctx.arcTo = (x1, y1, x2, y2, r, ...args) => { mark(x1, y1); mark(x2, y2); return arcTo(x1, y1, x2, y2, r, ...args); };
        const fill = bind('fill');
        ctx.fill = (...args) => { capturePathFill(); return fill(...args); };
        const fillRect = bind('fillRect');
        ctx.fillRect = (x, y, w, h, ...args) => {
          ops.push({ type: 'fillRect', fillStyle: String(ctx.fillStyle), bbox: { x, y, w, h } });
          return fillRect(x, y, w, h, ...args);
        };
        return ctx;
      }

      const ops = [];
      const original = HTMLCanvasElement.prototype.getContext;
      HTMLCanvasElement.prototype.getContext = function(type, ...rest) {
        const ctx = original.call(this, type, ...rest);
        if (type === '2d' && ctx) wrapContext(ctx, ops);
        return ctx;
      };
      let dataUrl;
      try {
        dataUrl = await window.renderEthvalShareImage();
      } finally {
        HTMLCanvasElement.prototype.getContext = original;
      }

      const g = id => (document.getElementById(id) || {}).textContent || '';
      return {
        dataUrl,
        ops,
        fairText: g('composite-price').trim(),
        currentText: g('summary-current').trim(),
        counts: {
          buy: +(g('buy-count').trim() || 0),
          hold: +(g('hold-count').trim() || 0),
          sell: +(g('sell-count').trim() || 0)
        }
      };
    });

    await saveDataUrlPng(`${fileBase}-share.png`, payload.dataUrl);

    const fair = parseUsd(payload.fairText);
    const current = parseUsd(payload.currentText);
    const maxValue = Math.max(fair, current) * 1.08;
    const expected = {
      compare: {
        x: 126,
        totalW: 500,
        fairW: Math.max(10, 500 * fair / maxValue),
        currentW: Math.max(10, 500 * current / maxValue)
      },
      signals: {
        totalW: 376,
        segmentCount: [payload.counts.buy, payload.counts.hold, payload.counts.sell].filter(n => n > 0).length
      }
    };

    const compareBars = payload.ops
      .filter(op => op.type === 'pathFill' && op.bbox.x >= 120 && op.bbox.x <= 130 && op.bbox.y >= 420 && op.bbox.y <= 440 && op.bbox.h >= 9 && op.bbox.h <= 12)
      .sort((a, b) => b.bbox.w - a.bbox.w);
    const signalRects = payload.ops
      .filter(op => op.type === 'fillRect' && op.bbox.x >= 700 && op.bbox.x <= 1110 && op.bbox.y >= 410 && op.bbox.y <= 430 && op.bbox.h >= 11 && op.bbox.h <= 13 && op.bbox.w > 0.5)
      .sort((a, b) => a.bbox.x - b.bbox.x);

    const compareBg = compareBars[0] || null;
    const compareCandidates = compareBars.slice(1);
    let compareFair = null;
    let compareCurrent = null;
    if (compareCandidates.length) {
      compareFair = compareCandidates
        .slice()
        .sort((a, b) => Math.abs(a.bbox.w - expected.compare.fairW) - Math.abs(b.bbox.w - expected.compare.fairW))[0] || null;
      compareCurrent = compareCandidates
        .filter(item => item !== compareFair)
        .sort((a, b) => Math.abs(a.bbox.w - expected.compare.currentW) - Math.abs(b.bbox.w - expected.compare.currentW))[0] || null;
      if (!compareCurrent && compareCandidates.length > 1) compareCurrent = compareCandidates.find(item => item !== compareFair) || null;
    }

    const sample = {
      expected,
      comparison: {
        bg: compareBg,
        fair: compareFair,
        current: compareCurrent
      },
      signals: {
        segments: signalRects,
        coverage: signalRects.length ? {
          x: signalRects[0].bbox.x,
          y: signalRects[0].bbox.y,
          w: signalRects[signalRects.length - 1].bbox.x + signalRects[signalRects.length - 1].bbox.w - signalRects[0].bbox.x,
          h: signalRects[0].bbox.h
        } : null
      }
    };
    writeJson(`${fileBase}-share.json`, sample);
    return sample;
  }

  async function captureMobileSignoff(page, fileBase) {
    const sample = await page.evaluate(() => {
      const cta = document.querySelector('.summary-signoff .summary-cta');
      const children = cta ? [...cta.children].map((el, index) => {
        const r = el.getBoundingClientRect();
        const cs = getComputedStyle(el);
        return {
          index,
          className: el.className,
          text: (el.textContent || '').trim(),
          top: Math.round(r.top),
          left: Math.round(r.left),
          right: Math.round(r.right),
          visible: cs.display !== 'none' && cs.visibility !== 'hidden' && r.width > 0 && r.height > 0
        };
      }) : [];
      const dividers = children.filter(item => /\bcontact-divider\b/.test(item.className));
      let orphanedDividerCount = 0;
      for (const divider of dividers.filter(item => item.visible)) {
        const peers = children.filter(item => item.visible && item.top === divider.top && !/\bcontact-divider\b/.test(item.className));
        if (!peers.length) { orphanedDividerCount++; continue; }
        const leftMost = Math.min(...peers.map(item => item.left));
        const rightMost = Math.max(...peers.map(item => item.right));
        if (Math.abs(divider.left - leftMost) <= 2 || Math.abs(divider.right - rightMost) <= 2) orphanedDividerCount++;
      }
      return {
        viewportWidth: window.innerWidth,
        visibleDividerCount: dividers.filter(item => item.visible).length,
        orphanedDividerCount,
        items: children
      };
    });
    writeJson(`${fileBase}-signoff.json`, sample);
    return sample;
  }

  const cases = [
    { width: 390, height: 844, from: 'light', to: 'dark' },
    { width: 390, height: 844, from: 'dark', to: 'light' },
    { width: 1280, height: 900, from: 'light', to: 'dark' },
    { width: 1280, height: 900, from: 'dark', to: 'light' }
  ];

  try {
    for (const entry of cases) {
      const ctx = await browser.newContext({ viewport: { width: entry.width, height: entry.height }, deviceScaleFactor: entry.width < 500 ? 2 : 1 });
      const page = await ctx.newPage();
      const caseName = `${entry.width}-${entry.from}-to-${entry.to}`;
      const fileBase = path.join(outDir, caseName);
      await page.addInitScript(theme => {
        try {
          localStorage.setItem('ethval-theme', theme);
          localStorage.removeItem('onboardingDismissed');
        } catch (_) {}
      }, entry.from);
      const pageErrors = [];
      page.on('pageerror', err => pageErrors.push(String(err)));
      await waitForDashboard(page);

      const before = await captureThemeState(page, `${fileBase}-before`);
      if (before.storageTheme !== entry.from) report.failures.push(`${caseName}: initial ethval-theme ${before.storageTheme || 'null'}`);

      await page.locator('#theme-toggle').click({ timeout: 10000 });
      await page.waitForFunction(expected => localStorage.getItem('ethval-theme') === expected, entry.to, { timeout: 15000 });
      await page.waitForTimeout(3000);

      const after = await captureThemeState(page, `${fileBase}-after`);
      const themeSample = { toDark: entry.to === 'dark', before, after };
      const themeFails = assertThemeTransition(themeSample);

      const shareSample = await captureShareGeometry(page, `${fileBase}-after`);
      const shareFails = assertShareGeometry(shareSample);

      const signoffSample = entry.width <= 600 ? await captureMobileSignoff(page, `${fileBase}-after`) : null;
      const signoffFails = signoffSample ? assertMobileSignoff(signoffSample) : [];

      // A deterministic test-only replacement proves screenshots never reuse a
      // detached ElementHandle when an asynchronous summary render replaces DOM.
      const stale = await page.locator('.summary-signoff').first().elementHandle();
      await page.locator('.summary-signoff').first().evaluate(el => el.replaceWith(el.cloneNode(true)));
      const staleDetached = !(await stale.evaluate(el => el.isConnected));
      await page.locator('.summary-signoff').first().screenshot({ path: `${fileBase}-rerender.png` });
      await stale.dispose();
      if (!staleDetached) report.failures.push(`${caseName}: rerender fixture did not detach original`);

      const caseResult = {
        rerenderFixture: { staleDetached, freshLocatorScreenshot: true },
        case: caseName,
        viewport: { width: entry.width, height: entry.height },
        transition: `${entry.from}->${entry.to}`,
        before,
        after,
        themeFails,
        shareFails,
        signoffFails,
        pageErrors
      };
      report.cases.push(caseResult);
      writeJson(`${fileBase}.json`, caseResult);
      if (themeFails.length || shareFails.length || signoffFails.length || pageErrors.length) {
        report.failures.push(`${caseName}: ${(themeFails.concat(shareFails, signoffFails, pageErrors.map(e => `pageerror ${e}`))).join('; ')}`);
      }
      await ctx.close();
    }
  } finally {
    await browser.close();
  }

  report.fixtures = runNegativeFixtures(outDir);
  writeJson(path.join(outDir, 'report.json'), report);
  return report;
}

async function main() {
  const args = process.argv.slice(2);
  const fixturesOnly = args[0] === '--fixtures-only';
  const outDir = fixturesOnly ? (args[1] || OUT_DEFAULT) : (args[1] || OUT_DEFAULT);
  mkdirp(outDir);

  if (fixturesOnly) {
    const report = { fixtures: runNegativeFixtures(outDir), outputDir: outDir, mode: 'fixtures-only' };
    writeJson(path.join(outDir, 'report.json'), report);
    console.log(`prevention fixtures: PASS (${outDir})`);
    return;
  }

  const url = args[0];
  if (!url) {
    console.error('usage: node .qa/prevention_check.cjs <url> [outdir]');
    console.error('   or: node .qa/prevention_check.cjs --fixtures-only [outdir]');
    process.exit(2);
  }

  const report = await runBrowser(url, outDir);
  if (report.failures.length) {
    console.log('PREVENTION FAIL');
    report.failures.forEach(line => console.log('FAIL', line));
    process.exit(1);
  }
  console.log(`PREVENTION PASS (${outDir})`);
}

main().catch(err => {
  const outDir = process.argv[3] || process.argv[2] || OUT_DEFAULT;
  try {
    mkdirp(outDir);
    writeJson(path.join(outDir, 'blocked.json'), { error: String(err), stack: err && err.stack ? String(err.stack) : null });
  } catch (_) {}
  console.error(err);
  process.exit(1);
});
