// Targeted regression: the mobile summary cards should not stack flex gap + both card margins.
// 2026-10-17 screenshot issue: community summary -> signoff gap rendered at 46px on 390.
// Keep the mobile card-to-card gap tight while preserving desktop spacing and nearby rhythm.
const { launch } = require('./browser.cjs');

const RUNS = [
  { name: '390-ko-dark', width: 390, height: 844, lang: 'ko', theme: 'dark', want: { proofMin: 18, proofMax: 24, cardMin: 16, cardMax: 22, signoffMin: 22, signoffMax: 30 } },
  { name: '390-en-light', width: 390, height: 844, lang: 'en', theme: 'light', want: { proofMin: 18, proofMax: 24, cardMin: 16, cardMax: 22, signoffMin: 22, signoffMax: 30 } },
  { name: '1280-en-dark', width: 1280, height: 900, lang: 'en', theme: 'dark', want: { cardMin: 14, cardMax: 18, signoffMin: 40, signoffMax: 52 } },
  { name: '1280-ko-light', width: 1280, height: 900, lang: 'ko', theme: 'light', want: { cardMin: 14, cardMax: 18, signoffMin: 40, signoffMax: 52 } },
];

(async () => {
  const url = process.argv[2];
  for (const theme of ['light', 'dark']) RUNS.push({ name: `430-en-${theme}`, width: 430, height: 932, lang: 'en', theme, want: { proofMin: 18, proofMax: 24, cardMin: 16, cardMax: 22, signoffMin: 22, signoffMax: 30 } });
  const browser = await launch();
  const fails = [];
  try {
    for (const run of RUNS) {
      const page = await browser.newPage({ viewport: { width: run.width, height: run.height }, deviceScaleFactor: 2 });
      await page.addInitScript(([lang, theme]) => {
        localStorage.setItem('ethval-lang', lang);
        localStorage.setItem('ethval-theme', theme);
      }, [run.lang, run.theme]);
      await page.goto(url, { waitUntil: 'load', timeout: 45000 });
      await page.waitForTimeout(10000);
      const data = await page.evaluate(() => {
        const proof = document.querySelector('[data-i18n="summary.proofOfRating"]');
        const community = document.querySelector('.summary-description .community-stats-box');
        const signoff = document.querySelector('.summary-description .summary-signoff');
        const newsTitle = document.querySelector('#news-impact-panel .metrics-subgroup-title');
        const desc = document.querySelector('.summary-description');
        if (!proof || !community || !signoff || !newsTitle || !desc) return { missing: true };
        const gap = (a, b) => Math.round(b.getBoundingClientRect().top - a.getBoundingClientRect().bottom);
        const box = (el) => {
          const r = el.getBoundingClientRect();
          return { left: Math.round(r.left), right: Math.round(r.right), width: Math.round(r.width), height: Math.round(r.height) };
        };
        return {
          missing: false,
          descGap: getComputedStyle(desc).gap,
          proofToCommunity: gap(proof, community),
          communityToSignoff: gap(community, signoff),
          signoffBottom: Math.round(signoff.getBoundingClientRect().bottom - signoff.querySelector('.summary-cta').getBoundingClientRect().bottom),
          signoffToNews: gap(signoff, newsTitle),
          docW: document.documentElement.scrollWidth,
          community: box(community),
          signoff: box(signoff),
        };
      });
      console.log(run.name, JSON.stringify(data));
      if (data.missing) fails.push(`${run.name}: missing summary nodes`);
      if (data.docW > run.width) fails.push(`${run.name}: horizontal scroll ${data.docW}`);
      if (data.community.right > run.width + 1 || data.signoff.right > run.width + 1) fails.push(`${run.name}: card exceeds viewport`);
      if (run.want.proofMin != null && (data.proofToCommunity < run.want.proofMin || data.proofToCommunity > run.want.proofMax)) {
        fails.push(`${run.name}: proof->community ${data.proofToCommunity}px want ${run.want.proofMin}-${run.want.proofMax}`);
      }
      if (data.communityToSignoff < run.want.cardMin || data.communityToSignoff > run.want.cardMax) {
        fails.push(`${run.name}: community->signoff ${data.communityToSignoff}px want ${run.want.cardMin}-${run.want.cardMax}`);
      }
      if (data.signoffToNews < run.want.signoffMin || data.signoffToNews > run.want.signoffMax) {
        fails.push(`${run.name}: signoff->news ${data.signoffToNews}px want ${run.want.signoffMin}-${run.want.signoffMax}`);
      }
      if (run.width < 769 && data.signoffBottom !== 18) fails.push(`${run.name}: bottom ${data.signoffBottom}`);
      await page.locator('.summary-description').screenshot({path: `/tmp/mobile-summary-${run.name}.png`});
      await page.close();
    }
  } finally {
    await browser.close();
  }
  if (fails.length) {
    console.log(`MOBILE-SUMMARY-GAP FAIL\n${fails.join('\n')}`);
    process.exit(1);
  }
  console.log('MOBILE-SUMMARY-GAP ALL PASS');
})();
