// 모바일 탭 줄(.nav-bar): 탭이 탭 줄 스크롤 영역 밖(왼쪽)으로 잘리지 않는지 + 넘치면 스크롤로 끝까지 닿는지.
// 인자로 URL을 주면 그 주소(qa_all/라이브), 없으면 로컬 파일을 ethval.com에 매핑. LIVE=1 이면 ethval.com 그대로.
const ARG = process.argv[2];
const { chromium } = require('playwright');
const fs = require('fs'), path = require('path');
const ROOT = path.resolve(__dirname, '..');
async function mapLocal(p) {
  await p.route('https://ethval.com/**', async (route) => {
    const u = new URL(route.request().url()); let f = decodeURIComponent(u.pathname); if (f === '/') f = '/index.html';
    const fp = path.join(ROOT, f);
    if (fs.existsSync(fp) && fs.statSync(fp).isFile()) {
      const ct = { '.html': 'text/html', '.js': 'application/javascript', '.css': 'text/css', '.svg': 'image/svg+xml', '.png': 'image/png', '.json': 'application/json' }[path.extname(fp).toLowerCase()] || 'application/octet-stream';
      return route.fulfill({ status: 200, contentType: ct, body: fs.readFileSync(fp) });
    }
    return route.continue();
  });
}
(async () => {
  const b = await chromium.launch(); let fails = 0;
  for (const w of [360, 390, 430, 768]) for (const lang of ['en', 'ko', 'ja', 'zh']) {
    const p = await b.newPage({ viewport: { width: w, height: 800 } });
    if (!process.env.LIVE && !ARG) await mapLocal(p);
    const base = ARG ? ARG.replace(/\/?$/, '/') : 'https://ethval.com/';
    await p.goto(`${base}?lang=${lang}`, { waitUntil: 'domcontentloaded' });
    await p.waitForTimeout(2500);
    const r = await p.evaluate(() => {
      const bar = document.querySelector('.nav-bar'); if (!bar || getComputedStyle(bar).display === 'none') return { hidden: true };
      bar.scrollLeft = 0;
      const br = bar.getBoundingClientRect(); const items = [...bar.querySelectorAll('.nav-item')];
      const first = items[0].getBoundingClientRect();
      bar.scrollLeft = bar.scrollWidth;
      const last = items[items.length - 1].getBoundingClientRect();
      const res = { firstLeft: Math.round(first.left - br.left), lastRightGap: Math.round(br.right - last.right), over: bar.scrollWidth > bar.clientWidth + 1, docOver: document.documentElement.scrollWidth > innerWidth };
      bar.scrollLeft = 0; return res;
    });
    const bad = !r.hidden && (r.firstLeft < 0 || r.lastRightGap < 0 || r.docOver);
    if (bad) fails++;
    console.log(bad ? 'FAIL' : 'ok  ', w, lang, JSON.stringify(r));
    await p.close();
  }
  await b.close();
  console.log(fails ? `NAVBAR_CLIP FAILS ${fails}` : 'NAVBAR_CLIP ALL PASS'); process.exit(fails ? 1 : 0);
})();
