#!/usr/bin/env node
/* ETHval — static model landing pages (SEO): /models/<id>.html (en) + /models/{ko,zh,ja}/<id>.html,
 * a hub page per language (/models/index.html, /models/<lang>/index.html), sitemap.xml and robots.txt.
 *
 * Nothing on these pages is typed in by hand. The script opens the site itself (this checkout, served
 * locally) in a headless browser and reads, for each language:
 *   - name / formula / description / caveat / references  → the 03 methodology card of that model
 *   - constants (key, value, meaning) + fixed numbers in the formula → the 03 "Constants & assumptions" table
 *   - fair value per day → state.historicalFairValues, the same daily series the site's model charts draw
 *     (computed by the page from the stored daily Supabase tables)
 *   - ETH close per day → state.priceHistory (Supabase historical_eth_price)
 * The page value is the LAST COMPLETE UTC DAY (today is skipped: its last point is the live card value),
 * printed with that date and the build time. No value is estimated or filled.
 *
 * usage: node scripts/build-model-pages.cjs            (needs the local `playwright` package + a cached
 *        chromium build under ~/Library/Caches/ms-playwright — same setup as the QA scripts)
 * Re-run whenever the models / texts change, or to refresh the dated values; then commit models/ + reports/ + sitemap.xml.
 * Also writes the monthly valuation reports (/reports/, scripts/build-reports.cjs) from the same data.
 */
const http = require('http'), fs = require('fs'), path = require('path'), os = require('os');
const ROOT = path.resolve(__dirname, '..');
// playwright is a dev-only tool: installed under .qa/node_modules (not a site/collector dependency)
const { chromium } = require(require.resolve('playwright', { paths: [path.join(ROOT, '.qa'), ROOT] }));
const SITE = 'https://ethval.com/';
const LANGS = ['en', 'ko', 'zh', 'ja'];
const HREFLANG = { en: 'en', ko: 'ko', zh: 'zh-Hans', ja: 'ja' };
const HASH = { en: '', ko: 'kr', zh: 'zh', ja: 'ja' }; // site language hash prefixes
const MODELS = ['tvlMultiple', 'stakingScarcity', 'metcalfe', 'dcf', 'l2Ecosystem', 'ps', 'revenueYield',
  'commitmentPremium', 'appCapital', 'validatorEcon', 'ethMonetary', 'ecosystemSettlement'];
const SLUG = { tvlMultiple: 'tvl-multiple', stakingScarcity: 'staking-scarcity', metcalfe: 'metcalfe', dcf: 'staking-dcf',
  l2Ecosystem: 'l2-ecosystem', ps: 'price-to-sales', revenueYield: 'fee-yield', commitmentPremium: 'liquidity-premium',
  appCapital: 'app-capital', validatorEcon: 'validator-economics', ethMonetary: 'eth-monetary', ecosystemSettlement: 'ecosystem-settlement' };
const HISTORY_DAYS = 365;

const UI = {
  en: { lang: 'English', home: 'Dashboard', models: 'Valuation models', titleSfx: 'ETH fair value model', hubTitle: '12 ETH valuation models',
        hubLead: 'Each page shows one model: its value on the last complete day, the formula, the constants it uses and its one-year history.',
        hubDesc: 'The 12 models ETHval averages into its composite ETH fair value — value, formula and constants for each.',
        fv: 'Fair value', close: 'ETH close', gap: 'Gap to price', asOf: d => `Last complete day ${d} (UTC)`,
        built: (b, d) => `Built ${b} UTC from the stored daily data. Values are for ${d}; the live value is on the dashboard.`,
        formula: 'Formula', about: 'How it works', caveat: 'Limits', consts: 'Constants', constK: 'Constant', constV: 'Value', constM: 'Meaning',
        inline: 'Fixed in formula', none: 'No constant — inputs only', hist: 'One-year history', histLead: (a, b) => `Daily fair value (purple) vs ETH close (grey), ${a} – ${b}.`,
        live: 'Open the live chart', refs: 'References', others: 'Other models', desc: (n, v, d) => `${n}: ETH fair value ${v} on ${d}. Formula, constants and one-year history.`,
        under: 'above price', over: 'below price', disclaimer: 'Model output, not investment advice.' },
  ko: { lang: '한국어', home: '대시보드', models: '밸류에이션 모델', titleSfx: 'ETH 적정가 모델', hubTitle: 'ETH 밸류에이션 모델 12개',
        hubLead: '모델마다 한 페이지씩: 마지막 완료일의 값, 공식, 사용하는 상수, 1년 이력을 보여줍니다.',
        hubDesc: 'ETHval이 종합 적정가로 평균하는 12개 모델 — 모델별 값, 공식, 상수.',
        fv: '적정가', close: 'ETH 종가', gap: '가격 대비', asOf: d => `마지막 완료일 ${d} (UTC)`,
        built: (b, d) => `${b} UTC에 저장된 일별 데이터로 생성했습니다. 값은 ${d} 기준이며, 실시간 값은 대시보드에서 볼 수 있습니다.`,
        formula: '공식', about: '작동 방식', caveat: '한계', consts: '상수', constK: '상수', constV: '값', constM: '뜻',
        inline: '공식에 고정된 수', none: '상수 없음 — 입력값만 사용', hist: '1년 이력', histLead: (a, b) => `일별 적정가(보라)와 ETH 종가(회색), ${a} – ${b}.`,
        live: '실시간 차트 열기', refs: '참고 자료', others: '다른 모델', desc: (n, v, d) => `${n}: ${d} 기준 ETH 적정가 ${v}. 공식, 상수, 1년 이력.`,
        under: '가격보다 높음', over: '가격보다 낮음', disclaimer: '모델 계산값이며 투자 조언이 아닙니다.' },
  zh: { lang: '中文', home: '仪表盘', models: '估值模型', titleSfx: 'ETH 公允价值模型', hubTitle: '12 个 ETH 估值模型',
        hubLead: '每个模型一页：最后完整一天的数值、公式、所用常数及一年历史。',
        hubDesc: 'ETHval 平均为综合公允价值的 12 个模型——各模型的数值、公式与常数。',
        fv: '公允价值', close: 'ETH 收盘价', gap: '相对价格', asOf: d => `最后完整一天 ${d} (UTC)`,
        built: (b, d) => `于 ${b} UTC 依据存储的每日数据生成。数值为 ${d}；实时数值见仪表盘。`,
        formula: '公式', about: '原理', caveat: '局限', consts: '常数', constK: '常数', constV: '数值', constM: '含义',
        inline: '公式内固定数', none: '无常数——仅用输入值', hist: '一年历史', histLead: (a, b) => `每日公允价值（紫）与 ETH 收盘价（灰），${a} – ${b}。`,
        live: '打开实时图表', refs: '参考资料', others: '其他模型', desc: (n, v, d) => `${n}：${d} 的 ETH 公允价值 ${v}。公式、常数与一年历史。`,
        under: '高于价格', over: '低于价格', disclaimer: '模型计算结果，不构成投资建议。' },
  ja: { lang: '日本語', home: 'ダッシュボード', models: 'バリュエーションモデル', titleSfx: 'ETH 適正価値モデル', hubTitle: 'ETH バリュエーションモデル 12 個',
        hubLead: 'モデルごとに 1 ページ：最後の確定日の値、式、使う定数、1 年の履歴。',
        hubDesc: 'ETHval が総合適正価値として平均する 12 モデル — 各モデルの値・式・定数。',
        fv: '適正価値', close: 'ETH 終値', gap: '価格との差', asOf: d => `最後の確定日 ${d} (UTC)`,
        built: (b, d) => `${b} UTC に保存済みの日次データから生成。値は ${d} 時点、リアルタイム値はダッシュボードにある。`,
        formula: '式', about: '仕組み', caveat: '限界', consts: '定数', constK: '定数', constV: '値', constM: '意味',
        inline: '式に固定された数', none: '定数なし — 入力値のみ', hist: '1 年の履歴', histLead: (a, b) => `日次の適正価値（紫）と ETH 終値（グレー）、${a} – ${b}。`,
        live: 'リアルタイムチャートを開く', refs: '参考資料', others: '他のモデル', desc: (n, v, d) => `${n}：${d} の ETH 適正価値 ${v}。式・定数・1 年の履歴。`,
        under: '価格より高い', over: '価格より低い', disclaimer: 'モデルの計算値であり、投資助言ではない。' }
};

const esc = s => String(s == null ? '' : s).replace(/[&<>"]/g, c => ({ '&': '&amp;', '<': '&lt;', '>': '&gt;', '"': '&quot;' }[c]));
const relPath = (lang, file) => (lang === 'en' ? 'models/' : `models/${lang}/`) + file;
const pageUrl = (lang, id) => SITE + relPath(lang, id ? SLUG[id] + '.html' : '');
const usd = v => '$' + (v >= 100 ? v.toLocaleString('en-US', { maximumFractionDigits: 0 }) : v.toLocaleString('en-US', { minimumFractionDigits: 2, maximumFractionDigits: 2 }));
const pct = v => (v >= 0 ? '+' : '') + v.toFixed(1) + '%';

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

async function extract() {
  const srv = await serve();
  const url = `http://127.0.0.1:${srv.address().port}/`;
  const exe = findChrome();
  const b = await chromium.launch(exe ? { executablePath: exe } : {});
  const p = await b.newPage({ viewport: { width: 1280, height: 900 } });
  await p.goto(url, { waitUntil: 'domcontentloaded' });
  await p.waitForFunction(() => typeof state !== 'undefined' && state.historicalDataLoaded && state.priceHistory && state.priceHistory.length > 400 &&
    document.querySelectorAll('#model-constants tr[data-model]').length >= 12, null, { timeout: 240000 });
  await p.waitForTimeout(1500);
  const data = await p.evaluate(({ MODELS, DAYS }) => {
    const ymd = d => new Date(d).toISOString().slice(0, 10);
    const today = new Date().toISOString().slice(0, 10);
    const close = new Map(); state.priceHistory.forEach(x => { if (x.value > 0) close.set(ymd(x.date), x.value); });
    const out = { today, consts: Object.assign({}, MODEL_CONSTANTS), models: {} };
    MODELS.forEach(m => {
      const H = (state.historicalFairValues[m] || []).filter(x => x.value > 0 && isFinite(x.value) && ymd(x.date) < today);
      const last = H[H.length - 1];
      const D = last ? ymd(last.date) : null;
      // 유효숫자 10자리: 같은 입력이어도 합산 순서 때문에 끝자리(1e-14)만 흔들려 차트 좌표 0.1px이
      // 뒤집히고 봇 커밋이 생기던 것 방지(2026-10-15 d06d96e 실측). 보이는 값·좌표에는 영향 없음.
      const r10 = v => Number(v.toPrecision(10));
      const series = H.slice(-DAYS).map(x => [ymd(x.date), r10(x.value), close.get(ymd(x.date)) || null]);
      out.models[m] = { date: D, value: last ? r10(last.value) : null, close: D ? close.get(D) || null : null, series };
    });
    return out;
  }, { MODELS, DAYS: HISTORY_DAYS });
  data.text = {};
  for (const lang of LANGS) {
    await p.evaluate(l => setLanguage(l, false), lang);
    await p.waitForTimeout(800);
    data.text[lang] = await p.evaluate(MODELS => {
      const o = {};
      MODELS.forEach(m => {
        const c = document.querySelector(`#ratings .method-card[data-model="${m}"]`);
        const q = s => (c && c.querySelector(s) ? c.querySelector(s).textContent.trim().replace(/\s+/g, ' ') : '');
        const row = document.querySelector(`#model-constants tr[data-model="${m}"]`);
        const consts = [], inline = [];
        if (row) row.querySelectorAll('.mc-c').forEach(cell => {
          const k = cell.querySelector('code'), v = cell.querySelector('b'), s = cell.querySelector('span');
          if (cell.classList.contains('mc-none')) return;
          if (cell.classList.contains('mc-inline')) inline.push({ nums: v ? v.textContent : '', expr: s ? s.textContent : '' });
          else consts.push({ key: k ? k.textContent : '', value: v ? v.textContent : '', meaning: s ? s.textContent : '' });
        });
        o[m] = { name: q('.method-name'), formula: q('.method-formula'), desc: q('.method-desc'), note: q('.method-note'),
          refs: c ? [...c.querySelectorAll('.method-references a')].map(a => ({ t: a.textContent.trim(), u: a.href })) : [],
          consts, inline, lang: document.documentElement.lang };
      });
      return o;
    }, MODELS);
  }
  await b.close(); srv.close();
  return data;
}

function sparkline(series) {
  const W = 720, H = 200, P = { l: 0, r: 0, t: 8, b: 8 };
  const vals = series.flatMap(s => [s[1], s[2]]).filter(v => v > 0);
  const lo = Math.min(...vals), hi = Math.max(...vals);
  const y = v => P.t + (H - P.t - P.b) * (1 - (Math.log(v) - Math.log(lo)) / ((Math.log(hi) - Math.log(lo)) || 1));
  const x = i => P.l + (W - P.l - P.r) * (series.length > 1 ? i / (series.length - 1) : 0);
  const line = k => { let d = '', pen = false; series.forEach((s, i) => { if (s[k] > 0) { d += (pen ? 'L' : 'M') + x(i).toFixed(1) + ' ' + y(s[k]).toFixed(1); pen = true; } else pen = false; }); return d; };
  const ticks = [lo, Math.sqrt(lo * hi), hi];
  // lines in a stretched SVG (fixed 200px tall); labels are HTML in a fixed 66px left gutter / 22px bottom strip,
  // so they keep their real font size at any width
  const SH = 200, top = v => (y(v) / H * SH - 8).toFixed(1) + 'px';
  return `<div class="sw"><svg class="spark" viewBox="0 0 ${W} ${H}" preserveAspectRatio="none" aria-hidden="true">` +
    ticks.map(t => `<line x1="0" x2="${W}" y1="${y(t).toFixed(1)}" y2="${y(t).toFixed(1)}" class="g" vector-effect="non-scaling-stroke"/>`).join('') +
    `<path d="${line(2)}" class="p" vector-effect="non-scaling-stroke"/><path d="${line(1)}" class="v" vector-effect="non-scaling-stroke"/></svg>` +
    ticks.map(t => `<span class="yl" style="top:${top(t)}">${esc(usd(t))}</span>`).join('') +
    `<span class="xl">${series[0][0]}</span><span class="xl r">${series[series.length - 1][0]}</span></div>`;
}

const CSS = `:root{--bg:#0b0b12;--card:#14141f;--line:#262636;--tx:#ececf3;--mu:#a3a3b8;--ac:#a78bfa;--pos:#34d399;--neg:#f87171}
@media (prefers-color-scheme:light){:root{--bg:#f7f7fb;--card:#fff;--line:#e3e3ec;--tx:#16161f;--mu:#55556a;--ac:#6d28d9;--pos:#047857;--neg:#b91c1c}}
*{box-sizing:border-box}body{margin:0;background:var(--bg);color:var(--tx);font:16px/1.6 -apple-system,BlinkMacSystemFont,"Segoe UI","Apple SD Gothic Neo","Noto Sans KR","Hiragino Sans","PingFang SC",sans-serif;word-break:keep-all;overflow-wrap:anywhere}
a{color:var(--ac)}.w{max-width:880px;margin:0 auto;padding:20px 16px 48px}
header{display:flex;flex-wrap:wrap;gap:8px 16px;align-items:center;justify-content:space-between;margin-bottom:20px}
.brand{font-weight:800;font-size:20px;text-decoration:none;color:var(--tx)}.brand b{color:var(--ac)}
.langs{display:flex;gap:6px;flex-wrap:wrap}.langs a{font-size:14px;padding:4px 10px;border:1px solid var(--line);border-radius:8px;text-decoration:none;color:var(--mu)}.langs a[aria-current]{color:var(--tx);border-color:var(--ac)}
.crumb{font-size:14px;color:var(--mu);margin:0 0 6px}.crumb a{color:var(--mu)}
h1{font-size:28px;line-height:1.25;margin:0 0 16px}h2{font-size:20px;margin:28px 0 10px}
.card{background:var(--card);border:1px solid var(--line);border-radius:14px;padding:16px 18px;margin:0 0 14px}
.kpis{display:grid;grid-template-columns:repeat(3,minmax(0,1fr));gap:12px}.kpi span{display:block;font-size:14px;color:var(--mu)}.kpi b{display:block;font-size:24px;font-variant-numeric:tabular-nums}
.pos{color:var(--pos)}.neg{color:var(--neg)}.meta{font-size:14px;color:var(--mu);margin:10px 0 0}
code,.f{font-family:ui-monospace,SFMono-Regular,Menlo,monospace;font-size:15px}.f{display:block;background:var(--bg);border:1px solid var(--line);border-radius:10px;padding:10px 12px}
table{width:100%;border-collapse:collapse;font-size:15px}th,td{text-align:left;padding:8px 6px;border-bottom:1px solid var(--line);vertical-align:top}th{color:var(--mu);font-weight:600;white-space:nowrap}td.n{font-variant-numeric:tabular-nums;white-space:nowrap}
td.k{white-space:nowrap}td.k code{font-size:13px}td.x code{font-size:13px;overflow-wrap:anywhere}
.sw{position:relative;padding:0 0 24px 70px}.spark{width:100%;height:200px;display:block}.spark .g{stroke:var(--line);stroke-width:1}.spark path{fill:none;stroke-width:2}.spark .v{stroke:var(--ac)}.spark .p{stroke:var(--mu);stroke-width:1.5;opacity:.7}
.yl,.xl{position:absolute;font-size:12px;line-height:16px;color:var(--mu);font-variant-numeric:tabular-nums;white-space:nowrap}.yl{left:0;width:64px;text-align:right}.xl{bottom:0;left:70px}.xl.r{left:auto;right:0}
.btn{display:inline-block;background:var(--ac);color:#fff;text-decoration:none;font-weight:700;padding:10px 16px;border-radius:10px;margin-top:6px}
@media (prefers-color-scheme:light){.btn{color:#fff}}
ul.ml{list-style:none;padding:0;margin:0;display:grid;grid-template-columns:repeat(auto-fill,minmax(240px,1fr));gap:8px}ul.ml a{display:block;padding:10px 12px;border:1px solid var(--line);border-radius:10px;text-decoration:none;color:var(--tx);background:var(--card)}ul.ml small{display:block;color:var(--mu);font-size:14px}
ul.refs{padding-left:20px}footer{margin-top:32px;font-size:14px;color:var(--mu)}
@media (max-width:560px){h1{font-size:24px}.kpis{grid-template-columns:1fr}.kpi b{font-size:22px}}`;

function head(lang, title, desc, canon, alts) {
  return `<!DOCTYPE html>
<html lang="${lang}">
<head>
<meta charset="UTF-8">
<meta name="viewport" content="width=device-width, initial-scale=1.0">
<title>${esc(title)}</title>
<meta name="description" content="${esc(desc)}">
<link rel="canonical" href="${canon}">
${alts}
<meta property="og:type" content="article">
<meta property="og:title" content="${esc(title)}">
<meta property="og:description" content="${esc(desc)}">
<meta property="og:url" content="${canon}">
<meta property="og:site_name" content="ETHval">
<meta name="twitter:card" content="summary">
<link rel="icon" href="data:image/svg+xml,<svg xmlns='http://www.w3.org/2000/svg' viewBox='0 0 28 28'><rect width='28' height='28' rx='8' fill='%237c3aed'/><path d='M14 5.5 L20 14 L14 22.5 L8 14 Z M8 14 L14 17.2 L20 14 M14 17.2 L14 22.5' fill='none' stroke='white' stroke-width='1.8' stroke-linejoin='round'/></svg>">
<style>${CSS}</style>
</head>`;
}
const altLinks = id => LANGS.map(l => `<link rel="alternate" hreflang="${HREFLANG[l]}" href="${pageUrl(l, id)}">`).join('\n') +
  `\n<link rel="alternate" hreflang="x-default" href="${pageUrl('en', id)}">`;
const depth = lang => (lang === 'en' ? '../' : '../../');
const langBar = (lang, id) => '<nav class="langs" aria-label="Language">' + LANGS.map(l =>
  `<a href="${pageUrl(l, id)}" hreflang="${HREFLANG[l]}" lang="${l}"${l === lang ? ' aria-current="page"' : ''}>${UI[l].lang}</a>`).join('') + '</nav>';
const homeHref = (lang, sec) => depth(lang) + (HASH[lang] || sec ? '#' + [HASH[lang], sec].filter(Boolean).join('/') : '');

function modelPage(data, lang, id, built) {
  const U = UI[lang], T = data.text[lang][id], M = data.models[id];
  const gap = M.close ? (M.value / M.close - 1) * 100 : null;
  const title = `${T.name} — ${U.titleSfx} | ETHval`;
  const desc = U.desc(T.name, usd(M.value), M.date);
  const series = M.series;
  const ctab = T.consts.length || T.inline.length
    ? `<table><thead><tr><th>${U.constK}</th><th>${U.constV}</th><th>${U.constM}</th></tr></thead><tbody>` +
      T.consts.map(c => `<tr><td class="k"><code>${esc(c.key)}</code></td><td class="n">${esc(c.value)}</td><td>${esc(c.meaning)}</td></tr>`).join('') +
      T.inline.map(c => `<tr><td>${esc(U.inline)}</td><td class="n">${esc(c.nums)}</td><td class="x"><code>${esc(c.expr)}</code></td></tr>`).join('') + '</tbody></table>'
    : `<p>${esc(U.none)}</p>`;
  const json = { model: id, date: M.date, value: M.value, close: M.close, built, series };
  const others = MODELS.filter(m => m !== id).map(m => `<li><a href="${SLUG[m]}.html">${esc(data.text[lang][m].name)}<small>${esc(usd(data.models[m].value))} · ${data.models[m].date}</small></a></li>`).join('');
  return head(lang, title, desc, pageUrl(lang, id), altLinks(id)) + `
<body>
<div class="w">
<header><a class="brand" href="${homeHref(lang)}">ETH<b>val</b></a>${langBar(lang, id)}</header>
<p class="crumb"><a href="${homeHref(lang)}">${U.home}</a> › <a href="index.html">${U.models}</a></p>
<h1>${esc(T.name)} <small style="font-weight:500;color:var(--mu);font-size:.6em;display:block">${U.titleSfx}</small></h1>
<section class="card" id="value" data-model="${id}" data-date="${M.date}" data-value="${M.value}" data-close="${M.close || ''}">
<div class="kpis">
<div class="kpi"><span>${U.fv}</span><b>${esc(usd(M.value))}</b></div>
<div class="kpi"><span>${U.close}</span><b>${M.close ? esc(usd(M.close)) : '—'}</b></div>
<div class="kpi"><span>${U.gap}</span><b class="${gap == null ? '' : gap >= 0 ? 'pos' : 'neg'}">${gap == null ? '—' : pct(gap)}</b></div>
</div>
<p class="meta">${esc(U.asOf(M.date))} · ${esc(U.built(built, M.date))}</p>
<a class="btn" href="${homeHref(lang, 'ratings')}">${U.live} →</a>
</section>
<h2>${U.formula}</h2>
<code class="f">${esc(T.formula)}</code>
<h2>${U.about}</h2>
<p>${esc(T.desc)}</p>
${T.note ? `<p><b>${U.caveat}:</b> ${esc(T.note)}</p>` : ''}
<h2>${U.consts}</h2>
<div class="card">${ctab}</div>
<h2>${U.hist}</h2>
<div class="card">${series.length > 1 ? sparkline(series) : ''}<p class="meta">${esc(U.histLead(series[0][0], series[series.length - 1][0]))}</p></div>
${T.refs.length ? `<h2>${U.refs}</h2><ul class="refs">${T.refs.map(r => `<li><a href="${esc(r.u)}" rel="noopener" target="_blank">${esc(r.t)}</a></li>`).join('')}</ul>` : ''}
<h2>${U.others}</h2>
<ul class="ml">${others}</ul>
<footer>${esc(U.disclaimer)} · <a href="${homeHref(lang)}">ethval.com</a> · by Hashed</footer>
</div>
<script type="application/json" id="model-data">${JSON.stringify(json).replace(/</g, '\\u003c')}</script>
</body>
</html>
`;
}

function hubPage(data, lang, built) {
  const U = UI[lang];
  const items = MODELS.map(m => `<li><a href="${SLUG[m]}.html" data-model="${m}">${esc(data.text[lang][m].name)}<small>${esc(U.fv)} ${esc(usd(data.models[m].value))} · ${data.models[m].date}</small></a></li>`).join('');
  return head(lang, `${U.hubTitle} | ETHval`, U.hubDesc, pageUrl(lang, null), altLinks(null)) + `
<body>
<div class="w">
<header><a class="brand" href="${homeHref(lang)}">ETH<b>val</b></a>${langBar(lang, null)}</header>
<p class="crumb"><a href="${homeHref(lang)}">${U.home}</a></p>
<h1>${U.hubTitle}</h1>
<p>${esc(U.hubLead)}</p>
<ul class="ml">${items}</ul>
<p class="meta">${esc(U.built(built, data.models[MODELS[0]].date))}</p>
<a class="btn" href="${homeHref(lang, 'ratings')}">${U.live} →</a>
<footer>${esc(U.disclaimer)} · <a href="${homeHref(lang)}">ethval.com</a> · by Hashed</footer>
</div>
</body>
</html>
`;
}

function sitemap(builtDay, reports) {
  const urls = [{ loc: SITE, alts: null }].concat([null].concat(MODELS).flatMap(id => LANGS.map(l => ({ loc: pageUrl(l, id), alts: id === undefined ? null : id }))));
  const body = urls.map(u => {
    if (u.alts === null && u.loc === SITE) return `  <url><loc>${SITE}</loc><lastmod>${builtDay}</lastmod></url>`;
    const id = u.alts;
    const alts = LANGS.map(l => `    <xhtml:link rel="alternate" hreflang="${HREFLANG[l]}" href="${pageUrl(l, id)}"/>`).join('\n') +
      `\n    <xhtml:link rel="alternate" hreflang="x-default" href="${pageUrl('en', id)}"/>`;
    return `  <url><loc>${u.loc}</loc><lastmod>${builtDay}</lastmod>\n${alts}\n  </url>`;
  }).join('\n') + (reports ? '\n' + reports.urls.flatMap(u => u.locs.map(loc =>
    `  <url><loc>${loc}</loc><lastmod>${builtDay}</lastmod>\n` + u.alts.map(([h, a]) => `    <xhtml:link rel="alternate" hreflang="${h}" href="${a}"/>`).join('\n') +
    `\n    <xhtml:link rel="alternate" hreflang="x-default" href="${u.xdefault}"/>\n  </url>`)).join('\n') : '');
  return `<?xml version="1.0" encoding="UTF-8"?>\n<urlset xmlns="http://www.sitemaps.org/schemas/sitemap/0.9" xmlns:xhtml="http://www.w3.org/1999/xhtml">\n${body}\n</urlset>\n`;
}

(async () => {
  const data = await extract();
  const bad = [];
  MODELS.forEach(m => {
    const M = data.models[m];
    if (!(M.value > 0) || !M.date || M.series.length < 2) bad.push(`${m}: no stored value`);
    LANGS.forEach(l => { const T = data.text[l][m]; if (!T.name || !T.formula || !T.desc) bad.push(`${l}/${m}: missing text`); if (T.lang !== l) bad.push(`${l}/${m}: page lang ${T.lang}`); });
  });
  if (bad.length) { console.error('build aborted:\n' + bad.join('\n')); process.exit(1); }
  const now = new Date(), built = now.toISOString().slice(0, 16).replace('T', ' ');
  for (const lang of LANGS) {
    const dir = path.join(ROOT, lang === 'en' ? 'models' : `models/${lang}`);
    fs.mkdirSync(dir, { recursive: true });
    for (const id of MODELS) fs.writeFileSync(path.join(dir, SLUG[id] + '.html'), modelPage(data, lang, id, built));
    fs.writeFileSync(path.join(dir, 'index.html'), hubPage(data, lang, built));
  }
  // monthly valuation reports (/reports/) from the same extracted data — scripts/build-reports.cjs
  const reports = require('./build-reports.cjs').build(data, { ROOT, MODELS, SLUG, SITE, HREFLANG, HASH, built, usd, esc, sparkline, CSS });
  fs.writeFileSync(path.join(ROOT, 'sitemap.xml'), sitemap(now.toISOString().slice(0, 10), reports));
  fs.writeFileSync(path.join(ROOT, 'robots.txt'), `User-agent: *\nAllow: /\n\nSitemap: ${SITE}sitemap.xml\n`);
  const dates = [...new Set(MODELS.map(m => data.models[m].date))];
  console.log(`built ${LANGS.length * (MODELS.length + 1)} pages, values for ${dates.join(', ')}, built ${built} UTC`);
  console.log(`reports: ${reports.months.length} months (${reports.months[0]} – ${reports.latest}) x ${LANGS.length} langs + hubs`);
})().catch(e => { console.error(e); process.exit(1); });
