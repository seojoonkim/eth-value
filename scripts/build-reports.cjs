/* ETHval — monthly valuation report pages: /reports/YYYY-MM.html (en) + /reports/{ko,zh,ja}/YYYY-MM.html,
 * a hub per language (/reports/index.html, /reports/<lang>/index.html) and /reports/index.json (latest month,
 * read by the 06 research section link on the dashboard).
 *
 * Called by scripts/build-model-pages.cjs with the data it already read from the site in a headless browser
 * (nothing typed in by hand):
 *   data.models[m].series = [[YYYY-MM-DD, fair value, ETH close], ...] — the stored daily model values
 *   (state.historicalFairValues) for the last 365 COMPLETE UTC days, and the Supabase daily close.
 *   data.text[lang][m].name = model name in each language (03 methodology card).
 * Per day: composite = simple average of the models that have a value that day (the same rule as the 01.3 fair
 * value band chart); gap = close / composite − 1; zone = band of fv-band.js (edges −50/−25/0/+25/+50/+100 %).
 * Month change = month-end day vs the last day of the previous month (both must be in the stored window, so a
 * month is only built when its baseline day exists). The current month is "month to date" through the last
 * complete day. Days without a value are left out; nothing is estimated, filled or carried forward.
 */
'use strict';
const fs = require('fs'), path = require('path');

const LANGS = ['en', 'ko', 'zh', 'ja'];
const EDGES = [-50, -25, 0, 25, 50, 100];
const ZONE_COLORS = ['#15803d', '#22c55e', '#86efac', '#fcd34d', '#fb923c', '#ef4444', '#b91c1c'];
const MONTHS = {
  en: ['January', 'February', 'March', 'April', 'May', 'June', 'July', 'August', 'September', 'October', 'November', 'December'],
};
const monthName = (lang, ym) => {
  const [y, m] = ym.split('-').map(Number);
  if (lang === 'en') return `${MONTHS.en[m - 1]} ${y}`;
  if (lang === 'ko') return `${y}년 ${m}월`;
  return `${y}年${m}月`; // zh, ja
};

const R = {
  en: { lang: 'English', home: 'Dashboard', hub: 'Monthly valuation reports', title: ym => `ETH valuation report — ${monthName('en', ym)}`,
        hubLead: 'One page per month: where the composite fair value ended, what moved it, and where the price sat in the fair value bands. Built automatically from the stored daily data.',
        hubDesc: 'Monthly ETH valuation reports from ETHval: composite fair value, model drivers and fair value band position per month.',
        desc: (ym, fv, d) => `ETH valuation report for ${monthName('en', ym)}: composite fair value ${fv} on ${d}, the models that moved it and the band position.`,
        mtd: d => `Month to date, through ${d} (last complete UTC day)`, full: (a, b) => `Full month, ${a} – ${b} (UTC)`,
        snap: 'Snapshot', fv: 'Composite fair value', close: 'ETH close', gap: 'Price vs fair value', zone: 'Band zone',
        change: 'Change over the month', from: (d) => `vs ${d} (previous month end)`, item: 'Item', start: 'Start', end: 'End', chg: 'Change',
        drivers: 'What moved the fair value', driversLead: (n) => `Each model's change over the month and its share of the composite change (model change ÷ ${n}, because the composite is a simple average of ${n} models).`,
        model: 'Model', contrib: 'Effect on composite', top: (a) => `Largest effects: ${a}.`, notAll: 'Not every model had a value on both days, so effects are not split.',
        bands: 'Band position', bandsLead: 'Days the ETH close spent in each fair value band this month (close vs that day\'s composite).',
        days: n => `${n} d`, zoneNow: z => `Ended in the ${z} band.`, gapRange: (a, b) => `Gap range in the month: ${a} to ${b}.`,
        chart: 'Daily composite fair value vs ETH close', chartLead: (a, b) => `Composite fair value (purple) and ETH close (grey), ${a} – ${b}.`,
        method: 'How this page is made',
        methodText: n => `Composite = simple average of the models that have a stored value that day (${n} models; the same rule as the 01.3 fair value band chart). Band edges = composite × 0.5 / 0.75 / 1.0 / 1.25 / 1.5 / 2.0. Past values are recomputed from the stored daily data with the current model formulas and constants, so they can differ from what the live page showed on that day. Days without data are left out, never filled.`,
        built: b => `Built ${b} UTC.`, prev: 'Previous month', next: 'Next month', all: 'All months', live: 'Open the live dashboard',
        below: 'below −50%', above: 'above +100%', disclaimer: 'Model output, not investment advice.', months: 'Month', endDay: 'End day' },
  ko: { lang: '한국어', home: '대시보드', hub: '월간 밸류에이션 리포트', title: ym => `ETH 밸류에이션 리포트 — ${monthName('ko', ym)}`,
        hubLead: '한 달에 한 페이지: 합성 적정가가 어디서 끝났는지, 무엇이 움직였는지, 시장가가 적정가 밴드 어디에 있었는지를 보여줍니다. 저장된 일별 데이터로 자동 생성합니다.',
        hubDesc: 'ETHval 월간 ETH 밸류에이션 리포트 — 달마다 합성 적정가, 모델별 변동 요인, 적정가 밴드 위치.',
        desc: (ym, fv, d) => `${monthName('ko', ym)} ETH 밸류에이션 리포트: ${d} 기준 합성 적정가 ${fv}, 움직인 모델, 밴드 위치.`,
        mtd: d => `이번 달 누적, ${d}까지 (마지막 완료일, UTC)`, full: (a, b) => `한 달 전체, ${a} – ${b} (UTC)`,
        snap: '스냅샷', fv: '합성 적정가', close: 'ETH 종가', gap: '적정가 대비 시장가', zone: '밴드 구간',
        change: '한 달 변화', from: (d) => `${d}(전월 말) 대비`, item: '항목', start: '시작', end: '끝', chg: '변화',
        drivers: '적정가를 움직인 요인', driversLead: (n) => `모델별 한 달 변화와 합성 적정가 변화에 준 영향(모델 변화 ÷ ${n}, 합성 적정가는 ${n}개 모델의 단순 평균이라서).`,
        model: '모델', contrib: '합성 적정가 영향', top: (a) => `영향이 큰 모델: ${a}.`, notAll: '두 날짜 모두 값이 있는 모델이 전부가 아니라 영향을 나누지 않았습니다.',
        bands: '밴드 위치', bandsLead: '이번 달 ETH 종가가 각 적정가 밴드에 머문 날 수(그날 합성 적정가 대비).',
        days: n => `${n}일`, zoneNow: z => `월말 기준 ${z} 구간.`, gapRange: (a, b) => `이번 달 괴리율 범위: ${a} ~ ${b}.`,
        chart: '일별 합성 적정가와 ETH 종가', chartLead: (a, b) => `합성 적정가(보라)와 ETH 종가(회색), ${a} – ${b}.`,
        method: '이 페이지를 만드는 방법',
        methodText: n => `합성 적정가 = 그날 저장된 값이 있는 모델의 단순 평균(${n}개 모델, 01.3 적정가 밴드 차트와 같은 규칙). 밴드 경계 = 합성 적정가 × 0.5 / 0.75 / 1.0 / 1.25 / 1.5 / 2.0. 과거 값은 저장된 일별 데이터를 지금의 모델 공식·상수로 다시 계산한 값이라, 그날 실시간 화면에 보였던 값과 다를 수 있습니다. 데이터가 없는 날은 빼고 채우지 않습니다.`,
        built: b => `${b} UTC 생성.`, prev: '이전 달', next: '다음 달', all: '전체 달', live: '실시간 대시보드 열기',
        below: '−50% 아래', above: '+100% 위', disclaimer: '모델 계산값이며 투자 조언이 아닙니다.', months: '월', endDay: '기준일' },
  zh: { lang: '中文', home: '仪表盘', hub: '月度估值报告', title: ym => `ETH 估值报告 — ${monthName('zh', ym)}`,
        hubLead: '每月一页：综合公允价值收于何处、由哪些模型推动、市价处于公允价值区间带的哪个位置。依据存储的每日数据自动生成。',
        hubDesc: 'ETHval 每月 ETH 估值报告——综合公允价值、模型变动因素与公允价值区间带位置。',
        desc: (ym, fv, d) => `${monthName('zh', ym)} ETH 估值报告：${d} 综合公允价值 ${fv}、推动变化的模型与区间位置。`,
        mtd: d => `本月至今，截至 ${d}（最后完整一天，UTC）`, full: (a, b) => `整月，${a} – ${b}（UTC）`,
        snap: '快照', fv: '综合公允价值', close: 'ETH 收盘价', gap: '市价相对公允价值', zone: '区间',
        change: '本月变化', from: (d) => `相对 ${d}（上月末）`, item: '项目', start: '月初', end: '月末', chg: '变化',
        drivers: '推动公允价值的因素', driversLead: (n) => `各模型本月变化及其对综合公允价值变化的影响（模型变化 ÷ ${n}，因为综合值是 ${n} 个模型的简单平均）。`,
        model: '模型', contrib: '对综合值的影响', top: (a) => `影响最大：${a}。`, notAll: '并非所有模型在两天都有数值，因此未拆分影响。',
        bands: '区间位置', bandsLead: '本月 ETH 收盘价位于各公允价值区间的天数（相对当日综合公允价值）。',
        days: n => `${n} 天`, zoneNow: z => `月末位于 ${z} 区间。`, gapRange: (a, b) => `本月偏离率范围：${a} 至 ${b}。`,
        chart: '每日综合公允价值与 ETH 收盘价', chartLead: (a, b) => `综合公允价值（紫）与 ETH 收盘价（灰），${a} – ${b}。`,
        method: '本页的生成方法',
        methodText: n => `综合公允价值 = 当日有存储数值的模型的简单平均（${n} 个模型，与 01.3 公允价值区间带图规则相同）。区间边界 = 综合值 × 0.5 / 0.75 / 1.0 / 1.25 / 1.5 / 2.0。过去的数值是用当前模型公式与常数对存储的每日数据重新计算的，可能与当日实时页面显示的数值不同。无数据的日期不计入，不做填补。`,
        built: b => `于 ${b} UTC 生成。`, prev: '上个月', next: '下个月', all: '全部月份', live: '打开实时仪表盘',
        below: '低于 −50%', above: '高于 +100%', disclaimer: '模型计算结果，不构成投资建议。', months: '月份', endDay: '基准日' },
  ja: { lang: '日本語', home: 'ダッシュボード', hub: '月次バリュエーションレポート', title: ym => `ETH バリュエーションレポート — ${monthName('ja', ym)}`,
        hubLead: '1 か月 1 ページ：合成適正価値がどこで終わったか、何が動かしたか、市場価格が適正価値バンドのどこにあったか。保存済みの日次データから自動生成。',
        hubDesc: 'ETHval の月次 ETH バリュエーションレポート — 月ごとの合成適正価値・モデル別の変動要因・適正価値バンドの位置。',
        desc: (ym, fv, d) => `${monthName('ja', ym)}の ETH バリュエーションレポート：${d} 時点の合成適正価値 ${fv}、動かしたモデル、バンド位置。`,
        mtd: d => `月初来、${d} まで（最後の確定日、UTC）`, full: (a, b) => `1 か月全体、${a} – ${b}（UTC）`,
        snap: 'スナップショット', fv: '合成適正価値', close: 'ETH 終値', gap: '適正価値比の市場価格', zone: 'バンドゾーン',
        change: '月間の変化', from: (d) => `${d}（前月末）比`, item: '項目', start: '月初', end: '月末', chg: '変化',
        drivers: '適正価値を動かした要因', driversLead: (n) => `モデルごとの月間変化と合成適正価値の変化への影響（モデルの変化 ÷ ${n}。合成値は ${n} モデルの単純平均のため）。`,
        model: 'モデル', contrib: '合成値への影響', top: (a) => `影響が大きいモデル：${a}。`, notAll: '両日とも値があるモデルが全部ではないため、影響は分けていない。',
        bands: 'バンド位置', bandsLead: '今月 ETH 終値が各適正価値バンドにあった日数（その日の合成適正価値比）。',
        days: n => `${n}日`, zoneNow: z => `月末時点で ${z} のゾーン。`, gapRange: (a, b) => `今月の乖離率の範囲：${a} 〜 ${b}。`,
        chart: '日次の合成適正価値と ETH 終値', chartLead: (a, b) => `合成適正価値（紫）と ETH 終値（グレー）、${a} – ${b}。`,
        method: 'このページの作り方',
        methodText: n => `合成適正価値 = その日に保存値があるモデルの単純平均（${n} モデル、01.3 適正価値バンドチャートと同じ規則）。バンド境界 = 合成値 × 0.5 / 0.75 / 1.0 / 1.25 / 1.5 / 2.0。過去の値は保存済みの日次データを現在のモデル式・定数で再計算した値で、当日のリアルタイム画面の値とは異なる場合がある。データのない日は除外し、補完しない。`,
        built: b => `${b} UTC に生成。`, prev: '前月', next: '翌月', all: 'すべての月', live: 'リアルタイムのダッシュボードを開く',
        below: '−50% 未満', above: '+100% 超', disclaimer: 'モデルの計算値であり、投資助言ではない。', months: '月', endDay: '基準日' }
};

const sgn = v => (v > 0 ? '+' : v < 0 ? '−' : '');
const pctS = v => sgn(v) + Math.abs(v).toFixed(1) + '%';
const usdS = (usd, v) => sgn(v) + usd(Math.abs(v));
function zoneOf(dev) { for (let i = 0; i < EDGES.length; i++) if (dev < EDGES[i]) return i; return EDGES.length; }
const ep = v => (v > 0 ? '+' : v < 0 ? '−' : '') + Math.abs(v) + '%';
function zoneName(i, U) { if (i === 0) return U.below; if (i === EDGES.length) return U.above; return ep(EDGES[i - 1]) + ' ~ ' + ep(EDGES[i]); }

// daily composite from the stored per-model series
function daily(data, MODELS) {
  const per = {}, close = new Map();
  MODELS.forEach(m => {
    per[m] = new Map();
    data.models[m].series.forEach(([d, v, c]) => { if (v > 0) per[m].set(d, v); if (c > 0) close.set(d, c); });
  });
  const days = [...new Set(MODELS.flatMap(m => [...per[m].keys()]))].sort();
  const out = new Map();
  days.forEach(d => {
    let s = 0, n = 0; MODELS.forEach(m => { const v = per[m].get(d); if (v) { s += v; n++; } });
    if (!n) return;
    const fv = s / n, c = close.get(d) || null;
    const o = { date: d, fv, n, close: c };
    if (c) { o.gap = (c / fv - 1) * 100; o.zone = zoneOf(o.gap); }
    out.set(d, o);
  });
  return { per, days: out };
}

function monthsToBuild(days) {
  const keys = [...days.keys()];
  const yms = [...new Set(keys.map(d => d.slice(0, 7)))];
  const res = [];
  yms.forEach(ym => {
    const [y, m] = ym.split('-').map(Number);
    const prevEnd = new Date(Date.UTC(y, m - 1, 0)).toISOString().slice(0, 10); // last day of previous month
    if (!days.has(prevEnd)) return; // baseline must be stored — otherwise the month is only partly in the window
    const inMonth = keys.filter(d => d.startsWith(ym));
    if (!inMonth.length) return;
    const monthEnd = new Date(Date.UTC(y, m, 0)).toISOString().slice(0, 10);
    res.push({ ym, base: prevEnd, start: inMonth[0], end: inMonth[inMonth.length - 1], monthEnd, days: inMonth });
  });
  return res;
}

function build(data, ctx) {
  const { ROOT, MODELS, SITE, HREFLANG, HASH, built, usd, esc, sparkline, CSS } = ctx;
  const { per, days } = daily(data, MODELS);
  const months = monthsToBuild(days);
  if (!months.length) throw new Error('reports: no month with a stored baseline day');
  const latest = months[months.length - 1].ym;
  const rel = (lang, file) => (lang === 'en' ? 'reports/' : `reports/${lang}/`) + file;
  const url = (lang, ym) => SITE + rel(lang, ym ? ym + '.html' : '');
  const depth = lang => (lang === 'en' ? '../' : '../../');
  const homeHref = (lang, sec) => depth(lang) + (HASH[lang] || sec ? '#' + [HASH[lang], sec].filter(Boolean).join('/') : '');
  const alts = ym => LANGS.map(l => `<link rel="alternate" hreflang="${HREFLANG[l]}" href="${url(l, ym)}">`).join('\n') +
    `\n<link rel="alternate" hreflang="x-default" href="${url('en', ym)}">`;
  const langBar = (lang, ym) => '<nav class="langs" aria-label="Language">' + LANGS.map(l =>
    `<a href="${url(l, ym)}" hreflang="${HREFLANG[l]}" lang="${l}"${l === lang ? ' aria-current="page"' : ''}>${R[l].lang}</a>`).join('') + '</nav>';
  const head = (lang, title, desc, canon, alt) => `<!DOCTYPE html>
<html lang="${lang}">
<head>
<meta charset="UTF-8">
<meta name="viewport" content="width=device-width, initial-scale=1.0">
<title>${esc(title)}</title>
<meta name="description" content="${esc(desc)}">
<link rel="canonical" href="${canon}">
${alt}
<meta property="og:type" content="article">
<meta property="og:title" content="${esc(title)}">
<meta property="og:description" content="${esc(desc)}">
<meta property="og:url" content="${canon}">
<meta property="og:site_name" content="ETHval">
<meta name="twitter:card" content="summary">
<link rel="icon" href="data:image/svg+xml,<svg xmlns='http://www.w3.org/2000/svg' viewBox='0 0 28 28'><rect width='28' height='28' rx='8' fill='%237c3aed'/><path d='M14 5.5 L20 14 L14 22.5 L8 14 Z M8 14 L14 17.2 L20 14 M14 17.2 L14 22.5' fill='none' stroke='white' stroke-width='1.8' stroke-linejoin='round'/></svg>">
<style>${CSS}${EXTRA_CSS}</style>
</head>`;

  const pages = [];
  const summary = [];
  months.forEach((M, i) => {
    const b = days.get(M.base), e = days.get(M.end);
    const allBoth = b.n === MODELS.length && e.n === MODELS.length;
    const drivers = MODELS.map(m => {
      const v0 = per[m].get(M.base), v1 = per[m].get(M.end);
      return { m, v0: v0 || null, v1: v1 || null, d: v0 && v1 ? v1 - v0 : null, pct: v0 && v1 ? (v1 / v0 - 1) * 100 : null,
               eff: allBoth && v0 && v1 ? (v1 - v0) / MODELS.length : null };
    }).sort((x, y) => Math.abs(y.eff == null ? (y.d || 0) / MODELS.length : y.eff) - Math.abs(x.eff == null ? (x.d || 0) / MODELS.length : x.eff));
    const zc = new Array(EDGES.length + 1).fill(0); let gmin = Infinity, gmax = -Infinity, nz = 0;
    M.days.forEach(d => { const o = days.get(d); if (o.zone == null) return; zc[o.zone]++; nz++; gmin = Math.min(gmin, o.gap); gmax = Math.max(gmax, o.gap); });
    const series = [M.base].concat(M.days).map(d => { const o = days.get(d); return [d, o.fv, o.close]; });
    const isMtd = M.end < M.monthEnd;
    const json = { month: M.ym, base: M.base, end: M.end, monthToDate: isMtd, built,
      composite: { base: b.fv, end: e.fv, nBase: b.n, nEnd: e.n }, close: { base: b.close, end: e.close },
      gap: e.gap == null ? null : e.gap, zone: e.zone == null ? null : e.zone, zoneDays: zc,
      models: Object.fromEntries(drivers.map(x => [x.m, [x.v0, x.v1]])), series };
    summary.push({ ym: M.ym, end: M.end, fv: e.fv, close: e.close, gap: e.gap, zone: e.zone, mtd: isMtd });
    const prev = months[i - 1], next = months[i + 1];
    LANGS.forEach(lang => {
      const U = R[lang], N = m => data.text[lang][m].name;
      const title = U.title(M.ym);
      const rows = [
        [U.fv, usd(b.fv), usd(e.fv), pctS((e.fv / b.fv - 1) * 100)],
        [U.close, b.close ? usd(b.close) : '—', e.close ? usd(e.close) : '—', b.close && e.close ? pctS((e.close / b.close - 1) * 100) : '—'],
        [U.gap, b.gap == null ? '—' : pctS(b.gap), e.gap == null ? '—' : pctS(e.gap), b.gap != null && e.gap != null ? sgn(e.gap - b.gap) + Math.abs(e.gap - b.gap).toFixed(1) + ' pp' : '—']
      ];
      const top = allBoth ? drivers.slice(0, 3).map(x => `${N(x.m)} ${usdS(usd, x.eff)}`).join(', ') : '';
      const maxZ = Math.max(1, ...zc);
      const body = `
<body>
<div class="w">
<header><a class="brand" href="${homeHref(lang)}">ETH<b>val</b></a>${langBar(lang, M.ym)}</header>
<p class="crumb"><a href="${homeHref(lang)}">${U.home}</a> › <a href="index.html">${U.hub}</a></p>
<h1>${esc(title)}</h1>
<p class="meta" style="margin-top:-6px">${esc(isMtd ? U.mtd(M.end) : U.full(M.start, M.end))}</p>
<section class="card" id="snapshot" data-month="${M.ym}" data-end="${M.end}" data-fv="${e.fv}" data-close="${e.close || ''}" data-zone="${e.zone == null ? '' : e.zone}">
<div class="kpis k4">
<div class="kpi"><span>${U.fv}</span><b>${esc(usd(e.fv))}</b></div>
<div class="kpi"><span>${U.close}</span><b>${e.close ? esc(usd(e.close)) : '—'}</b></div>
<div class="kpi"><span>${U.gap}</span><b class="${e.gap == null ? '' : e.gap >= 0 ? 'neg' : 'pos'}">${e.gap == null ? '—' : pctS(e.gap)}</b></div>
<div class="kpi"><span>${U.zone}</span><b class="zn">${e.zone == null ? '—' : `<i style="background:${ZONE_COLORS[e.zone]}"></i>${esc(zoneName(e.zone, U))}`}</b></div>
</div>
</section>
<h2>${U.change}</h2>
<div class="card"><p class="meta" style="margin:0 0 6px">${esc(U.from(M.base))}</p>
<table class="ct"><thead><tr><th>${U.item}</th><th class="r">${U.start}<small>${M.base}</small></th><th class="r">${U.end}<small>${M.end}</small></th><th class="r">${U.chg}</th></tr></thead><tbody>
${rows.map(r => `<tr><td>${esc(r[0])}</td><td class="n r">${esc(r[1])}</td><td class="n r">${esc(r[2])}</td><td class="n r">${esc(r[3])}</td></tr>`).join('\n')}
</tbody></table></div>
<h2>${U.drivers}</h2>
<div class="card" id="drivers"><p class="meta" style="margin:0 0 6px">${esc(U.driversLead(MODELS.length))}</p>
<table class="dt"><thead><tr><th>${U.model}</th><th class="r">${U.start}</th><th class="r">${U.end}</th><th class="r">${U.chg}</th><th class="r">${U.contrib}</th></tr></thead><tbody>
${drivers.map(x => `<tr data-model="${x.m}"><td><a href="${depth(lang)}${lang === 'en' ? 'models/' : `models/${lang}/`}${ctx.SLUG[x.m]}.html">${esc(N(x.m))}</a></td><td class="n r">${x.v0 ? esc(usd(x.v0)) : '—'}</td><td class="n r">${x.v1 ? esc(usd(x.v1)) : '—'}</td><td class="n r">${x.pct == null ? '—' : pctS(x.pct)}</td><td class="n r ${x.eff == null ? '' : x.eff >= 0 ? 'pos' : 'neg'}">${x.eff == null ? '—' : esc(usdS(usd, x.eff))}</td></tr>`).join('\n')}
</tbody></table>
<p class="meta">${esc(allBoth ? U.top(top) : U.notAll)}</p></div>
<h2>${U.bands}</h2>
<div class="card" id="bands"><p class="meta" style="margin:0 0 10px">${esc(U.bandsLead)}</p>
<ul class="zb">${zc.map((c, z) => `<li data-zone="${z}"${e.zone === z ? ' class="cur"' : ''}><span class="zl"><i style="background:${ZONE_COLORS[z]}"></i>${esc(zoneName(z, U))}</span><span class="zt"><span class="zf" style="width:${(c / maxZ * 100).toFixed(1)}%;background:${ZONE_COLORS[z]}"></span></span><b>${U.days(c)}</b></li>`).reverse().join('')}</ul>
<p class="meta">${esc((e.zone == null ? '' : U.zoneNow(zoneName(e.zone, U)) + ' ') + (nz ? U.gapRange(pctS(gmin), pctS(gmax)) : ''))}</p></div>
<h2>${U.chart}</h2>
<div class="card">${series.length > 1 ? sparkline(series) : ''}<p class="meta">${esc(U.chartLead(series[0][0], series[series.length - 1][0]))}</p></div>
<h2>${U.method}</h2>
<p class="note">${esc(U.methodText(MODELS.length))} ${esc(U.built(built))}</p>
<nav class="pn">${prev ? `<a href="${prev.ym}.html">← ${U.prev} · ${esc(monthName(lang, prev.ym))}</a>` : '<span></span>'}<a href="index.html">${U.all}</a>${next ? `<a href="${next.ym}.html">${U.next} · ${esc(monthName(lang, next.ym))} →</a>` : '<span></span>'}</nav>
<a class="btn" href="${homeHref(lang)}">${U.live} →</a>
<footer>${esc(U.disclaimer)} · <a href="${homeHref(lang)}">ethval.com</a> · by Hashed</footer>
</div>
<script type="application/json" id="report-data">${JSON.stringify(json).replace(/</g, '\\u003c')}</script>
</body>
</html>
`;
      pages.push({ file: rel(lang, M.ym + '.html'), html: head(lang, title + ' | ETHval', U.desc(M.ym, usd(e.fv), M.end), url(lang, M.ym), alts(M.ym)) + body });
    });
  });
  LANGS.forEach(lang => {
    const U = R[lang];
    const items = summary.slice().reverse().map(s => `<tr data-month="${s.ym}"><td><a href="${s.ym}.html">${esc(monthName(lang, s.ym))}</a>${s.mtd ? ' <small>MTD</small>' : ''}</td><td class="n">${s.end}</td><td class="n r">${esc(usd(s.fv))}</td><td class="n r">${s.close ? esc(usd(s.close)) : '—'}</td><td class="n r">${s.gap == null ? '—' : pctS(s.gap)}</td></tr>`).join('\n');
    const html = head(lang, `${U.hub} | ETHval`, U.hubDesc, url(lang, null), alts(null)) + `
<body>
<div class="w">
<header><a class="brand" href="${homeHref(lang)}">ETH<b>val</b></a>${langBar(lang, null)}</header>
<p class="crumb"><a href="${homeHref(lang)}">${U.home}</a></p>
<h1>${U.hub}</h1>
<p>${esc(U.hubLead)}</p>
<div class="card"><table class="ht"><thead><tr><th>${U.months}</th><th>${U.endDay}</th><th class="r">${U.fv}</th><th class="r">${U.close}</th><th class="r">${U.gap}</th></tr></thead><tbody>
${items}
</tbody></table></div>
<p class="note">${esc(U.methodText(MODELS.length))} ${esc(U.built(built))}</p>
<a class="btn" href="${homeHref(lang)}">${U.live} →</a>
<footer>${esc(U.disclaimer)} · <a href="${homeHref(lang)}">ethval.com</a> · by Hashed</footer>
</div>
</body>
</html>
`;
    pages.push({ file: rel(lang, 'index.html'), html });
  });
  // write: replace the generated tree (only files this script makes) so a month that left the window is not kept stale
  const dir = path.join(ROOT, 'reports');
  LANGS.forEach(l => { const d = l === 'en' ? dir : path.join(dir, l); if (fs.existsSync(d)) fs.readdirSync(d).filter(f => /^(\d{4}-\d{2}|index)\.html$/.test(f)).forEach(f => fs.unlinkSync(path.join(d, f))); });
  pages.forEach(p => { const f = path.join(ROOT, p.file); fs.mkdirSync(path.dirname(f), { recursive: true }); fs.writeFileSync(f, p.html); });
  const s = summary[summary.length - 1];
  fs.writeFileSync(path.join(dir, 'index.json'), JSON.stringify({ latest, end: s.end, monthToDate: s.mtd, months: summary.map(x => x.ym), built }) + '\n');
  return { months: months.map(m => m.ym), latest, urls: [null].concat(months.map(m => m.ym)).map(ym => ({ ym, alts: LANGS.map(l => [HREFLANG[l], url(l, ym)]), locs: LANGS.map(l => url(l, ym)), xdefault: url('en', ym) })) };
}

const EXTRA_CSS = `
.kpis.k4{grid-template-columns:repeat(4,minmax(0,1fr))}.zn{display:flex!important;align-items:center;gap:8px;font-size:18px!important}.zn i,.zl i{display:inline-block;width:12px;height:12px;border-radius:3px;flex:0 0 12px}
th.r,td.r{text-align:right}th small{display:block;font-weight:400;font-size:12px}td small{color:var(--mu);font-size:12px}
.dt td:first-child a{color:var(--tx)}
ul.zb{list-style:none;margin:0;padding:0;display:grid;gap:6px}ul.zb li{display:grid;grid-template-columns:150px 1fr 48px;align-items:center;gap:10px;font-size:14px}
ul.zb li.cur .zl{font-weight:700}.zl{display:flex;align-items:center;gap:8px;color:var(--mu);white-space:nowrap}ul.zb li.cur .zl{color:var(--tx)}
.zt{height:10px;background:var(--bg);border:1px solid var(--line);border-radius:5px;overflow:hidden}.zf{display:block;height:100%}ul.zb b{text-align:right;font-variant-numeric:tabular-nums}
.note{font-size:14px;color:var(--mu)}
.pn{display:flex;flex-wrap:wrap;justify-content:space-between;gap:8px 16px;margin:24px 0 8px;font-size:15px}
@media (max-width:760px){.kpis.k4{grid-template-columns:repeat(2,minmax(0,1fr))}}
@media (max-width:560px){.kpis.k4{grid-template-columns:repeat(2,minmax(0,1fr))}.kpis.k4 .kpi b{font-size:20px}.zn{font-size:16px!important}
 .dt th:nth-child(2),.dt td:nth-child(2),.dt th:nth-child(3),.dt td:nth-child(3){display:none}table{font-size:14px}th,td{padding:7px 4px}
 ul.zb li{grid-template-columns:118px 1fr 40px;font-size:13px}.ht th:nth-child(2),.ht td:nth-child(2){display:none}.pn{flex-direction:column}
 .ht th,.dt th,.ct th{white-space:normal;vertical-align:bottom}.ht td small{display:block}}`;

module.exports = { build, daily, monthsToBuild, zoneOf, EDGES };
