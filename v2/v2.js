/* ETHval 2.0 — representative fair value, valuation band, track record, data freshness.
 * Headline method (decided 2026-10-09):
 *   1. Exclude price-anchored models (their formula multiplies the market price).
 *   2. Smooth each remaining model with a 30-day trailing mean.
 *   3. Take the median across active models.  Range = 25th–75th position of the same set.
 * Pure helpers are exposed on window.ETHvalV2 so the main app, charts and tests share one source.
 */
(function (root) {
  'use strict';

  var ANCHORED = ['dcf', 'validatorEcon', 'stakingScarcity', 'commitmentPremium'];
  var INDEPENDENT = ['tvlMultiple', 'metcalfe', 'l2Ecosystem', 'appCapital', 'ethMonetary', 'ecosystemSettlement', 'ps', 'revenueYield'];
  var AXES = [
    { id: 'cash', models: ['ps', 'revenueYield'] },
    { id: 'network', models: ['tvlMultiple', 'l2Ecosystem', 'appCapital', 'metcalfe'] },
    { id: 'money', models: ['ethMonetary', 'ecosystemSettlement'] }
  ];
  var WINDOW = 30;
  var CACHE_KEY = 'ethval:v2headline';
  var STALE_DAYS = 7;

  // ── i18n ────────────────────────────────────────────────────────────
  var T = {
    en: {
      eyebrow: 'ETH fair value', asOf: 'as of', vsMarket: 'vs market price', market: 'Market',
      rangeLabel: 'Model range (middle 50%)', method: 'Median of {n} independent models, each smoothed over 30 days. Price-anchored models are shown but excluded.',
      below: 'Price is below the model range', within: 'Price is inside the model range', above: 'Price is above the model range',
      axes: 'Three ways to value ETH', cash: 'Cash flow', cashQ: 'Do fees justify the price?', network: 'Network & usage', networkQ: 'Is ETH priced like the economy built on it?',
      money: 'Money & settlement', moneyQ: 'Is ETH priced like the money it settles?',
      track: 'Track record', trackText: 'Over the last {years} years, when this fair value sat 15%+ above price, ETH was higher 180 days later {hit}% of the time ({n} days). The fair value was above price on {above}% of days.',
      trackNote: 'One market cycle of data — read it as context, not a forecast.',
      bandTitle: '3-year valuation band', lPrice: 'ETH price', lFair: 'Fair value', lBand: 'Model range',
      stale: 'Stale inputs', staleText: 'not updated since', anchored: 'Price-anchored · excluded', staleBadge: 'Stale since',
      loading: 'Calculating…'
    },
    ko: {
      eyebrow: 'ETH 적정가', asOf: '기준', vsMarket: '현재가 대비', market: '현재가',
      rangeLabel: '모델 범위 (가운데 50%)', method: '독립 모델 {n}개를 각각 30일 평균한 뒤 가운데 값을 씁니다. 현재가를 곱해 만드는 모델은 보여주되 계산에서는 뺍니다.',
      below: '현재가가 모델 범위보다 낮아요', within: '현재가가 모델 범위 안에 있어요', above: '현재가가 모델 범위보다 높아요',
      axes: 'ETH를 보는 세 가지 관점', cash: '현금흐름', cashQ: '수수료 수익이 가격을 받치나?', network: '네트워크·사용', networkQ: '그 위 경제 규모만큼의 값인가?',
      money: '화폐·결제', moneyQ: '결제 화폐로서의 수요만큼인가?',
      track: '과거 적중 기록', trackText: '지난 {years}년 동안 이 적정가가 현재가보다 15% 이상 높았던 날, 180일 뒤 가격이 올라 있던 비율은 {hit}%였습니다 ({n}일). 적정가가 현재가보다 높았던 날은 전체의 {above}%입니다.',
      trackNote: '시장 사이클 한 번 분량의 데이터라 예측이 아니라 참고 맥락으로 보세요.',
      bandTitle: '3년 적정가 밴드', lPrice: 'ETH 가격', lFair: '적정가', lBand: '모델 범위',
      stale: '갱신이 멈춘 데이터', staleText: '이후 갱신 없음', anchored: '현재가 연동 · 계산 제외', staleBadge: '마지막 갱신',
      loading: '계산 중…'
    },
    zh: {
      eyebrow: 'ETH 公允价值', asOf: '截至', vsMarket: '相对市价', market: '市价',
      rangeLabel: '模型区间（中间 50%）', method: '取 {n} 个独立模型（各自 30 日平滑）的中位数。与价格挂钩的模型仅展示，不参与计算。',
      below: '价格低于模型区间', within: '价格位于模型区间内', above: '价格高于模型区间',
      axes: '估值 ETH 的三种视角', cash: '现金流', cashQ: '手续费能支撑价格吗？', network: '网络与使用', networkQ: '与其上的经济规模相称吗？',
      money: '货币与结算', moneyQ: '与其结算的货币需求相称吗？',
      track: '历史表现', trackText: '过去 {years} 年中，当公允价值高于价格 15% 以上时，180 天后 ETH 上涨的比例为 {hit}%（{n} 天）。公允价值高于价格的天数占 {above}%。',
      trackNote: '仅一个市场周期的数据，请作为参考而非预测。',
      bandTitle: '3 年估值区间', lPrice: 'ETH 价格', lFair: '公允价值', lBand: '模型区间',
      stale: '已停止更新的数据', staleText: '之后未更新', anchored: '与价格挂钩 · 已排除', staleBadge: '最后更新',
      loading: '计算中…'
    },
    ja: {
      eyebrow: 'ETH 適正価格', asOf: '時点', vsMarket: '市場価格比', market: '市場価格',
      rangeLabel: 'モデルレンジ（中央 50%）', method: '独立モデル {n} 個をそれぞれ 30 日平滑し、その中央値を使います。価格連動モデルは表示のみで計算から除外します。',
      below: '価格はモデルレンジより下', within: '価格はモデルレンジ内', above: '価格はモデルレンジより上',
      axes: 'ETH を測る 3 つの視点', cash: 'キャッシュフロー', cashQ: '手数料収入で価格を支えられるか？', network: 'ネットワーク・利用', networkQ: '上に築かれた経済規模に見合うか？',
      money: '貨幣・決済', moneyQ: '決済通貨としての需要に見合うか？',
      track: '過去の実績', trackText: '過去 {years} 年間、この適正価格が価格を 15% 以上上回った日に、180 日後の価格が上昇していた割合は {hit}%（{n} 日）。適正価格が価格を上回った日は全体の {above}% です。',
      trackNote: '1 回の市場サイクル分のデータです。予測ではなく参考としてご覧ください。',
      bandTitle: '3 年バリュエーションバンド', lPrice: 'ETH 価格', lFair: '適正価格', lBand: 'モデルレンジ',
      stale: '更新が止まったデータ', staleText: '以降更新なし', anchored: '価格連動 · 除外', staleBadge: '最終更新',
      loading: '計算中…'
    }
  };
  function lang() { var l = (document.documentElement.lang || 'en').slice(0, 2); return T[l] ? l : 'en'; }
  function t(k, vars) {
    var s = (T[lang()][k] !== undefined ? T[lang()][k] : T.en[k]) || k;
    if (vars) Object.keys(vars).forEach(function (v) { s = s.split('{' + v + '}').join(vars[v]); });
    return s;
  }

  // ── pure math ───────────────────────────────────────────────────────
  function dayKey(d) { var x = d instanceof Date ? d : new Date(d); return isNaN(x) ? null : x.toISOString().slice(0, 10); }
  function median(xs) {
    if (!xs.length) return null;
    var s = xs.slice().sort(function (a, b) { return a - b; }), m = s.length >> 1;
    return s.length % 2 ? s[m] : (s[m - 1] + s[m]) / 2;
  }
  function quartiles(xs) {
    var s = xs.slice().sort(function (a, b) { return a - b; });
    return { lo: s[Math.floor(s.length / 4)], hi: s[Math.min(s.length - 1, Math.floor(3 * s.length / 4))] };
  }
  /** Trailing mean over the last WINDOW valid points of one model series → Map(dayKey → value). */
  function smooth(series, win) {
    var out = new Map(), q = [], sum = 0;
    (series || []).forEach(function (p) {
      var v = +p.value, k = dayKey(p.date);
      if (!k || !(v > 0) || !isFinite(v)) return;
      q.push(v); sum += v;
      if (q.length > (win || WINDOW)) sum -= q.shift();
      out.set(k, sum / q.length);
    });
    return out;
  }
  var _cache = { ref: null, sm: {} };
  function smoothed(hist) {
    if (_cache.ref !== hist) { _cache = { ref: hist, sm: {} }; }
    return function (id) {
      if (!_cache.sm[id]) _cache.sm[id] = smooth(hist[id]);
      return _cache.sm[id];
    };
  }
  /** Representative value on one day from active models. */
  function valueOn(hist, ids, key) {
    var get = smoothed(hist), vals = [];
    ids.forEach(function (id) { var v = hist[id] && get(id).get(key); if (v > 0) vals.push(v); });
    if (!vals.length) return null;
    var q = quartiles(vals);
    return { value: median(vals), lo: q.lo, hi: q.hi, n: vals.length };
  }
  /** Composite series aligned with priceData (array of {date}) — used by charts and history. */
  function seriesFor(hist, ids, priceData) {
    var last = null;
    return (priceData || []).map(function (p) {
      var r = valueOn(hist, ids, dayKey(p.date));
      if (r) last = r.value;
      return r ? r.value : last;
    });
  }
  function compositeSeries(hist, priceData, ids) {
    ids = ids || activeModels();
    return (priceData || []).map(function (p) {
      var r = valueOn(hist, ids, dayKey(p.date));
      return r ? { date: p.date, value: r.value } : null;
    }).filter(Boolean);
  }
  /** Track record of the headline method (uses only data the page already has). */
  function trackRecord(hist, priceData, ids, horizon) {
    horizon = horizon || 180;
    var pts = (priceData || []).filter(function (p) { return p.value > 0; });
    var fv = seriesFor(hist, ids, pts), sig = 0, hit = 0, above = 0, n = 0;
    for (var i = 0; i < pts.length; i++) {
      if (!(fv[i] > 0)) continue;
      n++;
      if (fv[i] > pts[i].value) above++;
      if (i + horizon < pts.length && fv[i] / pts[i].value - 1 > 0.15) {
        sig++;
        if (pts[i + horizon].value > pts[i].value) hit++;
      }
    }
    return n ? { signals: sig, hitPct: sig ? Math.round(100 * hit / sig) : null, abovePct: Math.round(100 * above / n),
      years: Math.max(1, Math.round(pts.length / 365)) } : null;
  }

  // ── app integration ─────────────────────────────────────────────────
  function S() { try { return (0, eval)('typeof state !== "undefined" ? state : null'); } catch (e) { return null; } }
  function activeModels() {
    if (typeof document === 'undefined') return INDEPENDENT.slice();
    var cards = document.querySelectorAll('.valuation-model');
    if (!cards.length) return INDEPENDENT.slice();
    var ids = [];
    cards.forEach(function (c) {
      var inp = c.querySelector('.model-toggle input');
      if (inp && inp.checked) ids.push(c.dataset.model);
    });
    return ids;
  }
  function readCache() {
    try { var c = JSON.parse(localStorage.getItem(CACHE_KEY) || 'null'); return c && Date.now() - c.t < 864e5 ? c : null; } catch (e) { return null; }
  }
  /**
   * Single source of truth for the headline.  Returns {value, lo, hi, n, source} or null.
   * spot: [{id, price}] from current cards — only used if history never arrives.
   */
  function headline(spot) {
    var st = S(), hist = st && st.historicalFairValues, ph = st && st.priceHistory;
    var ids = activeModels();
    if (hist && ph && ph.length && Object.keys(hist).length > 1) {
      var key = dayKey(ph[ph.length - 1].date), r = valueOn(hist, ids, key);
      if (r) {
        r.source = 'smoothed'; r.date = key;
        if (ids.join() === INDEPENDENT.filter(function (m) { return ids.indexOf(m) >= 0; }).join() && ids.length === INDEPENDENT.length) {
          try { localStorage.setItem(CACHE_KEY, JSON.stringify({ t: Date.now(), value: r.value, lo: r.lo, hi: r.hi, n: r.n, date: key })); } catch (e) {}
        }
        return r;
      }
    }
    var c = readCache();
    if (c && ids.length === INDEPENDENT.length) return { value: c.value, lo: c.lo, hi: c.hi, n: c.n, date: c.date, source: 'cache' };
    if (spot && spot.length && root.__ethvalV2SpotOk) {
      var vals = spot.map(function (s) { return s.price; }).filter(function (v) { return v > 0; });
      if (vals.length) { var q = quartiles(vals); return { value: median(vals), lo: q.lo, hi: q.hi, n: vals.length, source: 'spot' }; }
    }
    return null;
  }
  setTimeout(function () { root.__ethvalV2SpotOk = true; }, 20000).unref?.();

  // ── rendering ───────────────────────────────────────────────────────
  function $(id) { return document.getElementById(id); }
  function money(v) {
    if (!(v > 0)) return '$—';
    return '$' + (v >= 100 ? Math.round(v).toLocaleString('en-US') : v.toFixed(1));
  }
  function pct(v) { return (v >= 0 ? '+' : '−') + Math.abs(v).toFixed(0) + '%'; }
  function setText(id, s) { var el = $(id); if (el) el.textContent = s; }

  function renderStatic() {
    setText('v2-eyebrow', t('eyebrow'));
    setText('v2-range-label', t('rangeLabel'));
    setText('v2-axes-title', t('axes'));
    setText('v2-band-title', t('bandTitle'));
    setText('v2-track-title', t('track'));
    setText('v2-track-note', t('trackNote'));
    setText('v2-l-price', t('lPrice')); setText('v2-l-fair', t('lFair')); setText('v2-l-band', t('lBand'));
    AXES.forEach(function (a) { setText('v2-axis-' + a.id + '-name', t(a.id)); setText('v2-axis-' + a.id + '-q', t(a.id + 'Q')); });
    document.querySelectorAll('.v2-anchored-badge').forEach(function (b) { b.textContent = t('anchored'); });
  }

  function renderHero() {
    var st = S(); if (!st) return;
    var price = st.price;
    var spot = [];
    document.querySelectorAll('.valuation-model').forEach(function (c) {
      var inp = c.querySelector('.model-toggle input');
      if (inp && inp.checked && +c.dataset.price > 0) spot.push({ id: c.dataset.model, price: +c.dataset.price });
    });
    var h = headline(spot);
    var hero = $('v2-hero'); if (!hero) return;
    hero.classList.toggle('is-loading', !h || !(price > 0));
    if (!h || !(price > 0)) { setText('v2-value', t('loading')); return; }
    if (lastHeadline !== h.value && typeof root.recalculateWeightedAverage === 'function') {
      lastHeadline = h.value;
      try { root.recalculateWeightedAverage(); } catch (e) {}
    }
    var gap = (h.value / price - 1) * 100;
    setText('v2-value', money(h.value));
    var gapEl = $('v2-gap');
    if (gapEl) {
      var L = lang();
      gapEl.textContent = (L === 'en') ? pct(gap) + ' ' + t('vsMarket') + ' ' + money(price)
        : t('market') + ' ' + money(price) + ' · ' + pct(gap);
      gapEl.className = 'v2-gap ' + (gap >= 0 ? 'up' : 'down');
    }
    setText('v2-asof', h.date ? t('asOf') + ' ' + h.date : '');
    setText('v2-range', money(h.lo) + ' – ' + money(h.hi));
    setText('v2-method', t('method', { n: h.n }));
    var pos = price < h.lo ? 'below' : price > h.hi ? 'above' : 'within';
    var verdict = $('v2-verdict');
    if (verdict) { verdict.textContent = t(pos); verdict.className = 'v2-verdict ' + pos; }
    // Gauge: log scale between min/max of {lo, hi, price, value} with padding
    var pts = [h.lo, h.hi, price, h.value], mn = Math.min.apply(null, pts) * 0.8, mx = Math.max.apply(null, pts) * 1.2;
    var x = function (v) { return (Math.log(v / mn) / Math.log(mx / mn)) * 100; };
    var band = $('v2-gauge-band'), mPrice = $('v2-gauge-price'), mFair = $('v2-gauge-fair');
    if (band) { band.style.left = x(h.lo) + '%'; band.style.width = (x(h.hi) - x(h.lo)) + '%'; }
    if (mPrice) { mPrice.style.left = x(price) + '%'; mPrice.setAttribute('data-label', t('market') + ' ' + money(price)); }
    if (mFair) { mFair.style.left = x(h.value) + '%'; mFair.setAttribute('data-label', money(h.value)); }

    // Axes
    var hist = st.historicalFairValues, ph = st.priceHistory;
    var key = ph && ph.length ? dayKey(ph[ph.length - 1].date) : null;
    AXES.forEach(function (a) {
      var r = hist && key ? valueOn(hist, a.models, key) : null;
      var v = r ? r.value : median(a.models.map(function (m) { var c = document.querySelector('.valuation-model[data-model="' + m + '"]'); return c ? +c.dataset.price : 0; }).filter(function (v) { return v > 0; }));
      setText('v2-axis-' + a.id + '-value', money(v));
      var g = v > 0 ? (v / price - 1) * 100 : null, el = $('v2-axis-' + a.id + '-gap');
      if (el) { el.textContent = g === null ? '' : pct(g); el.className = 'v2-axis-gap ' + (g === null ? '' : g >= 0 ? 'up' : 'down'); }
    });

    // Track record
    if (hist && ph && ph.length > 400) {
      var tr = trackRecord(hist, ph, activeModels(), 180);
      if (tr && tr.hitPct !== null) setText('v2-track-text', t('trackText', { years: tr.years, hit: tr.hitPct, n: tr.signals, above: tr.abovePct }));
    }
    renderBand();
    renderFreshness();
  }

  var bandChart = null, bandSig = '', lastHeadline = null;
  function renderBand() {
    var st = S(), cv = $('v2-band-canvas');
    if (!st || !cv || !root.Chart || !st.historicalFairValues || !st.priceHistory || st.priceHistory.length < 30) return;
    var ph = st.priceHistory, ids = activeModels(), step = ph.length > 700 ? 3 : 1;
    var sig = ph.length + '|' + ids.join() + '|' + document.body.classList.contains('dark') + '|' + lang();
    if (sig === bandSig) return;
    bandSig = sig;
    var labels = [], price = [], fair = [], lo = [], hi = [];
    for (var i = 0; i < ph.length; i += step) {
      var p = ph[i], r = valueOn(st.historicalFairValues, ids, dayKey(p.date));
      labels.push(new Date(p.date)); price.push(p.value); fair.push(r ? r.value : null); lo.push(r ? r.lo : null); hi.push(r ? r.hi : null);
    }
    var css = getComputedStyle(document.body);
    var cAccent = css.getPropertyValue('--v2-accent').trim() || '#4f6bed';
    var cInk = css.getPropertyValue('--v2-ink').trim() || '#111';
    var cBand = css.getPropertyValue('--v2-band').trim() || 'rgba(79,107,237,.12)';
    var cGrid = css.getPropertyValue('--v2-grid').trim() || 'rgba(0,0,0,.06)';
    var cMuted = css.getPropertyValue('--v2-muted').trim() || '#888';
    var data = { labels: labels, datasets: [
      { label: 'hi', data: hi, borderWidth: 0, pointRadius: 0, fill: '+1', backgroundColor: cBand, tension: 0.2 },
      { label: 'lo', data: lo, borderWidth: 0, pointRadius: 0, fill: false, tension: 0.2 },
      { label: 'fair', data: fair, borderColor: cAccent, borderWidth: 2, pointRadius: 0, fill: false, tension: 0.2 },
      { label: 'price', data: price, borderColor: cInk, borderWidth: 1.5, pointRadius: 0, fill: false, tension: 0.1 }
    ] };
    var opts = { responsive: true, maintainAspectRatio: false, animation: false, interaction: { mode: 'index', intersect: false },
      plugins: { legend: { display: false }, tooltip: { filter: function (it) { return it.dataset.label === 'fair' || it.dataset.label === 'price'; },
        callbacks: { title: function (it) { return dayKey(it[0].label ? new Date(it[0].parsed.x) : null) || ''; },
          label: function (it) { return (it.dataset.label === 'fair' ? t('lFair') : t('lPrice')) + ' ' + money(it.parsed.y); } } } },
      scales: { x: { type: 'time', time: { unit: 'quarter' }, grid: { display: false }, ticks: { color: cMuted, maxTicksLimit: 7, font: { size: 10 } } },
        y: { type: 'logarithmic', grid: { color: cGrid }, ticks: { color: cMuted, font: { size: 10 }, maxTicksLimit: 5,
          callback: function (v) { return '$' + (v >= 1000 ? (v / 1000).toFixed(v % 1000 ? 1 : 0) + 'k' : v); } } } } };
    if (bandChart) bandChart.destroy();
    bandChart = new root.Chart(cv.getContext('2d'), { type: 'line', data: data, options: opts });
  }

  var STALE_SOURCES = [
    { key: 'fundingHistory', el: 'funding-rate', name: { en: 'Funding rate', ko: '펀딩비' } },
    { key: 'openInterestHistory', el: null, name: { en: 'Open interest', ko: '미결제약정' } },
    { key: 'reserveHistory', el: 'exchange-reserve', name: { en: 'Exchange reserve', ko: '거래소 보유량' } },
    { key: 'l2TotalVolumeHistory', el: 'l2-total-volume-value', name: { en: 'L2 total volume (ETH Monetary · Ecosystem Settlement input)', ko: 'L2 총거래량 (ETH Monetary · Ecosystem Settlement 입력)' } },
    { key: 'l2VolumeHistory', el: 'l2-volume-value', name: { en: 'L2 volume', ko: 'L2 거래량' } },
    { key: 'l2ActiveAddrHistory', el: null, name: { en: 'L2 active addresses', ko: 'L2 활성 주소' } },
    { key: 'l2StablecoinSupplyHistory', el: null, name: { en: 'L2 stablecoin supply', ko: 'L2 스테이블코인 공급' } }
  ];
  function staleList() {
    var st = S(); if (!st) return [];
    var now = Date.now(), out = [];
    STALE_SOURCES.forEach(function (s) {
      var a = st[s.key]; if (!a || !a.length) return;
      var last = a[a.length - 1] && a[a.length - 1].date; var d = last ? new Date(last) : null;
      if (d && (now - d) / 864e5 > STALE_DAYS) out.push({ src: s, date: dayKey(d) });
    });
    return out;
  }
  function renderFreshness() {
    var list = staleList(), box = $('v2-stale');
    if (!box) return;
    box.hidden = !list.length;
    if (!list.length) return;
    var l = lang() === 'ko' ? 'ko' : 'en';
    box.innerHTML = '<strong>' + t('stale') + '</strong> ' + list.map(function (x) {
      return '<span class="v2-stale-item">' + (x.src.name[l] || x.src.name.en) + ' · ' + x.date + '</span>';
    }).join('');
    list.forEach(function (x) {
      if (!x.src.el) return;
      var el = $(x.src.el); if (!el) return;
      var card = el.closest('.metric-card') || el.parentElement;
      if (card && !card.querySelector('.v2-stale-badge')) {
        var b = document.createElement('span'); b.className = 'v2-stale-badge'; card.appendChild(b);
      }
      var badge = card && card.querySelector('.v2-stale-badge');
      if (badge) badge.textContent = t('staleBadge') + ' ' + x.date;
    });
  }

  // ── setup on DOM ready ──────────────────────────────────────────────
    function setupModels() {
    ANCHORED.forEach(function (id) {
      var card = document.querySelector('.valuation-model[data-model="' + id + '"]');
      if (card && !card.querySelector('.v2-anchored-badge')) {
        var b = document.createElement('span'); b.className = 'v2-anchored-badge'; b.textContent = t('anchored');
        var row = card.querySelector('.model-row-1'); if (row) row.appendChild(b);
        card.classList.add('v2-anchored', 'disabled');
        var inp = card.querySelector('.model-toggle input'); if (inp) inp.checked = false;
      }
      var btn = document.querySelector('.legend-btn[data-model="' + id + '"]');
      if (btn) btn.classList.remove('active');
    });
    // Live visitors pill → header (it overlapped the first model card when fixed bottom-left)
    var live = $('live-visitors'), right = document.querySelector('.header-right');
    if (live && right && live.parentElement !== right) right.insertBefore(live, right.firstChild);
  }
  var scheduled = false;
  function refresh() {
    if (scheduled) return; scheduled = true;
    (root.requestAnimationFrame || setTimeout)(function () { scheduled = false; try { renderStatic(); renderHero(); } catch (e) { console.warn('[v2] render', e); } });
  }
  function init() {
    setupModels();
    renderStatic();
    renderHero();
    new MutationObserver(function () { bandSig = ''; refresh(); }).observe(document.documentElement, { attributes: true, attributeFilter: ['lang'] });
    new MutationObserver(function () { bandSig = ''; refresh(); }).observe(document.body, { attributes: true, attributeFilter: ['class'] });
    document.addEventListener('change', function (e) { if (e.target && e.target.closest && e.target.closest('.model-toggle')) { bandSig = ''; refresh(); } });
    var tries = 0, iv = setInterval(function () { refresh(); if (++tries > 40) clearInterval(iv); }, 1500);
  }

  root.ETHvalV2 = { ANCHORED: ANCHORED, INDEPENDENT: INDEPENDENT, AXES: AXES, median: median, quartiles: quartiles, smooth: smooth,
    valueOn: valueOn, seriesFor: seriesFor, compositeSeries: compositeSeries, trackRecord: trackRecord, headline: headline,
    activeModels: activeModels, refresh: refresh, staleList: staleList, dayKey: dayKey };
  if (typeof document !== 'undefined') {
    if (document.readyState === 'loading') document.addEventListener('DOMContentLoaded', init); else init();
  }
  if (typeof module !== 'undefined') module.exports = root.ETHvalV2;
})(typeof window !== 'undefined' ? window : globalThis);
