/* ETHval — Model track record (backtest) panel, under the fair value band card (01.3).
 * For every day where all 12 models have a fair value in the page's history (state.historicalFairValues)
 * and a stored price exists, bucket the day by price gap vs the composite fair value of that day
 * (simple average of the 12 models, same as the headline) and look up the stored ETH price exactly
 * 30 / 90 / 180 calendar days later. Cells show the median forward return and the number of days.
 * No price is interpolated or carried forward: a day whose +h date has no stored price is left out.
 * Fewer than MIN_N days in a cell → "hold" (no number shown).
 */
(function () {
  'use strict';

  var MODELS = ['tvlMultiple', 'stakingScarcity', 'metcalfe', 'dcf', 'l2Ecosystem', 'ps',
                'revenueYield', 'commitmentPremium', 'appCapital', 'validatorEcon', 'ethMonetary', 'ecosystemSettlement'];
  var EDGES = [-40, -20, 0];          // gap % edges → buckets: < −40, −40~−20, −20~0, ≥ 0
  var HORIZONS = [30, 90, 180];       // days after
  var MIN_N = 10;                     // fewer days than this → hold

  var T = {
    en: { title: 'MODEL TRACK RECORD (BACKTEST)', sub: 'ETH return after each day, grouped by how far the price was from the composite fair value that day',
          gap: 'Gap vs fair value', all: 'All days (baseline)', after: function (h) { return h + 'D later'; },
          days: 'days', hold: 'Hold', now: 'Now',
          holdNote: 'Hold = fewer than ' + MIN_N + ' days, no number shown.',
          note: function (a, b, n) { return 'Period: ' + a + ' – ' + b + ' (' + n + ' days with all 12 models and a stored price). Cell = median ETH return from that day\'s price to the stored price exactly 30/90/180 days later, and the number of days used. Days whose later price is not stored yet are left out, never filled. Dates under each gap = first and last month a day fell in that gap.'; },
          warn: 'Caution — overfitting risk. These fair values are recomputed: today\'s 12 model formulas, constants and current circulating supply applied to the stored inputs of each past day, so they are not the numbers the site showed at the time. Neighbouring days share most of their later window, so the real number of independent samples is far smaller than the day count, and the whole period is a single market cycle. Not a forecast and not investment advice.' },
    ko: { title: '모델 적중 기록 (백테스트)', sub: '그날 시장가가 합성 적정가에서 얼마나 떨어져 있었는지로 나눈, 이후 ETH 수익률',
          gap: '적정가 대비 괴리', all: '전체 날짜 (기준선)', after: function (h) { return h + '일 뒤'; },
          days: '일', hold: '판단 보류', now: '현재',
          holdNote: '판단 보류 = 표본 ' + MIN_N + '일 미만이라 숫자를 보여주지 않아.',
          note: function (a, b, n) { return '기간: ' + a + ' – ' + b + ' (12개 모델 값과 저장된 가격이 모두 있는 ' + n + '일). 칸 = 그날 가격 대비 정확히 30/90/180일 뒤 저장 가격의 ETH 수익률 중앙값과 사용한 일수. 뒤 가격이 아직 저장되지 않은 날은 빼고, 채우지 않아. 괴리 아래 날짜 = 그 구간에 속한 첫 달–마지막 달.'; },
          warn: '주의 — 과적합 위험. 이 적정가는 다시 계산한 값이야: 지금의 12개 모델 공식·상수와 현재 유통량을 과거 각 날짜의 저장 입력에 적용했기 때문에, 당시 사이트에 보였던 숫자가 아니야. 이웃한 날들은 이후 기간이 대부분 겹쳐서 실제 독립 표본은 일수보다 훨씬 적고, 전체 기간이 한 번의 시장 국면이야. 예측도 투자 조언도 아니야.' },
    zh: { title: '模型命中记录（回测）', sub: '按当日价格偏离综合公允价值的程度分组，此后的 ETH 收益率',
          gap: '相对公允价值偏离', all: '全部日期（基准）', after: function (h) { return h + '天后'; },
          days: '天', hold: '暂不判断', now: '当前',
          holdNote: '暂不判断 = 样本少于 ' + MIN_N + ' 天，不显示数字。',
          note: function (a, b, n) { return '期间：' + a + ' – ' + b + '（12 个模型值与已存价格齐全的 ' + n + ' 天）。单元格 = 从当日价格到恰好 30/90/180 天后已存价格的 ETH 收益率中位数，以及所用天数。之后价格尚未存储的日期不计入，不做填补。偏离区间下方的日期 = 落入该区间的首月–末月。'; },
          warn: '注意 — 过拟合风险。这些公允价值是重新计算的：把现在的 12 个模型公式、常数和当前流通量套用到过去每一天的已存输入上，因此并非当时网站显示的数字。相邻日期的后续区间大部分重叠，真正独立的样本远少于天数，且整个期间只是一个市场周期。这不是预测，也不是投资建议。' },
    ja: { title: 'モデル的中記録（バックテスト）', sub: 'その日の市場価格が合成適正価値からどれだけ離れていたかで分けた、その後の ETH リターン',
          gap: '適正価値との乖離', all: '全期間（基準線）', after: function (h) { return h + '日後'; },
          days: '日', hold: '判断保留', now: '現在',
          holdNote: '判断保留 = サンプルが ' + MIN_N + ' 日未満のため数字を出さない。',
          note: function (a, b, n) { return '期間：' + a + ' – ' + b + '（12 モデルの値と保存価格がそろう ' + n + ' 日）。セル = その日の価格からちょうど 30/90/180 日後の保存価格までの ETH リターン中央値と使用日数。後の価格がまだ保存されていない日は除外し、補完しない。乖離の下の日付 = その区間に入った最初の月–最後の月。'; },
          warn: '注意 — 過学習のリスク。この適正価値は再計算値：現在の 12 モデルの式・定数と現在の流通量を過去各日の保存入力に当てはめたもので、当時サイトに表示されていた数字ではない。隣り合う日はその後の期間がほぼ重なるため、実際の独立サンプルは日数よりはるかに少なく、全期間が一つの市場局面にすぎない。予測でも投資助言でもない。' }
  };
  function lang() { var l = (document.documentElement.lang || 'en').slice(0, 2); return T[l] ? l : 'en'; }
  function ymd(d) { return new Date(d).toISOString().slice(0, 10); }
  function addDays(s, n) { return new Date(Date.parse(s + 'T00:00:00Z') + n * 864e5).toISOString().slice(0, 10); }
  function median(a) { var x = a.slice().sort(function (p, q) { return p - q; }), n = x.length;
    return n % 2 ? x[(n - 1) / 2] : (x[n / 2 - 1] + x[n / 2]) / 2; }
  function pct(v) { return (v > 0 ? '+' : (v < 0 ? '−' : '')) + Math.abs(v) + '%'; }
  function bucketOf(gap) { for (var i = 0; i < EDGES.length; i++) if (gap < EDGES[i]) return i; return EDGES.length; }
  function bucketName(i) {
    if (i === 0) return '< ' + pct(EDGES[0]);
    if (i === EDGES.length) return '≥ ' + pct(EDGES[EDGES.length - 1]);
    return pct(EDGES[i - 1]) + ' ~ ' + pct(EDGES[i]);
  }

  // Days with all 12 models + stored price. Returns { days:[{date, price, fv, gap}], price: Map }
  function rows() {
    if (typeof state === 'undefined' || !state || !state.historicalFairValues || !state.priceHistory) return null;
    var H = state.historicalFairValues, maps = [];
    for (var m = 0; m < MODELS.length; m++) {
      var arr = H[MODELS[m]]; if (!arr || !arr.length) return null;
      var mp = new Map();
      arr.forEach(function (d) { if (d.value > 0 && isFinite(d.value)) mp.set(ymd(d.date), d.value); });
      maps.push(mp);
    }
    var price = new Map();
    state.priceHistory.forEach(function (p) { if (p.value > 0 && isFinite(p.value)) price.set(ymd(p.date), p.value); });
    var days = [];
    Array.from(price.keys()).sort().forEach(function (k) {
      var s = 0, n = 0;
      maps.forEach(function (mp) { var v = mp.get(k); if (v) { s += v; n++; } });
      if (n !== MODELS.length) return;                   // only days where every model has a value
      var fv = s / n, p = price.get(k);
      days.push({ date: k, price: p, fv: fv, gap: (p / fv - 1) * 100 });
    });
    return { days: days, price: price };
  }

  // table[b][h] = { n, med, up } ; b = bucket index or 'all'
  function compute() {
    var r = rows(); if (!r || r.days.length < 2) return null;
    var tab = {}, keys = [];
    for (var b = 0; b <= EDGES.length; b++) keys.push(b);
    keys.push('all');
    keys.forEach(function (k) { tab[k] = {}; HORIZONS.forEach(function (h) { tab[k][h] = []; }); });
    var span = {};                                       // first / last day that fell in each bucket
    r.days.forEach(function (d) {
      var b = bucketOf(d.gap);
      if (!span[b]) span[b] = [d.date, d.date]; else span[b][1] = d.date;
      HORIZONS.forEach(function (h) {
        var f = r.price.get(addDays(d.date, h)); if (!f) return;   // exact date only, no fill
        var ret = (f / d.price - 1) * 100;
        tab[b][h].push(ret); tab.all[h].push(ret);
      });
    });
    var out = {};
    keys.forEach(function (k) { out[k] = {}; HORIZONS.forEach(function (h) {
      var a = tab[k][h]; out[k][h] = { n: a.length, med: a.length ? median(a) : null, up: a.length ? a.filter(function (x) { return x > 0; }).length / a.length * 100 : null };
    }); });
    return { table: out, span: span, first: r.days[0].date, last: r.days[r.days.length - 1].date, n: r.days.length, days: r.days };
  }

  function build() {
    var anchor = document.getElementById('fv-band');
    if (!anchor) return null;
    var box = document.getElementById('fv-backtest');
    if (box) return box;
    box = document.createElement('div');
    box.id = 'fv-backtest';
    box.className = 'fbt historical-main-chart';
    box.innerHTML = '<div class="conclusion-label fbt-title"></div><div class="fbt-sub"></div>' +
      '<div class="fbt-wrap"><table class="fbt-table"><thead></thead><tbody></tbody></table></div>' +
      '<div class="fbt-note"></div><div class="fbt-warn" role="note"></div>';
    anchor.parentNode.insertBefore(box, anchor.nextSibling);
    return box;
  }

  var lastKey = '';
  function render() {
    var box = build(); if (!box) return;
    var L = lang(), t = T[L];
    var res = compute();
    var curGap = null;
    if (res && typeof state !== 'undefined' && state.price > 0) {
      var lastFv = res.days[res.days.length - 1].fv; curGap = (state.price / lastFv - 1) * 100;
    }
    var curB = curGap === null ? -1 : bucketOf(curGap);
    var key = L + '|' + (res ? res.n + ':' + res.last : 'x') + '|' + curB;
    if (key === lastKey) return; lastKey = key;

    box.querySelector('.fbt-title').textContent = t.title;
    box.querySelector('.fbt-sub').textContent = t.sub;
    var thead = box.querySelector('thead'), tbody = box.querySelector('tbody');
    thead.innerHTML = ''; tbody.innerHTML = '';
    if (!res) { box.setAttribute('data-rows', '0'); box.querySelector('.fbt-note').textContent = ''; box.querySelector('.fbt-warn').textContent = t.warn; return; }

    var hr = document.createElement('tr');
    var th0 = document.createElement('th'); th0.scope = 'col'; th0.textContent = t.gap; hr.appendChild(th0);
    HORIZONS.forEach(function (h) { var th = document.createElement('th'); th.scope = 'col'; th.textContent = t.after(h); hr.appendChild(th); });
    thead.appendChild(hr);

    var order = []; for (var b = 0; b <= EDGES.length; b++) order.push(b); order.push('all');
    order.forEach(function (k) {
      var tr = document.createElement('tr'); tr.setAttribute('data-bucket', String(k));
      if (k === 'all') tr.className = 'fbt-all';
      if (k === curB) tr.className = 'fbt-cur';
      var th = document.createElement('th'); th.scope = 'row';
      var nm = document.createElement('span'); nm.textContent = k === 'all' ? t.all : bucketName(k); th.appendChild(nm);
      if (k !== 'all' && res.span[k]) {                 // when those days happened (clustered buckets are visible at a glance)
        var sp = document.createElement('small'); sp.className = 'fbt-span';
        var ym = function (s) { return s.slice(2, 7).replace('-', '.'); };
        sp.textContent = ym(res.span[k][0]) + (ym(res.span[k][0]) === ym(res.span[k][1]) ? '' : '–' + ym(res.span[k][1]));
        th.appendChild(sp);
      }
      if (k === curB) { var tag = document.createElement('em'); tag.className = 'fbt-now'; tag.textContent = t.now; th.appendChild(tag); }
      tr.appendChild(th);
      HORIZONS.forEach(function (h) {
        var c = res.table[k][h], td = document.createElement('td');
        td.setAttribute('data-n', String(c.n)); td.setAttribute('data-h', String(h));
        if (c.n < MIN_N) {
          td.className = 'fbt-hold';
          var hb = document.createElement('b'); hb.textContent = t.hold; td.appendChild(hb);
          td.setAttribute('data-med', '');
        } else {
          var v = document.createElement('b'); v.className = c.med >= 0 ? 'pos' : 'neg';
          v.textContent = (c.med >= 0 ? '+' : '−') + Math.abs(c.med).toFixed(1) + '%';
          td.appendChild(v); td.setAttribute('data-med', c.med.toFixed(2));
        }
        var s = document.createElement('small'); s.textContent = c.n.toLocaleString('en-US') + t.days.replace(/^(?=[a-z])/, ' '); td.appendChild(s);
        tr.appendChild(td);
      });
      tbody.appendChild(tr);
    });
    box.setAttribute('data-rows', String(order.length));
    box.setAttribute('data-first', res.first); box.setAttribute('data-last', res.last); box.setAttribute('data-days', String(res.n));
    box.setAttribute('data-cur-bucket', String(curB));
    box.querySelector('.fbt-note').textContent = t.note(res.first, res.last, res.n.toLocaleString('en-US')) + ' ' + t.holdNote;
    box.querySelector('.fbt-warn').textContent = t.warn;
  }

  var timer = 0;
  function schedule() { clearTimeout(timer); timer = setTimeout(render, 200); }
  function start() {
    var tries = 0;
    (function wait() {
      if (typeof state !== 'undefined' && state && state.historicalDataLoaded && state.historicalFairValues && document.getElementById('fv-band')) { render(); return; }
      if (++tries < 150) setTimeout(wait, 1000);
    })();
    new MutationObserver(schedule).observe(document.documentElement, { attributes: true, attributeFilter: ['lang'] });
    var hv = document.getElementById('chart-composite-value');
    if (hv) new MutationObserver(schedule).observe(hv, { childList: true, characterData: true, subtree: true });
  }
  if (document.readyState === 'loading') document.addEventListener('DOMContentLoaded', start); else start();
  window.ethvalBacktest = { render: function () { lastKey = ''; render(); }, compute: compute, EDGES: EDGES, HORIZONS: HORIZONS, MIN_N: MIN_N, bucketOf: bucketOf };
})();
