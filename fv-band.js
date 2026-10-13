/* ETHval — Fair value band chart (under 01.3 Historical Trends).
 * Market price drawn over colour bands at fixed multiples of the composite fair value:
 *   ×0.5 / ×0.75 / ×1.0 / ×1.25 / ×1.5 / ×2.0  (= −50 / −25 / 0 / +25 / +50 / +100 %)
 * Data: only the fair-value history the page already computed (state.historicalFairValues, 12 models)
 * and state.priceHistory. Composite per day = simple average of the models that have a value that day.
 * Days without a model value or price are left out (no carry-forward, no fill, no estimate).
 */
(function () {
  'use strict';

  var EDGES = [-50, -25, 0, 25, 50, 100];           // % vs composite fair value (published under the chart)
  var MODELS = ['tvlMultiple', 'stakingScarcity', 'metcalfe', 'dcf', 'l2Ecosystem', 'ps',
                'revenueYield', 'commitmentPremium', 'appCapital', 'validatorEcon', 'ethMonetary', 'ecosystemSettlement'];
  // zone i = between edge i-1 and edge i; zone 0 = below −50 %, zone 6 = above +100 %
  var ZONE_COLORS = ['#15803d', '#22c55e', '#86efac', '#fcd34d', '#fb923c', '#ef4444', '#b91c1c'];

  var T = {
    en: { title: 'FAIR VALUE BANDS', sub: 'Market price against bands set at fixed % of the composite fair value',
          now: 'Now', price: 'Price', fv: 'Fair value', zone: 'Zone', vs: 'vs fair value',
          under: 'below fair value', over: 'above fair value',
          below: 'below −50%', above: 'above +100%',
          note: function (n, a, b) { return 'Band edges = composite fair value × 0.5 / 0.75 / 1.0 / 1.25 / 1.5 / 2.0 (−50 / −25 / 0 / +25 / +50 / +100%). Composite = simple average of the 12 models on each day. ' + n + ' days, ' + a + ' – ' + b + '. Days without data are left blank, never filled.'; },
          empty: 'Not enough fair value history to draw bands.' },
    ko: { title: '적정가 밴드', sub: '합성 적정가의 고정 % 위치에 그린 색 띠 위의 시장가',
          now: '현재', price: '시장가', fv: '적정가', zone: '구간', vs: '적정가 대비',
          under: '적정가 아래', over: '적정가 위',
          below: '−50% 아래', above: '+100% 위',
          note: function (n, a, b) { return '밴드 경계 = 합성 적정가 × 0.5 / 0.75 / 1.0 / 1.25 / 1.5 / 2.0 (−50 / −25 / 0 / +25 / +50 / +100%). 합성 적정가 = 그날 12개 모델 값의 단순 평균. ' + a + ' – ' + b + ', ' + n + '일. 데이터가 없는 날은 비워 두고 채우지 않아.'; },
          empty: '밴드를 그릴 적정가 이력이 부족해.' },
    zh: { title: '公允价值区间带', sub: '市场价格与按综合公允价值固定百分比划分的色带',
          now: '当前', price: '市价', fv: '公允价值', zone: '区间', vs: '相对公允价值',
          under: '低于公允价值', over: '高于公允价值',
          below: '低于 −50%', above: '高于 +100%',
          note: function (n, a, b) { return '区间边界 = 综合公允价值 × 0.5 / 0.75 / 1.0 / 1.25 / 1.5 / 2.0（−50 / −25 / 0 / +25 / +50 / +100%）。综合公允价值 = 当日 12 个模型的简单平均。' + a + ' – ' + b + '，共 ' + n + ' 天。无数据的日期留空，不做填补。'; },
          empty: '公允价值历史不足，无法绘制区间带。' },
    ja: { title: '適正価値バンド', sub: '合成適正価値の固定％に引いた色帯と市場価格',
          now: '現在', price: '市場価格', fv: '適正価値', zone: 'ゾーン', vs: '適正価値比',
          under: '適正価値より下', over: '適正価値より上',
          below: '−50% 未満', above: '+100% 超',
          note: function (n, a, b) { return 'バンド境界 = 合成適正価値 × 0.5 / 0.75 / 1.0 / 1.25 / 1.5 / 2.0（−50 / −25 / 0 / +25 / +50 / +100%）。合成適正価値 = その日の 12 モデルの単純平均。' + a + ' – ' + b + '、' + n + '日分。データのない日は空白のままで、補完しない。'; },
          empty: 'バンドを描くための適正価値履歴が不足している。' }
  };
  function lang() {
    var l = (document.documentElement.lang || 'en').slice(0, 2);
    return T[l] ? l : 'en';
  }
  function pct(v) { return (v > 0 ? '+' : (v < 0 ? '−' : '')) + Math.abs(v) + '%'; }
  function zoneName(i, t) {
    if (i === 0) return t.below;
    if (i === EDGES.length) return t.above;
    return pct(EDGES[i - 1]) + ' ~ ' + pct(EDGES[i]);
  }
  function zoneOf(dev) { // dev in % (price vs fair value)
    for (var i = 0; i < EDGES.length; i++) if (dev < EDGES[i]) return i;
    return EDGES.length;
  }
  function money(v) { return '$' + v.toLocaleString('en-US', { minimumFractionDigits: 1, maximumFractionDigits: 1 }); }
  function ymd(d) { return new Date(d).toISOString().slice(0, 10); }
  function dayKey(d) { return ymd(d); }

  // Build the daily series from data already on the page. Returns null if nothing usable.
  function series(days) {
    if (typeof state === 'undefined' || !state || !state.historicalFairValues || !state.priceHistory) return null;
    var H = state.historicalFairValues, maps = [];
    MODELS.forEach(function (m) {
      var arr = H[m]; if (!arr || !arr.length) return;
      var mp = new Map();
      arr.forEach(function (d) { var k = dayKey(d.date); if (d.value > 0 && isFinite(d.value)) mp.set(k, d.value); }); // last point of the day wins (page syncs the final point to the live valuation)
      maps.push(mp);
    });
    if (!maps.length) return null;
    var cut = Date.now() - days * 864e5, out = [];
    state.priceHistory.forEach(function (p) {
      if (!(p.value > 0) || +new Date(p.date) < cut) return;
      var k = dayKey(p.date), s = 0, n = 0;
      maps.forEach(function (mp) { var v = mp.get(k); if (v) { s += v; n++; } });
      if (!n) return;                                   // no fair value that day → leave it out
      out.push({ date: k, price: p.value, fv: s / n, n: n });
    });
    return out;
  }

  // Indices of the first charted day of every `step`-th calendar month (step = 1/2/3/6/12, smallest that fits
  // maxTicks), counted back from the latest month so the newest month always has a label. A month only gets a
  // tick if its first charted day is within its first 7 days, so a partial first month is not labelled as if
  // it started on the 1st.
  function monthTicks(labels, maxTicks) {
    var starts = [], seen = {};
    labels.forEach(function (s, i) {
      var k = s.slice(0, 7);
      if (seen[k]) return; seen[k] = 1;
      if (+s.slice(8, 10) <= 7) starts.push({ i: i, m: +s.slice(0, 4) * 12 + (+s.slice(5, 7) - 1) });
    });
    if (!starts.length) return [];
    var lastM = starts[starts.length - 1].m, steps = [1, 2, 3, 6, 12], pick = [];
    for (var k = 0; k < steps.length; k++) {
      pick = starts.filter(function (t) { return (lastM - t.m) % steps[k] === 0; });
      if (pick.length <= maxTicks) break;
    }
    return pick.map(function (t) { return t.i; });
  }

  var chart = null, period = 365, lastKey = '';
  function build() {
    var anchor = document.querySelector('#section-valuation .historical-main-chart');
    if (!anchor) return null;
    var box = document.getElementById('fv-band');
    if (box) return box;
    box = document.createElement('div');
    box.id = 'fv-band';
    box.className = 'fvb historical-main-chart';
    box.innerHTML =
      '<div class="fvb-head"><div class="conclusion-label fvb-title"></div><div class="fvb-tabs" role="tablist">' +
      '<button type="button" class="historical-tab" data-days="90">90D</button>' +
      '<button type="button" class="historical-tab active" data-days="365">1Y</button></div></div>' +
      '<div class="fvb-sub"></div>' +
      '<div class="fvb-now" aria-live="polite"></div>' +
      '<div class="fvb-area"><canvas id="fv-band-chart"></canvas></div>' +
      '<div class="fvb-legend"></div>' +
      '<div class="fvb-note"></div>';
    anchor.parentNode.insertBefore(box, anchor.nextSibling);
    box.querySelectorAll('.fvb-tabs button').forEach(function (b) {
      b.addEventListener('click', function () {
        period = +b.dataset.days;
        box.querySelectorAll('.fvb-tabs button').forEach(function (x) { x.classList.toggle('active', x === b); });
        lastKey = ''; render();
      });
    });
    return box;
  }

  function render() {
    var box = build(); if (!box || typeof Chart === 'undefined') return;
    var L = lang(), t = T[L], dark = document.body.classList.contains('dark');
    var data = series(period);
    var key = L + '|' + dark + '|' + period + '|' + (data ? data.length + ':' + (data.length && data[data.length - 1].fv) + ':' + (state.price || 0) : 'x');
    if (key === lastKey) return;
    lastKey = key;

    box.querySelector('.fvb-title').textContent = t.title;
    box.querySelector('.fvb-sub').textContent = t.sub;
    var nowEl = box.querySelector('.fvb-now'), noteEl = box.querySelector('.fvb-note'), legEl = box.querySelector('.fvb-legend');
    if (!data || data.length < 10) {
      nowEl.textContent = t.empty; noteEl.textContent = ''; legEl.textContent = '';
      if (chart) { chart.destroy(); chart = null; }
      box.setAttribute('data-points', '0');
      return;
    }
    var last = data[data.length - 1];
    // current reading: live price (same as the headline) vs the latest composite fair value
    var curPrice = (typeof state !== 'undefined' && state.price > 0) ? state.price : last.price;
    var dev = (curPrice / last.fv - 1) * 100, z = zoneOf(dev);
    nowEl.innerHTML = '';
    function chip(label, val, cls) {
      var s = document.createElement('span'); s.className = 'fvb-chip' + (cls ? ' ' + cls : '');
      var a = document.createElement('span'); a.className = 'fvb-chip-l'; a.textContent = label;
      var b = document.createElement('b'); b.textContent = val;
      s.appendChild(a); s.appendChild(b); return s;
    }
    nowEl.appendChild(chip(t.price, money(curPrice)));
    nowEl.appendChild(chip(t.fv, money(last.fv)));
    nowEl.appendChild(chip(t.vs, (dev >= 0 ? '+' : '−') + Math.abs(dev).toFixed(1) + '%', dev < 0 ? 'under' : 'over'));
    var zc = chip(t.zone, zoneName(z, t), 'zone');
    var sw = document.createElement('i'); sw.className = 'fvb-sw'; sw.style.background = ZONE_COLORS[z];
    zc.insertBefore(sw, zc.firstChild);
    nowEl.appendChild(zc);
    box.setAttribute('data-zone', String(z));
    box.setAttribute('data-dev', dev.toFixed(2));
    box.setAttribute('data-points', String(data.length));
    box.setAttribute('data-fv', last.fv.toFixed(2));

    legEl.innerHTML = '';
    for (var i = ZONE_COLORS.length - 1; i >= 0; i--) {
      var it = document.createElement('span'); it.className = 'fvb-leg' + (i === z ? ' cur' : '');
      var s2 = document.createElement('i'); s2.className = 'fvb-sw'; s2.style.background = ZONE_COLORS[i];
      it.appendChild(s2); it.appendChild(document.createTextNode(zoneName(i, t)));
      legEl.appendChild(it);
    }
    noteEl.textContent = t.note(data.length, data[0].date, last.date);

    var labels = data.map(function (d) { return d.date; });
    var alpha = dark ? '40' : '38';
    var sets = [];
    // lowest edge fills down to the axis floor (zone 0), each next edge fills to the previous one
    EDGES.forEach(function (e, i) {
      sets.push({
        label: pct(e), data: data.map(function (d) { return d.fv * (1 + e / 100); }),
        borderColor: e === 0 ? (dark ? 'rgba(230,237,243,0.75)' : 'rgba(36,41,46,0.7)') : (dark ? 'rgba(255,255,255,0.18)' : 'rgba(0,0,0,0.15)'),
        borderWidth: e === 0 ? 1.5 : 1, borderDash: e === 0 ? [4, 3] : [],
        backgroundColor: ZONE_COLORS[i] + alpha, fill: i === 0 ? 'start' : '-1',
        pointRadius: 0, tension: 0.2, order: 5
      });
    });
    // top zone (above +100 %) fills up to the axis ceiling
    sets.push({ label: '>' + pct(100), data: sets[sets.length - 1].data, borderWidth: 0, borderColor: 'transparent',
      backgroundColor: ZONE_COLORS[6] + alpha, fill: 'end', pointRadius: 0, tension: 0.2, order: 6 });
    sets.push({ label: t.price, data: data.map(function (d) { return d.price; }),
      borderColor: dark ? '#ffffff' : '#111827', borderWidth: 2, fill: false, pointRadius: 0, pointHoverRadius: 3, tension: 0.2, order: 0 });

    var lo = Infinity, hi = 0;
    data.forEach(function (d) { lo = Math.min(lo, d.price, d.fv * 0.5); hi = Math.max(hi, d.price, d.fv * 2); });
    var grid = dark ? 'rgba(255,255,255,0.06)' : 'rgba(0,0,0,0.06)', tick = dark ? '#8b949e' : '#57606a';
    var narrow = window.innerWidth < 600;
    var opts = {
      responsive: true, maintainAspectRatio: false, animation: false,
      interaction: { mode: 'index', intersect: false },
      plugins: {
        legend: { display: false },
        tooltip: {
          filter: function (it) { return it.datasetIndex === sets.length - 1 || it.dataset.label === pct(0); },
          callbacks: {
            label: function (it) {
              var d = data[it.dataIndex];
              if (it.datasetIndex === sets.length - 1) return t.price + ' ' + money(d.price) + ' (' + ((d.price / d.fv - 1) * 100 >= 0 ? '+' : '') + ((d.price / d.fv - 1) * 100).toFixed(1) + '%)';
              return t.fv + ' ' + money(d.fv) + ' · ' + d.n + '/12';
            }
          }
        }
      },
      scales: {
        // month ticks at an even step (1/2/3/6/12 months) aligned to calendar months, placed on the first
        // charted day of each month — Chart.js autoSkip picked every n-th index, which mixed 1- and 2-month gaps
        x: { type: 'category', grid: { display: false },
             afterBuildTicks: function (ax) { ax.ticks = monthTicks(labels, narrow ? 5 : 13).map(function (i) { return { value: i }; }); },
             ticks: { color: tick, maxRotation: 0, autoSkip: false, font: { size: 12 },
             callback: function (v) { var s = labels[v] || ''; return s.slice(2, 7).replace('-', '.'); } } },
        y: { type: 'logarithmic', min: lo * 0.95, max: hi * 1.05, grid: { color: grid },
             ticks: { color: tick, font: { size: 12 }, maxTicksLimit: 6,
               callback: function (v) { return v >= 1000 ? '$' + (v / 1000).toFixed(v >= 10000 ? 0 : 1) + 'k' : '$' + Math.round(v); } } }
      }
    };
    var cv = document.getElementById('fv-band-chart');
    if (chart) { chart.data.labels = labels; chart.data.datasets = sets; chart.options = opts; chart.update('none'); }
    else chart = new Chart(cv.getContext('2d'), { type: 'line', data: { labels: labels, datasets: sets }, options: opts });
  }

  var timer = 0;
  function schedule() { clearTimeout(timer); timer = setTimeout(render, 150); }
  function start() {
    var tries = 0;
    (function wait() {
      if (typeof state !== 'undefined' && state && state.historicalDataLoaded && state.historicalFairValues) { render(); return; }
      if (++tries < 120) setTimeout(wait, 1000);
    })();
    new MutationObserver(schedule).observe(document.documentElement, { attributes: true, attributeFilter: ['lang'] });
    new MutationObserver(schedule).observe(document.body, { attributes: true, attributeFilter: ['class'] });
    var hv = document.getElementById('chart-composite-value');
    if (hv) new MutationObserver(schedule).observe(hv, { childList: true, characterData: true, subtree: true });
  }
  if (document.readyState === 'loading') document.addEventListener('DOMContentLoaded', start); else start();
  window.ethvalFvBand = { render: function () { lastKey = ''; render(); }, EDGES: EDGES, series: series, monthTicks: monthTicks };
})();
