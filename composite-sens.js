/* ETHval — Composite sensitivity disclosure (section 03 Ratings, under the constants box).
 * The site's composite fair value is the simple average of the 12 models (unchanged here — disclosure only).
 * This panel shows how much that number depends on the averaging choice and on any single model:
 *  1) Method table: simple average (site) vs median vs trimmed mean (drop highest and lowest 1),
 *     for today (the 12 model cards) and for every past day where all 12 models have a value in the
 *     page's own history (state.historicalFairValues — same days the backtest panel uses).
 *  2) Leave-one-out table: the simple average without each model, today and over the history.
 * Nothing is fetched, filled or estimated: today's numbers come from the rendered model cards
 * (all 12, ignoring the on/off toggles), past numbers from the stored history the charts already use.
 */
(function () {
  'use strict';

  var MODELS = ['tvlMultiple', 'stakingScarcity', 'metcalfe', 'dcf', 'l2Ecosystem', 'ps',
                'revenueYield', 'commitmentPremium', 'appCapital', 'validatorEcon', 'ethMonetary', 'ecosystemSettlement'];

  var T = {
    en: { title: 'How sensitive is the composite?', meta: function (a, m, t) { return 'Average ' + a + ' · median ' + m + ' · trimmed ' + t; },
          lead: 'The headline fair value is the simple average of the 12 models. That choice stays as it is; this box only shows how much the number would differ with another way of combining the same 12 values, and how far one model can move it.',
          h1: 'Averaging method', h2: 'Remove one model',
          method: 'Method', today: 'Today', vsAvg: 'vs average', hist: function (n) { return 'Past ' + n + ' days: vs average'; },
          mean: 'Simple average (site)', median: 'Median', trimmed: 'Trimmed mean (drop top & bottom 1)',
          model: 'Model left out', without: 'Average without it', change: 'Change',
          histMove: function (n) { return 'Past ' + n + ' days: typical change'; },
          cellNote: 'Past columns: median of the daily difference, with the lowest and highest day in brackets.',
          note: function (a, b, n) { return 'Today = the 12 model cards above (on/off toggles ignored). Past = ' + a + ' – ' + b + ', the ' + n + ' days where all 12 models have a value in the stored history used by the charts. Past fair values are recomputed with today\'s formulas and constants, so they are not the numbers the site showed at the time. Nothing is filled or estimated.'; },
          wait: 'Waiting for model values…' },
    ko: { title: '합성 적정가는 얼마나 민감할까?', meta: function (a, m, t) { return '평균 ' + a + ' · 중앙값 ' + m + ' · 절사평균 ' + t; },
          lead: '대표 적정가는 12개 모델의 단순 평균이야. 이 방식은 그대로 두고, 같은 12개 값을 다른 방식으로 합치면 숫자가 얼마나 달라지는지, 모델 하나가 얼마나 움직이는지만 보여줘.',
          h1: '합치는 방식', h2: '모델 하나 빼기',
          method: '방식', today: '오늘', vsAvg: '평균 대비', hist: function (n) { return '과거 ' + n + '일: 평균 대비'; },
          mean: '단순 평균 (사이트)', median: '중앙값', trimmed: '절사평균 (최고·최저 1개씩 제외)',
          model: '뺀 모델', without: '뺀 뒤 평균', change: '변화',
          histMove: function (n) { return '과거 ' + n + '일: 보통 변화'; },
          cellNote: '과거 칸: 날마다 차이의 중앙값, 괄호 안은 가장 낮은 날과 가장 높은 날.',
          note: function (a, b, n) { return '오늘 = 위 12개 모델 카드 값(켜기/끄기 무시). 과거 = ' + a + ' – ' + b + ', 차트가 쓰는 저장 이력에서 12개 모델 값이 모두 있는 ' + n + '일. 과거 적정가는 지금의 공식·상수로 다시 계산한 값이라 당시 사이트에 보였던 숫자가 아니야. 채우거나 추정한 값은 없어.'; },
          wait: '모델 값을 기다리는 중…' },
    zh: { title: '综合公允价值有多敏感？', meta: function (a, m, t) { return '平均 ' + a + ' · 中位数 ' + m + ' · 截尾平均 ' + t; },
          lead: '主公允价值是 12 个模型的简单平均。这个方法保持不变；这里只展示用其他方式合并同样 12 个值时数字会差多少，以及单个模型能让它移动多少。',
          h1: '合并方式', h2: '去掉一个模型',
          method: '方式', today: '今天', vsAvg: '相对平均', hist: function (n) { return '过去 ' + n + ' 天：相对平均'; },
          mean: '简单平均（本站）', median: '中位数', trimmed: '截尾平均（去掉最高、最低各 1 个）',
          model: '去掉的模型', without: '去掉后的平均', change: '变化',
          histMove: function (n) { return '过去 ' + n + ' 天：通常变化'; },
          cellNote: '过去列：每日差异的中位数，括号内为最低日和最高日。',
          note: function (a, b, n) { return '今天 = 上方 12 个模型卡片的值（忽略开关）。过去 = ' + a + ' – ' + b + '，图表所用已存历史中 12 个模型值齐全的 ' + n + ' 天。过去的公允价值是用现在的公式和常数重新计算的，并非当时网站显示的数字。没有填补或估算的值。'; },
          wait: '正在等待模型数值…' },
    ja: { title: '合成適正価値はどれくらい敏感か', meta: function (a, m, t) { return '平均 ' + a + ' · 中央値 ' + m + ' · トリム平均 ' + t; },
          lead: '代表の適正価値は 12 モデルの単純平均。この方式は変えず、同じ 12 個の値を別の方法でまとめると数字がどれだけ変わるか、1 つのモデルがどれだけ動かすかだけを示す。',
          h1: 'まとめ方', h2: 'モデルを 1 つ外す',
          method: '方式', today: '今日', vsAvg: '平均との差', hist: function (n) { return '過去 ' + n + ' 日：平均との差'; },
          mean: '単純平均（サイト）', median: '中央値', trimmed: 'トリム平均（最高・最低を 1 つずつ除外）',
          model: '外したモデル', without: '外した後の平均', change: '変化',
          histMove: function (n) { return '過去 ' + n + ' 日：通常の変化'; },
          cellNote: '過去の列：日ごとの差の中央値、括弧内は最も低い日と最も高い日。',
          note: function (a, b, n) { return '今日 = 上の 12 モデルカードの値（オン/オフは無視）。過去 = ' + a + ' – ' + b + '、チャートが使う保存履歴で 12 モデルの値がそろう ' + n + ' 日。過去の適正価値は現在の式・定数で再計算した値で、当時サイトに表示されていた数字ではない。補完・推定した値はない。'; },
          wait: 'モデル値を待っています…' }
  };
  function lang() { var l = (document.documentElement.lang || 'en').slice(0, 2); return T[l] ? l : 'en'; }
  function ymd(d) { return new Date(d).toISOString().slice(0, 10); }
  function sorted(a) { return a.slice().sort(function (p, q) { return p - q; }); }
  function mean(a) { var s = 0; for (var i = 0; i < a.length; i++) s += a[i]; return s / a.length; }
  function median(a) { var x = sorted(a), n = x.length; return n % 2 ? x[(n - 1) / 2] : (x[n / 2 - 1] + x[n / 2]) / 2; }
  function trimmed(a) { return mean(sorted(a).slice(1, -1)); }
  function money(v) { return '$' + Math.round(v).toLocaleString('en-US'); }
  function pct(v) { var r = Math.round(v * 10) / 10; return (r > 0 ? '+' : (r < 0 ? '−' : '')) + Math.abs(r).toFixed(1) + '%'; }
  function pct0(v) { var r = Math.round(v); return (r > 0 ? '+' : (r < 0 ? '−' : '')) + Math.abs(r) + '%'; }

  // Today: all 12 model cards with a valid price (toggles ignored — this is about the site's default composite).
  function today() {
    var by = {};
    document.querySelectorAll('.valuation-model').forEach(function (c) {
      var p = parseFloat(c.dataset.price), n = c.querySelector('.model-name');
      if (p > 0 && isFinite(p)) by[c.dataset.model] = { price: p, name: n ? n.textContent.trim() : c.dataset.model };
    });
    var out = [];
    for (var i = 0; i < MODELS.length; i++) { if (!by[MODELS[i]]) return null; out.push({ m: MODELS[i], name: by[MODELS[i]].name, price: by[MODELS[i]].price }); }
    return out;
  }

  // Past: per day, the 12 model values (only days where all 12 exist).
  function history() {
    if (typeof state === 'undefined' || !state || !state.historicalFairValues) return null;
    var H = state.historicalFairValues, maps = [];
    for (var m = 0; m < MODELS.length; m++) {
      var arr = H[MODELS[m]]; if (!arr || !arr.length) return null;
      var mp = new Map(); arr.forEach(function (d) { if (d.value > 0 && isFinite(d.value)) mp.set(ymd(d.date), d.value); });
      maps.push(mp);
    }
    var days = [];
    Array.from(maps[0].keys()).sort().forEach(function (k) {
      var v = [];
      for (var i = 0; i < maps.length; i++) { var x = maps[i].get(k); if (!x) return; v.push(x); }
      days.push({ date: k, v: v });
    });
    return days.length ? days : null;
  }

  function stat(a) { return { med: median(a), min: Math.min.apply(null, a), max: Math.max.apply(null, a) }; }

  function compute() {
    var t = today(); if (!t) return null;
    var vals = t.map(function (x) { return x.price; }), avg = mean(vals);
    var res = { today: { mean: avg, median: median(vals), trimmed: trimmed(vals) }, models: [], days: 0 };
    var h = history(), dMed = [], dTrim = [], loo = MODELS.map(function () { return []; });
    if (h) {
      h.forEach(function (d) {
        var a = mean(d.v);
        dMed.push((median(d.v) / a - 1) * 100); dTrim.push((trimmed(d.v) / a - 1) * 100);
        for (var i = 0; i < d.v.length; i++) loo[i].push(((a * d.v.length - d.v[i]) / (d.v.length - 1) / a - 1) * 100);
      });
      res.days = h.length; res.first = h[0].date; res.last = h[h.length - 1].date;
      res.hist = { median: stat(dMed), trimmed: stat(dTrim) };
    }
    t.forEach(function (x, i) {
      var w = (avg * vals.length - x.price) / (vals.length - 1);
      var o = { m: x.m, name: x.name, without: w, change: (w / avg - 1) * 100 };
      if (h) { var abs = loo[i].map(Math.abs); o.hist = stat(loo[i]); o.histAbsMed = median(abs); }
      res.models.push(o);
    });
    res.models.sort(function (p, q) { return Math.abs(q.change) - Math.abs(p.change); });
    return res;
  }

  function el(tag, cls, text) { var e = document.createElement(tag); if (cls) e.className = cls; if (text != null) e.textContent = text; return e; }
  function histCell(s) {
    var td = el('td'); td.appendChild(el('b', null, pct(s.med)));
    td.appendChild(el('small', null, '(' + pct0(s.min) + ' ~ ' + pct0(s.max) + ')'));
    td.setAttribute('data-med', s.med.toFixed(3)); return td;
  }

  var root = null, lastKey = '';
  function render() {
    if (!root) return;
    var L = lang(), t = T[L], res = compute();
    var key = L + '|' + (res ? [res.today.mean.toFixed(4), res.days, res.last].join(':') : 'x');
    if (key === lastKey) return; lastKey = key;
    var open = root.querySelector('details') ? root.querySelector('details').open : false;
    root.innerHTML = '';
    var box = el('details', 'mc-box csx-box'); box.open = open;
    var sum = el('summary'); sum.appendChild(el('span', 'mc-title', t.title));
    var meta = el('span', 'mc-sum-meta csx-meta', res ? t.meta(money(res.today.mean), money(res.today.median), money(res.today.trimmed)) : t.wait);
    sum.appendChild(meta); box.appendChild(sum);
    root.appendChild(box);
    if (!res) { root.setAttribute('data-ready', '0'); return; }

    box.appendChild(el('p', 'mc-lead', t.lead));
    // 1) methods
    box.appendChild(el('div', 'mc-h', t.h1));
    var tb = el('table', 'csx-table csx-methods'), th = el('thead'), hr = el('tr');
    [t.method, t.today, t.vsAvg].concat(res.days ? [t.hist(res.days.toLocaleString('en-US'))] : []).forEach(function (s, i) { var c = el('th', null, s); c.scope = 'col'; hr.appendChild(c); });
    th.appendChild(hr); tb.appendChild(th);
    var body = el('tbody');
    [['mean', t.mean], ['median', t.median], ['trimmed', t.trimmed]].forEach(function (r) {
      var tr = el('tr'); tr.setAttribute('data-method', r[0]); if (r[0] === 'mean') tr.className = 'csx-site';
      var h = el('th', null, r[1]); h.scope = 'row'; tr.appendChild(h);
      var v = res.today[r[0]], d = (v / res.today.mean - 1) * 100;
      var c1 = el('td'); c1.appendChild(el('b', null, money(v))); c1.setAttribute('data-v', v.toFixed(2)); tr.appendChild(c1);
      var c2 = el('td'); c2.appendChild(el('b', null, r[0] === 'mean' ? '—' : pct(d))); c2.setAttribute('data-d', d.toFixed(3)); tr.appendChild(c2);
      if (res.days) tr.appendChild(r[0] === 'mean' ? el('td', 'csx-dash', '—') : histCell(res.hist[r[0]]));
      body.appendChild(tr);
    });
    tb.appendChild(body); var w1 = el('div', 'csx-wrap'); w1.appendChild(tb); box.appendChild(w1);

    // 2) leave one out
    box.appendChild(el('div', 'mc-h', t.h2));
    var tb2 = el('table', 'csx-table csx-loo'), th2 = el('thead'), hr2 = el('tr');
    [t.model, t.without, t.change].concat(res.days ? [t.histMove(res.days.toLocaleString('en-US'))] : []).forEach(function (s) { var c = el('th', null, s); c.scope = 'col'; hr2.appendChild(c); });
    th2.appendChild(hr2); tb2.appendChild(th2);
    var b2 = el('tbody');
    res.models.forEach(function (o) {
      var tr = el('tr'); tr.setAttribute('data-model', o.m);
      var h = el('th', null, o.name); h.scope = 'row'; tr.appendChild(h);
      var c1 = el('td'); c1.appendChild(el('b', null, money(o.without))); c1.setAttribute('data-v', o.without.toFixed(2)); tr.appendChild(c1);
      var c2 = el('td'); var bb = el('b', o.change >= 0 ? 'pos' : 'neg', pct(o.change)); c2.appendChild(bb); c2.setAttribute('data-d', o.change.toFixed(3)); tr.appendChild(c2);
      if (res.days) tr.appendChild(histCell(o.hist));
      b2.appendChild(tr);
    });
    tb2.appendChild(b2); var w2 = el('div', 'csx-wrap'); w2.appendChild(tb2); box.appendChild(w2);
    if (res.days) box.appendChild(el('p', 'csx-note', t.cellNote));
    box.appendChild(el('p', 'csx-note', res.days ? t.note(res.first, res.last, res.days.toLocaleString('en-US')) : t.note('—', '—', 0)));
    root.setAttribute('data-ready', '1'); root.setAttribute('data-days', String(res.days));
    if (res.days) { root.setAttribute('data-first', res.first); root.setAttribute('data-last', res.last); }
  }

  var timer = 0;
  function schedule() { clearTimeout(timer); timer = setTimeout(render, 250); }
  function init() {
    var tries = 0;
    (function place() {
      var mc = document.getElementById('model-constants');
      if (!mc) { if (++tries < 120) setTimeout(place, 500); return; }
      if (document.getElementById('composite-sens')) return;
      root = el('div', 'mc-wrap csx-wrap-outer'); root.id = 'composite-sens';
      mc.insertAdjacentElement('afterend', root);
      render();
      var n = 0, iv = setInterval(function () { render(); if ((root.getAttribute('data-days') > 0) || ++n > 120) clearInterval(iv); }, 1000);
      new MutationObserver(schedule).observe(document.documentElement, { attributes: true, attributeFilter: ['lang'] });
      var cp = document.getElementById('composite-price');
      if (cp) new MutationObserver(schedule).observe(cp, { attributes: true, attributeFilter: ['data-value'] });
    })();
  }
  if (document.readyState === 'loading') document.addEventListener('DOMContentLoaded', init); else init();
  window.ethvalSens = { compute: compute, render: function () { lastKey = ''; render(); }, MODELS: MODELS };
})();
