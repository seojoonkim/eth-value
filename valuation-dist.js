/* ETHval — Model spread strip inside the 01 Valuation price card.
 * Shows where every active model's fair value sits against the current price (log scale),
 * plus how many models read undervalued / fair / overvalued (same ±15% rule as the summary).
 * Reads the already-rendered .valuation-model cards; no new data source. */
(function () {
  'use strict';
  var T = {
    en: { title: 'Model spread', cur: 'Now', avg: 'Avg', under: 'Under', fair: 'Fair', over: 'Over', of: function (n) { return n + ' models'; }, note: 'log scale · each dot = one model' },
    ko: { title: '모델 분포', cur: '현재가', avg: '평균', under: '저평가', fair: '적정', over: '고평가', of: function (n) { return n + '개 모델'; }, note: '로그 눈금 · 점 하나 = 모델 하나' },
    zh: { title: '模型分布', cur: '现价', avg: '平均', under: '低估', fair: '合理', over: '高估', of: function (n) { return n + ' 个模型'; }, note: '对数刻度 · 每个点 = 一个模型' },
    ja: { title: 'モデル分布', cur: '現在値', avg: '平均', under: '割安', fair: '適正', over: '割高', of: function (n) { return n + 'モデル'; }, note: '対数目盛 · 点1つ = 1モデル' }
  };
  function lang() {
    var l = (document.documentElement.lang || 'en').slice(0, 2);
    return T[l] ? l : 'en';
  }
  function money(v) {
    if (v >= 10000) return '$' + Math.round(v).toLocaleString('en-US');
    return '$' + v.toLocaleString('en-US', { minimumFractionDigits: 1, maximumFractionDigits: 1 });
  }
  function currentPrice() {
    try { if (typeof state !== 'undefined' && state && state.price > 0) return state.price; } catch (e) { /* ignore */ }
    var el = document.getElementById('summary-current-price');
    var n = el ? parseFloat((el.textContent || '').replace(/[^0-9.]/g, '')) : NaN;
    return n > 0 ? n : 0;
  }
  function models() {
    var out = [];
    document.querySelectorAll('.valuation-model').forEach(function (card) {
      var t = card.querySelector('.model-toggle input');
      if (t && !t.checked) return;
      var p = parseFloat(card.dataset.price);
      if (!(p > 0)) return;
      var n = card.querySelector('.model-name');
      out.push({ name: n ? n.textContent.trim() : (card.dataset.model || ''), price: p });
    });
    return out;
  }
  function el(tag, cls, text) {
    var e = document.createElement(tag);
    if (cls) e.className = cls;
    if (text != null) e.textContent = text;
    return e;
  }
  var lastKey = '';
  function render() {
    var box = document.querySelector('#valuation .summary-valuation-box');
    if (!box) return;
    var ms = models(), cur = currentPrice();
    if (ms.length < 2 || !(cur > 0)) return;
    var L = lang(), t = T[L];
    var key = L + '|' + cur + '|' + ms.map(function (m) { return m.price; }).join(',');
    if (key === lastKey && box.querySelector('.vd')) return;
    lastKey = key;

    var prices = ms.map(function (m) { return m.price; });
    var avg = prices.reduce(function (a, b) { return a + b; }, 0) / prices.length;
    var lo = Math.min.apply(null, prices.concat([cur])), hi = Math.max.apply(null, prices.concat([cur]));
    var a = Math.log(lo), span = Math.log(hi) - a || 1;
    function edge(tag, x) { if (x < 12) tag.style.transform = 'translateX(-8px)'; else if (x > 88) tag.style.transform = 'translateX(calc(-100% + 8px))'; }
    function pos(v) { return ((Math.log(v) - a) / span) * 100; }
    var under = 0, fair = 0, over = 0;
    ms.forEach(function (m) {
      var d = (m.price - cur) / cur * 100;
      m.cls = d > 15 ? 'under' : (d < -15 ? 'over' : 'fair');
      if (m.cls === 'under') under++; else if (m.cls === 'over') over++; else fair++;
    });

    var root = box.querySelector('.vd');
    if (!root) { root = el('div', 'vd'); box.appendChild(root); }
    root.textContent = '';
    root.setAttribute('data-models', String(ms.length));

    var head = el('div', 'vd-head');
    head.appendChild(el('span', 'summary-label vd-title', t.title));
    head.appendChild(el('span', 'vd-count', t.of(ms.length)));
    root.appendChild(head);

    var strip = el('div', 'vd-strip');
    strip.setAttribute('role', 'img');
    strip.setAttribute('aria-label', t.title + ': ' + t.under + ' ' + under + ', ' + t.fair + ' ' + fair + ', ' + t.over + ' ' + over);
    var avgTag = el('span', 'vd-tag vd-tag-avg', t.avg + ' ' + money(avg));
    avgTag.style.left = pos(avg) + '%'; edge(avgTag, pos(avg));
    strip.appendChild(avgTag);
    var track = el('div', 'vd-track');
    // beeswarm: dots closer than ~4% go to alternate lanes so none hide behind another
    var laneLast = [-99, -99, -99], laneY = [0, -9, 9];
    ms.slice().sort(function (x, y) { return x.price - y.price; }).forEach(function (m) {
      var d = el('span', 'vd-dot ' + m.cls), x = pos(m.price), lane = 0;
      for (var k = 0; k < 3; k++) { if (x - laneLast[k] >= 4) { lane = k; break; } if (k === 2) lane = laneLast.indexOf(Math.min.apply(null, laneLast)); }
      laneLast[lane] = x;
      d.style.left = x + '%';
      if (laneY[lane]) d.style.marginTop = (laneY[lane] - 5) + 'px';
      d.title = m.name + ' ' + money(m.price);
      track.appendChild(d);
    });
    var cm = el('span', 'vd-mark vd-mark-cur'); cm.style.left = pos(cur) + '%'; track.appendChild(cm);
    var am = el('span', 'vd-mark vd-mark-avg'); am.style.left = pos(avg) + '%'; track.appendChild(am);
    strip.appendChild(track);
    var curTag = el('span', 'vd-tag vd-tag-cur', t.cur + ' ' + money(cur));
    curTag.style.left = pos(cur) + '%'; edge(curTag, pos(cur));
    strip.appendChild(curTag);
    root.appendChild(strip);

    var bar = el('div', 'vd-bar');
    [['under', under], ['fair', fair], ['over', over]].forEach(function (p) {
      if (!p[1]) return;
      var s = el('span', 'vd-seg ' + p[0]); s.style.flexGrow = p[1]; bar.appendChild(s);
    });
    root.appendChild(bar);

    var leg = el('div', 'vd-legend');
    [['under', t.under, under], ['fair', t.fair, fair], ['over', t.over, over]].forEach(function (p) {
      var i = el('span', 'vd-leg ' + p[0]);
      i.appendChild(el('i', 'vd-sw'));
      i.appendChild(el('span', 'vd-leg-name', p[1]));
      i.appendChild(el('b', 'vd-leg-n', String(p[2])));
      leg.appendChild(i);
    });
    root.appendChild(leg);
    root.appendChild(el('div', 'vd-note', t.note));
  }
  var timer = 0;
  function schedule() { clearTimeout(timer); timer = setTimeout(render, 120); }
  function start() {
    schedule();
    ['summary-fair-value', 'summary-current-price', 'summary-min-price'].forEach(function (id) {
      var n = document.getElementById(id);
      if (n) new MutationObserver(schedule).observe(n, { childList: true, characterData: true, subtree: true });
    });
    new MutationObserver(schedule).observe(document.documentElement, { attributes: true, attributeFilter: ['lang'] });
    document.addEventListener('change', function (e) { if (e.target && e.target.closest && e.target.closest('.model-toggle')) schedule(); });
  }
  if (document.readyState === 'loading') document.addEventListener('DOMContentLoaded', start); else start();
  window.ethvalValuationDist = { render: function () { lastKey = ''; render(); } };
})();
