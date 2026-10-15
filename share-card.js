/* ETHval share image (Simon 2026-10-15: "훨씬 더 예쁘고 실제 스크린샷처럼").
 * Drawn directly on a canvas (no html2canvas): a browser window with the live ETHval valuation panel inside.
 * All numbers are read from what the page already rendered, so the image matches the screen exactly.
 * 1200x675 (X/OG 16:9) at 2x. Theme follows the site theme. */
(function () {
  'use strict';
  var W = 1200, H = 675, S = 2;
  var FONT = '"Pretendard Variable", Pretendard, "Plus Jakarta Sans", -apple-system, BlinkMacSystemFont, "Apple SD Gothic Neo", sans-serif';
  var MONO = '"SF Mono", "JetBrains Mono", Menlo, Consolas, monospace';

  var T = {
    en: { fair: 'Composite fair value', cur: 'Current price', vs: 'vs current', spread: 'Model spread', now: 'Now', avg: 'Avg', models: 'models', under: 'Undervalued', fair2: 'Fair', over: 'Overvalued', signals: 'Model signals', buy: 'Buy', hold: 'Hold', sell: 'Sell', news: 'News impact · 7d', tag: 'ETH fair value from 12 valuation models', upd: 'Updated' },
    ko: { fair: '종합 적정 가치', cur: '현재 가격', vs: '현재가 대비', spread: '모델 분포', now: '현재가', avg: '평균', models: '개 모델', under: '저평가', fair2: '적정', over: '고평가', signals: '모델 신호', buy: '매수', hold: '보유', sell: '매도', news: '뉴스 영향 · 7일', tag: '12개 가치평가 모델로 본 ETH 적정 가치', upd: '업데이트' },
    zh: { fair: '综合合理价值', cur: '当前价格', vs: '相对现价', spread: '模型分布', now: '现价', avg: '平均', models: ' 个模型', under: '低估', fair2: '合理', over: '高估', signals: '模型信号', buy: '买入', hold: '持有', sell: '卖出', news: '新闻影响 · 7天', tag: '12 个估值模型得出的 ETH 合理价值', upd: '更新' },
    ja: { fair: '総合適正価値', cur: '現在価格', vs: '現在値比', spread: 'モデル分布', now: '現在値', avg: '平均', models: 'モデル', under: '割安', fair2: '適正', over: '割高', signals: 'モデルシグナル', buy: '買い', hold: '保有', sell: '売り', news: 'ニュース影響 · 7日', tag: '12の評価モデルで見るETH適正価値', upd: '更新' }
  };
  function lang() { var l = (document.documentElement.lang || 'en').slice(0, 2); return T[l] ? l : 'en'; }
  function txt(id) { var e = document.getElementById(id); return e ? (e.textContent || '').trim() : ''; }
  function num(s) { var n = parseFloat(String(s || '').replace(/[^0-9.\-]/g, '')); return isFinite(n) ? n : NaN; }

  function readData() {
    var models = [];
    document.querySelectorAll('.valuation-model').forEach(function (card) {
      var t = card.querySelector('.model-toggle input');
      if (t && !t.checked) return;
      var p = parseFloat(card.dataset.price);
      if (p > 0) models.push(p);
    });
    var cur = 0;
    try { if (typeof state !== 'undefined' && state && state.price > 0) cur = state.price; } catch (e) { /* ignore */ }
    if (!(cur > 0)) cur = num(txt('summary-current'));
    var verdictEl = document.getElementById('verdict');
    var tone = verdictEl ? (['bullish', 'bearish', 'neutral'].find(function (c) { return verdictEl.classList.contains(c); }) || 'neutral') : 'neutral';
    var ns = document.querySelector('.ni-v-score');
    var upd = document.querySelector('[data-i18n="hero.dataUpdated"], .data-updated, #data-updated');
    return {
      fair: txt('composite-price'), current: txt('summary-current'), curN: cur,
      diff: txt('composite-diff'), verdict: verdictEl ? verdictEl.textContent.trim() : '', tone: tone,
      buy: +txt('buy-count') || 0, hold: +txt('hold-count') || 0, sell: +txt('sell-count') || 0,
      n: txt('conclusion-model-count') || String(models.length), models: models,
      news: ns ? ns.textContent.trim() : '', newsTone: ns ? (ns.classList.contains('bear') || /^-/.test(ns.textContent.trim()) ? 'bear' : 'bull') : ''
    };
  }

  function rr(c, x, y, w, h, r) { c.beginPath(); c.moveTo(x + r, y); c.arcTo(x + w, y, x + w, y + h, r); c.arcTo(x + w, y + h, x, y + h, r); c.arcTo(x, y + h, x, y, r); c.arcTo(x, y, x + w, y, r); c.closePath(); }
  function fitText(c, s, maxW, size, weight, family) {
    var f = size; c.font = weight + ' ' + f + 'px ' + family;
    while (c.measureText(s).width > maxW && f > 10) { f -= 1; c.font = weight + ' ' + f + 'px ' + family; }
    return f;
  }

  function draw(d) {
    var dark = document.body.classList.contains('dark');
    var L = T[lang()];
    var P = dark ? {
      bg1: '#1b1530', bg2: '#0b0b10', win: '#141419', chrome: '#1d1d24', bar: '#26262f', card: '#1c1c23', text: '#f5f5f7', sub: '#a1a1aa', faint: '#6e6e78', line: 'rgba(255,255,255,0.08)', accent: '#a78bfa'
    } : {
      bg1: '#efe9ff', bg2: '#f6f7fb', win: '#ffffff', chrome: '#f2f2f5', bar: '#e6e6ec', card: '#f6f6f9', text: '#1d1d1f', sub: '#6e6e73', faint: '#9a9aa2', line: 'rgba(0,0,0,0.07)', accent: '#6d4fe0'
    };
    var G = dark ? '#34d399' : '#14935f', R = dark ? '#f87171' : '#d93848', Y = dark ? '#fbbf24' : '#b7860b';
    var toneC = d.tone === 'bullish' ? G : d.tone === 'bearish' ? R : Y;

    var cv = document.createElement('canvas'); cv.width = W * S; cv.height = H * S;
    var c = cv.getContext('2d'); c.scale(S, S); c.textBaseline = 'alphabetic';

    // backdrop: soft brand gradient + glow
    var g = c.createLinearGradient(0, 0, W, H); g.addColorStop(0, P.bg1); g.addColorStop(1, P.bg2);
    c.fillStyle = g; c.fillRect(0, 0, W, H);
    var rg = c.createRadialGradient(W * 0.8, -40, 10, W * 0.8, -40, 620);
    rg.addColorStop(0, dark ? 'rgba(139,92,246,0.35)' : 'rgba(139,92,246,0.22)'); rg.addColorStop(1, 'rgba(139,92,246,0)');
    c.fillStyle = rg; c.fillRect(0, 0, W, H);

    // window with shadow
    var wx = 56, wy = 44, ww = W - 112, wh = H - 88;
    c.save(); c.shadowColor = dark ? 'rgba(0,0,0,0.55)' : 'rgba(40,24,90,0.22)'; c.shadowBlur = 50; c.shadowOffsetY = 18;
    rr(c, wx, wy, ww, wh, 18); c.fillStyle = P.win; c.fill(); c.restore();
    c.save(); rr(c, wx, wy, ww, wh, 18); c.clip();
    // chrome bar
    c.fillStyle = P.chrome; c.fillRect(wx, wy, ww, 46);
    [['#ff5f57', 0], ['#febc2e', 1], ['#28c840', 2]].forEach(function (a) { c.beginPath(); c.arc(wx + 24 + a[1] * 20, wy + 23, 6.5, 0, Math.PI * 2); c.fillStyle = a[0]; c.fill(); });
    rr(c, wx + ww / 2 - 170, wy + 11, 340, 24, 12); c.fillStyle = P.bar; c.fill();
    c.fillStyle = P.sub; c.font = '500 13px ' + FONT; c.textAlign = 'center';
    c.fillText('🔒 ethval.com', wx + ww / 2, wy + 28); c.textAlign = 'left';
    c.fillStyle = P.line; c.fillRect(wx, wy + 46, ww, 1);
    c.restore();

    var x0 = wx + 40, y0 = wy + 46 + 34, inner = ww - 80;
    // app header
    var lg = c.createLinearGradient(x0, y0, x0 + 34, y0 + 34); lg.addColorStop(0, '#667eea'); lg.addColorStop(0.5, '#764ba2'); lg.addColorStop(1, '#f093fb');
    rr(c, x0, y0, 34, 34, 9); c.fillStyle = lg; c.fill();
    c.fillStyle = '#fff'; c.font = '700 20px ' + FONT; c.textAlign = 'center'; c.fillText('Ξ', x0 + 17, y0 + 24); c.textAlign = 'left';
    c.fillStyle = P.text; c.font = '700 22px ' + FONT; c.fillText('ETHval', x0 + 46, y0 + 25);
    c.fillStyle = P.sub; c.font = '500 14px ' + FONT;
    var loc = { ko: 'ko-KR', zh: 'zh-CN', ja: 'ja-JP', en: 'en-US' }[lang()];
    var stamp = new Date().toLocaleString(loc, { year: 'numeric', month: 'short', day: 'numeric', hour: '2-digit', minute: '2-digit' });
    c.textAlign = 'right'; c.fillText(stamp, x0 + inner, y0 + 23); c.textAlign = 'left';

    // left panel: hero number
    var py = y0 + 62, lw = 560, PH = 360;
    rr(c, x0, py, lw, PH, 16); c.fillStyle = P.card; c.fill();
    var lx = x0 + 30;
    c.fillStyle = P.sub; c.font = '600 15px ' + FONT; c.fillText(L.fair + ' · ' + d.n + ' / 12', lx, py + 44);
    var fs = fitText(c, d.fair || '$--', lw - 60, 76, '800', FONT);
    c.fillStyle = P.text; c.fillText(d.fair || '$--', lx, py + 52 + fs);
    var yy = py + 60 + fs + 30;
    // diff pill + verdict
    var diffS = (d.diff || '').replace(/\s+/g, ' ');
    var pct = (diffS.match(/[+\-−]?\d[\d.,]*%/) || [diffS])[0] + ' ' + L.vs;
    c.font = '700 20px ' + FONT; var pw = c.measureText(pct).width + 30;
    rr(c, lx, yy, pw, 38, 19); c.fillStyle = toneC + (dark ? '33' : '1f'); c.fill();
    c.fillStyle = toneC; c.fillText(pct, lx + 15, yy + 26);
    c.font = '600 17px ' + FONT; c.fillStyle = toneC;
    var vtxt = (d.verdict || '').replace(/[▲▼◆]\s*/g, '');
    if (/^[A-Z\s]+$/.test(vtxt)) vtxt = vtxt.charAt(0) + vtxt.slice(1).toLowerCase();
    c.fillText((d.tone === 'bullish' ? '▲ ' : d.tone === 'bearish' ? '▼ ' : '') + vtxt, lx + pw + 14, yy + 25);
    // price vs fair value bar (fills the gap between hero and current-price row)
    var num = function (s) { var m = String(s || '').replace(/,/g, '').match(/\d+(\.\d+)?/); return m ? parseFloat(m[0]) : NaN; };
    var fv = num(d.fair), cp = num(d.current);
    if (fv > 0 && cp > 0) {
      var by = yy + 74, bw = lw - 60, mx = Math.max(fv, cp) * 1.08;
      rr(c, lx, by, bw, 10, 5); c.fillStyle = P.line; c.fill();
      var cw = Math.max(10, bw * cp / mx), fw = Math.max(10, bw * fv / mx);
      rr(c, lx, by, fw, 10, 5); c.fillStyle = toneC + (dark ? '55' : '40'); c.fill();
      rr(c, lx, by, cw, 10, 5); c.fillStyle = P.text; c.fill();
      c.font = '600 12px ' + FONT; c.fillStyle = P.sub;
      c.textAlign = cw > 90 ? 'right' : 'left'; c.fillText(L.now, cw > 90 ? lx + cw : lx, by + 30);
      c.fillStyle = toneC; c.textAlign = 'right'; c.fillText(L.fair, lx + fw, by + 30); c.textAlign = 'left';
    }
    // current price row
    var cy = py + PH - 70;
    c.fillStyle = P.line; c.fillRect(lx, cy, lw - 60, 1);
    c.fillStyle = P.sub; c.font = '500 16px ' + FONT; c.fillText(L.cur, lx, cy + 40);
    c.fillStyle = P.text; c.font = '700 24px ' + FONT; c.textAlign = 'right'; c.fillText(d.current || '$--', x0 + lw - 30, cy + 42); c.textAlign = 'left';

    // right panel: model spread + signals
    var rx = x0 + lw + 20, rw = inner - lw - 20;
    rr(c, rx, py, rw, PH, 16); c.fillStyle = P.card; c.fill();
    var ix = rx + 26, iw = rw - 52;
    c.fillStyle = P.sub; c.font = '600 15px ' + FONT; c.fillText(L.spread, ix, py + 44);
    c.textAlign = 'right'; c.fillStyle = P.faint; c.font = '500 13px ' + FONT; c.fillText(d.models.length + L.models.replace(/^(\S)/, function (m) { return lang() === 'en' ? ' ' + m : m; }), ix + iw, py + 44); c.textAlign = 'left';
    var ms = d.models, cur = d.curN;
    if (ms.length >= 2 && cur > 0) {
      var lo = Math.min.apply(null, ms.concat([cur])), hi = Math.max.apply(null, ms.concat([cur]));
      var a = Math.log(lo), span = (Math.log(hi) - a) || 1;
      var X = function (v) { return ix + 8 + (Math.log(v) - a) / span * (iw - 16); };
      var ty = py + 118;
      rr(c, ix, ty - 2, iw, 4, 2); c.fillStyle = P.line; c.fill();
      var avg = ms.reduce(function (s, v) { return s + v; }, 0) / ms.length;
      var placed = [];
      ms.slice().sort(function (p, q) { return p - q; }).forEach(function (v) {
        var dd = (v - cur) / cur * 100, col = dd > 15 ? G : dd < -15 ? R : P.faint;
        var x = X(v), lane = 0;
        while (placed.some(function (p) { return p.l === lane && Math.abs(p.x - x) < 13; })) lane++;
        placed.push({ x: x, l: lane });
        var y = ty + (lane === 0 ? 0 : (lane % 2 ? -1 : 1) * Math.ceil(lane / 2) * 13);
        c.beginPath(); c.arc(x, y, 6, 0, Math.PI * 2); c.fillStyle = col; c.fill();
        c.lineWidth = 2; c.strokeStyle = P.card; c.stroke();
      });
      var nx = X(cur);
      c.fillStyle = P.text; c.fillRect(nx - 1, ty - 34, 2, 58);
      c.font = '600 13px ' + FONT; c.textAlign = nx > ix + iw - 60 ? 'right' : nx < ix + 60 ? 'left' : 'center';
      c.fillText(L.now, nx, ty + 46);
      var ax = X(avg); c.fillStyle = P.accent; c.fillRect(ax - 1, ty - 34, 2, 58);
      c.textAlign = ax > ix + iw - 60 ? 'right' : ax < ix + 60 ? 'left' : 'center';
      c.fillText(L.avg, ax, ty - 42); c.textAlign = 'left';
    }
    // signals bar
    var sy = py + 214, tot = (d.buy + d.hold + d.sell) || 1, bx = ix;
    c.fillStyle = P.sub; c.font = '600 15px ' + FONT; c.fillText(L.signals, ix, sy);
    c.save(); rr(c, ix, sy + 16, iw, 12, 6); c.clip();
    [[d.buy, G], [d.hold, P.faint], [d.sell, R]].forEach(function (s) { var w = iw * s[0] / tot; c.fillStyle = s[1]; c.fillRect(bx, sy + 16, w, 12); bx += w; });
    c.restore();
    var colW = iw / 3;
    [[d.buy, L.buy, G], [d.hold, L.hold, P.text], [d.sell, L.sell, R]].forEach(function (s, i) {
      var cx = ix + colW * i;
      c.fillStyle = s[2]; c.font = '800 30px ' + FONT; c.fillText(String(s[0]), cx, sy + 72);
      var w = c.measureText(String(s[0])).width;
      c.fillStyle = P.sub; c.font = '500 14px ' + FONT; c.fillText(s[1], cx + w + 8, sy + 70);
    });
    if (d.news) {
      c.fillStyle = P.faint; c.font = '500 13px ' + FONT; c.fillText(L.news, ix, py + PH - 26);
      c.fillStyle = d.newsTone === 'bear' ? R : G; c.font = '700 14px ' + FONT; c.textAlign = 'right'; c.fillText(d.news, ix + iw, py + PH - 26); c.textAlign = 'left';
    }

    // footer line
    var fy = py + PH + 40;
    c.fillStyle = P.sub; c.font = '500 15px ' + FONT; c.fillText(L.tag, x0, fy);
    c.fillStyle = P.accent; c.font = '700 15px ' + FONT; c.textAlign = 'right'; c.fillText('ethval.com', x0 + inner, fy); c.textAlign = 'left';
    return cv;
  }

  window.renderEthvalShareImage = function () {
    var ready = (document.fonts && document.fonts.ready) ? document.fonts.ready : Promise.resolve();
    return ready.then(function () { return draw(readData()).toDataURL('image/png'); });
  };
})();
