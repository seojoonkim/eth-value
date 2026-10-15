/* ETHval — Relative value: ETH vs SOL vs BTC (section 03, right after the Triple Point box).
 * P/Fees = market cap ÷ annualised fees, P/Revenue = market cap ÷ annualised revenue.
 * Data (two single-source tables, one source each, never mixed, never filled):
 *   historical_chain_fees ← DefiLlama /summary/fees/<chain> (dailyFees, dailyRevenue; complete UTC days)
 *   historical_chain_mcap ← CoinGecko /coins/<id>/market_chart (market_caps, 00:00 UTC snapshots)
 * Window: the latest 30 UTC days that all three chains have. Annualised = 30-day sum × 365 / 30.
 * Market cap = CoinGecko snapshot at 00:00 UTC right after the window ends (= that day's close).
 * A chain with a missing day in the window, or zero revenue, shows "—" with the reason (no estimate).
 */
(function () {
  'use strict';
  var CH = ['ethereum', 'solana', 'bitcoin'];
  var NAME = { ethereum: 'ETH', solana: 'SOL', bitcoin: 'BTC' };
  var COLOR = { ethereum: '#627eea', solana: '#14b8a6', bitcoin: '#f59e0b' };
  var WIN = 30, DAY = 864e5;
  var LLAMA = { ethereum: 'https://defillama.com/chain/ethereum', solana: 'https://defillama.com/chain/solana', bitcoin: 'https://defillama.com/chain/bitcoin' };

  var T = {
    en: {
      title: 'Relative value: ETH vs SOL vs BTC',
      lead: 'How many years of network fees (and revenue) the market cap is paying for. Lower = cheaper per dollar of fees. Same window, same sources and the same formula for all three chains.',
      colChain: 'Chain', colMcap: 'Market cap', colPF: 'P/Fees', colPR: 'P/Revenue',
      annFees: function (v) { return 'fees ' + v + '/yr'; }, annRev: function (v) { return 'revenue ' + v + '/yr'; },
      noRev: 'no revenue', short: function (n) { return 'only ' + n + '/30 days'; },
      chartH: 'P/Fees over time (each point = 30 days ending that day, log scale)',
      win: function (a, b) { return 'Window ' + a + ' ~ ' + b + ' (30 UTC days); market cap at ' + b + ' close.'; },
      defs: 'Fees and revenue use DefiLlama\u2019s own per-chain definitions. Fees = all transaction fees users pay (ETH also includes blob fees). Revenue = the part that is burned: ETH base + blob fees, SOL base fees. Bitcoin burns nothing, so its revenue is 0 and P/Revenue cannot be computed. That is a difference in design, not missing data. Annualised = 30-day sum \u00d7 365 \u00f7 30. If the source is missing even one day, every 30-day window that includes it is left out (a gap in the line), not estimated.',
      gaps: function (list) { return 'Days missing at the source in this period: ' + list + '. The line breaks for the 30 days after each.'; },
      gapItem: function (n, k, last) { return n + ' days (' + k + (k === 1 ? ' stretch' : ' stretches') + ', last ' + last + ')'; },
      src: 'Sources: fees/revenue DefiLlama, market cap CoinGecko (collected twice a day, nothing filled in).',
      wait: 'Loading\u2026', err: 'Data unavailable'
    },
    ko: {
      title: '상대가치: ETH vs SOL vs BTC',
      lead: '시가총액이 네트워크 수수료(와 매출) 몇 년 치를 값으로 치르고 있는지 보여줘. 낮을수록 수수료 1달러당 싸게 거래되는 거야. 세 체인 모두 같은 기간·같은 출처·같은 공식으로 계산해.',
      colChain: '체인', colMcap: '시가총액', colPF: 'P/Fees', colPR: 'P/Revenue',
      annFees: function (v) { return '수수료 연 ' + v; }, annRev: function (v) { return '매출 연 ' + v; },
      noRev: '매출 없음', short: function (n) { return '30일 중 ' + n + '일만 있음'; },
      chartH: '시간에 따른 P/Fees (각 점 = 그날까지 30일, 로그 눈금)',
      win: function (a, b) { return '기간 ' + a + ' ~ ' + b + ' (UTC 30일), 시가총액은 ' + b + ' 종가 기준.'; },
      defs: '수수료·매출은 DefiLlama가 체인마다 정한 정의를 그대로 써. 수수료 = 사용자가 낸 거래 수수료 전체(ETH는 blob 수수료 포함). 매출 = 그중 소각되는 부분: ETH는 기본 수수료+blob 수수료, SOL은 기본 수수료. 비트코인은 소각이 없어서 매출이 0이고 P/Revenue를 계산할 수 없어. 데이터가 빠진 게 아니라 설계가 다른 거야. 연환산 = 30일 합 \u00d7 365 \u00f7 30. 출처에 하루라도 빠진 날이 있으면 그날이 들어간 30일 구간은 추정하지 않고 비워 둬(선이 끊김).',
      gaps: function (list) { return '이 기간 출처에 빠진 날: ' + list + '. 빠진 날 뒤 30일 동안 선이 끊겨.'; },
      gapItem: function (n, k, last) { return n + '일(' + k + '구간, 마지막 ' + last + ')'; },
      src: '출처: 수수료·매출 DefiLlama, 시가총액 CoinGecko (하루 2번 수집, 빈 날은 채우지 않음).',
      wait: '불러오는 중\u2026', err: '데이터를 불러오지 못했어'
    },
    ja: {
      title: '相対価値：ETH・SOL・BTC',
      lead: '時価総額がネットワーク手数料（と収益）の何年分に相当するかを示します。低いほど手数料1ドルあたり割安です。3チェーンとも同じ期間・同じ出典・同じ式で計算しています。',
      colChain: 'チェーン', colMcap: '時価総額', colPF: 'P/Fees', colPR: 'P/Revenue',
      annFees: function (v) { return '手数料 年' + v; }, annRev: function (v) { return '収益 年' + v; },
      noRev: '収益なし', short: function (n) { return '30日中' + n + '日のみ'; },
      chartH: 'P/Feesの推移（各点＝その日までの30日、対数目盛）',
      win: function (a, b) { return '期間 ' + a + '～' + b + '（UTC 30日）、時価総額は' + b + '終値。'; },
      defs: '手数料・収益はDefiLlamaがチェーンごとに定めた定義をそのまま使用。手数料＝ユーザーが支払う取引手数料の全額（ETHはblob手数料を含む）。収益＝そのうちバーンされる分：ETHは基本手数料＋blob手数料、SOLは基本手数料。ビットコインはバーンがないため収益は0で、P/Revenueは計算できません（データ欠損ではなく設計の違い）。年換算＝30日合計×365÷30。出典に1日でも欠けた日があれば、その日を含む30日区間は推定せず空白にします（線が途切れます）。',
      gaps: function (list) { return 'この期間に出典で欠けている日：' + list + '。欠けた日から30日間は線が途切れます。'; },
      gapItem: function (n, k, last) { return n + '日（' + k + '区間、最終 ' + last + '）'; },
      src: '出典：手数料・収益 DefiLlama、時価総額 CoinGecko（1日2回収集、欠けた日は埋めない）。',
      wait: '読み込み中\u2026', err: 'データを取得できません'
    },
    zh: {
      title: '相对价值：ETH・SOL・BTC',
      lead: '市值相当于多少年的网络手续费（及收入）。数值越低，每1美元手续费对应的价格越便宜。三条链使用相同区间、相同来源、相同公式。',
      colChain: '链', colMcap: '市值', colPF: 'P/Fees', colPR: 'P/Revenue',
      annFees: function (v) { return '手续费 年' + v; }, annRev: function (v) { return '收入 年' + v; },
      noRev: '无收入', short: function (n) { return '30天中仅' + n + '天'; },
      chartH: 'P/Fees走势（每点＝截至当日的30天，对数刻度）',
      win: function (a, b) { return '区间 ' + a + ' ~ ' + b + '（UTC 30天），市值取' + b + '收盘。'; },
      defs: '手续费与收入沿用DefiLlama对各链的定义。手续费＝用户支付的全部交易费（ETH含blob费）。收入＝其中被销毁的部分：ETH为基础费＋blob费，SOL为基础费。比特币没有销毁，收入为0，无法计算P/Revenue，这是设计差异，不是数据缺失。年化＝30天合计×365÷30。来源缺少任何一天时，包含该日的30天区间不作估算而留空（曲线断开）。',
      gaps: function (list) { return '此期间来源缺失的日期：' + list + '。缺失日之后30天曲线断开。'; },
      gapItem: function (n, k, last) { return n + '天（' + k + '段，最后 ' + last + '）'; },
      src: '来源：手续费/收入 DefiLlama，市值 CoinGecko（每天采集两次，缺失日期不填补）。',
      wait: '加载中\u2026', err: '无法获取数据'
    }
  };
  function lang() { var l = (document.documentElement.lang || 'en').slice(0, 2); return T[l] ? l : 'en'; }
  function big(v) {
    var a = Math.abs(v);
    if (a >= 1e12) return '$' + (v / 1e12).toFixed(2) + 'T';
    if (a >= 1e9) return '$' + (v / 1e9).toFixed(1) + 'B';
    if (a >= 1e6) return '$' + (v / 1e6).toFixed(1) + 'M';
    return '$' + Math.round(v).toLocaleString('en-US');
  }
  function mult(x) { return (x >= 100 ? Math.round(x).toLocaleString('en-US') : x.toFixed(1)) + 'x'; }
  function iso(t) { return new Date(t).toISOString().slice(0, 10); }
  function el(tag, cls, text) { var e = document.createElement(tag); if (cls) e.className = cls; if (text != null) e.textContent = text; return e; }

  var S = { status: 'loading', fees: {}, mcap: {}, chart: null };

  function base() { return (typeof SUPABASE_URL !== 'undefined' ? SUPABASE_URL : 'https://rliwxedrifwxbudcutqs.supabase.co') + '/rest/v1/'; }
  function get(q) {
    var key = typeof SUPABASE_ANON_KEY !== 'undefined' ? SUPABASE_ANON_KEY : '';
    return fetch(base() + q, { headers: { apikey: key, Authorization: 'Bearer ' + key } }).then(function (r) { if (!r.ok) throw new Error('HTTP ' + r.status); return r.json(); });
  }
  function load() {
    var since = iso(Date.now() - 420 * DAY);
    var qs = [];
    CH.forEach(function (c) {
      qs.push(get('historical_chain_fees?select=date,fees_usd,revenue_usd&chain=eq.' + c + '&date=gte.' + since + '&order=date.asc&limit=1000').then(function (rows) {
        var m = {}; rows.forEach(function (r) { m[r.date] = { f: r.fees_usd == null ? null : +r.fees_usd, r: r.revenue_usd == null ? null : +r.revenue_usd }; }); S.fees[c] = m;
      }));
      qs.push(get('historical_chain_mcap?select=date,mcap_usd&chain=eq.' + c + '&date=gte.' + since + '&order=date.asc&limit=1000').then(function (rows) {
        var m = {}; rows.forEach(function (r) { m[r.date] = +r.mcap_usd; }); S.mcap[c] = m;
      }));
    });
    Promise.all(qs).then(function () { S.status = 'ok'; render(true); }, function () { S.status = 'error'; render(true); });
  }

  // Ratios for the 30-day window ending at `end` (ISO date). Strict: every day must exist, no fill.
  function windowAt(c, end) {
    var F = S.fees[c] || {}, sumF = 0, sumR = 0, nF = 0, nR = 0, t = Date.parse(end + 'T00:00:00Z');
    for (var i = 0; i < WIN; i++) {
      var d = F[iso(t - i * DAY)];
      if (d && d.f != null) { sumF += d.f; nF++; }
      if (d && d.r != null) { sumR += d.r; nR++; }
    }
    var mc = (S.mcap[c] || {})[iso(t + DAY)];
    var annF = nF === WIN ? sumF * 365 / WIN : null, annR = nR === WIN ? sumR * 365 / WIN : null;
    return {
      chain: c, mcap: mc || null, nF: nF, nR: nR, annF: annF, annR: annR,
      pf: mc && annF > 0 ? mc / annF : null, pr: mc && annR > 0 ? mc / annR : null
    };
  }
  function latestEnd() {
    // newest fees date present for all three chains whose next-day market cap also exists
    var dates = Object.keys(S.fees.ethereum || {}).sort().reverse();
    for (var i = 0; i < dates.length; i++) {
      var d = dates[i], nx = iso(Date.parse(d + 'T00:00:00Z') + DAY);
      if (CH.every(function (c) { return (S.fees[c] || {})[d] && (S.mcap[c] || {})[nx]; })) return d;
    }
    return null;
  }
  function compute() {
    if (S.status !== 'ok') return null;
    var end = latestEnd(); if (!end) return null;
    var start = iso(Date.parse(end + 'T00:00:00Z') - (WIN - 1) * DAY);
    return { end: end, start: start, rows: CH.map(function (c) { return windowAt(c, end); }) };
  }
  function series(c) {
    var out = [], ds = Object.keys(S.fees[c] || {}).sort();
    ds.forEach(function (d) { var w = windowAt(c, d), x = Date.parse(d + 'T00:00:00Z');
      if (w.pf) out.push({ x: x, y: w.pf }); else if (out.length && out[out.length - 1].y !== null) out.push({ x: x, y: null }); }); // gap stays a gap
    return out;
  }

  // Days with no fees row between the first loaded day and `end`, per chain (counted, never filled).
  function gapList(end) {
    var first = Object.keys(S.fees.ethereum || {}).sort()[0]; if (!first) return [];
    var out = [];
    CH.forEach(function (c) {
      var F = S.fees[c] || {}, n = 0, k = 0, last = null, prev = false;
      for (var t = Date.parse(first + 'T00:00:00Z'); t <= Date.parse(end + 'T00:00:00Z'); t += DAY) {
        var d = iso(t), miss = !(F[d] && F[d].f != null);
        if (miss) { n++; last = d; if (!prev) k++; }
        prev = miss;
      }
      if (n) out.push({ c: c, n: n, k: k, last: last });
    });
    return out;
  }

  var root = null, lastKey = '';
  function render(force) {
    if (!root) return;
    var L = lang(), t = T[L], res = compute();
    var key = L + '|' + S.status + '|' + (res ? res.end : '');
    if (!force && key === lastKey) return; lastKey = key;
    var prev = root.querySelector('details'), open = prev ? prev.open : false;
    if (S.chart) { S.chart.destroy(); S.chart = null; }
    root.innerHTML = '';
    var box = el('details', 'mc-box rvx-box'); box.open = open;
    var sum = el('summary'); sum.appendChild(el('span', 'mc-title', t.title));
    var meta = S.status === 'error' ? t.err : !res ? t.wait : 'P/Fees ' + res.rows.map(function (r) { return NAME[r.chain] + ' ' + (r.pf ? mult(r.pf) : '\u2014'); }).join(' \u00b7 ');
    sum.appendChild(el('span', 'mc-sum-meta rvx-meta', meta)); box.appendChild(sum); root.appendChild(box);
    root.setAttribute('data-ready', res ? '1' : '0');
    box.addEventListener('toggle', function () { if (box.open) drawChart(); });
    if (!res) return;
    root.setAttribute('data-end', res.end);

    box.appendChild(el('p', 'mc-lead', t.lead));
    var tb = el('table', 'csx-table rvx-table'), th = el('thead'), hr = el('tr');
    [t.colChain, t.colMcap, t.colPF, t.colPR].forEach(function (s) { var c = el('th', null, s); c.scope = 'col'; hr.appendChild(c); });
    th.appendChild(hr); tb.appendChild(th);
    var body = el('tbody');
    res.rows.forEach(function (r) {
      var tr = el('tr'); tr.setAttribute('data-chain', r.chain);
      ['mcap', 'annF', 'annR', 'pf', 'pr', 'nF', 'nR'].forEach(function (k) { if (r[k] != null) tr.setAttribute('data-' + k.toLowerCase(), String(r[k])); });
      var h = el('th'); h.scope = 'row'; var dot = el('span', 'rvx-dot'); dot.style.background = COLOR[r.chain]; h.appendChild(dot); h.appendChild(document.createTextNode(NAME[r.chain])); tr.appendChild(h);
      var m = el('td'); m.appendChild(el('b', null, r.mcap ? big(r.mcap) : '\u2014')); tr.appendChild(m);
      var pf = el('td');
      if (r.pf) { pf.appendChild(el('b', null, mult(r.pf))); pf.appendChild(el('small', null, t.annFees(big(r.annF)))); }
      else { pf.className = 'csx-dash'; pf.appendChild(el('b', null, '\u2014')); pf.appendChild(el('small', null, r.nF < WIN ? t.short(r.nF) : t.noRev)); }
      tr.appendChild(pf);
      var pr = el('td');
      if (r.pr) { pr.appendChild(el('b', null, mult(r.pr))); pr.appendChild(el('small', null, t.annRev(big(r.annR)))); }
      else { pr.className = 'csx-dash'; pr.appendChild(el('b', null, '\u2014')); pr.appendChild(el('small', null, r.nR < WIN ? (r.nR === 0 && r.annF ? t.noRev : t.short(r.nR)) : t.noRev)); }
      tr.appendChild(pr);
      body.appendChild(tr);
    });
    tb.appendChild(body); var w = el('div', 'csx-wrap rvx-wrap'); w.appendChild(tb); box.appendChild(w);
    box.appendChild(el('p', 'csx-note rvx-win', t.win(res.start, res.end)));
    box.appendChild(el('div', 'mc-h', t.chartH));
    var cw = el('div', 'rvx-chart'); var cv = el('canvas'); cv.id = 'relval-chart'; cw.appendChild(cv); box.appendChild(cw);
    var lg = el('div', 'rvx-legend'); CH.forEach(function (c) { var s = el('span'); var d = el('span', 'rvx-dot'); d.style.background = COLOR[c]; s.appendChild(d); s.appendChild(document.createTextNode(NAME[c])); lg.appendChild(s); }); box.appendChild(lg);
    box.appendChild(el('p', 'csx-note', t.defs));
    var sp = el('p', 'csx-note rvx-src'); sp.appendChild(document.createTextNode(t.src + ' '));
    CH.forEach(function (c, i) { var a = el('a', null, 'DefiLlama ' + NAME[c]); a.href = LLAMA[c]; a.target = '_blank'; a.rel = 'noopener'; sp.appendChild(a); if (i < CH.length - 1) sp.appendChild(document.createTextNode(' \u00b7 ')); });
    box.appendChild(sp);
    var gl = gapList(res.end);
    if (gl.length) box.appendChild(el('p', 'csx-note rvx-gaps', t.gaps(gl.map(function (g) { return NAME[g.c] + ' ' + t.gapItem(g.n, g.k, g.last); }).join(', '))));
    if (box.open) drawChart();
  }

  // x-axis month labels ("Nov '25") are ~45px wide at 12px; one label per ~84px keeps ≥20px air
  // (measured 2026-10-15: 6 labels at 390 → 3-7px gaps, at 360 → -2px overlap). 390 → 4, 360 → 3, 768 → 6 (cap).
  function xTicks(cv, w) { w = w || (cv && cv.parentNode ? cv.parentNode.clientWidth : 0) || 600; return Math.max(2, Math.min(6, Math.floor(w / 84))); }

  function drawChart() {
    var cv = document.getElementById('relval-chart');
    if (!cv || typeof Chart === 'undefined' || S.status !== 'ok') return;
    if (S.chart) { S.chart.destroy(); S.chart = null; }
    var dark = document.body.classList.contains('dark'), tc = dark ? '#9ca3af' : '#6b7280';
    var sets = CH.map(function (c) { return { label: NAME[c], data: series(c), borderColor: COLOR[c], backgroundColor: COLOR[c], borderWidth: 1.6, pointRadius: 0, tension: 0.15, spanGaps: false }; });
    S.chart = new Chart(cv.getContext('2d'), {
      type: 'line', data: { datasets: sets },
      options: {
        responsive: true, maintainAspectRatio: false, animation: false, parsing: false,
        // re-pick the x tick count when the box width changes (rotate / resize)
        onResize: function (c, size) { var n = xTicks(null, size.width); if (c.options.scales.x.ticks.maxTicksLimit !== n) { c.options.scales.x.ticks.maxTicksLimit = n; c.update('none'); } },
        interaction: { mode: 'index', intersect: false },
        plugins: { legend: { display: false }, tooltip: { callbacks: {
          title: function (c) { return iso(c[0].parsed.x); },
          label: function (c) { return c.dataset.label + ' ' + mult(c.parsed.y); } } } },
        scales: {
          x: { type: 'time', time: { unit: 'month', displayFormats: { month: "MMM ''yy" } }, grid: { display: false }, ticks: { font: { size: 12 }, color: tc, maxTicksLimit: xTicks(cv), autoSkip: true, autoSkipPadding: 16, maxRotation: 0 } },
          y: { type: 'logarithmic', position: 'right', afterDataLimits: function (sc) { sc.max *= 1.5; sc.min /= 1.25; }, // headroom so no line rides the box edge grid: { color: dark ? 'rgba(255,255,255,0.06)' : 'rgba(0,0,0,0.06)' },
            ticks: { font: { size: 12 }, color: tc, callback: function (v) { var s = String(v); return /^(1|2|5)0*$/.test(s) ? mult(v) : ''; } } }
        }
      }
    });
    root.setAttribute('data-chart', '1');
  }

  function init() {
    var tries = 0;
    (function place() {
      var anchor = document.getElementById('triple-point') || (tries > 40 ? document.getElementById('composite-sens') : null);
      if (!anchor) { if (++tries < 120) setTimeout(place, 500); return; }
      if (document.getElementById('relval')) return;
      root = el('div', 'mc-wrap rvx-outer'); root.id = 'relval';
      anchor.insertAdjacentElement('afterend', root);
      render(true); load();
      new MutationObserver(function () { render(false); }).observe(document.documentElement, { attributes: true, attributeFilter: ['lang'] });
      var wasDark = document.body.classList.contains('dark');
      new MutationObserver(function () { var d = document.body.classList.contains('dark'); if (d !== wasDark) { wasDark = d; if (S.chart) drawChart(); } }).observe(document.body, { attributes: true, attributeFilter: ['class'] });
    })();
  }
  if (document.readyState === 'loading') document.addEventListener('DOMContentLoaded', init); else init();
  window.ethvalRelval = { state: S, compute: compute, windowAt: windowAt, series: series, WIN: WIN };
})();
