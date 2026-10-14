/* ETHval — Live burn counter + burn leaderboard (section 02.3 Supply Dynamics, after the supply commentary).
 * One source only: ultrasound.money public API /api/fees/grouped-analysis-1 (no key, CORS open).
 *   feesBurned.feesBurned{1h,24h,7d}(+Usd)  = ETH burned in the window (wei)
 *   leaderboards.leaderboard{24h,7d}        = top 100 burners in the window as the source groups them
 *                                             (contract / "ETH transfers" / "new contracts" / "blob fees")
 * Share = entry burn ÷ the source's own window total. Names/categories are the source's labels; unlabelled
 * contracts are shown by address only (we never guess an app name). The source gives blob fees as ONE line for
 * all L2s together, so there is no per-L2 ranking here — that split is not in the data, so it is not drawn.
 * Fetched only when the box is near the viewport; refreshed every 60 s while open and the tab is visible.
 * On failure: "Data unavailable" (no fallback, no estimate).
 */
(function () {
  'use strict';
  var API = 'https://ultrasound.money/api/fees/grouped-analysis-1';
  var SRC = 'https://ultrasound.money/';
  var TOP = 10, WINS = ['24h', '7d'];

  var T = {
    en: {
      title: 'Live burn: who is burning ETH',
      lead: 'ETH destroyed by fees right now, and which apps and contracts burned the most. Every number comes straight from ultrasound.money, refreshed every minute while this box is open.',
      k1h: 'Last 1h', k24h: 'Last 24h', k7d: 'Last 7d',
      tab: { '24h': '24h', '7d': '7d' }, boardH: 'Top burners',
      colRank: '#', colWho: 'App / contract', colEth: 'ETH burned', colShare: 'Share',
      xfer: 'ETH transfers', create: 'New contract deployments', blob: 'Blob fees (all L2s combined)', unl: 'Unlabelled contract',
      cover: function (n, p) { return 'The source\u2019s top ' + n + ' together burned ' + p + ' of this window; the rest is spread over smaller contracts.'; },
      asof: function (b, t) { return 'Block ' + b + ' \u00b7 ' + t + ' UTC'; },
      meta: function (v) { return '24h ' + v + ' ETH burned'; },
      l2: 'Per-L2 ranking is not shown: the source reports blob fees (what L2s pay to post data) as one combined line, not per rollup. Names and categories are the source\u2019s own labels; contracts it has not labelled are shown by address only.',
      src: 'Source: ultrasound.money (burn totals and leaderboard, nothing estimated).',
      wait: 'Loading\u2026', err: 'Data unavailable'
    },
    ko: {
      title: '실시간 소각: 누가 ETH를 태우고 있나',
      lead: '지금 수수료로 사라지는 ETH 양과, 가장 많이 태운 앱·컨트랙트야. 숫자는 전부 ultrasound.money에서 그대로 가져오고, 이 상자를 열어 두면 1분마다 새로 고쳐.',
      k1h: '최근 1시간', k24h: '최근 24시간', k7d: '최근 7일',
      tab: { '24h': '24시간', '7d': '7일' }, boardH: '소각 상위',
      colRank: '#', colWho: '앱 / 컨트랙트', colEth: '소각 ETH', colShare: '비중',
      xfer: 'ETH 송금', create: '새 컨트랙트 배포', blob: 'Blob 수수료 (L2 전체 합계)', unl: '이름 없는 컨트랙트',
      cover: function (n, p) { return '출처 기준 상위 ' + n + '곳이 이 기간 소각의 ' + p + '를 차지하고, 나머지는 작은 컨트랙트들에 흩어져 있어.'; },
      asof: function (b, t) { return '블록 ' + b + ' \u00b7 ' + t + ' UTC'; },
      meta: function (v) { return '24시간 ' + v + ' ETH 소각'; },
      l2: 'L2별 순위는 보여주지 않아: 출처가 blob 수수료(L2가 데이터를 올릴 때 내는 비용)를 롤업별이 아니라 한 줄 합계로만 줘. 이름·분류는 출처가 붙인 그대로이고, 출처가 이름을 붙이지 않은 컨트랙트는 주소로만 표시해.',
      src: '출처: ultrasound.money (소각 합계·순위, 추정값 없음).',
      wait: '불러오는 중\u2026', err: '데이터를 불러오지 못했어'
    },
    ja: {
      title: 'リアルタイムのバーン：誰がETHを燃やしているか',
      lead: '手数料で今消えているETHの量と、最も多くバーンしたアプリ・コントラクト。数値はすべてultrasound.moneyからそのまま取得し、このボックスを開いている間は1分ごとに更新します。',
      k1h: '直近1時間', k24h: '直近24時間', k7d: '直近7日',
      tab: { '24h': '24時間', '7d': '7日' }, boardH: 'バーン上位',
      colRank: '#', colWho: 'アプリ／コントラクト', colEth: 'バーンETH', colShare: '比率',
      xfer: 'ETH送金', create: '新規コントラクトのデプロイ', blob: 'Blob手数料（L2全体の合計）', unl: '名称なしのコントラクト',
      cover: function (n, p) { return '出典の上位' + n + '件でこの期間のバーンの' + p + 'を占め、残りは小さなコントラクトに分散しています。'; },
      asof: function (b, t) { return 'ブロック ' + b + ' \u00b7 ' + t + ' UTC'; },
      meta: function (v) { return '24時間 ' + v + ' ETHバーン'; },
      l2: 'L2別の順位は表示しません：出典はblob手数料（L2がデータを投稿する費用）をロールアップ別ではなく1行の合計でしか提供していません。名称・分類は出典のラベルをそのまま使い、ラベルのないコントラクトはアドレスのみ表示します。',
      src: '出典：ultrasound.money（バーン合計・ランキング、推定値なし）。',
      wait: '読み込み中\u2026', err: 'データを取得できません'
    },
    zh: {
      title: '实时销毁：谁在销毁ETH',
      lead: '当前因手续费被销毁的ETH数量，以及销毁最多的应用和合约。所有数字直接来自ultrasound.money，展开此框时每分钟刷新一次。',
      k1h: '最近1小时', k24h: '最近24小时', k7d: '最近7天',
      tab: { '24h': '24小时', '7d': '7天' }, boardH: '销毁排行',
      colRank: '#', colWho: '应用／合约', colEth: '销毁ETH', colShare: '占比',
      xfer: 'ETH转账', create: '新合约部署', blob: 'Blob费用（全部L2合计）', unl: '未标注的合约',
      cover: function (n, p) { return '来源前' + n + '名合计占该区间销毁的' + p + '，其余分散在较小的合约中。'; },
      asof: function (b, t) { return '区块 ' + b + ' \u00b7 ' + t + ' UTC'; },
      meta: function (v) { return '24小时销毁 ' + v + ' ETH'; },
      l2: '不显示各L2排名：来源只把blob费用（L2发布数据所付的费用）作为一行合计提供，没有按rollup拆分。名称和分类沿用来源的标注，来源未标注的合约只显示地址。',
      src: '来源：ultrasound.money（销毁合计与排行，无估算值）。',
      wait: '加载中\u2026', err: '无法获取数据'
    }
  };
  function lang() { var l = (document.documentElement.lang || 'en').slice(0, 2); return T[l] ? l : 'en'; }
  function el(tag, cls, text) { var e = document.createElement(tag); if (cls) e.className = cls; if (text != null) e.textContent = text; return e; }
  function eth(wei) { return Number(wei) / 1e18; }
  function fEth(v) { return v >= 1000 ? Math.round(v).toLocaleString('en-US') : v >= 100 ? v.toFixed(1) : v >= 1 ? v.toFixed(2) : v.toFixed(3); }
  function fUsd(v) { var a = Math.abs(v); return a >= 1e9 ? '$' + (v / 1e9).toFixed(2) + 'B' : a >= 1e6 ? '$' + (v / 1e6).toFixed(2) + 'M' : a >= 1e3 ? '$' + (v / 1e3).toFixed(1) + 'K' : '$' + Math.round(v); }
  function pct(x) { return (x * 100 >= 10 ? (x * 100).toFixed(0) : (x * 100).toFixed(1)) + '%'; }
  function shortAddr(a) { return a.slice(0, 6) + '\u2026' + a.slice(-4); }

  // Pure: one leaderboard window from the raw payload. Exposed for the QA recompute.
  function board(raw, w) {
    var tot = raw.feesBurned['feesBurned' + w], list = (raw.leaderboards['leaderboard' + w] || []).slice();
    list.sort(function (a, b) { return Number(b.fees) - Number(a.fees); });
    var top = list.slice(0, TOP).map(function (e, i) {
      return { rank: i + 1, id: e.id, type: e.type, name: e.name || null, category: e.category || null, address: e.address || null,
        eth: eth(e.fees), usd: e.feesUsd == null ? null : Number(e.feesUsd), share: Number(e.fees) / tot };
    });
    var all = list.reduce(function (s, e) { return s + Number(e.fees); }, 0);
    return { total: eth(tot), totalUsd: raw.feesBurned['feesBurned' + w + 'Usd'], rows: top, n: list.length, cover: all / tot };
  }

  var S = { status: 'idle', raw: null, win: '24h', fetchedAt: 0 };
  var root = null, timer = 0;

  function load() {
    if (S.status === 'loading') return;
    var first = !S.raw; if (first) S.status = 'loading';
    fetch(API, { cache: 'no-store' }).then(function (r) { if (!r.ok) throw new Error('HTTP ' + r.status); return r.json(); })
      .then(function (d) {
        if (!d || !d.feesBurned || !d.leaderboards || !d.leaderboards.leaderboard24h) throw new Error('shape');
        S.raw = d; S.status = 'ok'; S.fetchedAt = Date.now(); render();
      }, function () { if (!S.raw) { S.status = 'error'; render(); } else S.status = 'ok'; });
    if (first) render();
  }
  function tick() {
    clearTimeout(timer);
    timer = setTimeout(function () { var d = root && root.querySelector('details'); if (d && d.open && !document.hidden) load(); tick(); }, 60000);
  }

  function who(e, t) {
    var cell = el('th'); cell.scope = 'row';
    if (e.type === 'eth-transfers') cell.appendChild(el('b', null, t.xfer));
    else if (e.type === 'contract-creations') cell.appendChild(el('b', null, t.create));
    else if (e.type === 'blob-fees') cell.appendChild(el('b', null, t.blob));
    else if (e.address) {
      var a = el('a', null, e.name || shortAddr(e.address)); a.href = 'https://etherscan.io/address/' + e.address; a.target = '_blank'; a.rel = 'noopener';
      var b = el('b'); b.appendChild(a); cell.appendChild(b);
      cell.appendChild(el('small', null, e.name ? (e.category ? e.category + ' \u00b7 ' : '') + shortAddr(e.address) : t.unl));
    } else cell.appendChild(el('b', null, e.name || e.id));
    return cell;
  }

  function render() {
    if (!root) return;
    var L = lang(), t = T[L], d = S.raw;
    var prev = root.querySelector('details'), open = prev ? prev.open : false;
    root.innerHTML = '';
    var box = el('details', 'mc-box bbx-box'); box.open = open;
    var sum = el('summary'); sum.appendChild(el('span', 'mc-title', t.title));
    var meta = S.status === 'error' ? t.err : !d ? t.wait : t.meta(fEth(eth(d.feesBurned.feesBurned24h)));
    sum.appendChild(el('span', 'mc-sum-meta bbx-meta', meta)); box.appendChild(sum); root.appendChild(box);
    box.addEventListener('toggle', function () { if (box.open && (!S.raw || Date.now() - S.fetchedAt > 55000)) load(); });
    root.setAttribute('data-ready', d ? '1' : '0'); root.setAttribute('data-status', S.status);
    if (!d) return;
    root.setAttribute('data-block', String(d.number)); root.setAttribute('data-win', S.win);

    box.appendChild(el('p', 'mc-lead', t.lead));
    var k = el('div', 'bbx-kpis');
    [['1h', t.k1h], ['24h', t.k24h], ['7d', t.k7d]].forEach(function (p) {
      var c = el('div', 'bbx-kpi'); c.setAttribute('data-k', p[0]);
      c.appendChild(el('small', null, p[1]));
      c.appendChild(el('b', null, fEth(eth(d.feesBurned['feesBurned' + p[0]])) + ' ETH'));
      c.appendChild(el('span', null, fUsd(d.feesBurned['feesBurned' + p[0] + 'Usd'])));
      k.appendChild(c);
    });
    box.appendChild(k);
    var mined = d.latestBlockFees && d.latestBlockFees[0] ? d.latestBlockFees[0].minedAt : null;
    box.appendChild(el('p', 'csx-note bbx-asof', t.asof(Number(d.number).toLocaleString('en-US'), mined ? mined.slice(0, 16).replace('T', ' ') : '\u2014')));

    var hd = el('div', 'bbx-hd'); hd.appendChild(el('div', 'mc-h', t.boardH));
    var tabs = el('div', 'bbx-tabs'); tabs.setAttribute('role', 'group');
    WINS.forEach(function (w) {
      var b = el('button', 'bbx-tab' + (w === S.win ? ' active' : ''), t.tab[w]); b.type = 'button'; b.setAttribute('data-win', w);
      b.setAttribute('aria-pressed', w === S.win ? 'true' : 'false');
      b.addEventListener('click', function () { S.win = w; render(); });
      tabs.appendChild(b);
    });
    hd.appendChild(tabs); box.appendChild(hd);

    var bd = board(d, S.win);
    var tb = el('table', 'csx-table bbx-table'), th = el('thead'), hr = el('tr');
    [t.colRank, t.colWho, t.colEth, t.colShare].forEach(function (s) { var c = el('th', null, s); c.scope = 'col'; hr.appendChild(c); });
    th.appendChild(hr); tb.appendChild(th);
    var body = el('tbody');
    bd.rows.forEach(function (e) {
      var tr = el('tr'); tr.setAttribute('data-id', e.id); tr.setAttribute('data-eth', String(e.eth)); tr.setAttribute('data-share', String(e.share));
      tr.appendChild(el('td', 'bbx-rank', String(e.rank)));
      tr.appendChild(who(e, t));
      var v = el('td'); v.appendChild(el('b', null, fEth(e.eth))); if (e.usd != null) v.appendChild(el('small', null, fUsd(e.usd))); tr.appendChild(v);
      var s = el('td'); s.appendChild(el('b', null, pct(e.share))); var bar = el('span', 'bbx-bar'); bar.style.width = Math.min(100, e.share / Math.max(bd.rows[0].share, 1e-9) * 100) + '%'; s.appendChild(bar); tr.appendChild(s);
      body.appendChild(tr);
    });
    tb.appendChild(body); var wr = el('div', 'csx-wrap bbx-wrap'); wr.appendChild(tb); box.appendChild(wr);
    box.appendChild(el('p', 'csx-note bbx-cover', t.cover(bd.n, pct(bd.cover))));
    box.appendChild(el('p', 'csx-note', t.l2));
    var sp = el('p', 'csx-note bbx-src'); var a = el('a', null, t.src); a.href = SRC; a.target = '_blank'; a.rel = 'noopener'; sp.appendChild(a); box.appendChild(sp);
  }

  function init() {
    var tries = 0;
    (function place() {
      var anchor = document.getElementById('commentary-supply_dynamics');
      if (!anchor) { if (++tries < 120) setTimeout(place, 500); return; }
      if (document.getElementById('burn-board')) return;
      root = el('div', 'mc-wrap bbx-outer'); root.id = 'burn-board';
      anchor.insertAdjacentElement('afterend', root);
      render();
      if ('IntersectionObserver' in window) {
        var io = new IntersectionObserver(function (es) { if (es.some(function (x) { return x.isIntersecting; })) { io.disconnect(); load(); } }, { rootMargin: '800px 0px' });
        io.observe(root);
      } else load();
      tick();
      new MutationObserver(function () { render(); }).observe(document.documentElement, { attributes: true, attributeFilter: ['lang'] });
    })();
  }
  if (document.readyState === 'loading') document.addEventListener('DOMContentLoaded', init); else init();
  window.ethvalBurnBoard = { state: S, board: board, load: load, TOP: TOP, API: API };
})();
