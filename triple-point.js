/* ETHval — Triple Point breakdown (section 03 Ratings, under the composite sensitivity box).
 * Groups the 12 valuation models into the three asset "superclasses" of David Hoffman's
 * "Ether: The Triple Point Asset" (Bankless): Capital asset (staked ETH, claim on fees),
 * Consumable / commodity (ETH consumed and moved by network activity), Store of value (ETH locked
 * as collateral / backing assets settled on Ethereum).
 * The grouping is ETHval's own reading of that framing — the article does not map valuation models.
 * Contribution of an axis = sum of its models' current values ÷ 12, so the three axes add up exactly
 * to the composite (simple average of the 12 model cards). Toggles are ignored (site default composite).
 * Nothing is fetched, filled or estimated: values come from the rendered model cards.
 */
(function () {
  'use strict';

  var AXES = [
    { id: 'capital', models: ['dcf', 'ps', 'revenueYield', 'validatorEcon', 'stakingScarcity'] },
    { id: 'commodity', models: ['ethMonetary', 'ecosystemSettlement'] },
    { id: 'sov', models: ['tvlMultiple', 'metcalfe', 'appCapital', 'l2Ecosystem', 'commitmentPremium'] }
  ];
  var ALL = AXES.reduce(function (a, x) { return a.concat(x.models); }, []);
  var SRC = 'https://www.bankless.com/ether-a-new-model-for-money';

  var T = {
    en: {
      title: 'Triple Point breakdown', meta: function (a) { return a; },
      lead: 'Bankless\u2019s \u201cTriple Point Asset\u201d idea says ETH is three kinds of asset at once: a capital asset (staked, earns fees), a consumable commodity (spent and moved to use the network) and a store of value (locked as collateral). Here the 12 models are grouped into those three and each group\u2019s share of today\u2019s composite fair value is shown as a waterfall.',
      axis: { capital: 'Capital asset', commodity: 'Consumable / commodity', sov: 'Store of value' },
      axisWhy: { capital: 'Staked ETH, claim on fees, right to validate', commodity: 'ETH used up and moved by network activity', sov: 'ETH locked as collateral, backing assets on Ethereum' },
      total: 'Composite fair value', price: 'Price', nModels: function (n) { return n + ' models'; },
      avgModel: function (v) { return 'avg per model ' + v; },
      h2: 'Why each model sits where it does', colModel: 'Model', colAxis: 'Group', colWhy: 'Reason', colAdd: 'Adds',
      why: {
        dcf: 'Treats the staking yield as ETH\u2019s cash return and discounts it: staked ETH.',
        ps: 'Prices the fees the network earns: ETH as a claim on fees.',
        revenueYield: 'Fees divided by the staking yield: ETH priced like a yielding bond.',
        validatorEcon: 'Compares the staking yield with a target return: the right to validate.',
        stakingScarcity: 'Premium from ETH staked with validators: the article\u2019s \u201cstaked ETH = capital asset\u201d.',
        metcalfe: 'Network value from DeFi TVL: its only input is value locked in DeFi.',
        ethMonetary: 'Native ETH moved in transfers (MV=PQ): ETH spent as the network\u2019s medium of exchange.',
        ecosystemSettlement: 'All token volume settled on L1/L2: throughput that consumes block space.',
        tvlMultiple: 'Value locked in DeFi: the article\u2019s \u201cETH locked in DeFi = store of value\u201d.',
        appCapital: 'Stablecoins on Ethereum: dollar assets that Ethereum\u2019s security has to back.',
        l2Ecosystem: 'L1 + L2 locked value: collateral secured by Ethereum, rollups included.',
        commitmentPremium: 'Supply taken out of circulation (staking, DeFi, L2, lost): ETH held, not traded.'
      },
      note: 'Today = the 12 model cards above (on/off toggles ignored). Group share = sum of its model values \u00f7 12, so the three groups add up exactly to the composite. The groups have 5, 2 and 5 models, so a group\u2019s share also depends on how many models it has. compare the \u201cavg per model\u201d line too. The grouping is ETHval\u2019s own reading of the framing; the article does not assign valuation models.',
      src: 'Source: David Hoffman, \u201cEther: The Triple Point Asset\u201d, Bankless',
      wait: 'Waiting for model values\u2026'
    },
    ko: {
      title: '트리플 포인트로 나눠 보기', meta: function (a) { return a; },
      lead: 'Bankless의 \u2018트리플 포인트 자산\u2019은 ETH가 세 가지 자산을 동시에 겸한다는 설명이야: 자본자산(스테이킹해 수수료를 받음), 소비되는 상품(네트워크를 쓰려고 쓰고 옮김), 가치저장(담보로 묶임). 12개 모델을 이 셋으로 묶어 오늘의 합성 적정가에서 각 묶음이 차지하는 몫을 워터폴로 보여줘.',
      axis: { capital: '자본자산', commodity: '상품(소비재)', sov: '가치저장' },
      axisWhy: { capital: '스테이킹된 ETH, 수수료 청구권, 검증 권리', commodity: '네트워크 활동에 쓰이고 옮겨지는 ETH', sov: '담보로 묶인 ETH, 이더리움 위 자산을 떠받침' },
      total: '합성 적정가', price: '현재가', nModels: function (n) { return '모델 ' + n + '개'; },
      avgModel: function (v) { return '모델당 평균 ' + v; },
      h2: '모델별 분류 근거', colModel: '모델', colAxis: '묶음', colWhy: '근거', colAdd: '기여',
      why: {
        dcf: '스테이킹 수익률을 ETH의 현금 수익으로 보고 할인: 스테이킹된 ETH.',
        ps: '네트워크가 버는 수수료에 배수를 매김: 수수료 청구권으로서의 ETH.',
        revenueYield: '수수료를 스테이킹 수익률로 나눔: 이자 주는 채권처럼 본 ETH.',
        validatorEcon: '스테이킹 수익률을 목표 수익률과 비교: 검증할 권리의 가치.',
        stakingScarcity: '검증자에 묶인 ETH만큼 희소성 프리미엄: 원문의 \u2018스테이킹 ETH = 자본자산\u2019.',
        metcalfe: 'DeFi TVL로 구한 네트워크 가치: 입력이 DeFi에 묶인 가치뿐.',
        ethMonetary: 'ETH 자체 전송량(MV=PQ): 거래 수단으로 쓰이는 ETH.',
        ecosystemSettlement: 'L1/L2에서 정산된 전체 토큰 거래량: 블록 공간을 소비하는 처리량.',
        tvlMultiple: 'DeFi에 묶인 가치: 원문의 \u2018DeFi에 잠긴 ETH = 가치저장\u2019.',
        appCapital: '이더리움 위 스테이블코인: 이더리움 보안이 떠받쳐야 하는 달러 자산.',
        l2Ecosystem: 'L1+L2에 묶인 가치: 롤업까지 포함해 이더리움이 지키는 담보.',
        commitmentPremium: '유통에서 빠진 공급(스테이킹·DeFi·L2·분실): 거래되지 않고 보유되는 ETH.'
      },
      note: '오늘 = 위 12개 모델 카드 값(켜기/끄기 무시). 묶음 몫 = 그 묶음 모델 값의 합 \u00f7 12라서 세 묶음을 더하면 합성 적정가와 정확히 같아. 묶음마다 모델 수가 5·2·5개로 달라서 몫은 모델 수의 영향도 받아. \u2018모델당 평균\u2019도 같이 봐. 이 분류는 ETHval이 원문의 틀을 해석한 것이고, 원문이 밸류에이션 모델을 나눈 건 아니야.',
      src: '출처: David Hoffman, \u201cEther: The Triple Point Asset\u201d, Bankless',
      wait: '모델 값을 기다리는 중\u2026'
    },
    zh: {
      title: '三相点拆解', meta: function (a) { return a; },
      lead: 'Bankless 的\u201c三相点资产\u201d观点认为 ETH 同时是三类资产：资本资产（质押后获得费用收益）、消耗型商品（使用网络时被花掉和转移）、价值储存（作为抵押品被锁定）。这里把 12 个模型分成这三组，用瀑布图展示每组在今天综合公允价值中的份额。',
      axis: { capital: '资本资产', commodity: '商品（消耗品）', sov: '价值储存' },
      axisWhy: { capital: '质押的 ETH、费用索取权、验证权', commodity: '被网络活动消耗和转移的 ETH', sov: '作为抵押锁定的 ETH，支撑以太坊上的资产' },
      total: '综合公允价值', price: '现价', nModels: function (n) { return n + ' 个模型'; },
      avgModel: function (v) { return '每个模型平均 ' + v; },
      h2: '各模型的分组理由', colModel: '模型', colAxis: '分组', colWhy: '理由', colAdd: '贡献',
      why: {
        dcf: '把质押收益率当作 ETH 的现金回报并折现：质押的 ETH。',
        ps: '对网络赚取的费用给出倍数：作为费用索取权的 ETH。',
        revenueYield: '费用除以质押收益率：把 ETH 当作付息债券。',
        validatorEcon: '把质押收益率与目标回报比较：验证权的价值。',
        stakingScarcity: '质押给验证者的 ETH 带来稀缺溢价：原文的\u201c质押 ETH = 资本资产\u201d。',
        metcalfe: '由 DeFi TVL 得出的网络价值：唯一输入是锁在 DeFi 的价值。',
        ethMonetary: '原生 ETH 转账量（MV=PQ）：作为交易媒介被花掉的 ETH。',
        ecosystemSettlement: 'L1/L2 上结算的全部代币交易量：消耗区块空间的吞吐量。',
        tvlMultiple: '锁定在 DeFi 的价值：原文的\u201c锁在 DeFi 的 ETH = 价值储存\u201d。',
        appCapital: '以太坊上的稳定币：需要以太坊安全性来支撑的美元资产。',
        l2Ecosystem: 'L1 + L2 锁定价值：包括 Rollup 在内由以太坊保护的抵押品。',
        commitmentPremium: '退出流通的供应（质押、DeFi、L2、丢失）：被持有而非交易的 ETH。'
      },
      note: '今天 = 上方 12 个模型卡片的值（忽略开关）。分组份额 = 该组模型值之和 \u00f7 12，因此三组相加正好等于综合公允价值。三组分别有 5、2、5 个模型，份额也受模型数量影响，请同时看\u201c每个模型平均\u201d。此分组是 ETHval 对原文框架的解读，原文并未划分估值模型。',
      src: '来源：David Hoffman，\u201cEther: The Triple Point Asset\u201d，Bankless',
      wait: '正在等待模型数值\u2026'
    },
    ja: {
      title: 'トリプルポイントで分解', meta: function (a) { return a; },
      lead: 'Bankless の「トリプルポイント資産」は、ETH が 3 種類の資産を同時に兼ねるという考え方：資本資産（ステーキングして手数料を得る）、消費される商品（ネットワークを使うために使われ移動する）、価値の保存（担保としてロックされる）。12 モデルをこの 3 つにまとめ、今日の合成適正価値に占める各グループの分をウォーターフォールで示す。',
      axis: { capital: '資本資産', commodity: '商品（消費財）', sov: '価値の保存' },
      axisWhy: { capital: 'ステーキングされた ETH、手数料の請求権、検証する権利', commodity: 'ネットワーク活動で使われ移動する ETH', sov: '担保としてロックされた ETH、イーサリアム上の資産を支える' },
      total: '合成適正価値', price: '現在価格', nModels: function (n) { return n + ' モデル'; },
      avgModel: function (v) { return '1 モデル平均 ' + v; },
      h2: 'モデルごとの分類の理由', colModel: 'モデル', colAxis: 'グループ', colWhy: '理由', colAdd: '寄与',
      why: {
        dcf: 'ステーキング利回りを ETH の現金リターンとみて割り引く：ステーキングされた ETH。',
        ps: 'ネットワークが稼ぐ手数料に倍率を掛ける：手数料の請求権としての ETH。',
        revenueYield: '手数料をステーキング利回りで割る：利付債のように見た ETH。',
        validatorEcon: 'ステーキング利回りを目標リターンと比べる：検証する権利の価値。',
        stakingScarcity: 'バリデーターにロックされた ETH の希少性プレミアム：原文の「ステーキング ETH = 資本資産」。',
        metcalfe: 'DeFi TVL から求めるネットワーク価値：入力は DeFi にロックされた価値だけ。',
        ethMonetary: 'ETH そのものの送金量（MV=PQ）：交換手段として使われる ETH。',
        ecosystemSettlement: 'L1/L2 で決済された全トークン取引量：ブロックスペースを消費する処理量。',
        tvlMultiple: 'DeFi にロックされた価値：原文の「DeFi にロックされた ETH = 価値の保存」。',
        appCapital: 'イーサリアム上のステーブルコイン：イーサリアムのセキュリティが支えるべきドル資産。',
        l2Ecosystem: 'L1 + L2 のロック価値：ロールアップを含めイーサリアムが守る担保。',
        commitmentPremium: '流通から外れた供給（ステーキング・DeFi・L2・紛失）：取引されず保有される ETH。'
      },
      note: '今日 = 上の 12 モデルカードの値（オン/オフは無視）。グループの分 = そのモデル値の合計 \u00f7 12 なので、3 グループを足すと合成適正価値とぴったり一致する。グループのモデル数は 5・2・5 と違うため、分はモデル数の影響も受ける。「1 モデル平均」も合わせて見てほしい。この分類は ETHval による原文の枠組みの解釈で、原文が評価モデルを分けたものではない。',
      src: '出典：David Hoffman「Ether: The Triple Point Asset」Bankless',
      wait: 'モデル値を待っています\u2026'
    }
  };
  function lang() { var l = (document.documentElement.lang || 'en').slice(0, 2); return T[l] ? l : 'en'; }
  function money(v) { return '$' + Math.round(v).toLocaleString('en-US'); }
  // contributions under $10 keep cents so a small model never shows as "+$0"
  function moneyC(v) { return v < 10 ? '$' + v.toFixed(2) : money(v); }
  function pctS(v) { return (Math.round(v * 10) / 10).toFixed(1) + '%'; }

  function cards() {
    var by = {};
    document.querySelectorAll('.valuation-model').forEach(function (c) {
      var p = parseFloat(c.dataset.price), n = c.querySelector('.model-name');
      if (p > 0 && isFinite(p)) by[c.dataset.model] = { price: p, name: n ? n.textContent.trim() : c.dataset.model };
    });
    for (var i = 0; i < ALL.length; i++) if (!by[ALL[i]]) return null;
    return by;
  }

  function compute() {
    var by = cards(); if (!by) return null;
    var n = ALL.length, total = 0, axes = [], start = 0;
    ALL.forEach(function (m) { total += by[m].price / n; });
    AXES.forEach(function (ax) {
      var sum = 0, models = ax.models.map(function (m) { var c = by[m].price / n; sum += c; return { m: m, name: by[m].name, price: by[m].price, contrib: c }; });
      models.sort(function (a, b) { return b.contrib - a.contrib; });
      axes.push({ id: ax.id, contrib: sum, share: sum / total * 100, start: start, models: models, avg: sum * n / ax.models.length });
      start += sum;
    });
    var price = (typeof state !== 'undefined' && state && state.price > 0) ? state.price : null;
    return { total: total, axes: axes, price: price };
  }

  function el(tag, cls, text) { var e = document.createElement(tag); if (cls) e.className = cls; if (text != null) e.textContent = text; return e; }

  var root = null, lastKey = '';
  function render() {
    if (!root) return;
    var L = lang(), t = T[L], res = compute();
    var key = L + '|' + (res ? res.total.toFixed(4) + ':' + (res.price ? res.price.toFixed(2) : '') : 'x');
    if (key === lastKey) return; lastKey = key;
    var open = root.querySelector('details') ? root.querySelector('details').open : false;
    root.innerHTML = '';
    var box = el('details', 'mc-box tpx-box'); box.open = open;
    var sum = el('summary'); sum.appendChild(el('span', 'mc-title', t.title));
    var metaTxt = res ? res.axes.map(function (a) { return t.axis[a.id] + ' ' + pctS(a.share); }).join(' \u00b7 ') : t.wait;
    sum.appendChild(el('span', 'mc-sum-meta tpx-meta', t.meta(metaTxt))); box.appendChild(sum);
    root.appendChild(box);
    if (!res) { root.setAttribute('data-ready', '0'); return; }

    box.appendChild(el('p', 'mc-lead', t.lead));
    var scale = Math.max(res.total, res.price || 0);
    var wf = el('div', 'tpx-wf'); wf.setAttribute('role', 'list');
    function row(cls, label, sub, startV, val, valTxt, extra) {
      var r = el('div', 'tpx-row ' + cls); r.setAttribute('role', 'listitem');
      var head = el('div', 'tpx-head');
      var lab = el('div', 'tpx-label'); lab.appendChild(el('b', null, label)); if (sub) lab.appendChild(el('small', null, sub));
      head.appendChild(lab);
      var v = el('div', 'tpx-val'); v.appendChild(el('b', null, valTxt)); if (extra) v.appendChild(el('small', null, extra));
      head.appendChild(v); r.appendChild(head);
      var track = el('div', 'tpx-track');
      var bar = el('div', 'tpx-bar');
      bar.style.left = (startV / scale * 100) + '%'; bar.style.width = (val / scale * 100) + '%';
      track.appendChild(bar); r.appendChild(track);
      return { r: r, bar: bar, track: track };
    }
    res.axes.forEach(function (a) {
      var x = row('tpx-' + a.id, t.axis[a.id], t.axisWhy[a.id], a.start, a.contrib, '+' + money(a.contrib), pctS(a.share) + ' \u00b7 ' + t.nModels(a.models.length) + ' \u00b7 ' + t.avgModel(money(a.avg)));
      x.r.setAttribute('data-axis', a.id); x.r.setAttribute('data-contrib', a.contrib.toFixed(4)); x.r.setAttribute('data-start', a.start.toFixed(4));
      a.models.forEach(function (o) {
        var seg = el('span', 'tpx-seg'); seg.style.width = (o.contrib / a.contrib * 100) + '%';
        seg.title = o.name + ': ' + money(o.price) + ' \u00f7 12 = +' + money(o.contrib);
        seg.setAttribute('data-model', o.m); x.bar.appendChild(seg);
      });
      var chips = el('div', 'tpx-chips');
      a.models.forEach(function (o) { var c = el('span', 'tpx-chip'); c.setAttribute('data-model', o.m); c.appendChild(el('span', null, o.name)); c.appendChild(el('b', null, '+' + moneyC(o.contrib))); chips.appendChild(c); });
      x.r.appendChild(chips);
      wf.appendChild(x.r);
    });
    var tot = row('tpx-total', t.total, null, 0, res.total, money(res.total), null);
    tot.r.setAttribute('data-total', res.total.toFixed(4));
    if (res.price) {
      var mk = el('div', 'tpx-price'); mk.style.left = (res.price / scale * 100) + '%';
      mk.appendChild(el('span', null, t.price + ' ' + money(res.price)));
      tot.track.appendChild(mk); tot.r.setAttribute('data-price', res.price.toFixed(2));
    }
    wf.appendChild(tot.r);
    box.appendChild(wf);

    box.appendChild(el('div', 'mc-h', t.h2));
    var tb = el('table', 'csx-table tpx-table'), th = el('thead'), hr = el('tr');
    [t.colModel, t.colWhy, t.colAdd].forEach(function (s) { var c = el('th', null, s); c.scope = 'col'; hr.appendChild(c); });
    th.appendChild(hr); tb.appendChild(th);
    var body = el('tbody');
    res.axes.forEach(function (a) {
      var gr = el('tr', 'tpx-grp tpx-' + a.id); var gc = el('th', null, t.axis[a.id]); gc.colSpan = 3; gc.scope = 'colgroup'; gr.appendChild(gc); body.appendChild(gr);
      a.models.forEach(function (o) {
        var tr = el('tr'); tr.setAttribute('data-model', o.m); tr.setAttribute('data-axis', a.id);
        var h = el('th', null, o.name); h.scope = 'row'; tr.appendChild(h);
        tr.appendChild(el('td', 'tpx-why', t.why[o.m]));
        var c = el('td'); c.appendChild(el('b', null, '+' + moneyC(o.contrib))); c.setAttribute('data-v', o.contrib.toFixed(4)); tr.appendChild(c);
        body.appendChild(tr);
      });
    });
    tb.appendChild(body); var w = el('div', 'csx-wrap'); w.appendChild(tb); box.appendChild(w);
    box.appendChild(el('p', 'csx-note', t.note));
    var sp = el('p', 'csx-note tpx-src'); var a = el('a', null, t.src); a.href = SRC; a.target = '_blank'; a.rel = 'noopener'; sp.appendChild(a); box.appendChild(sp);
    root.setAttribute('data-ready', '1'); root.setAttribute('data-total', res.total.toFixed(4));
  }

  var timer = 0;
  function schedule() { clearTimeout(timer); timer = setTimeout(render, 250); }
  function init() {
    var tries = 0;
    (function place() {
      var anchor = document.getElementById('composite-sens');
      if (!anchor) { if (++tries < 120) setTimeout(place, 500); return; }
      if (document.getElementById('triple-point')) return;
      root = el('div', 'mc-wrap tpx-outer'); root.id = 'triple-point';
      anchor.insertAdjacentElement('afterend', root);
      render();
      var n = 0, iv = setInterval(function () { render(); if (root.getAttribute('data-ready') === '1' && ++n > 20) clearInterval(iv); else if (++n > 120) clearInterval(iv); }, 1000);
      new MutationObserver(schedule).observe(document.documentElement, { attributes: true, attributeFilter: ['lang'] });
      var cp = document.getElementById('composite-price');
      if (cp) new MutationObserver(schedule).observe(cp, { attributes: true, attributeFilter: ['data-value'] });
    })();
  }
  if (document.readyState === 'loading') document.addEventListener('DOMContentLoaded', init); else init();
  window.ethvalTriple = { compute: compute, render: function () { lastKey = ''; render(); }, AXES: AXES };
})();
