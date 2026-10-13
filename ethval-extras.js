/* ETHval extras (2026-10-14)
 * 1) ELI5 popover per valuation model (hover on desktop, tap on mobile)
 * 2) Fair-value trend chart: one switch hides/shows every individual model line (composite + price stay)
 * 3) "Last data update" stamp from scheduler_logs (collector runs every 12h)
 * 4) Share: visible share button -> share card modal (X / copy link / native share / download)
 * 5) Section 06 — ETH valuation research library
 */
(function () {
  'use strict';

  var LANGS = ['en', 'ko', 'zh', 'ja'];
  function lang() {
    var l = (typeof currentLang !== 'undefined' && currentLang) || document.documentElement.lang || 'en';
    l = String(l).slice(0, 2);
    return LANGS.indexOf(l) >= 0 ? l : 'en';
  }
  function t(map) { return map[lang()] || map.en; }

  // ---------- 1. ELI5 ----------
  var CAT = {
    cash:   { en: 'Cash flow', ko: '현금흐름', zh: '现金流', ja: 'キャッシュフロー' },
    network:{ en: 'Network & capital', ko: '네트워크·자본', zh: '网络与资本', ja: 'ネットワーク・資本' },
    money:  { en: 'Money & settlement', ko: '화폐·결제', zh: '货币与结算', ja: '通貨・決済' },
    supply: { en: 'Supply scarcity', ko: '공급 희소성', zh: '供应稀缺', ja: '供給希少性' }
  };
  var ELI5 = {
    dcf: { cat: 'cash',
      en: ['Like valuing a rental flat by the rent it pays.', 'Staking pays ETH holders a yearly reward. We add up those future rewards, discount them to today, and get a price.'],
      ko: ['월세 받는 집의 가치를 월세로 계산하는 것과 같아.', '스테이킹하면 매년 보상이 나와. 앞으로 받을 보상을 오늘 가치로 할인해 더한 값이 적정가야.'],
      zh: ['就像按租金估算一套出租房的价值。', '质押每年产生奖励。把未来奖励折现到今天并加总，就是公允价格。'],
      ja: ['家賃収入から賃貸物件の価値を測るのと同じ。', 'ステーキングは毎年報酬を生む。将来の報酬を現在価値に割り引いて合計した値が適正価格。'] },
    ps: { cat: 'cash',
      en: ['Like pricing a software company at a multiple of its sales.', 'Ethereum earns fees (L1 + blob fees). Yearly fees × 25 = network value, divided by ETH supply.'],
      ko: ['소프트웨어 회사를 매출의 몇 배로 값 매기는 방식이야.', '이더리움은 수수료(L1·블롭)를 벌어. 연간 수수료 × 25배를 네트워크 가치로 보고 ETH 개수로 나눠.'],
      zh: ['就像按销售额倍数为软件公司定价。', '以太坊赚取手续费（L1 + blob）。年手续费 × 25 = 网络价值，再除以 ETH 供应量。'],
      ja: ['ソフトウェア企業を売上の何倍で評価するのと同じ。', 'イーサリアムは手数料（L1・blob）を稼ぐ。年間手数料 × 25 をネットワーク価値とし、ETH 供給量で割る。'] },
    revenueYield: { cat: 'cash',
      en: ['Like asking: what price makes the fee income look like a normal bond yield?', 'Yearly fees ÷ staking APR gives the market cap that would yield that APR, divided by supply.'],
      ko: ['수수료 수입이 보통 채권 이자처럼 보이려면 가격이 얼마여야 할까를 묻는 방식이야.', '연간 수수료 ÷ 스테이킹 이율로 시가총액을 구하고 ETH 개수로 나눠.'],
      zh: ['就像问：价格多少时，手续费收入看起来像普通债券收益？', '年手续费 ÷ 质押年化收益 = 对应市值，再除以供应量。'],
      ja: ['手数料収入が普通の債券利回りに見える価格はいくらか、という問い。', '年間手数料 ÷ ステーキング利回りで時価総額を出し、供給量で割る。'] },
    validatorEcon: { cat: 'cash',
      en: ['Like a job that must pay at least a target wage to attract workers.', 'If staking pays less than the 6% target return, ETH looks pricey; if more, cheap. Price × (target ÷ APR).'],
      ko: ['사람을 모으려면 최소 목표 임금은 줘야 하는 일자리와 같아.', '스테이킹 이율이 목표 6%보다 낮으면 비싸고, 높으면 싸다고 봐. 가격 × (목표 ÷ 이율).'],
      zh: ['就像一份工作至少要付目标工资才能吸引人。', '质押收益低于 6% 目标则偏贵，高于则偏便宜。价格 × (目标 ÷ 年化)。'],
      ja: ['人を集めるには最低限の目標賃金が必要な仕事と同じ。', 'ステーキング利回りが目標 6% を下回れば割高、上回れば割安。価格 × (目標 ÷ 利回り)。'] },
    tvlMultiple: { cat: 'network',
      en: ['Like valuing a bank by the deposits it holds.', 'Money locked in Ethereum DeFi (TVL) × 7, the historical market-cap/TVL ratio, divided by supply.'],
      ko: ['은행을 맡겨진 예금 규모로 평가하는 것과 같아.', '이더리움 DeFi에 묶인 돈(TVL)에 과거 시총/TVL 비율인 7배를 곱하고 ETH 개수로 나눠.'],
      zh: ['就像按存款规模评估一家银行。', '锁在以太坊 DeFi 的资金（TVL）× 历史市值/TVL 比 7，再除以供应量。'],
      ja: ['預金残高で銀行を評価するのと同じ。', 'イーサリアム DeFi にロックされた資金（TVL）× 過去の時価総額/TVL 比 7 を供給量で割る。'] },
    l2Ecosystem: { cat: 'network',
      en: ['Like counting a mall’s value including the shops in its annexes.', 'Same as TVL Multiple, but Layer-2 TVL counts double because L2s settle on Ethereum and burn ETH.'],
      ko: ['쇼핑몰 가치를 별관 매장까지 포함해 세는 것과 같아.', 'TVL 방식과 같지만, L2는 이더리움에서 정산하고 ETH를 태우니까 L2 TVL을 2배로 쳐.'],
      zh: ['就像把附楼的店铺也算进商场价值。', '与 TVL 倍数相同，但 L2 在以太坊结算并销毁 ETH，所以 L2 TVL 计两倍。'],
      ja: ['別館の店舗も含めてモールの価値を数えるのと同じ。', 'TVL 倍率と同じだが、L2 はイーサリアムで決済し ETH を燃やすため L2 TVL を 2 倍で数える。'] },
    metcalfe: { cat: 'network',
      en: ['Like a phone network: each new user makes it more useful to everyone.', 'Value grows faster than usage (activity^1.5), using TVL as the activity proxy, divided by supply.'],
      ko: ['전화망처럼 사용자가 늘수록 모두에게 더 쓸모 있어져.', '가치는 사용량보다 빠르게(사용량^1.5) 커져. TVL을 사용량 대용으로 쓰고 ETH 개수로 나눠.'],
      zh: ['就像电话网络：每多一个用户，对所有人都更有用。', '价值增长快于使用量（活跃度^1.5），以 TVL 作为活跃度代理，再除以供应量。'],
      ja: ['電話網のように、利用者が増えるほど全員にとって便利になる。', '価値は利用量より速く伸びる（活動量^1.5）。TVL を活動量の代わりに使い、供給量で割る。'] },
    appCapital: { cat: 'network',
      en: ['Like valuing land by everything built on it.', 'All value living on Ethereum (stablecoins, tokens, NFTs, real-world assets) sets a floor for ETH’s market cap.'],
      ko: ['땅의 가치를 그 위에 지어진 모든 것으로 보는 방식이야.', '이더리움 위의 모든 자산(스테이블코인·토큰·NFT·실물자산)을 ETH 시가총액의 바닥으로 봐.'],
      zh: ['就像按地上建起的一切来评估土地。', '以太坊上的全部资产（稳定币、代币、NFT、RWA）构成 ETH 市值的底线。'],
      ja: ['その上に建つすべてで土地の価値を測るのと同じ。', 'イーサリアム上の全資産（ステーブルコイン・トークン・NFT・RWA）を ETH 時価総額の下限とみなす。'] },
    ethMonetary: { cat: 'money',
      en: ['Like asking how much cash a city needs for its daily shopping.', 'MV = PQ: yearly ETH transfer volume ÷ how often each ETH changes hands (6×/yr) = the ETH value needed.'],
      ko: ['도시가 매일 장 보려면 현금이 얼마나 필요한지 묻는 방식이야.', 'MV=PQ: 연간 ETH 송금액 ÷ ETH 한 개가 1년에 손바뀜하는 횟수(6회) = 필요한 ETH 가치.'],
      zh: ['就像问一座城市日常购物需要多少现金。', 'MV=PQ：年度 ETH 转账额 ÷ 每个 ETH 每年易手次数（6 次）= 所需 ETH 价值。'],
      ja: ['街の日々の買い物に現金がいくら必要かを問うのと同じ。', 'MV=PQ：年間 ETH 送金額 ÷ ETH 1 枚の年間回転数（6 回）= 必要な ETH 価値。'] },
    ecosystemSettlement: { cat: 'money',
      en: ['Like valuing a highway by all the trucks that use it, not just one brand.', 'MV = PQ on every token moving on Ethereum and L2s, with a fast stablecoin-like velocity (150×/yr).'],
      ko: ['고속도로 가치를 특정 회사 트럭이 아니라 다니는 모든 트럭으로 보는 방식이야.', '이더리움·L2에서 움직이는 모든 토큰에 MV=PQ를 적용해. 회전 속도는 스테이블코인 수준(연 150회).'],
      zh: ['就像按所有卡车而非单一品牌来评估一条高速公路。', '对以太坊与 L2 上流动的全部代币应用 MV=PQ，周转速度按稳定币水平（每年 150 次）。'],
      ja: ['特定の会社ではなく、通るすべてのトラックで高速道路を評価するのと同じ。', 'イーサリアムと L2 で動く全トークンに MV=PQ を適用し、回転速度はステーブルコイン並み（年 150 回）。'] },
    stakingScarcity: { cat: 'supply',
      en: ['Like concert tickets: fewer on sale means each one is worth more.', 'Staked ETH is off the market. Price × √(total supply ÷ unstaked supply).'],
      ko: ['콘서트 표처럼 팔리는 표가 적을수록 한 장의 값이 올라.', '스테이킹된 ETH는 시장에서 빠져. 가격 × √(전체 공급 ÷ 스테이킹 안 된 공급).'],
      zh: ['就像演唱会门票：在售越少，每张越值钱。', '质押的 ETH 退出市场流通。价格 × √(总供应 ÷ 未质押供应)。'],
      ja: ['コンサートチケットのように、売りに出る枚数が少ないほど 1 枚の価値が上がる。', 'ステークされた ETH は市場から外れる。価格 × √(総供給 ÷ 未ステーク供給)。'] },
    commitmentPremium: { cat: 'supply',
      en: ['Like a rare-wine market where most bottles sit in cellars and never trade.', 'ETH locked in staking, DeFi, L2s or lost leaves a smaller liquid float. Price × √(supply ÷ liquid float).'],
      ko: ['대부분 병이 창고에 잠겨 거래되지 않는 희귀 와인 시장과 같아.', '스테이킹·DeFi·L2에 묶였거나 잃어버린 ETH만큼 유통량이 줄어. 가격 × √(공급 ÷ 유통 가능량).'],
      zh: ['就像稀有葡萄酒市场：大多数酒都躺在酒窖里不交易。', '被质押、DeFi、L2 锁定或丢失的 ETH 让可流通量变小。价格 × √(供应 ÷ 可流通量)。'],
      ja: ['ほとんどのボトルが蔵に眠り取引されない希少ワイン市場と同じ。', 'ステーキング・DeFi・L2 にロック、または紛失した ETH の分だけ流通量が減る。価格 × √(供給 ÷ 流通量)。'] }
  };
  var UI = {
    how:   { en: 'How it is calculated', ko: '계산 방법', zh: '计算方法', ja: '計算方法' },
    info:  { en: 'What is this model?', ko: '이 모델은 뭐야?', zh: '这个模型是什么？', ja: 'このモデルとは？' },
    all:   { en: 'See all 4 model families', ko: '4가지 모델 분류 전체 보기', zh: '查看全部 4 类模型', ja: '4 つのモデル分類を見る' },
    hint:  { en: 'Tap ⓘ on any model for a plain-language explanation', ko: '모델의 ⓘ를 누르면 쉬운 설명이 나와', zh: '点击模型上的 ⓘ 查看通俗解释', ja: 'モデルの ⓘ をタップすると、やさしい説明が出る' }
  };

  var pop = null, popFor = null;
  function closePop() { if (pop) { pop.remove(); pop = null; popFor = null; } }
  function openPop(btn, id) {
    if (popFor === btn) { closePop(); return; }
    closePop();
    var e = ELI5[id]; if (!e) return;
    var L = e[lang()] || e.en;
    pop = document.createElement('div');
    pop.className = 'ev-eli5-pop';
    pop.setAttribute('role', 'dialog');
    var formula = (btn.closest('.valuation-model') || document).querySelector('.model-formula');
    pop.innerHTML = '<div class="ev-eli5-cat">' + t(CAT[e.cat]) + '</div>' +
      '<div class="ev-eli5-lead">' + L[0] + '</div>' +
      '<div class="ev-eli5-body">' + L[1] + '</div>' +
      (formula ? '<div class="ev-eli5-how">' + t(UI.how) + '</div><code class="ev-eli5-formula">' + formula.textContent + '</code>' : '') +
      '<a class="ev-eli5-all" href="#ev-model-families">' + t(UI.all) + ' →</a>';
    document.body.appendChild(pop);
    popFor = btn;
    var r = btn.getBoundingClientRect(), w = Math.min(320, window.innerWidth - 24);
    pop.style.width = w + 'px';
    var left = Math.max(12, Math.min(r.left + window.scrollX - 16, window.scrollX + window.innerWidth - w - 12));
    pop.style.left = left + 'px';
    var below = r.bottom + window.scrollY + 8, ph = pop.offsetHeight;
    if (r.bottom + ph + 16 > window.innerHeight && r.top > ph + 16) below = r.top + window.scrollY - ph - 8;
    pop.style.top = below + 'px';
  }
  function setupEli5() {
    document.querySelectorAll('.valuation-model[data-model]').forEach(function (card) {
      var id = card.dataset.model, nameEl = card.querySelector('.model-name');
      if (!ELI5[id] || !nameEl || card.querySelector('.ev-eli5-btn')) return;
      var b = document.createElement('button');
      b.type = 'button'; b.className = 'ev-eli5-btn'; b.textContent = 'i';
      b.setAttribute('aria-label', t(UI.info));
      b.addEventListener('click', function (ev) { ev.preventDefault(); ev.stopPropagation(); openPop(b, id); });
      b.addEventListener('mouseenter', function () { if (window.matchMedia('(hover: hover)').matches && popFor !== b) openPop(b, id); });
      nameEl.insertAdjacentElement('afterend', b);
    });
    document.addEventListener('click', function (ev) { if (pop && !pop.contains(ev.target)) closePop(); });
    document.addEventListener('keydown', function (ev) { if (ev.key === 'Escape') closePop(); });
    window.addEventListener('scroll', function () { if (pop && window.matchMedia('(hover: hover)').matches) closePop(); }, { passive: true });
    // anchor for the existing 4-family framework cards in section 03
    var fam = document.querySelector('.category-card');
    if (fam) { var grid = fam.parentElement; if (grid && !document.getElementById('ev-model-families')) grid.id = 'ev-model-families'; }
    // hint line under the 01.2 subgroup title
    var models = document.getElementById('valuation-models');
    if (models && !document.querySelector('.ev-eli5-hint')) {
      var h = document.createElement('div'); h.className = 'ev-eli5-hint';
      models.parentElement.insertAdjacentElement('beforebegin', h);
    }
  }

  // ---------- 2. Chart: all model lines on/off ----------
  var linesHidden = false;
  var BASE = ['Market Price', 'Composite'];
  function applyLines(chart) {
    chart.data.datasets.forEach(function (d) {
      if (BASE.indexOf(d.label) >= 0) return;
      if (linesHidden) { if (d._evPrev === undefined) d._evPrev = !!d.hidden; d.hidden = true; }
      else if (d._evPrev !== undefined) { d.hidden = d._evPrev; delete d._evPrev; }
    });
  }
  function hookChart() {
    if (typeof Chart === 'undefined' || !Chart.getChart) return null;
    var c = Chart.getChart('historical-main-chart'); if (!c) return null;
    if (!c._evHooked) {
      var orig = c.update.bind(c);
      c.update = function (m) { applyLines(c); return orig(m); };
      c._evHooked = true;
    }
    return c;
  }
  var linesBtn = null;
  function setupLinesToggle() {
    var row = document.querySelector('.legend-row-primary'); if (!row || linesBtn) return;
    linesBtn = document.createElement('button');
    linesBtn.type = 'button'; linesBtn.className = 'ev-lines-toggle';
    linesBtn.setAttribute('role', 'switch');
    linesBtn.addEventListener('click', function () {
      linesHidden = !linesHidden;
      var c = hookChart(); if (c) c.update('none');
      document.querySelector('.historical-legend-container').classList.toggle('ev-lines-off', linesHidden);
      renderLinesBtn();
    });
    row.appendChild(linesBtn);
    var tries = 0, iv = setInterval(function () { if (hookChart() || ++tries > 60) clearInterval(iv); }, 1000);
  }
  function renderLinesBtn() {
    if (!linesBtn) return;
    linesBtn.setAttribute('aria-checked', String(!linesHidden));
    linesBtn.innerHTML = '<span class="ev-switch" aria-hidden="true"></span>' + t({
      en: 'All model lines', ko: '모델 선 전체', zh: '全部模型线', ja: 'モデル線すべて' });
  }

  // ---------- 3. Last data update ----------
  var lastRun = null, stampEl = null;
  function fmtStamp(d) {
    var p = function (n) { return (n < 10 ? '0' : '') + n; };
    var off = -d.getTimezoneOffset(), sign = off >= 0 ? '+' : '-', a = Math.abs(off);
    var tz = (off === 540) ? 'KST' : 'UTC' + sign + Math.floor(a / 60) + (a % 60 ? ':' + p(a % 60) : '');
    return d.getFullYear() + '-' + p(d.getMonth() + 1) + '-' + p(d.getDate()) + ' ' + p(d.getHours()) + ':' + p(d.getMinutes()) + ' ' + tz;
  }
  function renderStamp() {
    if (!stampEl) return;
    if (!lastRun) { stampEl.hidden = true; return; }
    stampEl.hidden = false;
    var hrs = Math.max(0, Math.round((Date.now() - lastRun.getTime()) / 36e5));
    stampEl.innerHTML = '<span class="ev-dot" aria-hidden="true"></span>' + t({
      en: 'Data updated ' + fmtStamp(lastRun) + ' · refreshes every 12h',
      ko: '데이터 업데이트 ' + fmtStamp(lastRun) + ' · 12시간마다 갱신',
      zh: '数据更新于 ' + fmtStamp(lastRun) + ' · 每 12 小时刷新',
      ja: 'データ更新 ' + fmtStamp(lastRun) + ' · 12 時間ごとに更新' });
    stampEl.title = hrs + 'h ago';
  }
  function setupStamp() {
    var head = document.querySelector('#valuation .section-header'); if (!head) return;
    stampEl = document.createElement('div'); stampEl.className = 'ev-updated'; stampEl.hidden = true;
    head.appendChild(stampEl);
    if (typeof SUPABASE_URL === 'undefined' || typeof SUPABASE_ANON_KEY === 'undefined') return;
    fetch(SUPABASE_URL + '/rest/v1/scheduler_logs?select=run_timestamp&status=eq.success&order=run_timestamp.desc&limit=1', {
      headers: { apikey: SUPABASE_ANON_KEY, Authorization: 'Bearer ' + SUPABASE_ANON_KEY }
    }).then(function (r) { return r.ok ? r.json() : []; }).then(function (rows) {
      if (rows && rows[0] && rows[0].run_timestamp) { lastRun = new Date(rows[0].run_timestamp); renderStamp(); }
    }).catch(function () {});
  }

  // ---------- 4. Share ----------
  var shareBtn = null, modal = null;
  var SITE = 'https://ethval.com/';
  function shareText() {
    var g = function (id) { var e = document.getElementById(id); return e ? e.textContent.trim() : ''; };
    var fv = g('composite-price'), cur = g('summary-current'), up = g('summary-upside');
    return t({
      en: 'ETH fair value by 12 valuation models: ' + fv + ' (now ' + cur + ', ' + up + '). What do you think?',
      ko: '12개 밸류에이션 모델이 본 ETH 적정가: ' + fv + ' (현재 ' + cur + ', ' + up + '). 어떻게 봐?',
      zh: '12 个估值模型给出的 ETH 公允价值：' + fv + '（当前 ' + cur + '，' + up + '）。你怎么看？',
      ja: '12 の評価モデルによる ETH 適正価格：' + fv + '（現在 ' + cur + '、' + up + '）。どう思う？' });
  }
  var HASH = { ko: 'kr', zh: 'zh', ja: 'ja' };
  function shareUrl(src) { return SITE + '?utm_source=' + src + '&utm_medium=share' + (HASH[lang()] ? '#' + HASH[lang()] : ''); }
  function toast(msg) {
    var el = document.createElement('div'); el.className = 'ev-toast'; el.textContent = msg;
    document.body.appendChild(el); setTimeout(function () { el.remove(); }, 1800);
  }
  function buildModal() {
    if (document.getElementById('share-modal')) { modal = document.getElementById('share-modal'); return; }
    modal = document.createElement('div');
    modal.className = 'share-modal'; modal.id = 'share-modal';
    modal.innerHTML = '<div class="share-modal-content" role="dialog" aria-modal="true">' +
      '<div class="share-modal-header"><div class="share-modal-title" id="ev-share-title"></div>' +
      '<button type="button" class="share-modal-close" aria-label="Close">×</button></div>' +
      '<div class="share-modal-preview"><img id="share-preview-img" alt=""></div>' +
      '<div class="share-modal-actions">' +
      '<button type="button" class="share-modal-btn twitter" data-ev="x"></button>' +
      '<button type="button" class="share-modal-btn download" data-ev="copy"></button>' +
      '<button type="button" class="share-modal-btn download" data-ev="native"></button>' +
      '<button type="button" class="share-modal-btn download" data-ev="dl"></button>' +
      '</div></div>';
    document.body.appendChild(modal);
    modal.addEventListener('click', function (ev) {
      if (ev.target === modal || ev.target.closest('.share-modal-close')) { modal.classList.remove('active'); return; }
      var b = ev.target.closest('[data-ev]'); if (!b) return;
      var k = b.dataset.ev;
      if (k === 'x') window.open('https://twitter.com/intent/tweet?text=' + encodeURIComponent(shareText()) + '&url=' + encodeURIComponent(shareUrl('x')), '_blank', 'noopener');
      else if (k === 'copy') {
        var txt = shareText() + ' ' + shareUrl('copy');
        (navigator.clipboard ? navigator.clipboard.writeText(txt) : Promise.reject()).then(function () {
          toast(t({ en: 'Copied', ko: '복사했어', zh: '已复制', ja: 'コピーしました' })); }).catch(function () { window.prompt('', txt); });
      } else if (k === 'native') {
        var data = { title: 'ETHval', text: shareText(), url: shareUrl('native') };
        var img = window.shareImageData;
        if (img && navigator.canShare) {
          fetch(img).then(function (r) { return r.blob(); }).then(function (blob) {
            var f = new File([blob], 'ethval.png', { type: 'image/png' });
            if (navigator.canShare({ files: [f] })) data.files = [f];
            return navigator.share(data);
          }).catch(function () {});
        } else if (navigator.share) navigator.share(data).catch(function () {});
      } else if (k === 'dl' && window.shareImageData) {
        var a = document.createElement('a'); a.href = window.shareImageData; a.download = 'ethval-' + new Date().toISOString().slice(0, 10) + '.png'; a.click();
      }
    });
  }
  function renderShare() {
    if (shareBtn) shareBtn.innerHTML = '<svg viewBox="0 0 24 24" width="16" height="16" fill="none" stroke="currentColor" stroke-width="2" aria-hidden="true"><path d="M4 12v7a1 1 0 0 0 1 1h14a1 1 0 0 0 1-1v-7"/><path d="M16 6l-4-4-4 4"/><path d="M12 2v13"/></svg>' +
      t({ en: 'Share this valuation', ko: '이 평가 공유하기', zh: '分享这个估值', ja: 'この評価をシェア' });
    if (!modal) return;
    var s = function (sel, m) { var e = modal.querySelector(sel); if (e) e.textContent = t(m); };
    s('#ev-share-title', { en: 'Share card', ko: '공유 카드', zh: '分享卡片', ja: 'シェアカード' });
    s('[data-ev="x"]', { en: 'Post on X', ko: 'X에 올리기', zh: '发到 X', ja: 'X に投稿' });
    s('[data-ev="copy"]', { en: 'Copy link', ko: '링크 복사', zh: '复制链接', ja: 'リンクをコピー' });
    s('[data-ev="native"]', { en: 'Share…', ko: '다른 앱으로', zh: '其他应用', ja: '他のアプリへ' });
    s('[data-ev="dl"]', { en: 'Save image', ko: '이미지 저장', zh: '保存图片', ja: '画像を保存' });
    var nb = modal.querySelector('[data-ev="native"]'); if (nb) nb.hidden = !navigator.share;
  }
  function setupShare() {
    var panel = document.querySelector('.conclusion-panel'); if (!panel || shareBtn) return;
    buildModal();
    shareBtn = document.createElement('button');
    shareBtn.type = 'button'; shareBtn.className = 'ev-share-btn';
    shareBtn.addEventListener('click', function () {
      if (typeof openShareModal === 'function') openShareModal(); else modal.classList.add('active');
    });
    panel.appendChild(shareBtn);
    // share card: right domain + date
    var u = document.querySelector('.share-card-url'); if (u) u.textContent = 'ethval.com';
  }

  // ---------- 5. Section 06 research ----------
  var RESEARCH = [
    { org: 'CoinShares', year: 2026, url: 'https://coinshares.com/corp/insights/research-data/ethereum-5-year-valuation-framework/',
      title: 'Ethereum 5-Year Valuation Framework: Bear, Base & Bull Cases',
      d: { en: 'Scenario framework built on fees, staking and settlement demand.', ko: '수수료·스테이킹·결제 수요로 만든 약세·기본·강세 시나리오 틀', zh: '基于手续费、质押与结算需求的熊/基准/牛情景框架', ja: '手数料・ステーキング・決済需要に基づく弱気・基本・強気シナリオ' } },
    { org: 'Fidelity Digital Assets', year: 2023, url: 'https://www.fidelitydigitalassets.com/research-and-insights/ethereum-investment-thesis',
      title: 'Ethereum Investment Thesis',
      d: { en: 'Institutional case: ETH as a productive, yield-bearing asset.', ko: '기관 관점에서 본 “수익을 내는 자산” ETH 투자 논리', zh: '机构视角：ETH 作为能产生收益的资产', ja: '機関投資家の視点：利回りを生む資産としての ETH' } },
    { org: 'VanEck', year: 2024, url: 'https://www.vaneck.com/us/en/blogs/digital-assets/matthew-sigel-eth-2030-price-target/',
      title: 'ETH 2030 Price Target and Optimal Portfolio Allocations',
      d: { en: 'Discounted free-cash-flow model to 2030 with explicit assumptions.', ko: '2030년까지 현금흐름을 할인한 모델과 가정 공개', zh: '至 2030 年的现金流折现模型与明确假设', ja: '2030 年までの割引キャッシュフローモデルと前提' } },
    { org: 'ARK Invest', year: 2024, url: 'https://www.ark-invest.com/articles/analyst-research/why-ether-stands-out-among-digital-assets',
      title: 'Why Ether Stands Out Among Digital Assets',
      d: { en: 'Ether as money, commodity and yield asset at once.', ko: '화폐·원자재·수익자산 성격을 동시에 가진 ETH 분석', zh: 'ETH 同时具备货币、商品与收益资产属性', ja: '通貨・コモディティ・利回り資産を兼ねる ETH' } },
    { org: '21Shares', year: 2025, url: 'https://www.21shares.com/en-us/insights/how-to-value-bitcoin-ethereum-solana',
      title: 'How to value Bitcoin, Ethereum, and Solana',
      d: { en: 'Side-by-side valuation lenses for the three largest networks.', ko: '비트코인·이더리움·솔라나를 같은 틀로 비교한 가치평가', zh: '用同一框架比较三大网络的估值', ja: '3 大ネットワークを同じ枠組みで比較した評価' } },
    { org: 'Bankless', year: 2020, url: 'https://www.bankless.com/ether-a-new-model-for-money',
      title: 'Ether: The Triple Point Asset',
      d: { en: 'The classic essay: capital asset, consumable asset, store of value.', ko: '자본·소모·가치저장 자산이라는 고전적인 ETH 프레임', zh: '经典文章：资本资产、消耗型资产与价值储存', ja: '古典的論考：資本資産・消費資産・価値保存' } },
    { org: 'ultrasound.money', year: 'Live', url: 'https://ultrasound.money/',
      title: 'Ultra Sound Money — live supply & burn',
      d: { en: 'Real-time issuance, burn and supply growth.', ko: '발행·소각·공급 증감을 실시간으로 보는 대시보드', zh: '实时发行、销毁与供应变化', ja: '発行・バーン・供給増減をリアルタイムで確認' } },
    { org: 'ethereum.org', year: 'Hub', url: 'https://ethereum.org/reports/',
      title: 'Reports on Ethereum from institutions',
      d: { en: 'Curated index of institutional Ethereum reports.', ko: '기관들이 낸 이더리움 리포트 모음', zh: '机构以太坊报告汇总', ja: '機関によるイーサリアムレポート集' } }
  ];
  var researchSec = null;
  function setupResearch() {
    if (document.getElementById('research')) return;
    var disc = document.querySelector('.page-disclaimer'); if (!disc) return;
    researchSec = document.createElement('section');
    researchSec.className = 'section'; researchSec.id = 'research';
    disc.insertAdjacentElement('beforebegin', researchSec);
  }
  function renderResearch() {
    if (!researchSec) return;
    researchSec.innerHTML = '<div class="section-header"><div class="section-title"><span>' +
      t({ en: 'Research Library', ko: '리서치 라이브러리', zh: '研究资料库', ja: 'リサーチライブラリ' }) + '</span></div>' +
      '<p class="ev-research-lead">' + t({
        en: 'The best published thinking on how to value ETH — read the sources behind the models.',
        ko: 'ETH 가치를 어떻게 매길지에 대한 좋은 리서치를 모았어. 모델 뒤에 있는 원문을 직접 읽어봐.',
        zh: '汇集了关于如何为 ETH 估值的优秀研究——直接阅读模型背后的原文。',
        ja: 'ETH の価値をどう測るかについての優れたリサーチ集。モデルの背景にある原文を読んでみて。' }) + '</p></div>' +
      '<div class="ev-research-grid">' + RESEARCH.map(function (r) {
        return '<a class="ev-research-card" href="' + r.url + '" target="_blank" rel="noopener">' +
          '<div class="ev-research-meta"><span>' + r.org + '</span><span>' + r.year + '</span></div>' +
          '<div class="ev-research-title">' + r.title + '</div>' +
          '<div class="ev-research-desc">' + t(r.d) + '</div>' +
          '<div class="ev-research-go">' + t({ en: 'Read', ko: '읽기', zh: '阅读', ja: '読む' }) + ' ↗</div></a>';
      }).join('') + '</div>';
  }

  // ---------- language re-render ----------
  function renderAll() {
    var h = document.querySelector('.ev-eli5-hint'); if (h) h.textContent = t(UI.hint);
    document.querySelectorAll('.ev-eli5-btn').forEach(function (b) { b.setAttribute('aria-label', t(UI.info)); });
    closePop(); renderLinesBtn(); renderStamp(); renderShare(); renderResearch();
  }
  function init() {
    setupEli5(); setupLinesToggle(); setupStamp(); setupShare(); setupResearch(); renderAll();
    new MutationObserver(renderAll).observe(document.documentElement, { attributes: true, attributeFilter: ['lang'] });
    window.EthvalExtras = { ELI5: ELI5, RESEARCH: RESEARCH, lang: lang };
  }
  if (document.readyState === 'loading') document.addEventListener('DOMContentLoaded', init); else init();
})();
