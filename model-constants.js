/* ETHval — Constants & assumptions disclosure (section 03 Ratings, under the 12 model cards).
 * 1) Constants table: every value is read at runtime from the page's own code —
 *    MODEL_CONSTANTS (index.html) and the source of calculateSingleModel() — never typed in by hand.
 *    For each model we list the MODEL_CONSTANTS keys it uses and any fixed number written inline in its
 *    formula (unit conversions 0 / 1 / 100 / 365 / 1e9 are left out).
 * 2) Changelog: dated record of constant changes with the reason. The latest entry's `values` must equal
 *    MODEL_CONSTANTS — checked by .qa/constants_check.cjs (qa_all.sh), so a code change without a
 *    changelog entry fails QA.
 * 3) Input data downloads (CSV / JSON): the stored daily tables the 12 models read, fetched straight from
 *    the public read-only Supabase REST endpoint with the anon key the page already uses. Rows as stored.
 */
(function () {
  'use strict';

  // Dated changes to MODEL_CONSTANTS. Add a new entry (newest last) whenever a constant changes.
  // Rule (enforced by .qa/constants_history_check.cjs in qa_all.sh, and --strict in .qa/push.sh):
  //   1. change MODEL_CONSTANTS in index.html AND add an entry here in the same commit:
  //      { date: 'YYYY-MM-DD', commit: 'pending', values: { all 14 new values }, why: { en, ko, zh, ja } }
  //   2. after committing, replace 'pending' with that commit's short sha in a follow-up commit, then push.
  //   One entry per commit that changed a value; date = that commit's date.
  var CHANGELOG = [
    { date: '2025-12-28', commit: '7455368',
      values: { TVL_MULTIPLE: 7, L2_WEIGHT: 2, L2_MULTIPLE: 6, PS_RATIO: 25, DISCOUNT_RATE: 0.09, GROWTH_RATE: 0.03,
                RISK_FREE_RATE: 4.5, RISK_PREMIUM: 1.5, STABLES_RATIO: 0.28, LOST_ETH_PCT: 0.03, ETH_VELOCITY: 6,
                ECOSYSTEM_VELOCITY: 150, METCALFE_COEF: 2, METCALFE_EXP: 1.5 },
      why: { en: 'First version in this repository: all 14 constants added with these values. Reasons for each value are in the model cards above. No constant has changed since.',
             ko: '이 저장소의 첫 버전: 14개 상수를 이 값으로 넣었어. 값마다 이유는 위 모델 카드에 있어. 그 뒤로 바뀐 상수는 없어.',
             zh: '本仓库的第一个版本：以这些值加入全部 14 个常数。各数值的理由见上方模型卡片。此后没有常数改动。',
             ja: 'このリポジトリの最初のバージョン：14 個の定数をこの値で追加。各値の理由は上のモデルカードに記載。その後変更された定数はない。' } }
  ];

  // What each constant means (wording only — values always come from MODEL_CONSTANTS).
  var MEANING = {
    TVL_MULTIPLE: { en: 'Market cap ÷ DeFi TVL multiple', ko: '시가총액 ÷ DeFi TVL 배수', zh: '市值 ÷ DeFi TVL 倍数', ja: '時価総額 ÷ DeFi TVL 倍率' },
    L2_WEIGHT: { en: 'Weight on L2 TVL vs L1 TVL', ko: 'L1 대비 L2 TVL 가중치', zh: 'L2 TVL 相对 L1 的权重', ja: 'L1 に対する L2 TVL の重み' },
    L2_MULTIPLE: { en: 'Multiple on weighted TVL', ko: '가중 TVL에 곱하는 배수', zh: '加权 TVL 的倍数', ja: '加重 TVL に掛ける倍率' },
    PS_RATIO: { en: 'Price ÷ annual fee revenue', ko: '연간 수수료 대비 가격 배수', zh: '价格 ÷ 年手续费收入', ja: '価格 ÷ 年間手数料収入' },
    DISCOUNT_RATE: { en: 'Discount rate', ko: '할인율', zh: '折现率', ja: '割引率' },
    GROWTH_RATE: { en: 'Perpetual growth rate', ko: '영구 성장률', zh: '永续增长率', ja: '永久成長率' },
    RISK_FREE_RATE: { en: 'Risk-free rate (%)', ko: '무위험 수익률 (%)', zh: '无风险利率 (%)', ja: '無リスク金利 (%)' },
    RISK_PREMIUM: { en: 'Risk premium on top (%)', ko: '추가 위험 프리미엄 (%)', zh: '额外风险溢价 (%)', ja: '上乗せリスクプレミアム (%)' },
    STABLES_RATIO: { en: 'Stablecoins as share of all app capital', ko: '전체 앱 자본 중 스테이블코인 비중', zh: '稳定币占全部应用资本的比例', ja: 'アプリ資本全体に占めるステーブルコイン比率' },
    LOST_ETH_PCT: { en: 'Share of supply assumed lost', ko: '분실로 보는 공급 비중', zh: '视为丢失的供应比例', ja: '紛失とみなす供給比率' },
    ETH_VELOCITY: { en: 'ETH turnover per year', ko: 'ETH 연간 회전 수', zh: 'ETH 年周转次数', ja: 'ETH の年間回転数' },
    ECOSYSTEM_VELOCITY: { en: 'All-token turnover per year', ko: '전체 토큰 연간 회전 수', zh: '全部代币年周转次数', ja: '全トークンの年間回転数' },
    METCALFE_COEF: { en: 'Metcalfe coefficient', ko: '메트칼프 계수', zh: '梅特卡夫系数', ja: 'メトカーフ係数' },
    METCALFE_EXP: { en: 'Metcalfe exponent', ko: '메트칼프 지수', zh: '梅特卡夫指数', ja: 'メトカーフ指数' }
  };

  // Stored daily tables the 12 models read (only public read-only tables the page already loads).
  var DATASETS = [
    { table: 'historical_eth_price', cols: 'date,open,high,low,close,volume', used: 'Price' },
    { table: 'historical_eth_supply', cols: 'date,eth_supply,source', used: 'Supply' },
    { table: 'historical_ethereum_tvl', cols: 'date,tvl', used: 'TVL' },
    { table: 'historical_staking', cols: 'date,total_staked_eth,avg_apr,total_validators,source', used: 'Staked ETH, APR' },
    { table: 'historical_l2_tvl', cols: 'date,chain,tvl', used: 'L2 TVL' },
    { table: 'historical_gas_burn', cols: 'date,fees_usd,eth_burnt', used: 'L1 fees' },
    { table: 'historical_blob_data', cols: 'date,blob_count,blob_fee_eth,source', used: 'Blob fees' },
    { table: 'historical_stablecoins', cols: 'date,total_mcap,source', used: 'Stablecoins' },
    { table: 'historical_l1_total_volume', cols: 'date,eth_volume_usd,total_volume_usd', used: 'L1 volume' },
    { table: 'historical_l2_total_volume', cols: 'date,chain,native_volume_usd,total_volume_usd', used: 'L2 volume' }
  ];

  var T = {
    en: { count: function (n) { return n + ' constants'; }, title: 'Constants & assumptions', lead: 'Every fixed number the 12 models use, read live from the code that computes the fair value, so this table cannot drift from the site.',
          model: 'Model', constant: 'Constant', value: 'Value', inline: 'Fixed in formula', none: 'No constant, inputs only',
          log: 'Change history', logLead: 'Every change to these constants, with date and reason.', commit: 'commit',
          dl: 'Input data', dlLead: 'The stored daily tables the models read, exactly as stored (no edits, no gap filling). Today\'s card values may use a fresher live API value for the latest day.',
          table: 'Table', used: 'Used for', rows: 'rows', loading: 'Loading…', fail: 'Download failed. Try again', mismatch: 'Constants in code differ from the latest change-history entry.' },
    ko: { count: function (n) { return '상수 ' + n + '개'; }, title: '상수와 가정', lead: '12개 모델이 쓰는 고정 숫자 전부를 적정가를 계산하는 코드에서 그대로 읽어 보여줘. 그래서 이 표는 사이트 계산과 어긋날 수 없어.',
          model: '모델', constant: '상수', value: '값', inline: '공식에 고정된 수', none: '상수 없음, 입력값만 사용',
          log: '변경 이력', logLead: '이 상수들이 바뀔 때마다 날짜와 이유를 남겨.', commit: '커밋',
          dl: '입력 데이터', dlLead: '모델이 읽는 일별 저장 테이블을 저장된 그대로 받아(수정·빈칸 채우기 없음). 오늘 카드 값은 최신 하루에 한해 더 새로운 실시간 API 값을 쓸 수 있어.',
          table: '테이블', used: '쓰이는 곳', rows: '행', loading: '불러오는 중…', fail: '다운로드 실패, 다시 눌러줘', mismatch: '코드의 상수가 최신 변경 이력과 달라.' },
    zh: { count: function (n) { return n + ' 个常数'; }, title: '常数与假设', lead: '12 个模型使用的全部固定数字，直接从计算公允价值的代码中读取，因此此表不会与网站计算不一致。',
          model: '模型', constant: '常数', value: '数值', inline: '公式内固定数', none: '无常数，仅用输入值',
          log: '变更记录', logLead: '这些常数每次变更都记录日期与原因。', commit: '提交',
          dl: '输入数据', dlLead: '模型读取的每日存储表，按存储原样提供（不修改、不填补空缺）。今日卡片数值的最新一天可能使用更新的实时 API 值。',
          table: '表', used: '用于', rows: '行', loading: '加载中…', fail: '下载失败，请重试', mismatch: '代码中的常数与最新变更记录不一致。' },
    ja: { count: function (n) { return '定数 ' + n + ' 個'; }, title: '定数と前提', lead: '12 モデルが使う固定数値をすべて、適正価値を計算するコードから直接読み取って表示。だからこの表はサイトの計算とずれない。',
          model: 'モデル', constant: '定数', value: '値', inline: '式に固定された数', none: '定数なし、入力値のみ',
          log: '変更履歴', logLead: 'これらの定数が変わるたびに日付と理由を記録。', commit: 'コミット',
          dl: '入力データ', dlLead: 'モデルが読む日次保存テーブルを保存されたまま提供（編集・欠損補完なし）。今日のカード値は最新の 1 日に限り、より新しいリアルタイム API 値を使うことがある。',
          table: 'テーブル', used: '用途', rows: '行', loading: '読み込み中…', fail: 'ダウンロード失敗、もう一度押して', mismatch: 'コードの定数が最新の変更履歴と一致しない。' }
  };
  var USED = {
    'Price': { ko: '가격', zh: '价格', ja: '価格' }, 'Supply': { ko: '공급량', zh: '供应量', ja: '供給量' },
    'TVL': { ko: 'TVL', zh: 'TVL', ja: 'TVL' }, 'Staked ETH, APR': { ko: '스테이킹 ETH·APR', zh: '质押 ETH·APR', ja: 'ステーク ETH・APR' },
    'L2 TVL': { ko: 'L2 TVL', zh: 'L2 TVL', ja: 'L2 TVL' }, 'L1 fees': { ko: 'L1 수수료', zh: 'L1 手续费', ja: 'L1 手数料' },
    'Blob fees': { ko: '블롭 수수료', zh: 'Blob 费用', ja: 'Blob 手数料' }, 'Stablecoins': { ko: '스테이블코인', zh: '稳定币', ja: 'ステーブルコイン' },
    'L1 volume': { ko: 'L1 거래량', zh: 'L1 交易量', ja: 'L1 取引量' }, 'L2 volume': { ko: 'L2 거래량', zh: 'L2 交易量', ja: 'L2 取引量' }
  };

  function lang() { var l = (document.documentElement.lang || 'en').slice(0, 2); return T[l] ? l : 'en'; }
  function tr(m) { var l = lang(); return m[l] || m.en; }
  function esc(s) { return String(s).replace(/[&<>"]/g, function (c) { return { '&': '&amp;', '<': '&lt;', '>': '&gt;', '"': '&quot;' }[c]; }); }
  var UNIT = { '0': 1, '1': 1, '100': 1, '365': 1, '1e9': 1 };

  // Parse calculateSingleModel's own source: per model case → constants used + inline fixed numbers.
  function readModels() {
    if (typeof calculateSingleModel !== 'function' || typeof MODEL_CONSTANTS === 'undefined') return null;
    var src = calculateSingleModel.toString(), out = [];
    src.split(/case '/).slice(1).forEach(function (part) {
      var id = part.slice(0, part.indexOf("'"));
      var body = part.replace(/source:[^\n]*\n/g, '').replace(/toFixed\(\d+\)/g, '');
      var consts = [], inl = [], m, re = /C\.([A-Z0-9_]+)/g;
      while ((m = re.exec(body))) if (consts.indexOf(m[1]) < 0) consts.push(m[1]);
      var nre = /(^|[^\w.])(\d+(?:\.\d+)?(?:e\d+)?)(?![\w.])/g;
      while ((m = nre.exec(body))) if (!UNIT[m[2]] && inl.indexOf(m[2]) < 0) inl.push(m[2]);
      // the code line(s) where the inline numbers appear, shown as-is
      var lines = body.split('\n').map(function (s) { return s.trim(); }).filter(function (s) {
        return inl.some(function (n) { return new RegExp('(^|[^\\w.])' + n.replace('.', '\\.') + '(?![\\w.])').test(s); });
      });
      out.push({ id: id, consts: consts, inline: inl, expr: lines.join(' ').replace(/^(const|let|var)\s+/, '').replace(/[,;]\s*$/, '') });
    });
    return out;
  }
  function modelName(id) {
    var md = (typeof MODEL_METADATA !== 'undefined' && MODEL_METADATA[id]) || null;
    return md ? md.name : id;
  }
  function fmt(v) { return typeof v === 'number' ? String(v) : esc(v); }

  function latestValues() { return CHANGELOG[CHANGELOG.length - 1].values; }
  function mismatches() {
    var a = latestValues(), b = MODEL_CONSTANTS, bad = [];
    Object.keys(b).forEach(function (k) { if (a[k] !== b[k]) bad.push(k); });
    Object.keys(a).forEach(function (k) { if (!(k in b) && bad.indexOf(k) < 0) bad.push(k); });
    return bad;
  }

  var root = null;
  function render() {
    var models = readModels(); if (!models || !root) return;
    var L = T[lang()], open = root.querySelector('details[open]') ? true : false;
    var rows = models.map(function (r) {
      var cells = r.consts.map(function (k) {
        return '<div class="mc-c"><code>' + k + '</code><b>' + fmt(MODEL_CONSTANTS[k]) + '</b><span>' + esc(tr(MEANING[k] || { en: '' })) + '</span></div>';
      }).join('');
      if (r.inline.length) cells += '<div class="mc-c mc-inline"><code>' + esc(L.inline) + '</code><b>' + r.inline.map(esc).join(', ') + '</b><span class="mc-expr">' + esc(r.expr) + '</span></div>';
      if (!cells) cells = '<div class="mc-c mc-none"><span>' + esc(L.none) + '</span></div>';
      return '<tr data-model="' + r.id + '"><th>' + esc(modelName(r.id)) + '</th><td>' + cells + '</td></tr>';
    }).join('');
    var bad = mismatches();
    var log = CHANGELOG.slice().reverse().map(function (e) {
      var c = /^[0-9a-f]{7,40}$/.test(e.commit) ? '<a href="https://github.com/seojoonkim/eth-value/commit/' + e.commit + '" target="_blank" rel="noopener">' + L.commit + ' ' + e.commit + '</a>' : '';
      return '<li><div class="mc-log-h"><b>' + e.date + '</b>' + c + '</div>' +
        '<div class="mc-log-v">' + Object.keys(e.values).map(function (k) { return '<span><code>' + k + '</code> ' + e.values[k] + '</span>'; }).join('') + '</div>' +
        '<p>' + esc(tr(e.why)) + '</p></li>';
    }).join('');
    var dl = DATASETS.map(function (d) {
      var u = USED[d.used]; var used = (u && u[lang()]) || d.used;
      return '<tr><th><code>' + d.table.replace('historical_', '') + '</code><small>' + esc(used) + '</small></th>' +
        '<td><button type="button" class="mc-dl" data-t="' + d.table + '" data-f="csv">CSV</button>' +
        '<button type="button" class="mc-dl" data-t="' + d.table + '" data-f="json">JSON</button></td></tr>';
    }).join('');
    root.innerHTML =
      '<details class="mc-box"' + (open ? ' open' : '') + '><summary><span class="mc-title">' + esc(L.title) + '</span><span class="mc-sum-meta">' +
        esc(L.count(Object.keys(MODEL_CONSTANTS).length)) + ' · ' + esc(L.log) + ' · ' + esc(L.dl) + '</span></summary>' +
      '<p class="mc-lead">' + esc(L.lead) + '</p>' +
      (bad.length ? '<p class="mc-warn">' + esc(L.mismatch) + ' (' + bad.join(', ') + ')</p>' : '') +
      '<table class="mc-table"><thead><tr><th>' + esc(L.model) + '</th><td>' + esc(L.constant) + ' · ' + esc(L.value) + '</td></tr></thead><tbody>' + rows + '</tbody></table>' +
      '<h4 class="mc-h">' + esc(L.log) + '</h4><p class="mc-lead">' + esc(L.logLead) + '</p><ol class="mc-log">' + log + '</ol>' +
      '<h4 class="mc-h">' + esc(L.dl) + '</h4><p class="mc-lead">' + esc(L.dlLead) + '</p>' +
      '<table class="mc-dl-table"><tbody>' + dl + '</tbody></table></details>';
  }

  // ---- downloads (public read-only REST, paged; rows as stored) ----
  function cfg() { return (typeof SUPABASE_CONFIG !== 'undefined' && SUPABASE_CONFIG) || null; }
  function fetchAll(d) {
    var c = cfg(); if (!c) return Promise.reject(new Error('no config'));
    var order = d.cols.indexOf('chain') >= 0 ? 'date.asc,chain.asc' : 'date.asc';
    var all = [], page = 1000;
    function step(from) {
      return fetch(c.url + '/rest/v1/' + d.table + '?select=' + d.cols + '&order=' + order, {
        headers: { apikey: c.anonKey, Authorization: 'Bearer ' + c.anonKey, Range: from + '-' + (from + page - 1), 'Range-Unit': 'items' }
      }).then(function (r) { if (!r.ok && r.status !== 206) throw new Error('HTTP ' + r.status); return r.json(); })
        .then(function (rows) { all = all.concat(rows); return rows.length === page ? step(from + page) : all; });
    }
    return step(0);
  }
  function toCsv(cols, rows) {
    var q = function (v) { if (v === null || v === undefined) return ''; var s = String(v); return /[",\n]/.test(s) ? '"' + s.replace(/"/g, '""') + '"' : s; };
    return cols.join(',') + '\n' + rows.map(function (r) { return cols.map(function (k) { return q(r[k]); }).join(','); }).join('\n') + '\n';
  }
  function save(name, text, type) {
    var a = document.createElement('a'); a.href = URL.createObjectURL(new Blob([text], { type: type })); a.download = name;
    document.body.appendChild(a); a.click(); setTimeout(function () { URL.revokeObjectURL(a.href); a.remove(); }, 1000);
  }
  function onClick(ev) {
    var b = ev.target.closest && ev.target.closest('.mc-dl'); if (!b || b.disabled) return;
    var d = DATASETS.filter(function (x) { return x.table === b.dataset.t; })[0]; if (!d) return;
    var L = T[lang()], label = b.textContent; b.disabled = true; b.textContent = L.loading;
    fetchAll(d).then(function (rows) {
      var day = new Date().toISOString().slice(0, 10), base = 'ethval-' + d.table.replace('historical_', '') + '-' + day;
      if (b.dataset.f === 'csv') save(base + '.csv', toCsv(d.cols.split(','), rows), 'text/csv');
      else save(base + '.json', JSON.stringify(rows, null, 1), 'application/json');
      b.textContent = label + ' · ' + rows.length.toLocaleString() + ' ' + L.rows;
    }).catch(function () { b.textContent = L.fail; }).then(function () { b.disabled = false; });
  }

  function init() {
    var grid = document.querySelector('#ratings .methodology-grid'); if (!grid || document.getElementById('model-constants')) return;
    root = document.createElement('div'); root.id = 'model-constants'; root.className = 'mc-wrap';
    grid.insertAdjacentElement('afterend', root);
    root.addEventListener('click', onClick);
    var tries = 0, iv = setInterval(function () { if (readModels() || ++tries > 40) { clearInterval(iv); render(); } }, 250);
    new MutationObserver(render).observe(document.documentElement, { attributes: true, attributeFilter: ['lang'] });
    window.EthvalConstants = { CHANGELOG: CHANGELOG, DATASETS: DATASETS, readModels: readModels, mismatches: mismatches, fetchAll: fetchAll, toCsv: toCsv };
  }
  if (document.readyState === 'loading') document.addEventListener('DOMContentLoaded', init); else init();
})();
