/* ETHval — "ETHval vs Wall Street" simulator presets (BACKLOG #6).
 * Every number below was read directly from the original report PDF (local copies + extracted text in .qa/sources/):
 *  - CoinShares, "Ethereum Valuation Framework" (page "Published on May 28th, 2026"; data as of close 18 May 2026).
 *      fee table "Total fee revenue" (sections 3 and 5): 2031E Bear $202M, Base $1,194M, Bull $5,663M (annual).
 *      section 3 and section 6: base case "Stablecoin supply on Ethereum reaches ~US$450B by 2031". Bear/bull give no
 *      Ethereum-only stablecoin figure (bull's US$2.8T is the whole market) → stablecoins stay at today's value there.
 *      section 6 headline: Bear ~$1,443, Base ~$4,935, Bull ~$14,135 implied price by 2031.
 *  - VanEck, "ETH 2030 Price Target and Optimal Portfolio Allocations" (PDF dated JUNE 2024): 2030 revenue $78,501M
 *    base, which includes MEV and L2 settlement — not the same thing as the simulator's daily L1+blob fees, and
 *    ×100+ today's fees, beyond the slider's 10x. Shown as reference only (no preset), so sources are never mixed.
 * Only the inputs a report states directly are moved; everything else returns to today's value (slider 25).
 * Fees: the report's own 2026E fee revenue ($560M) is a different (wider) measure than this site's L1+blob fees,
 * so the absolute level is NOT copied. The preset applies the report's own growth (2031E ÷ 2026E, same table)
 * to today's site value. Stablecoins: the report's Ethereum-only figure ($450B, base) is the same measure as the
 * site's Ethereum stablecoin supply, so the level is used directly. Out-of-range (beyond 0.1x–10x) → not applied.
 */
(function () {
  'use strict';
  var CS_URL = 'https://coinshares.com/insights/research-data/ethereum-5-year-valuation-framework/';
  var CS_PDF = 'https://coinshares.citywire.com/wp-content/uploads/2026/07/Ethereum-Valuation-Framework-EN.pdf';
  var VE_PDF = 'https://www.vaneck.com/us/en/blogs/digital-assets/matthew-sigel-eth-2030-price-target.pdf';
  var CS_FEES_2026E = 560e6; // same table, 2026E column
  var PRESETS = [
    { id: 'cs-bear', org: 'CoinShares', yr: 2031, sc: 'bear', feesAnnual: 202e6, stables: null, target: 1443 },
    { id: 'cs-base', org: 'CoinShares', yr: 2031, sc: 'base', feesAnnual: 1194e6, stables: 450e9, target: 4935 },
    { id: 'cs-bull', org: 'CoinShares', yr: 2031, sc: 'bull', feesAnnual: 5663e6, stables: null, target: 14135 }
  ];
  var T = {
    en: { title: 'Wall Street report assumptions', bear: 'Bear', base: 'Base', bull: 'Bull',
      pick: 'Apply a published report\u2019s own numbers to the sliders. Only inputs the report states are moved; the rest stay at today\u2019s values.',
      applied: 'Applied', fees: 'fees: report growth', stables: 'stablecoins on Ethereum', kept: 'kept at today (report gives no Ethereum figure)',
      target: 'Report\u2019s own price', sim: 'this simulator', src: 'Source', pub: 'published', out: 'beyond the slider\u2019s 10x, not applied',
      ve: 'VanEck 2030 ($22k, Jun 2024): reference only, since its revenue includes MEV and L2 settlement, so it does not map onto these sliders.',
      none: 'no input fits the sliders, so the simulator is unchanged',
      locked: 'Unlock the simulator first (rate 2+ models).' },
    ko: { title: '월가 리포트 가정 적용', bear: '약세', base: '기본', bull: '강세',
      pick: '공개 리포트가 직접 적은 숫자를 슬라이더에 넣어봐. 리포트에 있는 항목만 움직이고 나머지는 오늘 값 그대로야.',
      applied: '적용', fees: '수수료: 리포트 성장률', stables: '이더리움 위 스테이블코인', kept: '오늘 값 유지(리포트에 이더리움 수치 없음)',
      target: '리포트 자체 가격', sim: '이 시뮬레이터', src: '출처', pub: '발행', out: '슬라이더 최대 10x 초과라 미적용',
      ve: 'VanEck 2030($22k, 2024년 6월): 참고만. 매출에 MEV·L2 정산이 포함돼 이 슬라이더와 기준이 달라.',
      none: '슬라이더에 넣을 수 있는 항목이 없어 시뮬레이터 값은 그대로야',
      locked: '먼저 시뮬레이터를 열어줘(모델 2개 이상 평가).' },
    zh: { title: '华尔街报告假设', bear: '熊市', base: '基准', bull: '牛市',
      pick: '将公开报告中直接给出的数字代入滑块。只调整报告明确给出的项目，其余保持今日数值。',
      applied: '已应用', fees: '手续费：报告增速', stables: '以太坊上的稳定币', kept: '保持今日数值（报告未给出以太坊数据）',
      target: '报告自身价格', sim: '本模拟器', src: '来源', pub: '发布', out: '超出滑块 10x 上限，未应用',
      ve: 'VanEck 2030（$22k，2024年6月）：仅供参考，其收入包含 MEV 与 L2 结算，与这些滑块口径不同。',
      none: '没有可代入滑块的项目，模拟器数值不变',
      locked: '请先解锁模拟器（评价 2 个以上模型）。' },
    ja: { title: 'ウォール街レポートの前提', bear: '弱気', base: '基本', bull: '強気',
      pick: '公開レポートが直接示す数値をスライダーに反映します。記載のある項目だけを動かし、他は今日の値のままです。',
      applied: '適用', fees: '手数料：レポートの伸び率', stables: 'イーサリアム上のステーブルコイン', kept: '今日の値を維持（レポートにイーサリアム単独の数値なし）',
      target: 'レポート自体の価格', sim: 'このシミュレーター', src: '出典', pub: '発行', out: 'スライダー上限 10x 超のため未適用',
      ve: 'VanEck 2030（$22k、2024年6月）：参考のみ。収益に MEV・L2 決済を含むため、このスライダーとは定義が異なります。',
      none: 'スライダーに反映できる項目がないため、シミュレーターは変わりません',
      locked: '先にシミュレーターを解除してください（2つ以上のモデルを評価）。' }
  };
  var active = null;
  function lang() { var l = (document.documentElement.lang || 'en').slice(0, 2); return T[l] ? l : 'en'; }
  function multToPos(m) { return m <= 1 ? (m - 0.1) / 0.9 * 25 : 25 + (m - 1) / 9 * 75; }
  function money(v) {
    if (v >= 1e12) return '$' + (v / 1e12).toFixed(2) + 'T';
    if (v >= 1e9) return '$' + (v / 1e9).toFixed(v >= 1e11 ? 0 : 1) + 'B';
    if (v >= 1e6) return '$' + (v / 1e6).toFixed(v >= 1e8 ? 0 : 2) + 'M';
    return '$' + Math.round(v).toLocaleString('en-US');
  }
  function price(v) { return '$' + Math.round(v).toLocaleString('en-US'); }
  function esc(s) { return String(s).replace(/[&<>"]/g, function (c) { return { '&': '&amp;', '<': '&lt;', '>': '&gt;', '"': '&quot;' }[c]; }); }
  // Exact multiplier for one input; null when out of the slider's 0.1x–10x range.
  function plan(p, base) {
    var out = { fees: null, stables: null };
    var fm = p.feesAnnual / CS_FEES_2026E;
    out.fees = { mult: fm, ok: fm >= 0.1 && fm <= 10 };
    if (p.stables) { var sm = p.stables / base.stablecoins; out.stables = { mult: sm, ok: sm >= 0.1 && sm <= 10 }; }
    return out;
  }
  function set(id, v) { var el = document.getElementById('sim-' + id); if (el) el.value = v; }
  function apply(p) {
    var sec = document.getElementById('simulator-section');
    if (sec && sec.classList.contains('gated-locked')) { active = 'locked'; render(); return; }
    var base = window.simBaseline && window.simBaseline();
    if (!base || !(base.fees > 0) || !(base.stablecoins > 0)) return;
    var pl = plan(p, base), ex = {};
    ['tvl', 'fees', 'staking', 'stables', 'activeAddresses'].forEach(function (k) { set(k, 25); });
    ['fees', 'stables'].forEach(function (k) {
      var q = pl[k]; if (!q || !q.ok) return;
      var pos = Math.round(multToPos(q.mult));
      set(k, pos); ex[k] = { pos: pos, mult: q.mult };
    });
    window.simPresetExact = ex;
    active = p.id;
    if (window.simRecalc) window.simRecalc();
    render();
  }
  function render() {
    var box = document.getElementById('sim-presets');
    if (!box) return;
    var t = T[lang()], base = window.simBaseline && window.simBaseline();
    var h = '<div class="spx-head"><span class="spx-title">' + esc(t.title) + '</span><span class="spx-chips"><span class="spx-org">CoinShares 2031</span>';
    PRESETS.forEach(function (p) {
      h += '<button type="button" class="spx-chip' + (active === p.id ? ' on' : '') + '" data-preset="' + p.id + '" aria-pressed="' + (active === p.id) + '" aria-label="' + esc(p.org + ' ' + p.yr + ' ' + t[p.sc]) + '">' +
        esc(t[p.sc]) + '</button>';
    });
    h += '</span></div>';
    var p = null; PRESETS.forEach(function (x) { if (x.id === active) p = x; });
    if (active === 'locked') {
      var tr = (typeof translations !== 'undefined' && typeof currentLang !== 'undefined' && translations[currentLang]) || {};
      h += '<p class="spx-note">' + esc(tr['gated.rateToUnlock'] || t.locked) + '</p>';
    } else if (p && base) {
      var pl = plan(p, base), parts = [];
      parts.push(t.fees + ' \u00d7' + pl.fees.mult.toFixed(2) + ' (' + money(CS_FEES_2026E) + ' \u2192 ' + money(p.feesAnnual) + ', 2026E\u21922031E)' + (pl.fees.ok ? '' : ', ' + t.out));
      if (pl.stables) parts.push(t.stables + ' ' + money(base.stablecoins) + ' \u2192 ' + money(p.stables) + (pl.stables.ok ? ' (\u00d7' + pl.stables.mult.toFixed(2) + ')' : ' (' + t.out + ')'));
      else parts.push(t.stables + ': ' + t.kept);
      var simEl = document.getElementById('sim-new-value');
      var any = pl.fees.ok || (pl.stables && pl.stables.ok);
      h += '<p class="spx-note"><b>' + esc(t.applied) + '</b> ' + esc(parts.join(' · ')) + '</p>' +
        '<p class="spx-note"><b>' + esc(t.target) + '</b> ' + esc(price(p.target)) + ' (' + p.yr + ')' +
        (any ? ' vs <b>' + esc(t.sim) + '</b> <span class="spx-sim">' + esc(simEl ? simEl.textContent : '') + '</span>' : '. ' + esc(t.none)) + '</p>' +
        '<p class="spx-note spx-src">' + esc(t.src) + ': <a href="' + CS_URL + '" target="_blank" rel="noopener">CoinShares, Ethereum Valuation Framework</a> (<a href="' + CS_PDF + '" target="_blank" rel="noopener">PDF</a>) · ' + esc(t.pub) + ' 2026-05-28</p>';
      h += '<p class="spx-note spx-src">' + esc(t.ve) + ' <a href="' + VE_PDF + '" target="_blank" rel="noopener">PDF</a></p>';
    } else {
      h += '<p class="spx-note">' + esc(t.pick) + '</p>';
    }
    box.innerHTML = h;
  }
  function onClick(e) {
    var b = e.target.closest && e.target.closest('.spx-chip');
    if (!b) return;
    PRESETS.forEach(function (p) { if (p.id === b.getAttribute('data-preset')) apply(p); });
  }
  // Any manual slider move or reset leaves the preset; the exact-multiplier override then stops matching by itself.
  function onManual() { if (active && active !== 'locked') { active = null; window.simPresetExact = {}; render(); } }
  var tmr = null;
  function schedule() { clearTimeout(tmr); tmr = setTimeout(render, 60); }
  function init() {
    var box = document.getElementById('sim-presets');
    if (!box) return;
    box.addEventListener('click', onClick);
    ['tvl', 'fees', 'staking', 'stables', 'activeAddresses'].forEach(function (k) {
      var s = document.getElementById('sim-' + k); if (s) s.addEventListener('input', onManual);
    });
    document.querySelectorAll('.sim-reset-btn, #simulator-reset-all').forEach(function (b) { b.addEventListener('click', onManual); });
    new MutationObserver(schedule).observe(document.documentElement, { attributes: true, attributeFilter: ['lang'] });
    var nv = document.getElementById('sim-new-value');
    if (nv) new MutationObserver(function () { if (active && active !== 'locked') schedule(); }).observe(nv, { childList: true, characterData: true, subtree: true });
    render();
  }
  if (document.readyState === 'loading') document.addEventListener('DOMContentLoaded', init); else init();
})();
