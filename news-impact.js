/* ETHval News Impact (v1.1)
 * Reads Supabase `news_events` + `historical_eth_price` and renders:
 *  1) #news-impact-strip   — 7-day net impact gauge, category chips, top event (top of Valuation)
 *  2) #news-impact-panel   — price chart with grouped event markers + matching event cards
 * Readability rules (v1.1):
 *  - Events are grouped into time buckets sized so markers never sit closer than ~11px.
 *    One ▲ (bullish) above and one ▼ (bearish) below the price per bucket; a number = events in it.
 *  - Marker colour = direction (green/red), size = strongest impact. Cards use the identical icon.
 *  - Cards list exactly the events drawn on the chart (same range / category / impact filter).
 *  - Card hover or list scroll highlights its bucket on the chart; tapping a marker filters the list to it.
 * Depends on globals from index.html: SUPABASE_URL, SUPABASE_ANON_KEY, Chart, currentLang (optional).
 */
(function () {
    'use strict';

    const CATS = ['macro', 'flows', 'regulation', 'protocol', 'security', 'competitors', 'ecosystem', 'market'];
    const CAT_COLOR = {
        macro: '#3b82f6', flows: '#10b981', regulation: '#a855f7', protocol: '#6366f1',
        security: '#ef4444', competitors: '#f59e0b', ecosystem: '#14b8a6', market: '#64748b'
    };
    const UP = '#10b981', DOWN = '#ef4444', FLAT = '#94a3b8';
    const T = {
        en: {
            scTitle: 'Call scorecard', scHint: 'Share of calls where ETH moved the called way 7 days later (bullish = up, bearish = down). Neutral calls excluded; 50% is a coin flip.', scAll: 'All categories', scHigh: 'High-impact calls only', scRel: 'vs BTC (all calls)', scRelHint: 'vs BTC = scored on ETH/BTC: bullish = ETH beat BTC, bearish = ETH lagged BTC. Removes the whole-market move.', scHold: 'Too few to judge', scOf: (k, n) => `${k} of ${n}`, scPend: n => `${n} newer calls wait for their 7 days.`, scMin: 'Under 10 calls: not judged yet.',
            up: 'Upcoming', upNone: 'No scheduled FOMC, CPI or mainnet upgrade in the next 60 days.', upSep: 'with projections', upTent: 'Fed dates are tentative until confirmed at the prior meeting.', upKind: { fomc: 'FOMC rate decision', cpi: 'US CPI release', upgrade: 'Ethereum mainnet upgrade' }, dday: n => n === 0 ? 'Today' : `D-${n}`,
            title: 'News Impact', mb: n => `${n} news`, mbTip: n => `${n} news in the last 30 days point to this metric. Tap to see them.`, mbHead: 'Last 30 days', mbClear: 'Clear', mxTitle: 'Impact by week', mxHint: 'Net score per category, last 4 weeks. Tap a cell to see its cards.', mxN: n => `${n} news`, lgScore: 'Net score (bullish − bearish)', lgCount: 'News count', lgHi: 'Includes high-impact news', lgBear: 'Bearish', lgBull: 'Bullish', mxClear: 'Clear', sub: 'How this week\'s news moves ETH', net7: '7D net impact', bull: 'Bullish', bear: 'Bearish', mixed: 'Mixed',
            top: 'Biggest event', all: 'All', none: 'No events in this range yet.', loading: 'Loading news…', err: 'News data unavailable right now.',
            impact: { 3: 'High', 2: 'Medium', 1: 'Low' }, horizon: { immediate: 'Immediate', short: 'Short-term', structural: 'Structural' },
            after: 'ETH after', sources: 'Sources', events: ' events', price: 'ETH price', panel: '01.1 — News Impact on Price',
            hint: '▲ bullish above the price · ▼ bearish below · bigger = stronger · number = events grouped there. Tap a marker to see its cards.',
            imp: { 3: 'High only', 2: 'Medium+', 1: 'All' }, showAll: 'Show all', selN: n => `${n} events`,
            scoreNote: 'Points: high 3 · medium 2 · low 1. Score = bullish − bearish.', last7: 'Last 7 days', drivers: 'What moved it', vBull: 'Good news outweighs bad this week', vBear: 'Bad news outweighs good this week', vMix: 'Good and bad news are balanced', pts: 'pts', noWeek: 'No major ETH news in the last 7 days.', model: 'AI-tagged',
            cats: { macro: 'Macro & Rates', flows: 'Fund Flows', regulation: 'Regulation & Tax', protocol: 'Ethereum Upgrades', security: 'Security', competitors: 'Competing Chains', ecosystem: 'L2 · DeFi · RWA', market: 'Market Structure' }
        },
        ko: {
            scTitle: '판단 성적표', scHint: '사건 7일 뒤 ETH 가격이 판단 방향(호재=상승, 악재=하락)대로 움직인 비율이에요. 중립 판단은 빼고, 50%는 동전 던지기 수준이에요.', scAll: '전체', scHigh: '상(큰 뉴스) 판단만', scRel: 'BTC 대비 (전체)', scRelHint: 'BTC 대비 = ETH/BTC 비율로 채점: 호재 = ETH가 BTC보다 더 오름, 악재 = 덜 오름. 시장 전체 방향 효과를 뺀 값이에요.', scHold: '판단 보류', scOf: (k, n) => `${n}건 중 ${k}건`, scPend: n => `최근 ${n}건은 7일이 아직 안 지나 채점 대기예요.`, scMin: '표본 10건 미만은 판단 보류로 표시해요.',
            up: '예정 일정', upNone: '앞으로 60일 안에 잡힌 FOMC·CPI·메인넷 업그레이드가 없어요.', upSep: '경제전망 포함', upTent: '연준 일정은 직전 회의에서 확정되기 전까지 잠정이에요.', upKind: { fomc: 'FOMC 금리 결정', cpi: '미국 CPI 발표', upgrade: '이더리움 메인넷 업그레이드' }, dday: n => n === 0 ? '오늘' : `D-${n}`,
            title: '뉴스 영향', mb: n => `관련 뉴스 ${n}건`, mbTip: n => `최근 30일 이 지표를 움직인 뉴스 ${n}건. 누르면 뉴스를 보여줘요.`, mbHead: '최근 30일', mbClear: '해제', mxTitle: '주별 영향 지도', mxHint: '최근 4주 카테고리별 뉴스 영향이에요. 칸을 누르면 해당 뉴스만 보여요.', mxN: n => `${n}건`, lgScore: '순영향 점수 (호재 − 악재)', lgCount: '뉴스 건수', lgHi: '큰 뉴스 포함', lgBear: '악재', lgBull: '호재', mxClear: '해제', sub: '이번 주 뉴스가 ETH 가격에 주는 영향', net7: '최근 7일 순영향', bull: '호재', bear: '악재', mixed: '혼재',
            top: '가장 큰 사건', all: '전체', none: '이 기간에는 아직 사건이 없어요.', loading: '뉴스 불러오는 중…', err: '지금은 뉴스 데이터를 불러올 수 없어요.',
            impact: { 3: '상', 2: '중', 1: '하' }, horizon: { immediate: '즉시', short: '단기', structural: '구조적' },
            after: '이후 ETH', sources: '출처', events: '건', price: 'ETH 가격', panel: '01.1 — 뉴스와 가격',
            hint: '가격 위 ▲는 호재, 아래 ▼는 악재예요. 클수록 영향이 크고, 숫자는 묶인 사건 수예요. 표시를 누르면 해당 카드만 보여요.',
            imp: { 3: '상만', 2: '중 이상', 1: '전체' }, showAll: '전체 보기', selN: n => `${n}건`,
            scoreNote: '점수: 상 3 · 중 2 · 하 1점. 순영향 = 호재 − 악재.', last7: '최근 7일', drivers: '무엇이 움직였나', vBull: '이번 주는 호재가 더 커요', vBear: '이번 주는 악재가 더 커요', vMix: '호재와 악재가 비슷해요', pts: '점', noWeek: '최근 7일 동안 큰 ETH 뉴스가 없어요.', model: 'AI 분류',
            cats: { macro: '거시·금리', flows: '자금 흐름', regulation: '제도·규제·과세', protocol: '이더리움 개선', security: '사고·보안', competitors: '경쟁 체인', ecosystem: 'L2·DeFi·RWA', market: '시장 구조·심리' }
        },
        zh: {
            scTitle: '判断成绩单', scHint: '事件 7 日后 ETH 价格按判断方向（利好=上涨，利空=下跌）变动的比例。不含中性判断；50% 相当于掷硬币。', scAll: '全部', scHigh: '仅高影响判断', scRel: '相对 BTC（全部）', scRelHint: '相对 BTC = 按 ETH/BTC 评分：利好 = ETH 跑赢 BTC，利空 = 跑输 BTC。剔除整体市场方向。', scHold: '暂不评判', scOf: (k, n) => `${n} 条中 ${k} 条`, scPend: n => `最近 ${n} 条未满 7 日，待评分。`, scMin: '样本少于 10 条时暂不评判。',
            up: '即将到来', upNone: '未来 60 日内没有已排定的 FOMC、CPI 或主网升级。', upSep: '含经济预测', upTent: '美联储日程在上次会议确认前均为暂定。', upKind: { fomc: 'FOMC 利率决议', cpi: '美国 CPI 公布', upgrade: '以太坊主网升级' }, dday: n => n === 0 ? '今天' : `D-${n}`,
            title: '新闻影响', mb: n => `相关新闻 ${n} 条`, mbTip: n => `近 30 日有 ${n} 条新闻影响此指标，点击查看。`, mbHead: '近 30 日', mbClear: '清除', mxTitle: '每周影响矩阵', mxHint: '近 4 周各类别新闻影响。点击格子只看对应新闻。', mxN: n => `${n}条`, lgScore: '净影响分（利好 − 利空）', lgCount: '新闻条数', lgHi: '含重大新闻', lgBear: '利空', lgBull: '利好', mxClear: '清除', sub: '本周新闻如何影响 ETH 价格', net7: '近 7 日净影响', bull: '利好', bear: '利空', mixed: '中性',
            top: '最大事件', all: '全部', none: '该区间暂无事件。', loading: '正在加载新闻…', err: '暂时无法加载新闻数据。',
            impact: { 3: '高', 2: '中', 1: '低' }, horizon: { immediate: '即时', short: '短期', structural: '结构性' },
            after: '之后 ETH', sources: '来源', events: '条', price: 'ETH 价格', panel: '01.1 — 新闻与价格',
            hint: '价格上方 ▲ 为利好，下方 ▼ 为利空；越大影响越强，数字为合并的事件数。点击标记只看对应卡片。',
            imp: { 3: '仅高', 2: '中及以上', 1: '全部' }, showAll: '查看全部', selN: n => `${n} 条`,
            scoreNote: '分值：高 3 · 中 2 · 低 1。净影响 = 利好 − 利空。', last7: '近 7 日', drivers: '主要推动因素', vBull: '本周利好大于利空', vBear: '本周利空大于利好', vMix: '利好与利空大致相当', pts: '分', noWeek: '近 7 日没有重大 ETH 新闻。', model: 'AI 标注',
            cats: { macro: '宏观·利率', flows: '资金流向', regulation: '监管·税收', protocol: '以太坊升级', security: '安全事件', competitors: '竞争公链', ecosystem: 'L2·DeFi·RWA', market: '市场结构' }
        },
        ja: {
            scTitle: '判定の成績表', scHint: '出来事から7日後のETH価格が判定どおり（好材料=上昇、悪材料=下落）に動いた割合。中立判定は除外、50%はコイン投げ並み。', scAll: '全体', scHigh: '大ニュースの判定のみ', scRel: 'BTC比（全体）', scRelHint: 'BTC比 = ETH/BTC比で採点：好材料 = ETHがBTCを上回る、悪材料 = 下回る。市場全体の方向を除いた値。', scHold: '判定保留', scOf: (k, n) => `${n}件中 ${k}件`, scPend: n => `直近${n}件は7日未経過のため採点待ち。`, scMin: 'サンプル10件未満は判定保留。',
            up: '今後の予定', upNone: '今後60日以内に予定されたFOMC・CPI・メインネットアップグレードはありません。', upSep: '経済見通しあり', upTent: 'FRBの日程は前回会合で確定するまで暫定です。', upKind: { fomc: 'FOMC 金利決定', cpi: '米CPI発表', upgrade: 'イーサリアム メインネット アップグレード' }, dday: n => n === 0 ? '今日' : `D-${n}`,
            title: 'ニュース影響', mb: n => `関連ニュース ${n}件`, mbTip: n => `直近30日でこの指標を動かしたニュース ${n}件。タップで表示。`, mbHead: '直近30日', mbClear: '解除', mxTitle: '週別インパクト表', mxHint: '直近4週・カテゴリ別のニュース影響。セルをタップで該当ニュースだけ表示。', mxN: n => `${n}件`, lgScore: '純影響スコア（好材料 − 悪材料）', lgCount: 'ニュース件数', lgHi: '大きなニュースを含む', lgBear: '悪材料', lgBull: '好材料', mxClear: '解除', sub: '今週のニュースが ETH 価格に与える影響', net7: '直近7日の純影響', bull: '好材料', bear: '悪材料', mixed: '中立',
            top: '最大の出来事', all: 'すべて', none: 'この期間のイベントはまだありません。', loading: 'ニュースを読み込み中…', err: '現在ニュースデータを取得できません。',
            impact: { 3: '大', 2: '中', 1: '小' }, horizon: { immediate: '即時', short: '短期', structural: '構造的' },
            after: 'その後のETH', sources: '出典', events: '件', price: 'ETH 価格', panel: '01.1 — ニュースと価格',
            hint: '価格の上 ▲ は好材料、下 ▼ は悪材料。大きいほど影響が強く、数字はまとめた件数。マーカーをタップで該当カードだけ表示。',
            imp: { 3: '大のみ', 2: '中以上', 1: 'すべて' }, showAll: 'すべて表示', selN: n => `${n}件`,
            scoreNote: '点数：大3・中2・小1。純影響 = 好材料 − 悪材料。', last7: '直近7日', drivers: '動かした要因', vBull: '今週は好材料が上回っています', vBear: '今週は悪材料が上回っています', vMix: '好材料と悪材料が拮抗しています', pts: '点', noWeek: '直近7日に大きなETHニュースはありません。', model: 'AI 分類',
            cats: { macro: 'マクロ・金利', flows: '資金フロー', regulation: '規制・税制', protocol: 'イーサリアム改善', security: 'セキュリティ', competitors: '競合チェーン', ecosystem: 'L2・DeFi・RWA', market: '市場構造' }
        }
    };
    const METRIC_LABEL = {
        price: 'Price', fear_greed: 'Fear & Greed', funding_rate: 'Funding', open_interest: 'Open Interest', exchange_reserve: 'Exchange Reserve',
        etf_flows: 'ETF Flows', staking: 'Staking', burn: 'Burn', supply: 'Supply', fees: 'Fees', tvl: 'TVL', stablecoins: 'Stablecoins',
        l2_activity: 'L2 Activity', dominance: 'Dominance', rates: 'Rates', dxy: 'Dollar', btc: 'BTC', volatility: 'Volatility', active_addresses: 'Active Addr.'
    };

    // news_events.metrics key -> Fundamentals card data-metric ids (only keys that have a matching card).
    const METRIC_CARDS = {
        fear_greed: ['feargreed'], funding_rate: ['funding'], open_interest: ['openInterest'], exchange_reserve: ['reserve'],
        staking: ['staking', 'stakingApr'], burn: ['daily-burn'], supply: ['supply', 'net-supply'], fees: ['fees'], tvl: ['tvl'],
        stablecoins: ['stablecoin', 'stables'], l2_activity: ['l2'], dominance: ['dominance'], btc: ['ethbtc'], volatility: ['volatility'],
        active_addresses: ['l1ActiveAddr'], rates: ['us10y'], dxy: ['dxy']
    };
    const MB_DAYS = 30;

    // minImp: impact filter (1 all, 2 medium+, 3 high only). sel: selected bucket key. hl: highlighted bucket key.
    const S = { events: [], cal: [], prices: [], cat: 'all', range: 90, minImp: 2, sel: null, hl: null, bd: 1, origin: 0, pts: [], chart: null, openId: null, week: null, metric: null, status: 'loading' };

    const lang = () => { const l = (typeof currentLang !== 'undefined' && currentLang) || document.documentElement.lang || 'en'; return T[l] ? l : 'en'; };
    const tr = () => T[lang()];
    const loc = (o) => (o && (o[lang()] || o.en)) || '';
    const esc = (s) => String(s == null ? '' : s).replace(/[&<>"']/g, c => ({ '&': '&amp;', '<': '&lt;', '>': '&gt;', '"': '&quot;', "'": '&#39;' }[c]));
    const dayMs = 86400000;
    const tOf = (e) => new Date(e.date + 'T00:00:00Z').getTime();
    const noHover = () => window.matchMedia && window.matchMedia('(hover: none)').matches;

    async function rest(path) {
        const url = (typeof SUPABASE_URL !== 'undefined' ? SUPABASE_URL : 'https://rliwxedrifwxbudcutqs.supabase.co') + '/rest/v1/' + path;
        const key = typeof SUPABASE_ANON_KEY !== 'undefined' ? SUPABASE_ANON_KEY : '';
        const r = await fetch(url, { headers: { apikey: key, Authorization: 'Bearer ' + key } });
        if (!r.ok) throw new Error('HTTP ' + r.status);
        return r.json();
    }

    async function load() {
        try {
            const since = new Date(Date.now() - 400 * dayMs).toISOString().slice(0, 10);
            const [ev, px, rx] = await Promise.all([
                rest(`news_events?select=id,date,category,direction,impact,horizon,metrics,title,summary,mechanism,sources,confidence&date=gte.${since}&order=date.desc,impact.desc&limit=1000`),
                rest(`historical_eth_price?select=date,close&date=gte.${since}&order=date.asc&limit=1000`),
                // ETH/BTC ratio (single source: Dune) for the market-neutral score; optional — a failure only hides that cell.
                rest(`historical_eth_btc?select=date,ratio&date=gte.${since}&order=date.asc&limit=1000`).catch(() => [])
            ]);
            S.events = ev || [];
            // Scheduled events are optional: a missing table/row must not hide the news itself.
            S.cal = await rest(`calendar_events?select=id,kind,name,date,date_start,starts_at,sep,source,source_url&date=gte.${isoDay(Date.now() - dayMs)}&order=date.asc&limit=12`).catch(() => []);
            S.prices = (px || []).map(p => ({ t: new Date(p.date + 'T00:00:00Z').getTime(), v: +p.close })).filter(p => p.v > 0);
            S.ratios = (rx || []).map(p => ({ t: new Date(p.date + 'T00:00:00Z').getTime(), v: +p.ratio })).filter(p => p.v > 0);
            S.status = 'ok';
        } catch (e) {
            console.warn('[news-impact]', e.message);
            S.status = 'error';
        }
        render();
    }

    function priceAt(t, P = S.prices) {
        if (!P.length) return null;
        let lo = 0, hi = P.length - 1;
        if (t <= P[0].t) return P[0].v;
        if (t >= P[hi].t) return t - P[hi].t <= 2 * dayMs ? P[hi].v : null;
        while (hi - lo > 1) { const m = (lo + hi) >> 1; if (P[m].t <= t) lo = m; else hi = m; }
        return P[lo].v;
    }
    function reaction(ev, days, P = S.prices) {
        const t0 = tOf(ev);
        const last = P.length ? P[P.length - 1].t : 0;
        if (t0 + days * dayMs > last) return null;
        const a = priceAt(t0, P), b = priceAt(t0 + days * dayMs, P);
        return a && b ? (b / a - 1) * 100 : null;
    }

    function netScore(days) {
        const cut = Date.now() - days * dayMs;
        let s = 0;
        const per = {};
        CATS.forEach(c => per[c] = { up: 0, down: 0, n: 0, s: 0, bu: 0, be: 0 });
        let bu = 0, be = 0;
        S.events.forEach(e => {
            if (tOf(e) < cut) return;
            const w = e.direction * e.impact;
            s += w;
            const p = per[e.category]; if (!p) return;
            p.n++; p.s += w; if (e.direction > 0) { p.up++; p.bu += e.impact; bu += e.impact; } else if (e.direction < 0) { p.down++; p.be += e.impact; be += e.impact; }
        });
        return { s: Math.max(-10, Math.min(10, s)), raw: s, per, bu, be };
    }

    function rangeCut() { return Date.now() - S.range * dayMs; }
    // Events drawn on the chart AND listed as cards — one filter for both so they always match.
    function visible() {
        if (S.metric) { const cut = Date.now() - MB_DAYS * dayMs; return S.events.filter(e => tOf(e) >= cut && (e.metrics || []).includes(S.metric)); }
        if (S.week != null) { const [a, b] = weekSpan(S.week); return S.events.filter(e => e.date >= a && e.date <= b && (S.cat === 'all' || e.category === S.cat)); }
        const cut = rangeCut();
        return S.events.filter(e => tOf(e) >= cut && e.impact >= S.minImp && (S.cat === 'all' || e.category === S.cat));
    }
    // Bucket size: markers at least ~11px apart for the current chart width.
    function computeBuckets() {
        const box = document.querySelector('#news-impact-panel .ni-chart-box');
        const w = Math.max(240, (box ? box.clientWidth : 600) - 60);
        let bd = Math.max(1, Math.ceil(S.range * 11 / w));
        if (bd > 3 && bd < 7) bd = 7; else if (bd === 3 && S.range > 90) bd = 7;
        S.bd = bd;
        const startDay = Math.floor(rangeCut() / dayMs) * dayMs;
        S.origin = startDay;
    }
    const bucketOf = (t) => Math.floor((t - S.origin) / (S.bd * dayMs));
    const bucketStart = (k) => S.origin + k * S.bd * dayMs;
    const bucketLabel = (k) => {
        const a = new Date(bucketStart(k)), b = new Date(bucketStart(k) + (S.bd - 1) * dayMs);
        const f = d => String(d.getUTCMonth() + 1).padStart(2, '0') + '.' + String(d.getUTCDate()).padStart(2, '0');
        return S.bd === 1 ? f(a) : f(a) + '–' + f(b);
    };

    // Impact matrix: 4 UTC weeks ending today (week 3 = latest). Counts every impact level.
    const isoDay = (t) => new Date(t).toISOString().slice(0, 10);
    const todayUtc = () => Date.parse(isoDay(Date.now()) + 'T00:00:00Z');
    function weekSpan(w) { const end = todayUtc() - (3 - w) * 7 * dayMs; return [isoDay(end - 6 * dayMs), isoDay(end)]; }
    const mmdd = (d) => d.slice(5).replace('-', '.');
    function matrixHtml() {
        const L = tr();
        const spans = [0, 1, 2, 3].map(weekSpan);
        const cell = {};
        S.events.forEach(e => {
            const w = spans.findIndex(([a, b]) => e.date >= a && e.date <= b);
            if (w < 0 || !CAT_COLOR[e.category]) return;
            const c = cell[e.category + w] || (cell[e.category + w] = { n: 0, s: 0, hi: 0 });
            c.n++; c.s += (e.direction > 0 ? 1 : e.direction < 0 ? -1 : 0) * e.impact; if (e.impact >= 3) c.hi++;
        });
        const head = spans.map(([a, b], w) => `<div class="ni-mx-wk${S.week === w ? ' on' : ''}"><span>${esc(mmdd(a))}–</span><span>${esc(mmdd(b))}</span></div>`).join('');
        const rows = CATS.map(cat => `<div class="ni-mx-cat"><i style="background:${CAT_COLOR[cat]}"></i><span>${esc(L.cats[cat])}</span></div>` + [0, 1, 2, 3].map(w => {
            const c = cell[cat + w];
            if (!c) return `<div class="ni-mx-cell empty" data-cat="${cat}" data-week="${w}" aria-hidden="true"><b>·</b></div>`;
            const a = Math.min(Math.abs(c.s), 6) / 6;
            const bg = c.s ? `color-mix(in srgb, ${c.s > 0 ? 'var(--up)' : 'var(--down)'} ${Math.round(10 + a * 30)}%, var(--bg-card))` : 'var(--bg-subtle)';
            const tone = c.s > 0 ? 'up' : c.s < 0 ? 'down' : 'flat';
            const lbl = `${L.cats[cat]} ${mmdd(spans[w][0])}–${mmdd(spans[w][1])}: ${c.s > 0 ? '+' : ''}${c.s}${L.pts}, ${L.selN(c.n)}`;
            return `<button type="button" class="ni-mx-cell ${tone}${S.week === w && S.cat === cat ? ' on' : ''}" data-cat="${cat}" data-week="${w}" style="background:${bg}" title="${esc(lbl)}" aria-label="${esc(lbl)}">${c.hi ? '<span class="ni-mx-big"></span>' : ''}<b>${c.s > 0 ? '+' : c.s < 0 ? '−' : ''}${Math.abs(c.s)}</b><small>${esc(L.mxN(c.n))}</small></button>`;
        }).join('')).join('');
        const pill = S.week != null ? `<button type="button" class="ni-wk-pill"><span>${esc(mmdd(spans[S.week][0]))}–${esc(mmdd(spans[S.week][1]))}${S.cat !== 'all' ? ' · ' + esc(L.cats[S.cat]) : ''}</span> ${esc(L.mxClear)} ×</button>` : '';
        return `<div class="ni-mx-head"><span class="ni-mx-title">${esc(L.mxTitle)}</span><span class="ni-mx-hint">${esc(L.mxHint)}</span>${pill}</div><div class="ni-mx-grid"><div class="ni-mx-corner"></div>${head}${rows}</div><div class="ni-mx-legend"><span class="ni-lg-key"><span class="ni-lg-cell"><b>+4</b><small>${esc(L.mxN(2))}</small></span><span class="ni-lg-txt"><span><b>+4</b> ${esc(L.lgScore)}</span><span><small>${esc(L.mxN(2))}</small> ${esc(L.lgCount)}</span></span></span><span class="ni-lg-row"><span class="ni-lg-dot"></span>${esc(L.lgHi)}</span><span class="ni-lg-row">${esc(L.lgBear)}<span class="ni-lg-scale"></span>${esc(L.lgBull)}</span></div>`;
    }

    // Call scorecard: did ETH move the called way 7 days after each event? Neutral calls are excluded.
    // Same price lookup as the card reactions (reaction()), so the score and the cards never disagree.
    const SC_DAYS = 7, SC_MIN = 10;
    function scoreData() {
        const per = {}; CATS.forEach(c => per[c] = { n: 0, k: 0 });
        let n = 0, k = 0, pend = 0;
        const high = { n: 0, k: 0 }, rel = { n: 0, k: 0, ok: (S.ratios || []).length > 0 };
        S.events.forEach(e => {
            if (!e.direction || !per[e.category]) return;
            const r = reaction(e, SC_DAYS);
            if (r == null) { pend++; return; }
            const hit = Math.sign(r) === Math.sign(e.direction);
            per[e.category].n++; n++; if (hit) { per[e.category].k++; k++; }
            if (e.impact === 3) { high.n++; if (hit) high.k++; }
            // Market-neutral: same 7-day window on ETH/BTC (ETH beat or lagged BTC). Only events with both prices scored.
            const q = rel.ok ? reaction(e, SC_DAYS, S.ratios) : null;
            if (q != null) { rel.n++; if (Math.sign(q) === Math.sign(e.direction)) rel.k++; }
        });
        return { per, n, k, pend, high, rel };
    }
    function scoreHtml() {
        const L = tr(), d = scoreData();
        const pct = (k, n) => Math.round(k / n * 1000) / 10;
        const cellHtml = (name, dot, k, n, cls) => {
            if (n < SC_MIN) return `<div class="ni-sc-cell hold${cls}"><span class="ni-sc-name">${dot}<span>${esc(name)}</span></span><span class="ni-sc-bar" aria-hidden="true"><u></u></span><small><b class="ni-sc-pct">${esc(L.scHold)}</b> · ${esc(L.scOf(k, n))}</small></div>`;
            const v = pct(k, n), tone = v >= 55 ? 'up' : v <= 45 ? 'down' : 'flat';
            return `<div class="ni-sc-cell ${tone}${cls}" data-n="${n}" data-k="${k}"><span class="ni-sc-name">${dot}<span>${esc(name)}</span></span><b class="ni-sc-pct">${v.toFixed(1)}%</b><span class="ni-sc-bar" aria-hidden="true"><i style="width:${v}%"></i><u></u></span><small>${esc(L.scOf(k, n))}</small></div>`;
        };
        const cells = CATS.map(c => cellHtml(L.cats[c], `<i class="ni-sc-dot" style="background:${CAT_COLOR[c]}"></i>`, d.per[c].k, d.per[c].n, '" data-cat="' + c)).join('');
        const all = cellHtml(L.scAll, '', d.k, d.n, ' all sum" data-sum="all');
        const high = cellHtml(L.scHigh, '', d.high.k, d.high.n, ' sum" data-sum="high');
        const rel = d.rel.ok ? cellHtml(L.scRel, '', d.rel.k, d.rel.n, ' sum" data-sum="rel" title="' + esc(L.scRelHint)) : '';
        const foot = [d.rel.ok ? L.scRelHint : '', d.pend ? L.scPend(d.pend) : '', L.scMin].filter(Boolean).map(esc).join(' ');
        return `<div class="ni-mx-head"><span class="ni-mx-title">${esc(L.scTitle)}</span><span class="ni-mx-hint">${esc(L.scHint)}</span></div><div class="ni-sc-top">${all}${high}${rel}</div><div class="ni-sc-grid">${cells}</div><p class="ni-sc-foot">${foot}</p>`;
    }

    function icon(dir, imp) {
        const s = [0, 10, 12, 14][imp] || 12;
        const c = dir > 0 ? UP : dir < 0 ? DOWN : FLAT;
        const path = dir > 0 ? 'M7 1 L13 12 L1 12 Z' : dir < 0 ? 'M1 2 L13 2 L7 13 Z' : 'M7 1 L13 7 L7 13 L1 7 Z';
        return `<svg class="ni-ico" width="${s}" height="${s}" viewBox="0 0 14 14" aria-hidden="true"><path d="${path}" fill="${c}"/></svg>`;
    }

    function chipsHtml(per) {
        const L = tr();
        const all = `<button type="button" class="ni-chip${S.cat === 'all' ? ' on' : ''}" data-cat="all">${esc(L.all)}</button>`;
        return all + CATS.map(c => {
            const p = per[c];
            const tone = p.s > 0 ? 'up' : p.s < 0 ? 'down' : 'flat';
            const cnt = p.n ? `<span class="ni-chip-n">${p.up ? `<span class="up">▲${p.up}</span>` : ''}${p.down ? `<span class="down">▼${p.down}</span>` : ''}${!p.up && !p.down ? `<span class="flat">·${p.n}</span>` : ''}</span>` : '';
            return `<button type="button" class="ni-chip${S.cat === c ? ' on' : ''}${p.n ? '' : ' empty'}" data-cat="${c}"><i style="background:${CAT_COLOR[c]}"></i>${esc(L.cats[c])}${cnt}</button>`;
        }).join('');
    }

    // Keep the strip directly under the price box even if a stale cached HTML still has it above (Simon 2026-10-13).
    function placeStrip() {
        const el = document.getElementById('news-impact-strip');
        const box = document.querySelector('.summary-valuation-box');
        if (!el || !box || box.nextElementSibling === el) return;
        if (!box.parentElement.classList.contains('summary-left')) {
            const wrap = document.createElement('div');
            wrap.className = 'summary-left';
            box.parentElement.insertBefore(wrap, box);
            wrap.appendChild(box);
        }
        box.insertAdjacentElement('afterend', el);
    }

    // Upcoming line: next FOMC / CPI / mainnet upgrade from first-party schedules (calendar_events), 60-day window.
    function upcomingHtml() {
        const L = tr();
        // D-day counts calendar days in the reader's own timezone: an exact-time release (CPI, upgrade) uses its
        // local date; a date-only US event (FOMC) uses its stated date.
        const now = Date.now();
        const localDay = (t) => { const d = new Date(t); return Date.UTC(d.getFullYear(), d.getMonth(), d.getDate()); };
        const today = localDay(now);
        const dayOf = (c) => c.starts_at ? localDay(Date.parse(c.starts_at)) : Date.parse(c.date + 'T00:00:00Z');
        const rows = (S.cal || []).filter(c => {
            const end = c.starts_at ? Date.parse(c.starts_at) : Date.parse(c.date + 'T23:59:59Z') + 5 * 3600000;
            return end >= now && dayOf(c) - today <= 60 * dayMs;
        }).slice(0, 3);
        const items = rows.map(c => {
            const d = Math.max(0, Math.round((dayOf(c) - today) / dayMs));
            const day = isoDay(dayOf(c));
            const when = c.date_start && c.date_start !== c.date ? `${mmdd(c.date_start)}–${mmdd(c.date)}` : mmdd(c.starts_at ? day : c.date);
            const time = c.starts_at ? ' ' + new Date(c.starts_at).toLocaleTimeString(lang() === 'en' ? 'en-US' : lang(), { hour: '2-digit', minute: '2-digit', hourCycle: 'h23', timeZoneName: 'short' }) : '';
            const nm = c.kind === 'upgrade' ? `${L.upKind.upgrade} · ${c.name}` : L.upKind[c.kind] || c.name;
            const tip = `${nm} — ${c.source}${c.kind === 'fomc' ? ' · ' + L.upTent : ''}`;
            return `<a class="ni-up-i k-${esc(c.kind)}" href="${esc(c.source_url)}" target="_blank" rel="noopener noreferrer" title="${esc(tip)}"><span class="ni-up-d${d <= 3 ? ' soon' : ''}">${esc(L.dday(d))}</span><span class="ni-up-n">${esc(nm)}${c.sep ? `<small>${esc(L.upSep)}</small>` : ''}</span><span class="ni-up-w">${esc(when + time)}</span></a>`;
        }).join('');
        return `<div class="ni-up"><div class="ni-sec-k">${esc(L.up)}</div>${items || `<div class="ni-up-none">${esc(L.upNone)}</div>`}</div>`;
    }

    function renderStrip() {
        placeStrip();
        const el = document.getElementById('news-impact-strip');
        if (!el) return;
        const L = tr();
        if (S.status !== 'ok') { el.innerHTML = `<div class="ni-strip-msg">${esc(S.status === 'error' ? L.err : L.loading)}</div>`; return; }
        const n = netScore(7);
        const tone = n.s > 0 ? 'up' : n.s < 0 ? 'down' : 'flat';
        const word = n.s > 0 ? L.bull : n.s < 0 ? L.bear : L.mixed;
        const pct = (n.s + 10) / 20 * 100;
        const week = S.events.filter(e => Date.now() - tOf(e) <= 7 * dayMs);
        const top = week.slice().sort((a, b) => b.impact - a.impact || Math.abs(b.direction) - Math.abs(a.direction) || b.date.localeCompare(a.date))[0];
        const verdict = n.raw > 0 ? L.vBull : n.raw < 0 ? L.vBear : L.vMix;
        const tot = n.bu + n.be;
        const bw = tot ? Math.round(n.bu / tot * 100) : 50;
        // Categories that had news this week, biggest net mover first.
        const rows = CATS.filter(c => n.per[c].n).sort((a, b) => Math.abs(n.per[b].s) - Math.abs(n.per[a].s) || n.per[b].n - n.per[a].n);
        const mx = Math.max(1, ...rows.map(c => Math.max(n.per[c].bu, n.per[c].be)));
        const rowHtml = rows.map(c => {
            const p = n.per[c], t = p.s > 0 ? 'up' : p.s < 0 ? 'down' : 'flat';
            return `<button type="button" class="ni-drv${S.cat === c ? ' on' : ''}" data-cat="${c}">
                <span class="ni-drv-name"><i style="background:${CAT_COLOR[c]}"></i>${esc(L.cats[c])}</span>
                <span class="ni-drv-bar" aria-hidden="true"><span class="be" style="width:${p.be / mx * 50}%"></span><span class="bu" style="width:${p.bu / mx * 50}%"></span></span>
                <span class="ni-drv-n ${t}">${p.s > 0 ? '+' : ''}${p.s}</span>
            </button>`;
        }).join('');
        const td = top ? top.date.slice(5).replace('-', '.') : '';
        el.innerHTML = `
            <div class="ni-v-head"><strong>${esc(L.title)}</strong><span>${esc(L.last7)} · ${week.length}${esc(L.events)}</span></div>
            ${week.length ? `
            <div class="ni-v-main" title="${esc(L.scoreNote)}">
                <span class="ni-v-score ${tone}">${n.raw > 0 ? '+' : ''}${n.raw}</span>
                <span class="ni-v-text"><b class="${tone}">${esc(word)}</b><span>${esc(verdict)}</span></span>
            </div>
            <div class="ni-tug">
                <div class="ni-tug-bar" aria-hidden="true"><span class="bu" style="width:${bw}%"></span><span class="be" style="width:${100 - bw}%"></span></div>
                <div class="ni-tug-k"><span class="up">▲ ${esc(L.bull)} ${n.bu}${esc(L.pts)}</span><span class="down">${esc(L.bear)} ${n.be}${esc(L.pts)} ▼</span></div>
            </div>
            <div class="ni-drvs">
                <div class="ni-sec-k">${esc(L.drivers)}</div>
                ${rowHtml}
            </div>
            ${top ? `<button type="button" class="ni-top" data-open="${esc(top.id)}"><span class="ni-top-k">${esc(L.top)} · ${esc(td)}${dirBadge(top)}</span><span class="ni-top-t">${esc(loc(top.title))}</span><span class="ni-top-m">${esc(loc(top.mechanism))}</span></button>` : ''}
            <div class="ni-v-note">${esc(L.scoreNote)}</div>` : `<div class="ni-strip-msg">${esc(L.noWeek)}</div>`}
            ${upcomingHtml()}`;
    }

    function dirBadge(e) {
        const L = tr();
        const tone = e.direction > 0 ? 'up' : e.direction < 0 ? 'down' : 'flat';
        return `<span class="ni-pill ${tone}">${esc(e.direction > 0 ? L.bull : e.direction < 0 ? L.bear : L.mixed)} · ${esc(L.impact[e.impact])}</span>`;
    }

    function cardHtml(e) {
        const L = tr();
        const r = [1, 7, 30].map(d => { const v = reaction(e, d); return v == null ? '' : `<span class="ni-rx ${v >= 0 ? 'up' : 'down'}">${d}D ${v >= 0 ? '+' : ''}${v.toFixed(1)}%</span>`; }).join('');
        const src = (e.sources || []).slice(0, 3).map(s => `<a href="${esc(s.url)}" target="_blank" rel="noopener noreferrer">${esc(s.source || 'link')}</a>`).join(' · ');
        const k = bucketOf(tOf(e));
        const cls = (S.openId === e.id ? ' open' : '') + (S.hl === k ? ' hl' : '');
        return `<article class="ni-card${cls}" data-id="${esc(e.id)}" data-b="${k}" style="--cat:${CAT_COLOR[e.category] || '#64748b'}">
            <div class="ni-card-top">${icon(e.direction, e.impact)}<span class="ni-date">${esc(e.date.slice(5).replace('-', '.'))}</span><span class="ni-cat"><i></i>${esc(L.cats[e.category] || e.category)}</span>${dirBadge(e)}<span class="ni-hz">${esc(L.horizon[e.horizon] || '')}</span></div>
            <div class="ni-card-title">${esc(loc(e.title))}</div>
            <div class="ni-card-mech">${esc(loc(e.mechanism))}</div>
            <div class="ni-card-more">
                ${e.summary ? `<p>${esc(loc(e.summary))}</p>` : ''}
                <div class="ni-metrics">${(e.metrics || []).map(m => `<span>${esc(METRIC_LABEL[m] || m)}</span>`).join('')}</div>
                ${r ? `<div class="ni-rxs"><span class="ni-rx-k">${esc(L.after)}</span>${r}</div>` : ''}
                ${src ? `<div class="ni-src"><span>${esc(L.sources)}</span> ${src}</div>` : ''}
            </div>
        </article>`;
    }

    function renderPanel() {
        const el = document.getElementById('news-impact-panel');
        if (!el) return;
        const L = tr();
        const title = el.querySelector('.ni-panel-title'); if (title) title.textContent = L.panel;
        const hint = el.querySelector('.ni-hint'); if (hint) hint.textContent = L.hint;
        const list = el.querySelector('.ni-list');
        const chips = el.querySelector('.ni-panel-chips');
        el.querySelectorAll('.ni-range button').forEach(b => b.classList.toggle('on', +b.dataset.range === S.range));
        const imp = el.querySelector('.ni-imp');
        if (imp) imp.innerHTML = [2, 3, 1].map(v => `<button type="button" data-imp="${v}" class="${S.minImp === v ? 'on' : ''}">${esc(L.imp[v])}</button>`).join('');
        if (S.status !== 'ok') { list.innerHTML = `<div class="ni-empty">${esc(S.status === 'error' ? L.err : L.loading)}</div>`; return; }
        chips.innerHTML = chipsHtml(netScore(S.range).per);
        let mx = el.querySelector('.ni-matrix');
        if (!mx) { mx = document.createElement('div'); mx.className = 'ni-matrix'; el.appendChild(mx); }
        mx.innerHTML = matrixHtml();
        let sc = el.querySelector('.ni-score');
        if (!sc) { sc = document.createElement('div'); sc.className = 'ni-score'; el.appendChild(sc); }
        sc.innerHTML = scoreHtml();
        computeBuckets();
        const evs = visible();
        const shown = S.sel == null ? evs : evs.filter(e => bucketOf(tOf(e)) === S.sel);
        const mHead = S.metric ? `<div class="ni-sel ni-msel"><span>${esc(METRIC_LABEL[S.metric] || S.metric)} · ${esc(L.mbHead)} · ${esc(L.selN(evs.length))}</span><button type="button" class="ni-metric-clear">${esc(L.mbClear)} ×</button></div>` : '';
        const selHead = S.sel == null ? mHead : `<div class="ni-sel"><span>${esc(bucketLabel(S.sel))} · ${esc(L.selN(shown.length))}</span><button type="button" class="ni-sel-clear">${esc(L.showAll)} ×</button></div>`;
        list.innerHTML = selHead + (shown.length ? shown.map(cardHtml).join('') : `<div class="ni-empty">${esc(L.none)}</div>`);
        list.scrollTop = 0;
        drawChart(evs);
    }

    function groupPoints(evs) {
        const groups = new Map();
        evs.forEach(e => {
            const k = bucketOf(tOf(e));
            const d = e.direction > 0 ? 1 : e.direction < 0 ? -1 : 0;
            const id = k + ':' + d;
            if (!groups.has(id)) groups.set(id, { k, dir: d, evs: [] });
            groups.get(id).evs.push(e);
        });
        const out = [];
        groups.forEach(g => {
            const t0 = bucketStart(g.k), t1 = t0 + (S.bd - 1) * dayMs;
            const ps = S.prices.filter(p => p.t >= t0 && p.t <= t1).map(p => p.v);
            const base = ps.length ? (g.dir > 0 ? Math.max(...ps) : g.dir < 0 ? Math.min(...ps) : ps[ps.length >> 1]) : priceAt(t0);
            if (base == null) return;
            out.push({ k: g.k, dir: g.dir, n: g.evs.length, imp: Math.max(...g.evs.map(e => e.impact)), evs: g.evs, x: t0 + (S.bd - 1) * dayMs / 2, base });
        });
        return out.sort((a, b) => a.x - b.x);
    }

    const radius = (p) => ([0, 4, 5.5, 7.5][p.imp] || 5) + Math.min(p.n - 1, 3) * 0.6 + (S.hl === p.k || S.sel === p.k ? 2.5 : 0);

    // Draws the highlight band and the per-marker event counts.
    const overlay = {
        id: 'niOverlay',
        beforeDatasetsDraw(chart) {
            const k = S.hl != null ? S.hl : S.sel;
            if (k == null) return;
            const { ctx, chartArea: a, scales: { x } } = chart;
            const x0 = x.getPixelForValue(bucketStart(k) - dayMs / 2), x1 = x.getPixelForValue(bucketStart(k) + (S.bd - 0.5) * dayMs);
            ctx.save();
            ctx.fillStyle = document.body.classList.contains('dark') ? 'rgba(148,163,184,0.16)' : 'rgba(100,116,139,0.14)';
            const w = Math.max(6, x1 - x0);
            ctx.fillRect((x0 + x1) / 2 - w / 2, a.top, w, a.bottom - a.top);
            ctx.restore();
        },
        afterDatasetsDraw(chart) {
            const meta = chart.getDatasetMeta(1);
            if (!meta || !meta.data) return;
            const { ctx } = chart;
            ctx.save();
            ctx.font = '600 9px ui-monospace, SFMono-Regular, Menlo, monospace';
            ctx.textAlign = 'center';
            ctx.fillStyle = document.body.classList.contains('dark') ? '#cbd5e1' : '#334155';
            meta.data.forEach((el, i) => {
                const p = S.pts[i];
                if (!p || p.n < 2) return;
                const r = radius(p);
                ctx.textBaseline = p.dir < 0 ? 'top' : 'bottom';
                ctx.fillText(String(p.n), el.x, p.dir < 0 ? el.y + r + 2 : el.y - r - 2);
            });
            ctx.restore();
        }
    };

    function drawChart(evs) {
        const cv = document.getElementById('news-impact-chart');
        if (!cv || typeof Chart === 'undefined') return;
        const L = tr();
        const cut = rangeCut();
        const line = S.prices.filter(p => p.t >= cut - dayMs).map(p => ({ x: p.t, y: p.v }));
        const vals = line.map(p => p.y);
        const lo = vals.length ? Math.min(...vals) : 0, hi = vals.length ? Math.max(...vals) : 1;
        const off = Math.max((hi - lo) * 0.07, 1);
        S.pts = groupPoints(evs).map(p => ({ ...p, y: p.base + p.dir * off }));
        const pts = S.pts;
        const dark = document.body.classList.contains('dark');
        const grid = dark ? 'rgba(255,255,255,0.05)' : 'rgba(0,0,0,0.05)';
        const tick = dark ? '#94a3b8' : '#6b7280';
        const lineC = dark ? '#e2e8f0' : '#1e293b';
        const data = {
            datasets: [
                { type: 'line', label: L.price, data: line, borderColor: lineC, borderWidth: 2, pointRadius: 0, tension: 0.2, order: 2,
                  fill: 'start', backgroundColor: dark ? 'rgba(226,232,240,0.05)' : 'rgba(30,41,59,0.05)' },
                {
                    type: 'scatter', label: 'events', data: pts, order: 1,
                    pointStyle: pts.map(p => p.dir === 0 ? 'rectRot' : 'triangle'),
                    rotation: pts.map(p => p.dir < 0 ? 180 : 0),
                    pointRadius: pts.map(radius),
                    pointHoverRadius: pts.map(p => radius(p) + 2),
                    backgroundColor: pts.map(p => p.dir > 0 ? UP : p.dir < 0 ? DOWN : FLAT),
                    borderColor: dark ? '#0d1117' : '#ffffff',
                    borderWidth: 1
                }
            ]
        };
        const options = {
            responsive: true, maintainAspectRatio: false, animation: false,
            layout: { padding: { top: 14, bottom: 4 } },
            interaction: { mode: 'nearest', intersect: true },
            scales: {
                x: { type: 'time', time: { unit: S.range > 120 ? 'month' : 'week' }, grid: { display: false }, ticks: { color: tick, maxTicksLimit: 6, maxRotation: 0 } },
                y: { suggestedMin: lo - off * 2.2, suggestedMax: hi + off * 2.2, grid: { color: grid }, ticks: { color: tick, maxTicksLimit: 5, callback: v => '$' + Math.round(v).toLocaleString() } }
            },
            plugins: {
                legend: { display: false },
                tooltip: {
                    enabled: !noHover(),
                    filter: i => i.datasetIndex === 1,
                    callbacks: {
                        title: items => { const r = items[0].raw; return r && r.evs ? `${bucketLabel(r.k)} · ${L.selN(r.n)}` : ''; },
                        label: i => i.raw.evs ? i.raw.evs.slice(0, 4).map(e => `${e.direction > 0 ? '▲' : e.direction < 0 ? '▼' : '◆'} ${loc(e.title)}`) : '',
                        afterBody: items => { const r = items[0].raw; return r && r.n > 4 ? `+${r.n - 4}` : ''; }
                    }
                }
            },
            onClick: (evt) => {
                const hit = nearestPoint(evt);
                S.sel = hit ? (S.sel === hit.k ? null : hit.k) : null;
                S.hl = null; S.openId = hit && hit.n === 1 ? hit.evs[0].id : null;
                renderPanel();
            }
        };
        if (S.chart) S.chart.destroy();
        S.chart = new Chart(cv.getContext('2d'), { data, options, plugins: [overlay] });
    }

    // Finger-friendly hit test: nearest marker within 22px.
    function nearestPoint(evt) {
        const meta = S.chart && S.chart.getDatasetMeta(1);
        if (!meta) return null;
        let best = null, bd = 22 * 22;
        meta.data.forEach((el, i) => {
            const dx = el.x - evt.x, dy = el.y - evt.y, d = dx * dx + dy * dy;
            if (d < bd) { bd = d; best = S.pts[i]; }
        });
        return best;
    }

    function setHighlight(k) {
        if (S.hl === k) return;
        S.hl = k;
        document.querySelectorAll('#news-impact-panel .ni-card').forEach(c => c.classList.toggle('hl', k != null && +c.dataset.b === k));
        if (S.chart) {
            const ds = S.chart.data.datasets[1];
            ds.pointRadius = S.pts.map(radius);
            ds.pointHoverRadius = S.pts.map(p => radius(p) + 2);
            S.chart.update('none');
        }
    }

    // As the card list scrolls, highlight the bucket of the first visible card on the chart.
    function syncFromScroll() {
        const list = document.querySelector('#news-impact-panel .ni-list');
        if (!list || S.sel != null) return;
        if (list.scrollTop < 4) { setHighlight(null); return; }
        const top = list.getBoundingClientRect().top + 6;
        const card = Array.from(list.querySelectorAll('.ni-card')).find(c => c.getBoundingClientRect().bottom > top);
        setHighlight(card ? +card.dataset.b : null);
    }

    function openCard(id, scroll) {
        S.openId = S.openId === id && !scroll ? null : id;
        const ev = S.events.find(e => e.id === id);
        if (ev && scroll) {
            if (S.cat !== 'all' && ev.category !== S.cat) S.cat = 'all';
            if (Date.now() - tOf(ev) > S.range * dayMs) S.range = 365;
            if (ev.impact < S.minImp) S.minImp = ev.impact;
            S.sel = null; S.week = null; S.metric = null;
        }
        if (!scroll) {
            const card = document.querySelector(`#news-impact-panel .ni-card[data-id="${CSS.escape(id)}"]`);
            if (card) {
                document.querySelectorAll('#news-impact-panel .ni-card.open').forEach(c => { if (c !== card) c.classList.remove('open'); });
                card.classList.toggle('open', S.openId === id);
                setHighlight(+card.dataset.b);
                return;
            }
        }
        render();
        if (scroll) {
            const card = document.querySelector(`#news-impact-panel .ni-card[data-id="${CSS.escape(id)}"]`);
            if (card) {
                const list = card.closest('.ni-list');
                if (list) list.scrollTop = card.offsetTop - list.offsetTop - 8;
                setHighlight(+card.dataset.b);
            }
        }
    }

    // "News that moved this metric" badge on each Fundamentals card (last 30 days, every impact level).
    function renderBadges() {
        document.querySelectorAll('.ni-mbadge').forEach(b => b.remove());
        if (S.status !== 'ok') return;
        const L = tr(), cut = Date.now() - MB_DAYS * dayMs;
        Object.entries(METRIC_CARDS).forEach(([key, ids]) => {
            const n = S.events.filter(e => tOf(e) >= cut && (e.metrics || []).includes(key)).length;
            if (!n) return;
            ids.forEach(id => {
                const info = document.querySelector(`.metric-card[data-metric="${CSS.escape(id)}"] .metric-info`);
                if (!info) return;
                const b = document.createElement('button');
                b.type = 'button'; b.className = 'ni-mbadge' + (S.metric === key ? ' on' : ''); b.dataset.nkey = key;
                b.title = L.mbTip(n); b.setAttribute('aria-label', L.mbTip(n));
                b.innerHTML = `<i aria-hidden="true"></i>${esc(L.mb(n))}`;
                info.appendChild(b);
            });
        });
    }

    function render() { renderStrip(); renderPanel(); renderBadges(); }

    function bind() {
        document.addEventListener('click', (e) => {
            const chip = e.target.closest('#news-impact-strip .ni-drv, #news-impact-panel .ni-chip');
            const mb = e.target.closest('.ni-mbadge');
            if (mb) { S.metric = mb.dataset.nkey; S.week = null; S.cat = 'all'; S.range = MB_DAYS; S.sel = null; S.hl = null; S.openId = null; render(); scrollPanel(); return; }
            if (e.target.closest('#news-impact-panel .ni-metric-clear')) { S.metric = null; S.range = 90; S.sel = null; S.hl = null; render(); return; }
            const mc = e.target.closest('#news-impact-panel .ni-mx-cell:not(.empty)');
            if (mc || chip || e.target.closest('#news-impact-panel .ni-range button, #news-impact-panel .ni-imp button, #news-impact-strip .ni-top, #news-impact-panel .ni-wk-pill')) S.metric = null;
            if (mc) {
                const w = +mc.dataset.week;
                if (S.week === w && S.cat === mc.dataset.cat) { S.week = null; S.cat = 'all'; } else { S.week = w; S.cat = mc.dataset.cat; }
                S.openId = null; S.sel = null; S.hl = null; renderStrip(); renderPanel();
                const g = document.querySelector('#news-impact-panel .ni-list');
                if (g && S.week != null) { const r = g.getBoundingClientRect(); if (r.top < 0 || r.top > innerHeight - 120) g.scrollIntoView({ behavior: 'smooth', block: 'start' }); }
                return;
            }
            if (e.target.closest('#news-impact-panel .ni-wk-pill')) { S.week = null; S.cat = 'all'; S.sel = null; S.hl = null; render(); return; }
            if (chip) { S.week = null; S.cat = chip.dataset.cat; S.openId = null; S.sel = null; S.hl = null; render(); if (chip.closest('#news-impact-strip')) scrollPanel(); return; }
            const rg = e.target.closest('#news-impact-panel .ni-range button');
            if (rg) { S.week = null; S.range = +rg.dataset.range; S.sel = null; S.hl = null; render(); return; }
            const im = e.target.closest('#news-impact-panel .ni-imp button');
            if (im) { S.week = null; S.minImp = +im.dataset.imp; S.sel = null; S.hl = null; render(); return; }
            if (e.target.closest('#news-impact-panel .ni-sel-clear')) { S.sel = null; S.hl = null; renderPanel(); return; }
            const top = e.target.closest('#news-impact-strip .ni-top');
            if (top) { scrollPanel(); openCard(top.dataset.open, true); return; }
            const card = e.target.closest('#news-impact-panel .ni-card');
            if (card && !e.target.closest('a')) openCard(card.dataset.id, false);
        });
        document.addEventListener('mouseover', (e) => {
            if (noHover()) return;
            const card = e.target.closest('#news-impact-panel .ni-card');
            if (card) setHighlight(+card.dataset.b);
        });
        document.addEventListener('mouseout', (e) => {
            if (noHover()) return;
            const card = e.target.closest('#news-impact-panel .ni-card');
            if (card && !card.contains(e.relatedTarget) && S.openId == null) setHighlight(null);
        });
        let raf = 0;
        document.addEventListener('scroll', (e) => {
            if (!(e.target instanceof Element) || !e.target.classList.contains('ni-list')) return;
            cancelAnimationFrame(raf); raf = requestAnimationFrame(syncFromScroll);
        }, true);
        let rt = 0;
        window.addEventListener('resize', () => { clearTimeout(rt); rt = setTimeout(() => { if (S.status === 'ok') renderPanel(); }, 200); });
        new MutationObserver(() => render()).observe(document.documentElement, { attributes: true, attributeFilter: ['lang'] });
        new MutationObserver(() => { if (S.status === 'ok') drawChart(visible()); }).observe(document.body, { attributes: true, attributeFilter: ['class'] });
    }
    function scrollPanel() {
        const p = document.getElementById('news-impact-panel');
        // Far jumps (e.g. from a Fundamentals badge, ~8000px on mobile) go instantly; smooth only for short hops.
        if (p) p.scrollIntoView({ behavior: Math.abs(p.getBoundingClientRect().top) > 2500 ? 'auto' : 'smooth', block: 'start' });
    }

    function start() { bind(); render(); load(); }
    window.ETHvalNewsImpact = { state: S, METRIC_CARDS, reload: load, render, visible, points: () => S.pts, scoreData };
    if (document.readyState === 'loading') document.addEventListener('DOMContentLoaded', start); else start();
})();
