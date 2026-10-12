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
            title: 'News Impact', sub: 'How this week\'s news moves ETH', net7: '7D net impact', bull: 'Bullish', bear: 'Bearish', mixed: 'Mixed',
            top: 'Biggest event', all: 'All', none: 'No events in this range yet.', loading: 'Loading news…', err: 'News data unavailable right now.',
            impact: { 3: 'High', 2: 'Medium', 1: 'Low' }, horizon: { immediate: 'Immediate', short: 'Short-term', structural: 'Structural' },
            after: 'ETH after', sources: 'Sources', events: ' events', price: 'ETH price', panel: '01.0 — News Impact on Price',
            hint: '▲ bullish above the price · ▼ bearish below · bigger = stronger · number = events grouped there. Tap a marker to see its cards.',
            imp: { 3: 'High only', 2: 'Medium+', 1: 'All' }, showAll: 'Show all', selN: n => `${n} events`,
            scoreNote: 'Weighted: high 3 · medium 2 · low 1, bullish minus bearish, last 7 days.', model: 'AI-tagged',
            cats: { macro: 'Macro & Rates', flows: 'Fund Flows', regulation: 'Regulation & Tax', protocol: 'Ethereum Upgrades', security: 'Security', competitors: 'Competing Chains', ecosystem: 'L2 · DeFi · RWA', market: 'Market Structure' }
        },
        ko: {
            title: '뉴스 영향', sub: '이번 주 뉴스가 ETH 가격에 주는 영향', net7: '최근 7일 순영향', bull: '호재', bear: '악재', mixed: '혼재',
            top: '가장 큰 사건', all: '전체', none: '이 기간에는 아직 사건이 없어요.', loading: '뉴스 불러오는 중…', err: '지금은 뉴스 데이터를 불러올 수 없어요.',
            impact: { 3: '상', 2: '중', 1: '하' }, horizon: { immediate: '즉시', short: '단기', structural: '구조적' },
            after: '이후 ETH', sources: '출처', events: '건', price: 'ETH 가격', panel: '01.0 — 뉴스와 가격',
            hint: '가격 위 ▲는 호재, 아래 ▼는 악재예요. 클수록 영향이 크고, 숫자는 묶인 사건 수예요. 표시를 누르면 해당 카드만 보여요.',
            imp: { 3: '상만', 2: '중 이상', 1: '전체' }, showAll: '전체 보기', selN: n => `${n}건`,
            scoreNote: '최근 7일, 상 3·중 2·하 1점으로 호재에서 악재를 뺀 값이에요.', model: 'AI 분류',
            cats: { macro: '거시·금리', flows: '자금 흐름', regulation: '제도·규제·과세', protocol: '이더리움 개선', security: '사고·보안', competitors: '경쟁 체인', ecosystem: 'L2·DeFi·RWA', market: '시장 구조·심리' }
        },
        zh: {
            title: '新闻影响', sub: '本周新闻如何影响 ETH 价格', net7: '近 7 日净影响', bull: '利好', bear: '利空', mixed: '中性',
            top: '最大事件', all: '全部', none: '该区间暂无事件。', loading: '正在加载新闻…', err: '暂时无法加载新闻数据。',
            impact: { 3: '高', 2: '中', 1: '低' }, horizon: { immediate: '即时', short: '短期', structural: '结构性' },
            after: '之后 ETH', sources: '来源', events: '条', price: 'ETH 价格', panel: '01.0 — 新闻与价格',
            hint: '价格上方 ▲ 为利好，下方 ▼ 为利空；越大影响越强，数字为合并的事件数。点击标记只看对应卡片。',
            imp: { 3: '仅高', 2: '中及以上', 1: '全部' }, showAll: '查看全部', selN: n => `${n} 条`,
            scoreNote: '近 7 日，高 3 · 中 2 · 低 1，利好减利空。', model: 'AI 标注',
            cats: { macro: '宏观·利率', flows: '资金流向', regulation: '监管·税收', protocol: '以太坊升级', security: '安全事件', competitors: '竞争公链', ecosystem: 'L2·DeFi·RWA', market: '市场结构' }
        },
        ja: {
            title: 'ニュース影響', sub: '今週のニュースが ETH 価格に与える影響', net7: '直近7日の純影響', bull: '好材料', bear: '悪材料', mixed: '中立',
            top: '最大の出来事', all: 'すべて', none: 'この期間のイベントはまだありません。', loading: 'ニュースを読み込み中…', err: '現在ニュースデータを取得できません。',
            impact: { 3: '大', 2: '中', 1: '小' }, horizon: { immediate: '即時', short: '短期', structural: '構造的' },
            after: 'その後のETH', sources: '出典', events: '件', price: 'ETH 価格', panel: '01.0 — ニュースと価格',
            hint: '価格の上 ▲ は好材料、下 ▼ は悪材料。大きいほど影響が強く、数字はまとめた件数。マーカーをタップで該当カードだけ表示。',
            imp: { 3: '大のみ', 2: '中以上', 1: 'すべて' }, showAll: 'すべて表示', selN: n => `${n}件`,
            scoreNote: '直近7日、大3・中2・小1で好材料から悪材料を引いた値。', model: 'AI 分類',
            cats: { macro: 'マクロ・金利', flows: '資金フロー', regulation: '規制・税制', protocol: 'イーサリアム改善', security: 'セキュリティ', competitors: '競合チェーン', ecosystem: 'L2・DeFi・RWA', market: '市場構造' }
        }
    };
    const METRIC_LABEL = {
        price: 'Price', fear_greed: 'Fear & Greed', funding_rate: 'Funding', open_interest: 'Open Interest', exchange_reserve: 'Exchange Reserve',
        etf_flows: 'ETF Flows', staking: 'Staking', burn: 'Burn', supply: 'Supply', fees: 'Fees', tvl: 'TVL', stablecoins: 'Stablecoins',
        l2_activity: 'L2 Activity', dominance: 'Dominance', rates: 'Rates', dxy: 'Dollar', btc: 'BTC', volatility: 'Volatility', active_addresses: 'Active Addr.'
    };

    // minImp: impact filter (1 all, 2 medium+, 3 high only). sel: selected bucket key. hl: highlighted bucket key.
    const S = { events: [], prices: [], cat: 'all', range: 90, minImp: 2, sel: null, hl: null, bd: 1, origin: 0, pts: [], chart: null, openId: null, status: 'loading' };

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
            const [ev, px] = await Promise.all([
                rest(`news_events?select=id,date,category,direction,impact,horizon,metrics,title,summary,mechanism,sources,confidence&date=gte.${since}&order=date.desc,impact.desc&limit=1000`),
                rest(`historical_eth_price?select=date,close&date=gte.${since}&order=date.asc&limit=1000`)
            ]);
            S.events = ev || [];
            S.prices = (px || []).map(p => ({ t: new Date(p.date + 'T00:00:00Z').getTime(), v: +p.close })).filter(p => p.v > 0);
            S.status = 'ok';
        } catch (e) {
            console.warn('[news-impact]', e.message);
            S.status = 'error';
        }
        render();
    }

    function priceAt(t) {
        const P = S.prices;
        if (!P.length) return null;
        let lo = 0, hi = P.length - 1;
        if (t <= P[0].t) return P[0].v;
        if (t >= P[hi].t) return t - P[hi].t <= 2 * dayMs ? P[hi].v : null;
        while (hi - lo > 1) { const m = (lo + hi) >> 1; if (P[m].t <= t) lo = m; else hi = m; }
        return P[lo].v;
    }
    function reaction(ev, days) {
        const t0 = tOf(ev);
        const last = S.prices.length ? S.prices[S.prices.length - 1].t : 0;
        if (t0 + days * dayMs > last) return null;
        const a = priceAt(t0), b = priceAt(t0 + days * dayMs);
        return a && b ? (b / a - 1) * 100 : null;
    }

    function netScore(days) {
        const cut = Date.now() - days * dayMs;
        let s = 0;
        const per = {};
        CATS.forEach(c => per[c] = { up: 0, down: 0, n: 0, s: 0 });
        S.events.forEach(e => {
            if (tOf(e) < cut) return;
            const w = e.direction * e.impact;
            s += w;
            const p = per[e.category]; if (!p) return;
            p.n++; p.s += w; if (e.direction > 0) p.up++; else if (e.direction < 0) p.down++;
        });
        return { s: Math.max(-10, Math.min(10, s)), raw: s, per };
    }

    function rangeCut() { return Date.now() - S.range * dayMs; }
    // Events drawn on the chart AND listed as cards — one filter for both so they always match.
    function visible() {
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
        el.innerHTML = `
            <div class="ni-strip-head">
                <div class="ni-strip-title"><strong>${esc(L.title)}</strong><span>${esc(L.sub)}</span></div>
                <div class="ni-gauge" title="${esc(L.scoreNote)}">
                    <span class="ni-gauge-label">${esc(L.net7)}</span>
                    <span class="ni-gauge-val ${tone}">${n.raw > 0 ? '+' : ''}${n.raw} <small>${esc(word)}</small></span>
                    <span class="ni-gauge-bar"><span class="ni-gauge-mid"></span><span class="ni-gauge-dot ${tone}" style="left:${pct}%"></span></span>
                    <span class="ni-gauge-count">${week.length}${esc(L.events)}</span>
                </div>
            </div>
            <div class="ni-chips" role="group">${chipsHtml(n.per)}</div>
            ${top ? `<button type="button" class="ni-top" data-open="${esc(top.id)}"><span class="ni-top-k">${esc(L.top)}${dirBadge(top)}</span><span class="ni-top-t">${esc(loc(top.title))}</span><span class="ni-top-m">${esc(loc(top.mechanism))}</span></button>` : ''}`;
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
        computeBuckets();
        const evs = visible();
        const shown = S.sel == null ? evs : evs.filter(e => bucketOf(tOf(e)) === S.sel);
        const selHead = S.sel == null ? '' : `<div class="ni-sel"><span>${esc(bucketLabel(S.sel))} · ${esc(L.selN(shown.length))}</span><button type="button" class="ni-sel-clear">${esc(L.showAll)} ×</button></div>`;
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
            S.sel = null;
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

    function render() { renderStrip(); renderPanel(); }

    function bind() {
        document.addEventListener('click', (e) => {
            const chip = e.target.closest('#news-impact-strip .ni-chip, #news-impact-panel .ni-chip');
            if (chip) { S.cat = chip.dataset.cat; S.openId = null; S.sel = null; S.hl = null; render(); if (chip.closest('#news-impact-strip')) scrollPanel(); return; }
            const rg = e.target.closest('#news-impact-panel .ni-range button');
            if (rg) { S.range = +rg.dataset.range; S.sel = null; S.hl = null; render(); return; }
            const im = e.target.closest('#news-impact-panel .ni-imp button');
            if (im) { S.minImp = +im.dataset.imp; S.sel = null; S.hl = null; render(); return; }
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
        if (p) p.scrollIntoView({ behavior: 'smooth', block: 'start' });
    }

    function start() { bind(); render(); load(); }
    window.ETHvalNewsImpact = { state: S, reload: load, render, visible, points: () => S.pts };
    if (document.readyState === 'loading') document.addEventListener('DOMContentLoaded', start); else start();
})();
