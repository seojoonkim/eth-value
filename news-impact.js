/* ETHval News Impact (v1.0)
 * Reads Supabase `news_events` + `historical_eth_price` and renders:
 *  1) #news-impact-strip   — 7-day net impact gauge, category chips, top event (top of Valuation)
 *  2) #news-impact-panel   — price chart with event markers + filterable event cards
 * Depends on globals from index.html: SUPABASE_URL, SUPABASE_ANON_KEY, Chart, currentLang (optional).
 */
(function () {
    'use strict';

    const CATS = ['macro', 'flows', 'regulation', 'protocol', 'security', 'competitors', 'ecosystem', 'market'];
    const CAT_COLOR = {
        macro: '#3b82f6', flows: '#10b981', regulation: '#a855f7', protocol: '#6366f1',
        security: '#ef4444', competitors: '#f59e0b', ecosystem: '#14b8a6', market: '#64748b'
    };
    const T = {
        en: {
            title: 'News Impact', sub: 'How this week\'s news moves ETH', net7: '7D net impact', bull: 'Bullish', bear: 'Bearish', mixed: 'Mixed',
            top: 'Biggest event', all: 'All', none: 'No events in this range yet.', loading: 'Loading news…', err: 'News data unavailable right now.',
            impact: { 3: 'High', 2: 'Medium', 1: 'Low' }, horizon: { immediate: 'Immediate', short: 'Short-term', structural: 'Structural' },
            after: 'ETH after', sources: 'Sources', events: ' events', price: 'ETH price', panel: '01.0 — News Impact on Price',
            hint: '▲ above the line = bullish, ▼ below = bearish. Size = impact. Click a marker to open its card.',
            scoreNote: 'Weighted: high 3 · medium 2 · low 1, bullish minus bearish, last 7 days.', model: 'AI-tagged',
            cats: { macro: 'Macro & Rates', flows: 'Fund Flows', regulation: 'Regulation & Tax', protocol: 'Ethereum Upgrades', security: 'Security', competitors: 'Competing Chains', ecosystem: 'L2 · DeFi · RWA', market: 'Market Structure' }
        },
        ko: {
            title: '뉴스 영향', sub: '이번 주 뉴스가 ETH 가격에 주는 영향', net7: '최근 7일 순영향', bull: '호재', bear: '악재', mixed: '혼재',
            top: '가장 큰 사건', all: '전체', none: '이 기간에는 아직 사건이 없어요.', loading: '뉴스 불러오는 중…', err: '지금은 뉴스 데이터를 불러올 수 없어요.',
            impact: { 3: '상', 2: '중', 1: '하' }, horizon: { immediate: '즉시', short: '단기', structural: '구조적' },
            after: '이후 ETH', sources: '출처', events: '건', price: 'ETH 가격', panel: '01.0 — 뉴스와 가격',
            hint: '선 위 ▲는 호재, 선 아래 ▼는 악재예요. 크기는 영향 크기예요. 표시를 누르면 해당 카드가 열려요.',
            scoreNote: '최근 7일, 상 3·중 2·하 1점으로 호재에서 악재를 뺀 값이에요.', model: 'AI 분류',
            cats: { macro: '거시·금리', flows: '자금 흐름', regulation: '제도·규제·과세', protocol: '이더리움 개선', security: '사고·보안', competitors: '경쟁 체인', ecosystem: 'L2·DeFi·RWA', market: '시장 구조·심리' }
        },
        zh: {
            title: '新闻影响', sub: '本周新闻如何影响 ETH 价格', net7: '近 7 日净影响', bull: '利好', bear: '利空', mixed: '中性',
            top: '最大事件', all: '全部', none: '该区间暂无事件。', loading: '正在加载新闻…', err: '暂时无法加载新闻数据。',
            impact: { 3: '高', 2: '中', 1: '低' }, horizon: { immediate: '即时', short: '短期', structural: '结构性' },
            after: '之后 ETH', sources: '来源', events: '条', price: 'ETH 价格', panel: '01.0 — 新闻与价格',
            hint: '线上方 ▲ 为利好，下方 ▼ 为利空，大小代表影响程度。点击标记查看详情。',
            scoreNote: '近 7 日，高 3 · 中 2 · 低 1，利好减利空。', model: 'AI 标注',
            cats: { macro: '宏观·利率', flows: '资金流向', regulation: '监管·税收', protocol: '以太坊升级', security: '安全事件', competitors: '竞争公链', ecosystem: 'L2·DeFi·RWA', market: '市场结构' }
        },
        ja: {
            title: 'ニュース影響', sub: '今週のニュースが ETH 価格に与える影響', net7: '直近7日の純影響', bull: '好材料', bear: '悪材料', mixed: '中立',
            top: '最大の出来事', all: 'すべて', none: 'この期間のイベントはまだありません。', loading: 'ニュースを読み込み中…', err: '現在ニュースデータを取得できません。',
            impact: { 3: '大', 2: '中', 1: '小' }, horizon: { immediate: '即時', short: '短期', structural: '構造的' },
            after: 'その後のETH', sources: '出典', events: '件', price: 'ETH 価格', panel: '01.0 — ニュースと価格',
            hint: '線の上 ▲ は好材料、下 ▼ は悪材料。大きさは影響度。マーカーをクリックで詳細。',
            scoreNote: '直近7日、大3・中2・小1で好材料から悪材料を引いた値。', model: 'AI 分類',
            cats: { macro: 'マクロ・金利', flows: '資金フロー', regulation: '規制・税制', protocol: 'イーサリアム改善', security: 'セキュリティ', competitors: '競合チェーン', ecosystem: 'L2・DeFi・RWA', market: '市場構造' }
        }
    };
    const METRIC_LABEL = {
        price: 'Price', fear_greed: 'Fear & Greed', funding_rate: 'Funding', open_interest: 'Open Interest', exchange_reserve: 'Exchange Reserve',
        etf_flows: 'ETF Flows', staking: 'Staking', burn: 'Burn', supply: 'Supply', fees: 'Fees', tvl: 'TVL', stablecoins: 'Stablecoins',
        l2_activity: 'L2 Activity', dominance: 'Dominance', rates: 'Rates', dxy: 'Dollar', btc: 'BTC', volatility: 'Volatility', active_addresses: 'Active Addr.'
    };

    const S = { events: [], prices: [], cat: 'all', range: 90, chart: null, openId: null, status: 'loading' };

    const lang = () => { const l = (typeof currentLang !== 'undefined' && currentLang) || document.documentElement.lang || 'en'; return T[l] ? l : 'en'; };
    const tr = () => T[lang()];
    const loc = (o) => (o && (o[lang()] || o.en)) || '';
    const esc = (s) => String(s == null ? '' : s).replace(/[&<>"']/g, c => ({ '&': '&amp;', '<': '&lt;', '>': '&gt;', '"': '&quot;', "'": '&#39;' }[c]));
    const dayMs = 86400000;

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
        const t0 = new Date(ev.date + 'T00:00:00Z').getTime();
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
            if (new Date(e.date + 'T00:00:00Z').getTime() < cut) return;
            const w = e.direction * e.impact;
            s += w;
            const p = per[e.category]; if (!p) return;
            p.n++; p.s += w; if (e.direction > 0) p.up++; else if (e.direction < 0) p.down++;
        });
        return { s: Math.max(-10, Math.min(10, s)), raw: s, per };
    }

    function inRange() {
        const cut = Date.now() - S.range * dayMs;
        return S.events.filter(e => new Date(e.date + 'T00:00:00Z').getTime() >= cut && (S.cat === 'all' || e.category === S.cat));
    }

    function chipsHtml(per) {
        const L = tr();
        const all = `<button type="button" class="ni-chip${S.cat === 'all' ? ' on' : ''}" data-cat="all">${esc(L.all)}</button>`;
        return all + CATS.map(c => {
            const p = per[c];
            const tone = p.s > 0 ? 'up' : p.s < 0 ? 'down' : 'flat';
            const cnt = p.n ? `<span class="ni-chip-n ${tone}">${p.up ? '▲' + p.up : ''}${p.up && p.down ? ' ' : ''}${p.down ? '▼' + p.down : ''}${!p.up && !p.down ? '·' + p.n : ''}</span>` : '';
            return `<button type="button" class="ni-chip${S.cat === c ? ' on' : ''}${p.n ? '' : ' empty'}" data-cat="${c}"><i style="background:${CAT_COLOR[c]}"></i>${esc(L.cats[c])}${cnt}</button>`;
        }).join('');
    }

    function renderStrip() {
        const el = document.getElementById('news-impact-strip');
        if (!el) return;
        const L = tr();
        if (S.status !== 'ok') { el.innerHTML = `<div class="ni-strip-msg">${esc(S.status === 'error' ? L.err : L.loading)}</div>`; return; }
        const n = netScore(7);
        const tone = n.s > 0 ? 'up' : n.s < 0 ? 'down' : 'flat';
        const word = n.s > 0 ? L.bull : n.s < 0 ? L.bear : L.mixed;
        const pct = (n.s + 10) / 20 * 100;
        const week = S.events.filter(e => Date.now() - new Date(e.date + 'T00:00:00Z').getTime() <= 7 * dayMs);
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
            ${top ? `<button type="button" class="ni-top" data-open="${esc(top.id)}"><span class="ni-top-k">${esc(L.top)}</span>${dirBadge(top)}<span class="ni-top-t">${esc(loc(top.title))}</span><span class="ni-top-m">${esc(loc(top.mechanism))}</span></button>` : ''}`;
    }

    function dirBadge(e) {
        const L = tr();
        const tone = e.direction > 0 ? 'up' : e.direction < 0 ? 'down' : 'flat';
        const arrow = e.direction > 0 ? '▲' : e.direction < 0 ? '▼' : '◆';
        return `<span class="ni-dir ${tone}">${arrow} ${esc(e.direction > 0 ? L.bull : e.direction < 0 ? L.bear : L.mixed)} · ${esc(L.impact[e.impact])}</span>`;
    }

    function cardHtml(e) {
        const L = tr();
        const r = [1, 7, 30].map(d => { const v = reaction(e, d); return v == null ? '' : `<span class="ni-rx ${v >= 0 ? 'up' : 'down'}">${d}D ${v >= 0 ? '+' : ''}${v.toFixed(1)}%</span>`; }).join('');
        const src = (e.sources || []).slice(0, 3).map(s => `<a href="${esc(s.url)}" target="_blank" rel="noopener noreferrer">${esc(s.source || 'link')}</a>`).join(' · ');
        return `<article class="ni-card${S.openId === e.id ? ' open' : ''}" data-id="${esc(e.id)}" style="--cat:${CAT_COLOR[e.category] || '#64748b'}">
            <div class="ni-card-top"><span class="ni-date">${esc(e.date.slice(5).replace('-', '.'))}</span><span class="ni-cat">${esc(L.cats[e.category] || e.category)}</span>${dirBadge(e)}<span class="ni-hz">${esc(L.horizon[e.horizon] || '')}</span></div>
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
        if (S.status !== 'ok') { list.innerHTML = `<div class="ni-empty">${esc(S.status === 'error' ? L.err : L.loading)}</div>`; return; }
        const per = netScore(S.range).per;
        chips.innerHTML = chipsHtml(per);
        const evs = inRange();
        list.innerHTML = evs.length ? evs.map(cardHtml).join('') : `<div class="ni-empty">${esc(L.none)}</div>`;
        drawChart(evs);
    }

    function drawChart(evs) {
        const cv = document.getElementById('news-impact-chart');
        if (!cv || typeof Chart === 'undefined') return;
        const L = tr();
        const cut = Date.now() - S.range * dayMs;
        const line = S.prices.filter(p => p.t >= cut - dayMs).map(p => ({ x: p.t, y: p.v }));
        const vals = line.map(p => p.y);
        const span = vals.length ? Math.max(...vals) - Math.min(...vals) : 0;
        const off = Math.max(span * 0.06, 1);
        const showLow = S.range <= 90 || S.cat !== 'all';
        const pts = evs.filter(e => showLow || e.impact >= 2).map(e => {
            const t = new Date(e.date + 'T00:00:00Z').getTime();
            const p = priceAt(t);
            if (p == null) return null;
            return { x: t, y: p + (e.direction >= 0 ? off : -off) * (e.direction === 0 ? 0 : 1), ev: e };
        }).filter(Boolean);
        const dark = document.body.classList.contains('dark');
        const grid = dark ? 'rgba(255,255,255,0.06)' : 'rgba(0,0,0,0.06)';
        const tick = dark ? '#94a3b8' : '#6b7280';
        const data = {
            datasets: [
                { type: 'line', label: L.price, data: line, borderColor: dark ? '#cbd5e1' : '#334155', borderWidth: 1.5, pointRadius: 0, tension: 0.15, order: 2 },
                {
                    type: 'scatter', label: 'events', data: pts, order: 1,
                    pointStyle: pts.map(p => p.ev.direction === 0 ? 'rectRot' : 'triangle'),
                    rotation: pts.map(p => p.ev.direction < 0 ? 180 : 0),
                    pointRadius: pts.map(p => [0, 4, 6, 9][p.ev.impact]),
                    pointHoverRadius: pts.map(p => [0, 6, 8, 11][p.ev.impact]),
                    backgroundColor: pts.map(p => CAT_COLOR[p.ev.category]),
                    borderColor: pts.map(p => p.ev.direction > 0 ? '#10b981' : p.ev.direction < 0 ? '#ef4444' : '#94a3b8'),
                    borderWidth: 1.5
                }
            ]
        };
        const options = {
            responsive: true, maintainAspectRatio: false, animation: false,
            interaction: { mode: 'nearest', intersect: true },
            scales: {
                x: { type: 'time', time: { unit: S.range > 120 ? 'month' : 'week' }, grid: { color: grid }, ticks: { color: tick, maxTicksLimit: 8 } },
                y: { grid: { color: grid }, ticks: { color: tick, callback: v => '$' + Math.round(v).toLocaleString() } }
            },
            plugins: {
                legend: { display: false },
                tooltip: {
                    filter: i => i.datasetIndex === 1 || i.chart.data.datasets[1].data.length === 0,
                    callbacks: {
                        title: items => { const r = items[0].raw; return r.ev ? `${r.ev.date} · ${L.cats[r.ev.category]}` : new Date(r.x).toISOString().slice(0, 10); },
                        label: i => i.raw.ev ? `${i.raw.ev.direction > 0 ? '▲' : i.raw.ev.direction < 0 ? '▼' : '◆'} ${loc(i.raw.ev.title)}` : `$${Math.round(i.raw.y).toLocaleString()}`,
                        afterLabel: i => i.raw.ev ? loc(i.raw.ev.mechanism) : ''
                    }
                }
            },
            onClick: (evt, els) => {
                const hit = els.find(x => x.datasetIndex === 1);
                if (hit) openCard(pts[hit.index].ev.id, true);
            }
        };
        if (S.chart) { S.chart.data = data; S.chart.options = options; S.chart.update('none'); }
        else S.chart = new Chart(cv.getContext('2d'), { data, options });
    }

    function openCard(id, scroll) {
        S.openId = S.openId === id && !scroll ? null : id;
        const ev = S.events.find(e => e.id === id);
        if (ev && S.cat !== 'all' && ev.category !== S.cat) S.cat = 'all';
        if (ev && Date.now() - new Date(ev.date + 'T00:00:00Z').getTime() > S.range * dayMs) S.range = 365;
        render();
        if (scroll) {
            const card = document.querySelector(`#news-impact-panel .ni-card[data-id="${CSS.escape(id)}"]`);
            if (card) {
                const list = card.closest('.ni-list');
                if (list) list.scrollTop = card.offsetTop - list.offsetTop - 8;
            }
        }
    }

    function render() { renderStrip(); renderPanel(); }

    function bind() {
        document.addEventListener('click', (e) => {
            const chip = e.target.closest('#news-impact-strip .ni-chip, #news-impact-panel .ni-chip');
            if (chip) { S.cat = chip.dataset.cat; S.openId = null; render(); if (chip.closest('#news-impact-strip')) scrollPanel(); return; }
            const rg = e.target.closest('#news-impact-panel .ni-range button');
            if (rg) { S.range = +rg.dataset.range; render(); return; }
            const top = e.target.closest('#news-impact-strip .ni-top');
            if (top) { scrollPanel(); openCard(top.dataset.open, true); return; }
            const card = e.target.closest('#news-impact-panel .ni-card');
            if (card && !e.target.closest('a')) openCard(card.dataset.id, false);
        });
        new MutationObserver(() => render()).observe(document.documentElement, { attributes: true, attributeFilter: ['lang'] });
        new MutationObserver(() => { if (S.status === 'ok') drawChart(inRange()); }).observe(document.body, { attributes: true, attributeFilter: ['class'] });
    }
    function scrollPanel() {
        const p = document.getElementById('news-impact-panel');
        if (p) p.scrollIntoView({ behavior: 'smooth', block: 'start' });
    }

    function start() { bind(); render(); load(); }
    window.ETHvalNewsImpact = { state: S, reload: load, render };
    if (document.readyState === 'loading') document.addEventListener('DOMContentLoaded', start); else start();
})();
