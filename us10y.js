// US 10Y Treasury yield card (Macro). Data: Supabase historical_us10y ← FRED DGS10 (scripts/data-collector.js).
// Business-day series: gaps (weekends/holidays) are left as gaps in time, never filled.
(function () {
    'use strict';
    const DAY = 86400000;
    const S = { rows: [], chart: null, period: 90, status: 'loading' };
    const T = {
        en: { asOf: 'as of', m1: '1M', noData: 'No data', bp: 'bp' },
        ko: { asOf: '기준', m1: '1개월', noData: '데이터 없음', bp: 'bp' },
        zh: { asOf: '截至', m1: '1个月', noData: '暂无数据', bp: '基点' },
        ja: { asOf: '時点', m1: '1ヶ月', noData: 'データなし', bp: 'bp' }
    };
    const lang = () => { const l = (typeof currentLang !== 'undefined' && currentLang) || document.documentElement.lang || 'en'; return T[l] ? l : 'en'; };
    const $ = (id) => document.getElementById(id);

    async function load() {
        const base = (typeof SUPABASE_URL !== 'undefined' ? SUPABASE_URL : 'https://rliwxedrifwxbudcutqs.supabase.co') + '/rest/v1/';
        const key = typeof SUPABASE_ANON_KEY !== 'undefined' ? SUPABASE_ANON_KEY : '';
        try {
            const r = await fetch(base + 'historical_us10y?select=date,yield_pct&order=date.desc&limit=1100', { headers: { apikey: key, Authorization: 'Bearer ' + key } });
            if (!r.ok) throw new Error('HTTP ' + r.status);
            const data = await r.json();
            S.rows = data.map(d => ({ date: d.date, t: Date.parse(d.date + 'T00:00:00Z'), v: parseFloat(d.yield_pct) }))
                .filter(d => Number.isFinite(d.v)).reverse();
            S.status = S.rows.length ? 'ok' : 'empty';
        } catch (e) { S.status = 'error'; S.rows = []; }
        render();
    }

    function header() {
        const L = T[lang()], v = $('us10y-value'), ch = $('us10y-change'), asof = $('us10y-asof');
        if (!v) return;
        if (S.status !== 'ok') { v.textContent = '--%'; if (ch) { ch.textContent = S.status === 'loading' ? '--' : L.noData; ch.className = 'change'; } return; }
        const last = S.rows[S.rows.length - 1];
        v.textContent = last.v.toFixed(2) + '%';
        if (asof) { asof.textContent = last.date; asof.title = `${L.asOf} ${last.date}`; }
        // 1M change: latest observation on or before (last date − 30 days); none → no change shown.
        const ref = [...S.rows].reverse().find(r => r.t <= last.t - 30 * DAY);
        if (ch) {
            if (!ref) { ch.textContent = '--'; ch.className = 'change'; return; }
            const bp = Math.round((last.v - ref.v) * 100);
            ch.textContent = `${bp > 0 ? '+' : ''}${bp}${L.bp} ${L.m1}`;
            // Higher yields tighten liquidity → bearish for risk assets (red); lower → green.
            ch.className = 'change' + (bp > 0 ? ' down' : bp < 0 ? ' up' : '');
        }
    }

    function chart() {
        const cv = $('us10y-chart');
        if (!cv || typeof Chart === 'undefined') return;
        const box = cv.parentElement && cv.parentElement.querySelector('.chart-loader');
        if (box) box.style.display = 'none';
        if (S.chart) { S.chart.destroy(); S.chart = null; }
        if (S.status !== 'ok') return;
        const last = S.rows[S.rows.length - 1].t;
        const pts = S.rows.filter(r => r.t >= last - S.period * DAY);
        const dark = document.body.classList.contains('dark');
        S.chart = new Chart(cv.getContext('2d'), {
            type: 'line',
            data: { datasets: [{ data: pts.map(p => ({ x: p.t, y: p.v })), borderColor: '#0ea5e9', backgroundColor: 'rgba(14,165,233,0.10)', borderWidth: 1.5, fill: true, tension: 0.2, pointRadius: 0, spanGaps: true }] },
            options: {
                responsive: true, maintainAspectRatio: false, animation: false,
                interaction: { mode: 'index', intersect: false },
                plugins: {
                    legend: { display: false },
                    tooltip: { callbacks: {
                        title: (c) => new Date(c[0].parsed.x).toISOString().slice(0, 10),
                        label: (c) => c.parsed.y.toFixed(2) + '%'
                    } }
                },
                scales: {
                    x: { type: 'time', time: { unit: S.period > 365 ? 'quarter' : 'month', displayFormats: { month: "MMM ''yy", quarter: "MMM ''yy" } }, grid: { display: false },
                        ticks: { font: { size: 8 }, color: dark ? '#9ca3af' : '#6b7280', maxTicksLimit: 5, autoSkip: true, maxRotation: 0 } },
                    y: { position: 'right', grid: { display: false },
                        ticks: { font: { size: 8 }, color: dark ? '#9ca3af' : '#6b7280', maxTicksLimit: 4, callback: (v) => v.toFixed(1) + '%' } }
                }
            }
        });
    }

    function render() { header(); chart(); }

    function bind() {
        document.querySelectorAll('.metric-tabs[data-chart="us10y"] .metric-tab').forEach(tab => {
            tab.addEventListener('click', () => {
                tab.closest('.metric-tabs').querySelectorAll('.metric-tab').forEach(t => t.classList.remove('active'));
                tab.classList.add('active');
                S.period = parseInt(tab.dataset.period, 10) || 90;
                chart();
            });
        });
        // Language switch (buttons, URL prefix, programmatic setLanguage) always updates <html lang>.
        new MutationObserver(header).observe(document.documentElement, { attributes: true, attributeFilter: ['lang'] });
        // Theme toggle: re-render so tick colours follow dark/light.
        let wasDark = document.body.classList.contains('dark');
        new MutationObserver(() => { const d = document.body.classList.contains('dark'); if (d !== wasDark) { wasDark = d; if (S.chart) chart(); } }).observe(document.body, { attributes: true, attributeFilter: ['class'] });
    }

    function init() { if (!$('us10y-chart')) return; bind(); header(); load(); }
    window.ETHvalUS10Y = { state: S, reload: load };
    if (document.readyState === 'loading') document.addEventListener('DOMContentLoaded', init); else init();
})();
