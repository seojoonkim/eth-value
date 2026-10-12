// Broad dollar index card (Macro). Data: Supabase historical_dxy ← FRED DTWEXBGS (scripts/data-collector.js). Not ICE DXY.
// Business-day series: gaps (weekends/holidays) are left as gaps in time, never filled.
(function () {
    'use strict';
    const DAY = 86400000;
    const S = { rows: [], chart: null, period: 90, status: 'loading' };
    const T = {
        en: { asOf: 'as of', m1: '1M', noData: 'No data', },
        ko: { asOf: '기준', m1: '1개월', noData: '데이터 없음', },
        zh: { asOf: '截至', m1: '1个月', noData: '暂无数据', },
        ja: { asOf: '時点', m1: '1ヶ月', noData: 'データなし', bp: 'bp' }
    };
    const lang = () => { const l = (typeof currentLang !== 'undefined' && currentLang) || document.documentElement.lang || 'en'; return T[l] ? l : 'en'; };
    const $ = (id) => document.getElementById(id);

    async function load() {
        const base = (typeof SUPABASE_URL !== 'undefined' ? SUPABASE_URL : 'https://rliwxedrifwxbudcutqs.supabase.co') + '/rest/v1/';
        const key = typeof SUPABASE_ANON_KEY !== 'undefined' ? SUPABASE_ANON_KEY : '';
        try {
            const r = await fetch(base + 'historical_dxy?select=date,index_value&order=date.desc&limit=1100', { headers: { apikey: key, Authorization: 'Bearer ' + key } });
            if (!r.ok) throw new Error('HTTP ' + r.status);
            const data = await r.json();
            S.rows = data.map(d => ({ date: d.date, t: Date.parse(d.date + 'T00:00:00Z'), v: parseFloat(d.index_value) }))
                .filter(d => Number.isFinite(d.v)).reverse();
            S.status = S.rows.length ? 'ok' : 'empty';
        } catch (e) { S.status = 'error'; S.rows = []; }
        render();
    }

    function header() {
        const L = T[lang()], v = $('dxy-value'), ch = $('dxy-change'), asof = $('dxy-asof');
        if (!v) return;
        if (S.status !== 'ok') { v.textContent = '--'; if (ch) { ch.textContent = S.status === 'loading' ? '--' : L.noData; ch.className = 'change'; } return; }
        const last = S.rows[S.rows.length - 1];
        v.textContent = last.v.toFixed(2);
        if (asof) { asof.textContent = last.date; asof.title = `${L.asOf} ${last.date}`; }
        // 1M change: latest observation on or before (last date − 30 days); none → no change shown.
        const ref = [...S.rows].reverse().find(r => r.t <= last.t - 30 * DAY);
        if (ch) {
            if (!ref) { ch.textContent = '--'; ch.className = 'change'; return; }
            const pct = (last.v / ref.v - 1) * 100, r2 = Math.round(pct * 100) / 100;
            ch.textContent = `${r2 > 0 ? '+' : ''}${r2.toFixed(2)}% ${L.m1}`;
            // Stronger dollar → tighter global liquidity → bearish for ETH (red); weaker → green.
            ch.className = 'change' + (r2 > 0 ? ' down' : r2 < 0 ? ' up' : '');
        }
    }

    function chart() {
        const cv = $('dxy-chart');
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
            data: { datasets: [{ data: pts.map(p => ({ x: p.t, y: p.v })), borderColor: '#16a34a', backgroundColor: 'rgba(22,163,74,0.10)', borderWidth: 1.5, fill: true, tension: 0.2, pointRadius: 0, spanGaps: true }] },
            options: {
                responsive: true, maintainAspectRatio: false, animation: false,
                interaction: { mode: 'index', intersect: false },
                plugins: {
                    legend: { display: false },
                    tooltip: { callbacks: {
                        title: (c) => new Date(c[0].parsed.x).toISOString().slice(0, 10),
                        label: (c) => c.parsed.y.toFixed(2)
                    } }
                },
                scales: {
                    x: { type: 'time', time: { unit: S.period > 365 ? 'quarter' : 'month', displayFormats: { month: "MMM ''yy", quarter: "MMM ''yy" } }, grid: { display: false },
                        ticks: { font: { size: 8 }, color: dark ? '#9ca3af' : '#6b7280', maxTicksLimit: 5, autoSkip: true, maxRotation: 0 } },
                    y: { position: 'right', grid: { display: false },
                        ticks: { font: { size: 8 }, color: dark ? '#9ca3af' : '#6b7280', maxTicksLimit: 4, callback: (v) => v.toFixed(0) } }
                }
            }
        });
    }

    function render() { header(); chart(); }

    function bind() {
        document.querySelectorAll('.metric-tabs[data-chart="dxy"] .metric-tab').forEach(tab => {
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

    function init() { if (!$('dxy-chart')) return; bind(); header(); load(); }
    window.ETHvalDXY = { state: S, reload: load };
    if (document.readyState === 'loading') document.addEventListener('DOMContentLoaded', init); else init();
})();
