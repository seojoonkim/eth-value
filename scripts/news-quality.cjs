/* ETHval news quality rules shared by the collector (write-time gate) and QA (read-time check).
 * Origin 2026-10-15 Argon audit, confirmed on live rows:
 *  - 10-12 "Bitget reports ~$388M unauthorized transfers" was the same 09-25 $387M Bitget hack, scored twice
 *    (collector only looked 3 days back for already-recorded events).
 *  - 10-02 en mechanism ended "price pressure ↓" on a bearish event (ko said 하방 압력 ↑).
 *  - 10-14 "ETFs end 10-day outflow streak" became ko "…연속 순유출 마감" (reads as outflows continuing).
 */
'use strict';

const NEG = {
    en: /pressure|sell|risk|outflow|volatil|liquidat|fear|headwind|drag|supply available|concern/i,
    ko: /압력|압박|매도|위험|유출|변동성|청산|공포|우려|부담/,
};

// Price effect at the end of a causal chain: 1 up, -1 down, 0 unknown/neutral wording.
function finalPolarity(s, l) {
    if (!s) return 0;
    const segs = String(s).split('→').map(x => x.trim()).filter(Boolean);
    if (!segs.length) return 0;
    const last = segs[segs.length - 1].split(';')[0].trim(); // "; concentration risk ↑" is a side note
    const pm = last.match(/([+\-−])\s*$/); if (pm) return pm[1] === '+' ? 1 : -1;
    if (/하방 압력$|ETH pressure$|ETH 하방/.test(last)) return -1;
    if (/upside|상승/.test(last) && /↑\s*$/.test(last)) return 1;
    const arrows = last.match(/[↑↓]/g); if (!arrows) return 0;
    let p = arrows[arrows.length - 1] === '↑' ? 1 : -1;
    const core = last.replace(/risk appetite|위험 선호/gi, ''); // a positive quantity despite "risk"
    if ((NEG[l] || NEG.en).test(core)) p = -p;
    return p;
}

// Languages whose chain ends against the event's bullish/bearish call.
function mechanismConflicts(ev) {
    const d = +ev.direction; if (!d || !ev.mechanism) return [];
    return ['en', 'ko'].filter(l => { const p = finalPolarity(ev.mechanism[l], l); return p && p !== d; });
}

const STOP = new Set(['The', 'U.S', 'US', 'ETH', 'Ether', 'Ethereum', 'Crypto', 'Spot', 'ETF', 'ETFs', 'SEC', 'Fed', 'After', 'Over', 'About', 'New']);
function actors(t) { return new Set((String(t).match(/\b[A-Z][A-Za-z0-9]{2,}\b/g) || []).filter(w => !STOP.has(w))); }
function amountsM(t) {
    const out = [];
    for (const m of String(t).matchAll(/\$\s?([\d,.]+)\s*(billion|bn|B|million|mn|M)\b/g)) {
        const v = parseFloat(m[1].replace(/,/g, '')); if (!isFinite(v)) continue;
        out.push(/^b/i.test(m[2]) ? v * 1000 : v);
    }
    return out;
}
const ONE_OFF = new Set(['security']);
// Same incident reported again (same named actor + same dollar amount within 15%, within 30 days).
function duplicateOf(ev, existing) {
    // One-off incidents only: recurring flows (e.g. Bitmine buying $68M then $75M) are separate events.
    if (!ONE_OFF.has(ev.category)) return null;
    const t = ev.title?.en || ev.title || '';
    const a = actors(t), m = amountsM(t);
    if (!a.size || !m.length) return null;
    const day = Date.parse(ev.date);
    for (const x of existing) {
        if (x.id === ev.id) continue;
        if (Math.abs(Date.parse(x.date) - day) > 30 * 86400000) continue;
        if (ev.category && x.category && ev.category !== x.category) continue;
        const xt = x.title?.en || x.title || '';
        if (![...actors(xt)].some(w => a.has(w))) continue;
        const xm = amountsM(xt);
        if (xm.some(v => m.some(u => Math.abs(u - v) / Math.max(u, v) <= 0.15))) return x;
    }
    return null;
}

module.exports = { finalPolarity, mechanismConflicts, duplicateOf, actors, amountsM };
