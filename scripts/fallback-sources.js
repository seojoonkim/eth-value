// Free public fallbacks for metrics whose primary source (CryptoQuant via proxy) stopped returning data.
// Each returns records shaped exactly like the existing Supabase table rows.
//  - funding_rate: Binance ETHUSDT perpetual, daily mean of 8h rates, stored in PERCENT
//    (same unit as the old CryptoQuant rows: the page prints value + '%').
//  - exchange_reserve: Coin Metrics community API SplyExNtv (ETH held on exchanges, daily).
//    Level differs from CryptoQuant's exchange set (~+8-13%), so we replace the whole 3y series
//    rather than splicing, to avoid a fake step change in the chart.
//  - open_interest: CoinGecko /derivatives snapshot, sum of ETH perpetual OI (USD) on major
//    exchanges. No free multi-exchange daily history exists, so this is forward-only (1 row/day).

const DAY = 864e5;
const isoDay = (ms) => new Date(ms).toISOString().slice(0, 10);

async function getJSON(url) {
    const ctl = new AbortController();
    const t = setTimeout(() => ctl.abort(), 30000);
    try {
        const res = await fetch(url, { signal: ctl.signal, headers: { 'User-Agent': 'ETHval/7.2', Accept: 'application/json' } });
        if (!res.ok) throw new Error(`HTTP ${res.status} ${url.split('?')[0]}`);
        return await res.json();
    } finally { clearTimeout(t); }
}

// Drop today's UTC day: it is still accumulating (same rule as checkAndRemoveIncomplete).
const completeDaysOnly = (records) => { const today = isoDay(Date.now()); return records.filter(r => r.date < today); };

async function fundingFromBinanceOnly(days = 1095) {
    const byDay = new Map();
    let start = Date.now() - days * DAY;
    for (let i = 0; i < 6; i++) {               // 1000 rows ≈ 333 days of 8h funding
        const rows = await getJSON(`https://fapi.binance.com/fapi/v1/fundingRate?symbol=ETHUSDT&startTime=${start}&limit=1000`);
        if (!Array.isArray(rows) || rows.length === 0) break;
        for (const r of rows) {
            const d = isoDay(r.fundingTime);
            if (!byDay.has(d)) byDay.set(d, []);
            byDay.get(d).push(parseFloat(r.fundingRate));
        }
        const last = rows[rows.length - 1].fundingTime;
        if (rows.length < 1000 || last >= Date.now() - DAY) break;
        start = last + 1;
    }
    const records = [...byDay.entries()]
        .filter(([, v]) => v.length >= 2)
        .map(([date, v]) => ({ date, funding_rate: +(v.reduce((s, x) => s + x, 0) / v.length * 100).toFixed(6), source: 'binance' }));
    return completeDaysOnly(records).sort((a, b) => a.date.localeCompare(b.date));
}

// Deribit ETH-PERPETUAL: hourly records carry `interest_8h` (fraction per 8h, same basis as Binance).
// Reachable from GitHub's US runners, where Binance returns HTTP 451 (geo-block).
async function fundingFromDeribit(days = 1095) {
    const byDay = new Map();
    const end = Date.now();
    for (let s = end - days * DAY; s < end; s += 20 * DAY) {      // 480 rows/call = 20 days hourly
        const j = await getJSON(`https://www.deribit.com/api/v2/public/get_funding_rate_history?instrument_name=ETH-PERPETUAL&start_timestamp=${s}&end_timestamp=${Math.min(s + 20 * DAY, end)}`);
        for (const r of j.result || []) {
            if (typeof r.interest_8h !== 'number') continue;
            const d = isoDay(r.timestamp);
            if (!byDay.has(d)) byDay.set(d, []);
            byDay.get(d).push(r.interest_8h);
        }
    }
    const records = [...byDay.entries()]
        .filter(([, v]) => v.length >= 12)
        .map(([date, v]) => ({ date, funding_rate: +(v.reduce((s, x) => s + x, 0) / v.length * 100).toFixed(6), source: 'deribit' }));
    return completeDaysOnly(records).sort((a, b) => a.date.localeCompare(b.date));
}

// Binance first (deepest liquidity); Deribit when Binance is geo-blocked or down.
async function fundingFromBinance(days = 1095) {
    try {
        const r = await fundingFromBinanceOnly(days);
        if (r.length > 100) return r;
        throw new Error(`only ${r.length} rows`);
    } catch (e) {
        console.log(`  ↪ funding: Binance unavailable (${e.message}) → Deribit`);
        return fundingFromDeribit(days);
    }
}

async function reserveFromCoinMetrics(days = 1095) {
    const start = isoDay(Date.now() - days * DAY);
    let url = `https://community-api.coinmetrics.io/v4/timeseries/asset-metrics?assets=eth&metrics=SplyExNtv&frequency=1d&start_time=${start}&page_size=10000`;
    const out = [];
    for (let i = 0; i < 5 && url; i++) {
        const j = await getJSON(url);
        for (const r of j.data || []) {
            const v = parseFloat(r.SplyExNtv);
            if (v > 0) out.push({ date: r.time.slice(0, 10), reserve_eth: v, source: 'coinmetrics' });
        }
        url = j.next_page_url || null;
    }
    return completeDaysOnly(out);
}

const MAJOR_DERIV = /^(Binance|Bybit|OKX|Bitget|Deribit|BitMEX|Kraken|HTX|Huobi|Bitfinex|Gate \(Futures\))/i;
async function openInterestFromCoinGecko() {
    const list = await getJSON('https://api.coingecko.com/api/v3/derivatives');
    const eth = (list || []).filter(x => x.contract_type === 'perpetual' && /^ETH$/i.test(x.index_id || '') && x.open_interest > 0 && MAJOR_DERIV.test(x.market || ''));
    const total = eth.reduce((s, x) => s + x.open_interest, 0);
    if (!(total > 1e9)) throw new Error(`CoinGecko OI implausible: ${total}`);
    // Snapshot taken now = value for today's date (OI is a level, not a daily sum).
    return [{ date: isoDay(Date.now()), open_interest: total, source: 'coingecko_major' }];
}

module.exports = { fundingFromBinance, fundingFromDeribit, reserveFromCoinMetrics, openInterestFromCoinGecko };
