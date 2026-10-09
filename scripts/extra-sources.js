// Real-data collectors for charts whose original feeds stopped (no synthetic values anywhere).
//  - Open interest: Binance public archive (data.binance.vision) daily metrics, USDT-M ETHUSDT + COIN-M
//    ETHUSD_PERP. Reachable from GitHub runners (fapi.binance.com is geo-blocked there). Full history 2021+.
//  - L2 stablecoin supply: DefiLlama per-chain circulating stablecoins (all pegs).
//  - L2 stablecoin volume: Dune tokens.transfers, per chain, USDC/USDT/DAI/USDe (same columns as the old table).
const zlib = require('zlib');

// Minimal single-entry ZIP reader (Binance archives hold one CSV, deflate or stored).
function unzipFirst(buf) {
    if (buf.readUInt32LE(0) !== 0x04034b50) throw new Error('not a zip');
    const method = buf.readUInt16LE(8);
    let csize = buf.readUInt32LE(18);
    const nameLen = buf.readUInt16LE(26), extraLen = buf.readUInt16LE(28);
    const start = 30 + nameLen + extraLen;
    if (csize === 0) {   // sizes in data descriptor → use the central directory
        const cd = buf.lastIndexOf(Buffer.from([0x50, 0x4b, 0x01, 0x02]));
        csize = buf.readUInt32LE(cd + 20);
    }
    const data = buf.subarray(start, start + csize);
    return (method === 8 ? zlib.inflateRawSync(data) : data).toString('utf8');
}

const dayList = (from, to) => {   // inclusive from, exclusive to, YYYY-MM-DD
    const out = [];
    for (let t = Date.parse(from); t < Date.parse(to); t += 864e5) out.push(new Date(t).toISOString().slice(0, 10));
    return out;
};

// Last 5-minute snapshot of the day (closest to 24:00 UTC), USD.
async function binanceOIDay(date) {
    const get = async (url) => {
        const r = await fetch(url);
        if (r.status === 404) return null;
        if (!r.ok) throw new Error(`HTTP ${r.status}`);
        const rows = unzipFirst(Buffer.from(await r.arrayBuffer())).trim().split('\n').slice(1).map(l => l.split(','));
        return rows.length ? rows[rows.length - 1] : null;
    };
    const um = await get(`https://data.binance.vision/data/futures/um/daily/metrics/ETHUSDT/ETHUSDT-metrics-${date}.zip`);
    const cm = await get(`https://data.binance.vision/data/futures/cm/daily/metrics/ETHUSD_PERP/ETHUSD_PERP-metrics-${date}.zip`);
    if (!um) return null;                                   // archive not published yet
    const umUsd = parseFloat(um[3]);                        // sum_open_interest_value (USDT)
    const cmUsd = cm ? parseFloat(cm[2]) * 10 : 0;          // contracts × $10 face value
    if (!(umUsd > 0)) return null;
    return { date, open_interest: umUsd + cmUsd, source: 'binance' };
}

async function binanceOIRange(from, to, { concurrency = 8, log = () => {} } = {}) {
    const days = dayList(from, to), out = [], failed = [];
    for (let i = 0; i < days.length; i += concurrency) {
        const part = await Promise.all(days.slice(i, i + concurrency).map(d => binanceOIDay(d).catch(e => { failed.push(`${d}: ${e.message}`); return null; })));
        out.push(...part.filter(Boolean));
        if (i % (concurrency * 20) === 0) log(`  ↪ binance OI ${days[i]} (${out.length} rows)`);
    }
    return { records: out, failed };
}

// DefiLlama per-chain stablecoin circulating supply → rows shaped like historical_l2_stablecoin_daily.
const L2_STABLE_CHAINS = { arbitrum: 'Arbitrum', base: 'Base', optimism: 'OP Mainnet', polygon: 'Polygon', zksync: 'zkSync Era', linea: 'Linea', scroll: 'Scroll' };
async function l2StablecoinSupplyRecords() {
    const byDate = {};
    for (const [key, name] of Object.entries(L2_STABLE_CHAINS)) {
        const r = await fetch(`https://stablecoins.llama.fi/stablecoincharts/${encodeURIComponent(name)}`);
        const j = r.ok ? await r.json() : null;
        if (!Array.isArray(j) || j.length < 100) throw new Error(`defillama stablecoins ${name} HTTP ${r.status}`);
        for (const x of j) {
            const date = new Date(+x.date * 1000).toISOString().slice(0, 10);
            const v = x.totalCirculatingUSD ? Object.values(x.totalCirculatingUSD).reduce((a, b) => a + (+b || 0), 0) : 0;
            (byDate[date] ||= { date })[key] = v;
        }
    }
    const today = new Date().toISOString().slice(0, 10);
    return Object.values(byDate)
        .filter(r => r.date < today)
        .map(r => { for (const c of Object.keys(L2_STABLE_CHAINS)) r[c] = r[c] || 0; r.total = Object.keys(L2_STABLE_CHAINS).reduce((a, c) => a + r[c], 0); return r; })
        .filter(r => r.total > 0)
        .sort((a, b) => a.date.localeCompare(b.date));
}

// Dune SQL: one chain, one window. Columns match historical_l2_stablecoin_volume.
const L2_STABLEVOL_CHAINS = ['arbitrum', 'base', 'optimism', 'polygon', 'zksync', 'linea', 'scroll'];
function l2StablecoinVolSQL(chain, fromDate, toDate) {
    return `SELECT DATE_TRUNC('day', block_time) AS date, '${chain}' AS chain,
  SUM(amount_usd) AS total_volume,
  SUM(CASE WHEN symbol = 'USDC' OR symbol = 'USDC.e' OR symbol = 'USDbC' THEN amount_usd ELSE 0 END) AS usdc_volume,
  SUM(CASE WHEN symbol = 'USDT' OR symbol = 'USDT0' OR symbol = 'USD₮0' THEN amount_usd ELSE 0 END) AS usdt_volume,
  SUM(CASE WHEN symbol = 'DAI' THEN amount_usd ELSE 0 END) AS dai_volume,
  SUM(CASE WHEN symbol = 'USDe' THEN amount_usd ELSE 0 END) AS usde_volume,
  COUNT(DISTINCT tx_hash) AS tx_count
FROM tokens.transfers
WHERE blockchain = '${chain}' AND block_time >= DATE '${fromDate}' AND block_time < DATE '${toDate}'
  AND symbol IN ('USDC', 'USDC.e', 'USDbC', 'USDT', 'USDT0', 'USD₮0', 'DAI', 'USDe')
  AND amount_usd > 0 AND amount_usd < 1e10
GROUP BY 1 ORDER BY 1`;
}

module.exports = { unzipFirst, binanceOIDay, binanceOIRange, l2StablecoinSupplyRecords, L2_STABLE_CHAINS, l2StablecoinVolSQL, L2_STABLEVOL_CHAINS, dayList };
