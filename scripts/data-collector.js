/**
 * ETHval Data Collector v6.0
 * 29개 전체 데이터셋 수집
 */

const { createClient } = require('@supabase/supabase-js');

const SUPABASE_URL = process.env.SUPABASE_URL;
const SUPABASE_KEY = process.env.SUPABASE_SERVICE_KEY;

if (!SUPABASE_URL || !SUPABASE_KEY) {
    console.error('❌ Missing SUPABASE_URL or SUPABASE_SERVICE_KEY');
    process.exit(1);
}

const supabase = createClient(SUPABASE_URL, SUPABASE_KEY);
const sleep = (ms) => new Promise(r => setTimeout(r, ms));

// ============================================================
// Helper Functions
// ============================================================
async function fetchJSON(url, retries = 3) {
    for (let i = 0; i < retries; i++) {
        try {
            const controller = new AbortController();
            const timeout = setTimeout(() => controller.abort(), 30000);
            const res = await fetch(url, {
                signal: controller.signal,
                headers: { 'User-Agent': 'ETHval/6.0', 'Accept': 'application/json' }
            });
            clearTimeout(timeout);
            if (!res.ok) throw new Error(`HTTP ${res.status}`);
            return await res.json();
        } catch (e) {
            if (i < retries - 1) await sleep(2000 * (i + 1));
        }
    }
    return null;
}

async function upsertBatch(table, records, conflict = 'date') {
    let saved = 0;
    for (let i = 0; i < records.length; i += 500) {
        const batch = records.slice(i, i + 500);
        const { error } = await supabase.from(table).upsert(batch, { onConflict: conflict });
        if (!error) saved += batch.length;
        else console.error(`  Error ${table}:`, error.message);
    }
    return saved;
}

const cutoff3Y = () => Date.now() / 1000 - (1095 * 24 * 60 * 60);

// ============================================================
// 1. ETH Price (Binance)
// ============================================================
async function collect_eth_price() {
    console.log('\n📈 [1/29] ETH Price...');
    const data = await fetchJSON('https://api.binance.com/api/v3/klines?symbol=ETHUSDT&interval=1d&limit=1100');
    if (!data) return 0;
    const records = data.map(k => ({
        date: new Date(k[0]).toISOString().split('T')[0],
        open: parseFloat(k[1]), high: parseFloat(k[2]), low: parseFloat(k[3]),
        close: parseFloat(k[4]), volume: parseFloat(k[5]), source: 'binance'
    }));
    return await upsertBatch('historical_eth_price', records);
}

// ============================================================
// 2. Ethereum TVL (DefiLlama)
// ============================================================
async function collect_ethereum_tvl() {
    console.log('\n🏦 [2/29] Ethereum TVL...');
    const data = await fetchJSON('https://api.llama.fi/v2/historicalChainTvl/Ethereum');
    if (!data) return 0;
    const records = data.filter(d => d.date > cutoff3Y() && d.tvl > 0).map(d => ({
        date: new Date(d.date * 1000).toISOString().split('T')[0],
        tvl: parseFloat(d.tvl.toFixed(2)), source: 'defillama'
    }));
    return await upsertBatch('historical_ethereum_tvl', records);
}

// ============================================================
// 3. L2 TVL (DefiLlama)
// ============================================================
async function collect_l2_tvl() {
    console.log('\n🔗 [3/29] L2 TVL...');
    const chains = ['Arbitrum', 'Optimism', 'Base', 'zkSync Era', 'Linea', 'Scroll', 'Blast'];
    const all = [];
    for (const chain of chains) {
        await sleep(300);
        const data = await fetchJSON(`https://api.llama.fi/v2/historicalChainTvl/${encodeURIComponent(chain)}`);
        if (data) {
            const recs = data.filter(d => d.date > cutoff3Y() && d.tvl > 0).map(d => ({
                date: new Date(d.date * 1000).toISOString().split('T')[0],
                chain, tvl: parseFloat(d.tvl.toFixed(2))
            }));
            all.push(...recs);
            console.log(`  ${chain}: ${recs.length}`);
        }
    }
    return await upsertBatch('historical_l2_tvl', all, 'date,chain');
}

// ============================================================
// 4. Protocol Fees (DefiLlama)
// ============================================================
async function collect_protocol_fees() {
    console.log('\n💰 [4/29] Protocol Fees...');
    const data = await fetchJSON('https://api.llama.fi/summary/fees/ethereum?dataType=dailyFees');
    if (!data?.totalDataChart) return 0;
    const records = data.totalDataChart.filter(d => d[1] > 0).map(d => ({
        date: new Date(d[0] * 1000).toISOString().split('T')[0],
        fees: parseFloat(d[1].toFixed(2)), source: 'defillama'
    }));
    return await upsertBatch('historical_protocol_fees', records);
}

// ============================================================
// 5. Staking Data (beaconcha.in)
// ============================================================
async function collect_staking() {
    console.log('\n🥩 [5/29] Staking Data...');
    const records = [];
    
    // Historical staked ether
    const chart = await fetchJSON('https://beaconcha.in/api/v1/chart/staked_ether');
    if (chart?.status === 'OK' && chart.data) {
        for (const item of chart.data) {
            if (Array.isArray(item) && item[1] > 0) {
                records.push({
                    date: new Date(item[0]).toISOString().split('T')[0],
                    total_staked_eth: parseFloat(item[1]),
                    total_validators: Math.floor(item[1] / 32),
                    avg_apr: null, source: 'beaconchain'
                });
            }
        }
    }
    
    // Current epoch
    const epoch = await fetchJSON('https://beaconcha.in/api/v1/epoch/latest');
    if (epoch?.status === 'OK' && epoch.data) {
        const today = new Date().toISOString().split('T')[0];
        const validators = epoch.data.validatorscount;
        const idx = records.findIndex(r => r.date === today);
        if (idx >= 0) {
            records[idx].total_staked_eth = validators * 32;
            records[idx].total_validators = validators;
        } else {
            records.push({ date: today, total_staked_eth: validators * 32, total_validators: validators, avg_apr: null, source: 'beaconchain' });
        }
    }
    
    // APR from Lido
    const lido = await fetchJSON('https://eth-api.lido.fi/v1/protocol/steth/apr/sma');
    if (lido?.data?.smaApr) {
        const today = new Date().toISOString().split('T')[0];
        const idx = records.findIndex(r => r.date === today);
        if (idx >= 0) records[idx].avg_apr = parseFloat(lido.data.smaApr.toFixed(2));
    }
    
    // Dedupe
    const unique = new Map();
    records.forEach(r => unique.set(r.date, r));
    return await upsertBatch('historical_staking', Array.from(unique.values()));
}

// ============================================================
// 6. Gas & Burn (calculated from fees/price)
// ============================================================
async function collect_gas_burn() {
    console.log('\n🔥 [6/29] Gas & Burn...');
    const { data: fees } = await supabase.from('historical_protocol_fees').select('date, fees').order('date');
    const { data: prices } = await supabase.from('historical_eth_price').select('date, close').order('date');
    if (!fees || !prices) return 0;
    
    const priceMap = new Map();
    prices.forEach(p => priceMap.set(p.date, parseFloat(p.close)));
    
    const records = [];
    for (const f of fees) {
        const price = priceMap.get(f.date);
        if (!price || !f.fees) continue;
        const burn = (f.fees * 0.80) / price;
        if (burn >= 50 && burn <= 50000) {
            records.push({ date: f.date, eth_burnt: parseFloat(burn.toFixed(2)), avg_gas_price_gwei: null, transaction_count: null, source: 'calculated' });
        }
    }
    return await upsertBatch('historical_gas_burn', records);
}

// ============================================================
// 7. Active Addresses (Etherscan or estimate)
// ============================================================
async function collect_active_addresses() {
    console.log('\n👥 [7/29] Active Addresses...');
    // Using transactions as proxy - real data would need Etherscan API
    const { data: txs } = await supabase.from('historical_transactions').select('date, tx_count').order('date');
    if (!txs || txs.length === 0) {
        console.log('  ⚠️ No transaction data, skipping');
        return 0;
    }
    const records = txs.map(t => ({
        date: t.date,
        active_addresses: Math.floor(t.tx_count * 0.4), // Rough estimate
        source: 'estimated'
    }));
    return await upsertBatch('historical_active_addresses', records);
}

// ============================================================
// 8. ETH Supply (Ultrasound.money or estimate)
// ============================================================
async function collect_eth_supply() {
    console.log('\n💎 [8/29] ETH Supply...');
    // Try ultrasound.money API
    const data = await fetchJSON('https://ultrasound.money/api/v2/fees/supply-over-time');
    if (data && Array.isArray(data)) {
        const records = data.slice(-1095).map(d => ({
            date: new Date(d.timestamp * 1000).toISOString().split('T')[0],
            eth_supply: parseFloat((d.supply / 1e18).toFixed(2)),
            source: 'ultrasound'
        }));
        return await upsertBatch('historical_eth_supply', records);
    }
    
    // Fallback: estimate from known values
    const today = new Date();
    const records = [];
    const baseSupply = 120400000;
    for (let i = 0; i < 1095; i++) {
        const date = new Date(today);
        date.setDate(date.getDate() - i);
        // ETH supply changes ~0.001% per day post-merge
        const daysDiff = i;
        const supply = baseSupply + (daysDiff * 100); // rough estimate
        records.push({
            date: date.toISOString().split('T')[0],
            eth_supply: supply,
            source: 'estimated'
        });
    }
    return await upsertBatch('historical_eth_supply', records);
}

// ============================================================
// 9. Fear & Greed (Alternative.me)
// ============================================================
async function collect_fear_greed() {
    console.log('\n😱 [9/29] Fear & Greed...');
    const data = await fetchJSON('https://api.alternative.me/fng/?limit=1095&format=json');
    if (!data?.data) return 0;
    const records = data.data.map(d => ({
        date: new Date(parseInt(d.timestamp) * 1000).toISOString().split('T')[0],
        value: parseInt(d.value),
        classification: d.value_classification,
        source: 'alternative_me'
    }));
    return await upsertBatch('historical_fear_greed', records);
}

// ============================================================
// 10. DEX Volume (DefiLlama)
// ============================================================
async function collect_dex_volume() {
    console.log('\n💱 [10/29] DEX Volume...');
    const data = await fetchJSON('https://api.llama.fi/overview/dexs/ethereum?excludeTotalDataChart=false&excludeTotalDataChartBreakdown=true&dataType=dailyVolume');
    if (!data?.totalDataChart) return 0;
    const records = data.totalDataChart.filter(d => d[1] > 0).map(d => ({
        date: new Date(d[0] * 1000).toISOString().split('T')[0],
        volume: parseFloat(d[1].toFixed(2)), source: 'defillama'
    }));
    return await upsertBatch('historical_dex_volume', records);
}

// ============================================================
// 11. Stablecoins All (DefiLlama)
// ============================================================
async function collect_stablecoins() {
    console.log('\n💵 [11/29] Stablecoins (All)...');
    const data = await fetchJSON('https://stablecoins.llama.fi/stablecoincharts/all');
    if (!data) return 0;
    const records = data.filter(d => d.date > cutoff3Y()).map(d => ({
        date: new Date(d.date * 1000).toISOString().split('T')[0],
        total_mcap: parseFloat((d.totalCirculatingUSD?.peggedUSD || d.totalCirculating?.peggedUSD || 0).toFixed(2)),
        source: 'defillama'
    })).filter(r => r.total_mcap > 0);
    return await upsertBatch('historical_stablecoins', records);
}

// ============================================================
// 12. Stablecoins ETH (DefiLlama)
// ============================================================
async function collect_stablecoins_eth() {
    console.log('\n🔷 [12/29] Stablecoins (ETH)...');
    const data = await fetchJSON('https://stablecoins.llama.fi/stablecoincharts/Ethereum');
    if (!data) return 0;
    const records = data.filter(d => d.date > cutoff3Y()).map(d => ({
        date: new Date(d.date * 1000).toISOString().split('T')[0],
        total_mcap: parseFloat((d.totalCirculatingUSD?.peggedUSD || d.totalCirculating?.peggedUSD || 0).toFixed(2)),
        source: 'defillama'
    })).filter(r => r.total_mcap > 0);
    return await upsertBatch('historical_stablecoins_eth', records);
}

// ============================================================
// 13. ETH/BTC Ratio (Binance)
// ============================================================
async function collect_eth_btc() {
    console.log('\n₿ [13/29] ETH/BTC...');
    const data = await fetchJSON('https://api.binance.com/api/v3/klines?symbol=ETHBTC&interval=1d&limit=1100');
    if (!data) return 0;
    const records = data.map(k => ({
        date: new Date(k[0]).toISOString().split('T')[0],
        ratio: parseFloat(parseFloat(k[4]).toFixed(6)), source: 'binance'
    }));
    return await upsertBatch('historical_eth_btc', records);
}

// ============================================================
// 14. Funding Rate (Binance)
// ============================================================
async function collect_funding_rate() {
    console.log('\n📊 [14/29] Funding Rate...');
    const data = await fetchJSON('https://fapi.binance.com/fapi/v1/fundingRate?symbol=ETHUSDT&limit=1000');
    if (!data) return 0;
    
    // Group by date and average
    const byDate = new Map();
    data.forEach(d => {
        const date = new Date(d.fundingTime).toISOString().split('T')[0];
        if (!byDate.has(date)) byDate.set(date, []);
        byDate.get(date).push(parseFloat(d.fundingRate));
    });
    
    const records = [];
    byDate.forEach((rates, date) => {
        const avg = rates.reduce((a, b) => a + b, 0) / rates.length;
        records.push({ date, funding_rate: parseFloat(avg.toFixed(8)), source: 'binance' });
    });
    
    return await upsertBatch('historical_funding_rate', records);
}

// ============================================================
// 15. Exchange Reserve (estimate)
// ============================================================
async function collect_exchange_reserve() {
    console.log('\n🏛️ [15/29] Exchange Reserve...');
    // Would need CryptoQuant/Glassnode for real data
    const { data: existing } = await supabase.from('historical_exchange_reserve').select('*').order('date', { ascending: false }).limit(1);
    if (existing && existing.length > 0) {
        console.log('  Using existing data');
        return existing.length;
    }
    console.log('  ⚠️ No source available');
    return 0;
}

// ============================================================
// 16. ETH Dominance (CoinGecko)
// ============================================================
async function collect_eth_dominance() {
    console.log('\n👑 [16/29] ETH Dominance...');
    const data = await fetchJSON('https://api.coingecko.com/api/v3/global');
    if (!data?.data?.market_cap_percentage?.eth) {
        console.log('  ⚠️ CoinGecko rate limited');
        return 0;
    }
    const today = new Date().toISOString().split('T')[0];
    const records = [{
        date: today,
        eth_dominance: parseFloat(data.data.market_cap_percentage.eth.toFixed(2)),
        btc_dominance: parseFloat(data.data.market_cap_percentage.btc.toFixed(2)),
        total_mcap: data.data.total_market_cap.usd,
        source: 'coingecko'
    }];
    return await upsertBatch('historical_eth_dominance', records);
}

// ============================================================
// 17. Blob Data (beaconcha.in)
// ============================================================
async function collect_blob_data() {
    console.log('\n🫧 [17/29] Blob Data...');
    // Limited API access - using existing or estimate
    const { data: existing } = await supabase.from('historical_blob_data').select('*').order('date', { ascending: false }).limit(1);
    if (existing && existing.length > 0) {
        console.log('  Using existing data');
        return existing.length;
    }
    console.log('  ⚠️ No public API available');
    return 0;
}

// ============================================================
// 18. Lending TVL (DefiLlama)
// ============================================================
async function collect_lending_tvl() {
    console.log('\n🏦 [18/29] Lending TVL...');
    const data = await fetchJSON('https://api.llama.fi/v2/historicalChainTvl/Ethereum');
    if (!data) return 0;
    // Estimate lending as ~50% of total TVL
    const records = data.filter(d => d.date > cutoff3Y() && d.tvl > 0).map(d => ({
        date: new Date(d.date * 1000).toISOString().split('T')[0],
        total_tvl: parseFloat((d.tvl * 0.5).toFixed(2)),
        source: 'defillama_estimated'
    }));
    return await upsertBatch('historical_lending_tvl', records);
}

// ============================================================
// 19. Volatility (calculated from price)
// ============================================================
async function collect_volatility() {
    console.log('\n📉 [19/29] Volatility...');
    const { data: prices } = await supabase.from('historical_eth_price').select('date, close').order('date');
    if (!prices || prices.length < 30) return 0;
    
    const records = [];
    for (let i = 30; i < prices.length; i++) {
        const window = prices.slice(i - 30, i);
        const returns = [];
        for (let j = 1; j < window.length; j++) {
            returns.push(Math.log(window[j].close / window[j-1].close));
        }
        const mean = returns.reduce((a, b) => a + b, 0) / returns.length;
        const variance = returns.reduce((a, b) => a + Math.pow(b - mean, 2), 0) / returns.length;
        const volatility = Math.sqrt(variance * 365) * 100; // Annualized
        
        records.push({
            date: prices[i].date,
            volatility_30d: parseFloat(volatility.toFixed(2)),
            source: 'calculated'
        });
    }
    return await upsertBatch('historical_volatility', records);
}

// ============================================================
// 20. NVT Ratio (calculated)
// ============================================================
async function collect_nvt() {
    console.log('\n📐 [20/29] NVT Ratio...');
    const { data: prices } = await supabase.from('historical_eth_price').select('date, close, volume').order('date');
    if (!prices) return 0;
    
    const ETH_SUPPLY = 120400000;
    const records = [];
    for (const p of prices) {
        if (!p.volume || p.volume === 0) continue;
        const mcap = p.close * ETH_SUPPLY;
        const nvt = mcap / (p.volume * p.close); // Simplified
        if (nvt > 0 && nvt < 1000) {
            records.push({
                date: p.date,
                nvt_ratio: parseFloat(nvt.toFixed(2)),
                market_cap: mcap,
                tx_volume: p.volume * p.close,
                source: 'calculated'
            });
        }
    }
    return await upsertBatch('historical_nvt', records);
}

// ============================================================
// 21. Transactions (DefiLlama)
// ============================================================
async function collect_transactions() {
    console.log('\n📝 [21/29] Transactions...');
    const data = await fetchJSON('https://api.llama.fi/summary/fees/ethereum?dataType=dailyFees');
    if (!data?.totalDataChart) return 0;
    // Estimate tx from fees (avg $5/tx)
    const records = data.totalDataChart.filter(d => d[1] > 0).map(d => ({
        date: new Date(d[0] * 1000).toISOString().split('T')[0],
        tx_count: Math.floor(d[1] / 5),
        source: 'estimated'
    }));
    return await upsertBatch('historical_transactions', records);
}

// ============================================================
// 22. L2 Transactions (DefiLlama)
// ============================================================
async function collect_l2_transactions() {
    console.log('\n🔗 [22/29] L2 Transactions...');
    const chains = ['Arbitrum', 'Optimism', 'Base'];
    const all = [];
    for (const chain of chains) {
        await sleep(300);
        const data = await fetchJSON(`https://api.llama.fi/summary/fees/${chain.toLowerCase()}?dataType=dailyFees`);
        if (data?.totalDataChart) {
            const recs = data.totalDataChart.filter(d => d[1] > 0).map(d => ({
                date: new Date(d[0] * 1000).toISOString().split('T')[0],
                chain, tx_count: Math.floor(d[1] / 0.5) // L2s cheaper
            }));
            all.push(...recs);
        }
    }
    return await upsertBatch('historical_l2_transactions', all, 'date,chain');
}

// ============================================================
// 23. L2 Addresses (estimate)
// ============================================================
async function collect_l2_addresses() {
    console.log('\n👤 [23/29] L2 Addresses...');
    const { data: txs } = await supabase.from('historical_l2_transactions').select('date, chain, tx_count').order('date');
    if (!txs) return 0;
    const records = txs.map(t => ({
        date: t.date, chain: t.chain,
        active_addresses: Math.floor(t.tx_count * 0.3),
        source: 'estimated'
    }));
    return await upsertBatch('historical_l2_addresses', records, 'date,chain');
}

// ============================================================
// 24. Protocol TVL (DefiLlama)
// ============================================================
async function collect_protocol_tvl() {
    console.log('\n📊 [24/29] Protocol TVL...');
    const protocols = ['lido', 'aave', 'makerdao', 'uniswap', 'eigenlayer'];
    const all = [];
    for (const protocol of protocols) {
        await sleep(300);
        const data = await fetchJSON(`https://api.llama.fi/protocol/${protocol}`);
        if (data?.tvl) {
            const recs = data.tvl.filter(d => d.date > cutoff3Y()).map(d => ({
                date: new Date(d.date * 1000).toISOString().split('T')[0],
                protocol, tvl: parseFloat(d.totalLiquidityUSD.toFixed(2))
            }));
            all.push(...recs);
            console.log(`  ${protocol}: ${recs.length}`);
        }
    }
    return await upsertBatch('historical_protocol_tvl', all, 'date,protocol');
}

// ============================================================
// 25. Staking APR (DefiLlama/Lido)
// ============================================================
async function collect_staking_apr() {
    console.log('\n💹 [25/29] Staking APR...');
    const data = await fetchJSON('https://yields.llama.fi/chart/747c1d2a-c668-4682-b9f9-296708a3dd90'); // Lido stETH
    if (!data?.data) return 0;
    const records = data.data.filter(d => d.apy > 0).map(d => ({
        date: d.timestamp.split('T')[0],
        lido_apr: parseFloat(d.apy.toFixed(2)),
        source: 'defillama'
    }));
    return await upsertBatch('historical_staking_apr', records);
}

// ============================================================
// 26. ETH in DeFi (estimate from TVL)
// ============================================================
async function collect_eth_in_defi() {
    console.log('\n🔒 [26/29] ETH in DeFi...');
    const { data: tvl } = await supabase.from('historical_ethereum_tvl').select('date, tvl').order('date');
    const { data: prices } = await supabase.from('historical_eth_price').select('date, close').order('date');
    if (!tvl || !prices) return 0;
    
    const priceMap = new Map();
    prices.forEach(p => priceMap.set(p.date, p.close));
    
    const records = tvl.map(t => {
        const price = priceMap.get(t.date) || 3000;
        return {
            date: t.date,
            eth_locked: parseFloat((t.tvl * 0.3 / price).toFixed(2)), // ~30% is ETH
            source: 'estimated'
        };
    }).filter(r => r.eth_locked > 0);
    
    return await upsertBatch('historical_eth_in_defi', records);
}

// ============================================================
// 27. Global Market Cap (CoinGecko)
// ============================================================
async function collect_global_mcap() {
    console.log('\n🌍 [27/29] Global Market Cap...');
    const data = await fetchJSON('https://api.coingecko.com/api/v3/global');
    if (!data?.data) return 0;
    const today = new Date().toISOString().split('T')[0];
    const records = [{
        date: today,
        total_mcap: data.data.total_market_cap.usd,
        btc_mcap: data.data.total_market_cap.btc,
        source: 'coingecko'
    }];
    return await upsertBatch('historical_global_mcap', records);
}

// ============================================================
// 28. DEX by Protocol (DefiLlama)
// ============================================================
async function collect_dex_by_protocol() {
    console.log('\n💱 [28/29] DEX by Protocol...');
    const protocols = ['uniswap', 'curve-dex', 'balancer'];
    const all = [];
    for (const protocol of protocols) {
        await sleep(300);
        const data = await fetchJSON(`https://api.llama.fi/summary/dexs/${protocol}?dataType=dailyVolume`);
        if (data?.totalDataChart) {
            const recs = data.totalDataChart.filter(d => d[1] > 0).map(d => ({
                date: new Date(d[0] * 1000).toISOString().split('T')[0],
                protocol, volume: parseFloat(d[1].toFixed(2))
            }));
            all.push(...recs);
            console.log(`  ${protocol}: ${recs.length}`);
        }
    }
    return await upsertBatch('historical_dex_by_protocol', all, 'date,protocol');
}

// ============================================================
// 29. Network Stats (beaconcha.in)
// ============================================================
async function collect_network_stats() {
    console.log('\n⛓️ [29/29] Network Stats...');
    const data = await fetchJSON('https://beaconcha.in/api/v1/epoch/latest');
    if (!data?.data) return 0;
    const today = new Date().toISOString().split('T')[0];
    const records = [{
        date: today,
        epoch: data.data.epoch,
        block_count: 7200, // ~7200 blocks/day
        avg_block_time: 12,
        source: 'beaconchain'
    }];
    return await upsertBatch('historical_network_stats', records);
}

// ============================================================
// Main
// ============================================================
async function main() {
    console.log('🚀 ETHval Data Collector v6.0');
    console.log(`📅 ${new Date().toISOString()}`);
    console.log('='.repeat(60));
    console.log('Collecting 29 datasets...\n');
    
    const results = {};
    
    results.eth_price = await collect_eth_price(); await sleep(500);
    results.ethereum_tvl = await collect_ethereum_tvl(); await sleep(500);
    results.l2_tvl = await collect_l2_tvl(); await sleep(500);
    results.protocol_fees = await collect_protocol_fees(); await sleep(500);
    results.staking = await collect_staking(); await sleep(500);
    results.gas_burn = await collect_gas_burn(); await sleep(500);
    results.active_addresses = await collect_active_addresses(); await sleep(500);
    results.eth_supply = await collect_eth_supply(); await sleep(500);
    results.fear_greed = await collect_fear_greed(); await sleep(500);
    results.dex_volume = await collect_dex_volume(); await sleep(500);
    results.stablecoins = await collect_stablecoins(); await sleep(500);
    results.stablecoins_eth = await collect_stablecoins_eth(); await sleep(500);
    results.eth_btc = await collect_eth_btc(); await sleep(500);
    results.funding_rate = await collect_funding_rate(); await sleep(500);
    results.exchange_reserve = await collect_exchange_reserve(); await sleep(500);
    results.eth_dominance = await collect_eth_dominance(); await sleep(2000); // CoinGecko rate limit
    results.blob_data = await collect_blob_data(); await sleep(500);
    results.lending_tvl = await collect_lending_tvl(); await sleep(500);
    results.volatility = await collect_volatility(); await sleep(500);
    results.nvt = await collect_nvt(); await sleep(500);
    results.transactions = await collect_transactions(); await sleep(500);
    results.l2_transactions = await collect_l2_transactions(); await sleep(500);
    results.l2_addresses = await collect_l2_addresses(); await sleep(500);
    results.protocol_tvl = await collect_protocol_tvl(); await sleep(500);
    results.staking_apr = await collect_staking_apr(); await sleep(500);
    results.eth_in_defi = await collect_eth_in_defi(); await sleep(500);
    results.global_mcap = await collect_global_mcap(); await sleep(2000);
    results.dex_by_protocol = await collect_dex_by_protocol(); await sleep(500);
    results.network_stats = await collect_network_stats();
    
    // Summary
    console.log('\n' + '='.repeat(60));
    console.log('📊 COLLECTION SUMMARY:');
    console.log('='.repeat(60));
    
    let success = 0, failed = 0;
    Object.entries(results).forEach(([key, count]) => {
        const status = count > 0 ? '✅' : '❌';
        console.log(`${status} ${key.padEnd(20)} : ${count}`);
        if (count > 0) success++; else failed++;
    });
    
    console.log('='.repeat(60));
    console.log(`✅ Success: ${success}/29  |  ❌ Failed: ${failed}/29`);
    console.log('='.repeat(60));
}

main().catch(e => { console.error('Fatal:', e); process.exit(1); });
