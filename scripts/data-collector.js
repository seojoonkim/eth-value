/**
 * ETHval Data Collector v4.0
 * 
 * 대시보드 기대 테이블/컬럼명에 맞춤:
 * - historical_ethereum_tvl (date, tvl)
 * - historical_protocol_fees (date, fees)
 * - historical_fear_greed (date, value, classification)
 * - historical_eth_btc (date, ratio)
 * - historical_dex_volume (date, volume)
 * - historical_stablecoins (date, total_mcap)
 * - historical_staking (date, total_staked_eth, avg_apr, total_validators)
 * - historical_gas_burn (date, eth_burnt, avg_gas_price_gwei, transaction_count)
 * - historical_nvt (date, nvt_ratio)
 * - historical_l2_tvl (date, chain, tvl)
 */

const { createClient } = require('@supabase/supabase-js');

const SUPABASE_URL = process.env.SUPABASE_URL;
const SUPABASE_KEY = process.env.SUPABASE_SERVICE_KEY;
const ETHERSCAN_API_KEY = process.env.ETHERSCAN_API_KEY || '';
const CRYPTOCOMPARE_API_KEY = process.env.CRYPTOCOMPARE_API_KEY || '';

const supabase = createClient(SUPABASE_URL, SUPABASE_KEY);

const sleep = (ms) => new Promise(resolve => setTimeout(resolve, ms));

async function fetchWithRetry(url, options = {}, retries = 3) {
  for (let i = 0; i < retries; i++) {
    try {
      const response = await fetch(url, {
        ...options,
        headers: {
          'User-Agent': 'ETHval-DataCollector/4.0',
          ...options.headers
        }
      });
      if (!response.ok) {
        throw new Error(`HTTP ${response.status}: ${response.statusText}`);
      }
      return response;
    } catch (error) {
      console.error(`Attempt ${i + 1} failed for ${url}:`, error.message);
      if (i < retries - 1) await sleep(2000 * (i + 1));
    }
  }
  throw new Error(`Failed to fetch ${url} after ${retries} attempts`);
}

// ============================================
// 1. TVL → historical_ethereum_tvl (date, tvl)
// ============================================
async function collectTVLData() {
  console.log('\n📈 Collecting TVL data → historical_ethereum_tvl...');
  
  try {
    const url = 'https://api.llama.fi/v2/historicalChainTvl/Ethereum';
    const response = await fetchWithRetry(url);
    const data = await response.json();
    
    if (!Array.isArray(data)) {
      throw new Error('Invalid response format');
    }
    
    const threeYearsAgo = Date.now() / 1000 - (3 * 365 * 24 * 60 * 60);
    
    const records = data
      .filter(item => item.date > threeYearsAgo)
      .map(item => ({
        date: new Date(item.date * 1000).toISOString().split('T')[0],
        tvl: item.tvl
      }));
    
    console.log(`Found ${records.length} TVL records`);
    
    for (let i = 0; i < records.length; i += 500) {
      const batch = records.slice(i, i + 500);
      const { error } = await supabase
        .from('historical_ethereum_tvl')
        .upsert(batch, { onConflict: 'date' });
      
      if (error) console.error('Error upserting TVL:', error.message);
    }
    
    console.log(`✅ Saved ${records.length} TVL records`);
    return records;
  } catch (error) {
    console.error('❌ TVL collection failed:', error.message);
    return [];
  }
}

// ============================================
// 2. Fees → historical_protocol_fees (date, fees)
// ============================================
async function collectFeesData() {
  console.log('\n💰 Collecting Fees data → historical_protocol_fees...');
  
  try {
    const url = 'https://api.llama.fi/summary/fees/ethereum?dataType=dailyFees';
    const response = await fetchWithRetry(url);
    const data = await response.json();
    
    if (!data.totalDataChart || !Array.isArray(data.totalDataChart)) {
      throw new Error('Invalid response format');
    }
    
    const records = data.totalDataChart.map(([timestamp, fees]) => ({
      date: new Date(timestamp * 1000).toISOString().split('T')[0],
      fees: fees
    }));
    
    console.log(`Found ${records.length} fees records`);
    
    for (let i = 0; i < records.length; i += 500) {
      const batch = records.slice(i, i + 500);
      const { error } = await supabase
        .from('historical_protocol_fees')
        .upsert(batch, { onConflict: 'date' });
      
      if (error) console.error('Error upserting fees:', error.message);
    }
    
    console.log(`✅ Saved ${records.length} fees records`);
    return records;
  } catch (error) {
    console.error('❌ Fees collection failed:', error.message);
    return [];
  }
}

// ============================================
// 3. Fear & Greed → historical_fear_greed (date, value, classification)
// ============================================
async function collectFearGreedData() {
  console.log('\n😱 Collecting Fear & Greed → historical_fear_greed...');
  
  try {
    const url = 'https://api.alternative.me/fng/?limit=1095&format=json';
    const response = await fetchWithRetry(url);
    const data = await response.json();
    
    if (!data.data || !Array.isArray(data.data)) {
      throw new Error('Invalid response format');
    }
    
    const records = data.data.map(item => ({
      date: new Date(parseInt(item.timestamp) * 1000).toISOString().split('T')[0],
      value: parseInt(item.value),
      classification: item.value_classification
    }));
    
    console.log(`Found ${records.length} Fear & Greed records`);
    
    for (let i = 0; i < records.length; i += 500) {
      const batch = records.slice(i, i + 500);
      const { error } = await supabase
        .from('historical_fear_greed')
        .upsert(batch, { onConflict: 'date' });
      
      if (error) console.error('Error upserting Fear & Greed:', error.message);
    }
    
    console.log(`✅ Saved ${records.length} Fear & Greed records`);
    return records;
  } catch (error) {
    console.error('❌ Fear & Greed collection failed:', error.message);
    return [];
  }
}

// ============================================
// 4. ETH/BTC → historical_eth_btc (date, ratio)
// ============================================
async function collectETHBTCRatio() {
  console.log('\n📉 Collecting ETH/BTC → historical_eth_btc...');
  
  try {
    const apiKey = CRYPTOCOMPARE_API_KEY ? `&api_key=${CRYPTOCOMPARE_API_KEY}` : '';
    const url = `https://min-api.cryptocompare.com/data/v2/histoday?fsym=ETH&tsym=BTC&limit=1095${apiKey}`;
    
    const response = await fetchWithRetry(url);
    const data = await response.json();
    
    if (data.Response !== 'Success' || !data.Data || !data.Data.Data) {
      throw new Error('Invalid response from CryptoCompare');
    }
    
    const records = data.Data.Data.map(item => ({
      date: new Date(item.time * 1000).toISOString().split('T')[0],
      ratio: item.close
    }));
    
    console.log(`Found ${records.length} ETH/BTC records`);
    
    for (let i = 0; i < records.length; i += 500) {
      const batch = records.slice(i, i + 500);
      const { error } = await supabase
        .from('historical_eth_btc')
        .upsert(batch, { onConflict: 'date' });
      
      if (error) console.error('Error upserting ETH/BTC:', error.message);
    }
    
    console.log(`✅ Saved ${records.length} ETH/BTC records`);
    return records;
  } catch (error) {
    console.error('❌ ETH/BTC collection failed:', error.message);
    return [];
  }
}

// ============================================
// 5. DEX Volume → historical_dex_volume (date, volume)
// ============================================
async function collectDEXVolumeData() {
  console.log('\n📊 Collecting DEX Volume → historical_dex_volume...');
  
  try {
    const url = 'https://api.llama.fi/overview/dexs/ethereum?excludeTotalDataChart=false&excludeTotalDataChartBreakdown=true&dataType=dailyVolume';
    const response = await fetchWithRetry(url);
    const data = await response.json();
    
    if (!data.totalDataChart || !Array.isArray(data.totalDataChart)) {
      throw new Error('Invalid response format');
    }
    
    const records = data.totalDataChart.map(([timestamp, volume]) => ({
      date: new Date(timestamp * 1000).toISOString().split('T')[0],
      volume: volume
    }));
    
    console.log(`Found ${records.length} DEX volume records`);
    
    for (let i = 0; i < records.length; i += 500) {
      const batch = records.slice(i, i + 500);
      const { error } = await supabase
        .from('historical_dex_volume')
        .upsert(batch, { onConflict: 'date' });
      
      if (error) console.error('Error upserting DEX volume:', error.message);
    }
    
    console.log(`✅ Saved ${records.length} DEX volume records`);
    return records;
  } catch (error) {
    console.error('❌ DEX Volume collection failed:', error.message);
    return [];
  }
}

// ============================================
// 6. Stablecoins → historical_stablecoins (date, total_mcap)
// ============================================
async function collectStablecoinData() {
  console.log('\n💵 Collecting Stablecoins → historical_stablecoins...');
  
  try {
    const url = 'https://stablecoins.llama.fi/stablecoincharts/ethereum';
    const response = await fetchWithRetry(url);
    const data = await response.json();
    
    if (!Array.isArray(data)) {
      throw new Error('Invalid response format');
    }
    
    const records = data.map(item => {
      let totalMcap = 0;
      if (item.totalCirculating) {
        for (const val of Object.values(item.totalCirculating)) {
          if (val && val.peggedUSD) {
            totalMcap += val.peggedUSD;
          }
        }
      }
      return {
        date: new Date(item.date * 1000).toISOString().split('T')[0],
        total_mcap: totalMcap
      };
    });
    
    console.log(`Found ${records.length} stablecoin records`);
    
    for (let i = 0; i < records.length; i += 500) {
      const batch = records.slice(i, i + 500);
      const { error } = await supabase
        .from('historical_stablecoins')
        .upsert(batch, { onConflict: 'date' });
      
      if (error) console.error('Error upserting stablecoins:', error.message);
    }
    
    console.log(`✅ Saved ${records.length} stablecoin records`);
    return records;
  } catch (error) {
    console.error('❌ Stablecoin collection failed:', error.message);
    return [];
  }
}

// ============================================
// 7. Staking → historical_staking (date, total_staked_eth, avg_apr, total_validators)
// ============================================
async function collectStakingData() {
  console.log('\n🥩 Collecting Staking → historical_staking...');
  
  try {
    let record = {
      date: new Date().toISOString().split('T')[0],
      total_staked_eth: null,
      avg_apr: null,
      total_validators: null
    };
    
    // Get current epoch data
    try {
      const epochUrl = 'https://beaconcha.in/api/v1/epoch/latest';
      const response = await fetchWithRetry(epochUrl);
      const data = await response.json();
      
      if (data.status === 'OK' && data.data) {
        const validatorCount = data.data.validatorscount;
        const avgBalance = data.data.averagevalidatorbalance / 1e9;
        const totalStaked = validatorCount * avgBalance;
        
        record.total_staked_eth = totalStaked;
        record.total_validators = validatorCount;
        
        console.log(`Current staking: ${(totalStaked / 1e6).toFixed(2)}M ETH, ${validatorCount.toLocaleString()} validators`);
      }
    } catch (e) {
      console.error('Epoch API failed:', e.message);
    }
    
    // Get ETH.STORE APR
    try {
      const ethstoreUrl = 'https://beaconcha.in/api/v1/ethstore/latest';
      const response = await fetchWithRetry(ethstoreUrl);
      const data = await response.json();
      
      if (data.status === 'OK' && data.data) {
        record.avg_apr = data.data.apr * 100;
        console.log(`Current staking APR: ${record.avg_apr.toFixed(2)}%`);
      }
    } catch (e) {
      console.error('ETH.STORE API failed:', e.message);
    }
    
    if (record.total_staked_eth) {
      const { error } = await supabase
        .from('historical_staking')
        .upsert([record], { onConflict: 'date' });
      
      if (error) {
        console.error('Error upserting staking:', error.message);
      } else {
        console.log(`✅ Saved staking record for ${record.date}`);
      }
    }
    
    return [record];
  } catch (error) {
    console.error('❌ Staking collection failed:', error.message);
    return [];
  }
}

// ============================================
// 8. Burn → historical_gas_burn (date, eth_burnt, avg_gas_price_gwei, transaction_count)
// ============================================
async function collectBurnData() {
  console.log('\n🔥 Collecting Burn → historical_gas_burn...');
  
  try {
    if (!ETHERSCAN_API_KEY) {
      console.log('⚠️ No Etherscan API key, skipping burn data');
      return [];
    }
    
    // Try v1 API first (more reliable)
    const url = `https://api.etherscan.io/api?module=stats&action=ethsupply2&apikey=${ETHERSCAN_API_KEY}`;
    const response = await fetchWithRetry(url);
    const data = await response.json();
    
    if (data.status !== '1' || !data.result) {
      throw new Error('Etherscan API failed');
    }
    
    const currentBurntFees = parseFloat(data.result.BurntFees) / 1e18;
    console.log(`Total burnt: ${currentBurntFees.toLocaleString()} ETH`);
    
    const today = new Date().toISOString().split('T')[0];
    
    // Calculate daily burn from yesterday
    const { data: yesterdayData } = await supabase
      .from('historical_gas_burn')
      .select('eth_burnt')
      .lt('date', today)
      .order('date', { ascending: false })
      .limit(1);
    
    // For cumulative tracking, we store total burnt and calculate daily diff in dashboard
    const record = {
      date: today,
      eth_burnt: currentBurntFees,  // This is cumulative
      avg_gas_price_gwei: null,
      transaction_count: null
    };
    
    const { error } = await supabase
      .from('historical_gas_burn')
      .upsert([record], { onConflict: 'date' });
    
    if (error) {
      console.error('Error upserting burn:', error.message);
    } else {
      console.log(`✅ Saved burn record for ${today}`);
    }
    
    return [record];
  } catch (error) {
    console.error('❌ Burn collection failed:', error.message);
    return [];
  }
}

// ============================================
// 9. NVT → historical_nvt (date, nvt_ratio)
// ============================================
async function collectNVTData() {
  console.log('\n📊 Collecting NVT → historical_nvt...');
  
  try {
    const csvUrl = 'https://raw.githubusercontent.com/coinmetrics/data/master/csv/eth.csv';
    const response = await fetchWithRetry(csvUrl);
    const csvText = await response.text();
    
    const lines = csvText.trim().split('\n');
    const headers = lines[0].split(',');
    
    const timeIdx = headers.indexOf('time');
    const capMrktIdx = headers.indexOf('CapMrktCurUSD');
    const txVolIdx = headers.indexOf('TxTfrValAdjUSD');
    const txVolNtvIdx = headers.indexOf('TxTfrValNtv');
    const priceIdx = headers.indexOf('PriceUSD');
    
    // Try to find NVT columns
    const nvtIdx = headers.indexOf('NVTAdj');
    const nvt90Idx = headers.indexOf('NVTAdj90');
    
    console.log(`Columns: time=${timeIdx}, mcap=${capMrktIdx}, txVol=${txVolIdx}, nvt=${nvtIdx}`);
    
    const threeYearsAgo = new Date();
    threeYearsAgo.setFullYear(threeYearsAgo.getFullYear() - 3);
    
    const records = [];
    
    for (let i = 1; i < lines.length; i++) {
      const cols = lines[i].split(',');
      const dateStr = cols[timeIdx];
      if (!dateStr) continue;
      
      const date = new Date(dateStr);
      if (date < threeYearsAgo) continue;
      
      const marketCap = parseFloat(cols[capMrktIdx]);
      const txVolume = parseFloat(cols[txVolIdx]) || parseFloat(cols[txVolNtvIdx]) * parseFloat(cols[priceIdx]);
      
      // Try existing NVT column first, then calculate
      let nvtValue = parseFloat(cols[nvtIdx]) || parseFloat(cols[nvt90Idx]);
      
      if ((!nvtValue || isNaN(nvtValue)) && marketCap > 0 && txVolume > 0) {
        nvtValue = marketCap / txVolume;
      }
      
      // Valid NVT range
      if (nvtValue && nvtValue > 5 && nvtValue < 1000) {
        records.push({
          date: dateStr,
          nvt_ratio: Math.round(nvtValue * 100) / 100
        });
      }
    }
    
    console.log(`Found ${records.length} NVT records`);
    
    for (let i = 0; i < records.length; i += 500) {
      const batch = records.slice(i, i + 500);
      const { error } = await supabase
        .from('historical_nvt')
        .upsert(batch, { onConflict: 'date' });
      
      if (error) console.error('Error upserting NVT:', error.message);
    }
    
    console.log(`✅ Saved ${records.length} NVT records`);
    return records;
  } catch (error) {
    console.error('❌ NVT collection failed:', error.message);
    return [];
  }
}

// ============================================
// 10. L2 TVL → historical_l2_tvl (date, chain, tvl)
// ============================================
async function collectL2TVLData() {
  console.log('\n🔗 Collecting L2 TVL → historical_l2_tvl...');
  
  const l2Chains = [
    { name: 'Arbitrum', key: 'arbitrum' },
    { name: 'Optimism', key: 'optimism' },
    { name: 'Base', key: 'base' },
    { name: 'zkSync Era', key: 'zksync' },
    { name: 'Linea', key: 'linea' },
    { name: 'Scroll', key: 'scroll' },
    { name: 'Blast', key: 'blast' }
  ];
  
  const allRecords = [];
  
  for (const chain of l2Chains) {
    try {
      const url = `https://api.llama.fi/v2/historicalChainTvl/${encodeURIComponent(chain.name)}`;
      const response = await fetchWithRetry(url);
      const data = await response.json();
      
      if (!Array.isArray(data)) continue;
      
      const threeYearsAgo = Date.now() / 1000 - (3 * 365 * 24 * 60 * 60);
      
      const records = data
        .filter(item => item.date > threeYearsAgo)
        .map(item => ({
          date: new Date(item.date * 1000).toISOString().split('T')[0],
          chain: chain.key,
          tvl: item.tvl
        }));
      
      allRecords.push(...records);
      console.log(`  ${chain.name}: ${records.length} records`);
      
      await sleep(200);
    } catch (error) {
      console.error(`  ${chain.name} failed:`, error.message);
    }
  }
  
  console.log(`Total L2 records: ${allRecords.length}`);
  
  // L2 TVL uses composite key (date, chain)
  for (let i = 0; i < allRecords.length; i += 500) {
    const batch = allRecords.slice(i, i + 500);
    const { error } = await supabase
      .from('historical_l2_tvl')
      .upsert(batch, { onConflict: 'date,chain' });
    
    if (error) console.error('Error upserting L2 TVL:', error.message);
  }
  
  console.log(`✅ Saved ${allRecords.length} L2 TVL records`);
  return allRecords;
}

// ============================================
// Main
// ============================================
async function main() {
  console.log('🚀 ETHval Data Collector v4.0 Starting...');
  console.log(`📅 ${new Date().toISOString()}`);
  console.log('='.repeat(50));
  
  const results = {};
  
  try {
    results.tvl = await collectTVLData();
    await sleep(1000);
    
    results.fees = await collectFeesData();
    await sleep(1000);
    
    results.fearGreed = await collectFearGreedData();
    await sleep(1000);
    
    results.ethBtc = await collectETHBTCRatio();
    await sleep(1000);
    
    results.dexVolume = await collectDEXVolumeData();
    await sleep(1000);
    
    results.stablecoins = await collectStablecoinData();
    await sleep(1000);
    
    results.staking = await collectStakingData();
    await sleep(1000);
    
    results.burn = await collectBurnData();
    await sleep(1000);
    
    results.nvt = await collectNVTData();
    await sleep(1000);
    
    results.l2tvl = await collectL2TVLData();
    
  } catch (error) {
    console.error('\n❌ Critical error:', error.message);
  }
  
  console.log('\n' + '='.repeat(50));
  console.log('📊 Collection Summary:');
  console.log(`  TVL (historical_ethereum_tvl): ${results.tvl?.length || 0}`);
  console.log(`  Fees (historical_protocol_fees): ${results.fees?.length || 0}`);
  console.log(`  Fear & Greed: ${results.fearGreed?.length || 0}`);
  console.log(`  ETH/BTC: ${results.ethBtc?.length || 0}`);
  console.log(`  DEX Volume: ${results.dexVolume?.length || 0}`);
  console.log(`  Stablecoins: ${results.stablecoins?.length || 0}`);
  console.log(`  Staking: ${results.staking?.length || 0}`);
  console.log(`  Burn: ${results.burn?.length || 0}`);
  console.log(`  NVT: ${results.nvt?.length || 0}`);
  console.log(`  L2 TVL: ${results.l2tvl?.length || 0}`);
  console.log('='.repeat(50));
  console.log('✅ Data collection completed!');
}

main().catch(console.error);
