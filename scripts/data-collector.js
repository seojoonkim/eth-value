/**
 * ETHval Data Collector v3.0
 * 
 * 수정사항:
 * - Etherscan API v2 대응
 * - NVT 계산 방식 변경 (Market Cap / Transaction Volume)
 * - CoinGecko → CryptoCompare로 변경 (ETH/BTC)
 * - L2 TVL source 컬럼 제거 (스키마 호환)
 */

const { createClient } = require('@supabase/supabase-js');

// Environment variables
const SUPABASE_URL = process.env.SUPABASE_URL;
const SUPABASE_KEY = process.env.SUPABASE_SERVICE_KEY;
const ETHERSCAN_API_KEY = process.env.ETHERSCAN_API_KEY || '';
const CRYPTOCOMPARE_API_KEY = process.env.CRYPTOCOMPARE_API_KEY || '';

const supabase = createClient(SUPABASE_URL, SUPABASE_KEY);

// Utility functions
const sleep = (ms) => new Promise(resolve => setTimeout(resolve, ms));

async function fetchWithRetry(url, options = {}, retries = 3) {
  for (let i = 0; i < retries; i++) {
    try {
      const response = await fetch(url, {
        ...options,
        headers: {
          'User-Agent': 'ETHval-DataCollector/3.0',
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
// 1. CoinMetrics CSV 데이터 수집
// NVT 직접 계산: Market Cap / Transaction Volume
// ============================================
async function collectCoinMetricsData() {
  console.log('\n📊 Collecting CoinMetrics data (single CSV download)...');
  
  try {
    const csvUrl = 'https://raw.githubusercontent.com/coinmetrics/data/master/csv/eth.csv';
    const response = await fetchWithRetry(csvUrl);
    const csvText = await response.text();
    
    const lines = csvText.trim().split('\n');
    const headers = lines[0].split(',');
    
    // 모든 컬럼 출력 (디버깅)
    console.log('Available columns:', headers.slice(0, 20).join(', '), '...');
    
    // 컬럼 인덱스 찾기
    const timeIdx = headers.indexOf('time');
    const priceIdx = headers.indexOf('PriceUSD');
    const capMrktIdx = headers.indexOf('CapMrktCurUSD');
    const txVolIdx = headers.indexOf('TxTfrValAdjUSD'); // 온체인 거래량
    const txVolNtvIdx = headers.indexOf('TxTfrValNtv'); // Native 거래량
    const splyCurIdx = headers.indexOf('SplyCur');
    
    // NVT 관련 컬럼 찾기 (여러 가능한 이름)
    const nvtIdx = headers.indexOf('NVTAdj');
    const nvt90Idx = headers.indexOf('NVTAdj90');
    const nvtAltIdx = headers.indexOf('NVT');
    
    console.log(`Columns found - time:${timeIdx}, price:${priceIdx}, mcap:${capMrktIdx}, txVol:${txVolIdx}, nvt:${nvtIdx}, nvt90:${nvt90Idx}`);
    console.log(`Total rows: ${lines.length - 1}`);
    
    // 최근 3년 필터링
    const threeYearsAgo = new Date();
    threeYearsAgo.setFullYear(threeYearsAgo.getFullYear() - 3);
    
    const nvtRecords = [];
    const priceRecords = [];
    
    for (let i = 1; i < lines.length; i++) {
      const cols = lines[i].split(',');
      const dateStr = cols[timeIdx];
      if (!dateStr) continue;
      
      const date = new Date(dateStr);
      if (date < threeYearsAgo) continue;
      
      const marketCap = parseFloat(cols[capMrktIdx]);
      const txVolume = parseFloat(cols[txVolIdx]) || parseFloat(cols[txVolNtvIdx]);
      const price = parseFloat(cols[priceIdx]);
      
      // NVT 계산: CSV에 있으면 사용, 없으면 직접 계산
      let nvtValue = parseFloat(cols[nvtIdx]) || parseFloat(cols[nvt90Idx]) || parseFloat(cols[nvtAltIdx]);
      
      // NVT가 없으면 직접 계산 (Market Cap / Daily Transaction Volume)
      if ((!nvtValue || isNaN(nvtValue)) && marketCap > 0 && txVolume > 0) {
        nvtValue = marketCap / txVolume;
      }
      
      // 유효한 NVT 범위 (10-500)
      if (nvtValue && nvtValue > 10 && nvtValue < 500) {
        nvtRecords.push({
          date: dateStr,
          nvt_ratio: Math.round(nvtValue * 100) / 100,
          market_cap: marketCap || null,
          transaction_volume: txVolume || null
        });
      }
      
      // 가격 데이터
      if (price && price > 0) {
        priceRecords.push({
          date: dateStr,
          price_usd: price,
          market_cap: marketCap || null,
          supply: parseFloat(cols[splyCurIdx]) || null
        });
      }
    }
    
    console.log(`Parsed ${nvtRecords.length} NVT records, ${priceRecords.length} price records`);
    
    // NVT 저장
    if (nvtRecords.length > 0) {
      for (let i = 0; i < nvtRecords.length; i += 500) {
        const batch = nvtRecords.slice(i, i + 500);
        const { error } = await supabase
          .from('historical_nvt')
          .upsert(batch, { onConflict: 'date' });
        
        if (error) {
          console.error('Error upserting NVT batch:', error.message);
        }
      }
      console.log(`✅ Saved ${nvtRecords.length} NVT records`);
    }
    
    return { nvtRecords, priceRecords };
  } catch (error) {
    console.error('❌ CoinMetrics collection failed:', error.message);
    return { nvtRecords: [], priceRecords: [] };
  }
}

// ============================================
// 2. Staking 데이터 (beaconcha.in)
// ============================================
async function collectStakingData() {
  console.log('\n🥩 Collecting Staking data (beaconcha.in)...');
  
  try {
    let stakingRecords = [];
    
    // 현재 epoch에서 데이터 가져오기
    try {
      const epochUrl = 'https://beaconcha.in/api/v1/epoch/latest';
      const response = await fetchWithRetry(epochUrl);
      const data = await response.json();
      
      if (data.status === 'OK' && data.data) {
        const validatorCount = data.data.validatorscount;
        const avgBalance = data.data.averagevalidatorbalance / 1e9;
        const totalStaked = validatorCount * avgBalance;
        
        stakingRecords.push({
          date: new Date().toISOString().split('T')[0],
          total_staked_eth: totalStaked,
          validator_count: validatorCount,
          staking_apr: null
        });
        
        console.log(`Current staking: ${(totalStaked / 1e6).toFixed(2)}M ETH, ${validatorCount.toLocaleString()} validators`);
      }
    } catch (e) {
      console.error('Epoch API failed:', e.message);
    }
    
    // ETH.STORE APR
    try {
      const ethstoreUrl = 'https://beaconcha.in/api/v1/ethstore/latest';
      const response = await fetchWithRetry(ethstoreUrl);
      const data = await response.json();
      
      if (data.status === 'OK' && data.data) {
        const apr = data.data.apr * 100;
        console.log(`Current staking APR: ${apr.toFixed(2)}%`);
        
        if (stakingRecords.length > 0) {
          stakingRecords[stakingRecords.length - 1].staking_apr = apr;
        }
      }
    } catch (e) {
      console.error('ETH.STORE API failed:', e.message);
    }
    
    // 저장
    if (stakingRecords.length > 0) {
      const { error } = await supabase
        .from('historical_staking')
        .upsert(stakingRecords, { onConflict: 'date' });
      
      if (error) {
        console.error('Error upserting staking data:', error.message);
      } else {
        console.log(`✅ Saved ${stakingRecords.length} staking records`);
      }
    }
    
    return stakingRecords;
  } catch (error) {
    console.error('❌ Staking collection failed:', error.message);
    return [];
  }
}

// ============================================
// 3. Daily Burn (Etherscan API v2)
// ============================================
async function collectBurnData() {
  console.log('\n🔥 Collecting Burn data (Etherscan v2)...');
  
  try {
    if (!ETHERSCAN_API_KEY) {
      console.log('⚠️ No Etherscan API key, skipping burn data');
      return [];
    }
    
    // Etherscan API v2 형식
    const url = `https://api.etherscan.io/v2/api?chainid=1&module=stats&action=ethsupply2&apikey=${ETHERSCAN_API_KEY}`;
    
    const response = await fetchWithRetry(url);
    const data = await response.json();
    
    if (data.status !== '1' || !data.result) {
      // v1 API 시도
      console.log('Trying Etherscan v1 API...');
      const urlV1 = `https://api.etherscan.io/api?module=stats&action=ethsupply2&apikey=${ETHERSCAN_API_KEY}`;
      const responseV1 = await fetchWithRetry(urlV1);
      const dataV1 = await responseV1.json();
      
      if (dataV1.status !== '1' || !dataV1.result) {
        throw new Error('Both Etherscan v1 and v2 APIs failed');
      }
      
      Object.assign(data, dataV1);
    }
    
    const currentBurntFees = parseFloat(data.result.BurntFees) / 1e18;
    const ethSupply = parseFloat(data.result.EthSupply) / 1e18;
    
    console.log(`Total burnt: ${currentBurntFees.toLocaleString()} ETH`);
    console.log(`ETH Supply: ${(ethSupply / 1e6).toFixed(2)}M`);
    
    const today = new Date().toISOString().split('T')[0];
    
    // 어제 데이터로 일일 burn 계산
    const { data: yesterdayData } = await supabase
      .from('historical_gas_burn')
      .select('cumulative_burn')
      .lt('date', today)
      .order('date', { ascending: false })
      .limit(1);
    
    let dailyBurn = null;
    if (yesterdayData && yesterdayData.length > 0 && yesterdayData[0].cumulative_burn) {
      dailyBurn = currentBurntFees - yesterdayData[0].cumulative_burn;
      if (dailyBurn < 0 || dailyBurn > 50000) dailyBurn = null; // 비정상 값 필터
    }
    
    const burnRecord = {
      date: today,
      eth_burnt: dailyBurn,
      cumulative_burn: currentBurntFees,
      avg_gas_price: null,
      total_transactions: null
    };
    
    if (dailyBurn) {
      console.log(`Daily burn: ${dailyBurn.toFixed(2)} ETH`);
    }
    
    const { error } = await supabase
      .from('historical_gas_burn')
      .upsert([burnRecord], { onConflict: 'date' });
    
    if (error) {
      console.error('Error upserting burn data:', error.message);
    } else {
      console.log(`✅ Saved burn record for ${today}`);
    }
    
    return [burnRecord];
  } catch (error) {
    console.error('❌ Burn collection failed:', error.message);
    return [];
  }
}

// ============================================
// 4. TVL (DefiLlama)
// ============================================
async function collectTVLData() {
  console.log('\n📈 Collecting TVL data (DefiLlama)...');
  
  try {
    const url = 'https://api.llama.fi/v2/historicalChainTvl/Ethereum';
    const response = await fetchWithRetry(url);
    const data = await response.json();
    
    if (!Array.isArray(data)) {
      throw new Error('Invalid response format');
    }
    
    const threeYearsAgo = Date.now() / 1000 - (3 * 365 * 24 * 60 * 60);
    
    const tvlRecords = data
      .filter(item => item.date > threeYearsAgo)
      .map(item => ({
        date: new Date(item.date * 1000).toISOString().split('T')[0],
        total_tvl: item.tvl
      }));
    
    console.log(`Found ${tvlRecords.length} TVL records`);
    
    for (let i = 0; i < tvlRecords.length; i += 500) {
      const batch = tvlRecords.slice(i, i + 500);
      const { error } = await supabase
        .from('historical_tvl')
        .upsert(batch, { onConflict: 'date' });
      
      if (error) {
        console.error('Error upserting TVL batch:', error.message);
      }
    }
    
    console.log(`✅ Saved ${tvlRecords.length} TVL records`);
    return tvlRecords;
  } catch (error) {
    console.error('❌ TVL collection failed:', error.message);
    return [];
  }
}

// ============================================
// 5. L2 TVL (DefiLlama) - source 컬럼 제거
// ============================================
async function collectL2TVLData() {
  console.log('\n🔗 Collecting L2 TVL data (DefiLlama)...');
  
  const l2Chains = ['Arbitrum', 'Optimism', 'Base', 'zkSync Era', 'Linea', 'Scroll', 'Blast'];
  const allRecords = [];
  
  for (const chain of l2Chains) {
    try {
      const url = `https://api.llama.fi/v2/historicalChainTvl/${encodeURIComponent(chain)}`;
      const response = await fetchWithRetry(url);
      const data = await response.json();
      
      if (!Array.isArray(data)) continue;
      
      const threeYearsAgo = Date.now() / 1000 - (3 * 365 * 24 * 60 * 60);
      
      const records = data
        .filter(item => item.date > threeYearsAgo)
        .map(item => ({
          date: new Date(item.date * 1000).toISOString().split('T')[0],
          chain: chain.toLowerCase().replace(' ', '_'),
          tvl: item.tvl
        }));
      
      allRecords.push(...records);
      console.log(`  ${chain}: ${records.length} records`);
      
      await sleep(200);
    } catch (error) {
      console.error(`  ${chain} failed:`, error.message);
    }
  }
  
  // 날짜별 그룹핑
  const dateMap = new Map();
  for (const record of allRecords) {
    if (!dateMap.has(record.date)) {
      dateMap.set(record.date, { date: record.date, chains: {}, total: 0 });
    }
    const entry = dateMap.get(record.date);
    entry.chains[record.chain] = record.tvl;
    entry.total += record.tvl;
  }
  
  // source 컬럼 없이 저장
  const l2Records = Array.from(dateMap.values()).map(entry => ({
    date: entry.date,
    total_l2_tvl: entry.total,
    arbitrum_tvl: entry.chains['arbitrum'] || 0,
    optimism_tvl: entry.chains['optimism'] || 0,
    base_tvl: entry.chains['base'] || 0,
    zksync_tvl: entry.chains['zksync_era'] || 0
  }));
  
  for (let i = 0; i < l2Records.length; i += 500) {
    const batch = l2Records.slice(i, i + 500);
    const { error } = await supabase
      .from('historical_l2_tvl')
      .upsert(batch, { onConflict: 'date' });
    
    if (error) {
      console.error('Error upserting L2 TVL batch:', error.message);
    }
  }
  
  console.log(`✅ Saved ${l2Records.length} L2 TVL records`);
  return l2Records;
}

// ============================================
// 6. Fees (DefiLlama)
// ============================================
async function collectFeesData() {
  console.log('\n💰 Collecting Fees data (DefiLlama)...');
  
  try {
    const url = 'https://api.llama.fi/summary/fees/ethereum?dataType=dailyFees';
    const response = await fetchWithRetry(url);
    const data = await response.json();
    
    if (!data.totalDataChart || !Array.isArray(data.totalDataChart)) {
      throw new Error('Invalid response format');
    }
    
    const feesRecords = data.totalDataChart.map(([timestamp, fees]) => ({
      date: new Date(timestamp * 1000).toISOString().split('T')[0],
      daily_fees_usd: fees
    }));
    
    console.log(`Found ${feesRecords.length} fees records`);
    
    for (let i = 0; i < feesRecords.length; i += 500) {
      const batch = feesRecords.slice(i, i + 500);
      const { error } = await supabase
        .from('historical_fees')
        .upsert(batch, { onConflict: 'date' });
      
      if (error) {
        console.error('Error upserting fees batch:', error.message);
      }
    }
    
    console.log(`✅ Saved ${feesRecords.length} fees records`);
    return feesRecords;
  } catch (error) {
    console.error('❌ Fees collection failed:', error.message);
    return [];
  }
}

// ============================================
// 7. DEX Volume (DefiLlama)
// ============================================
async function collectDEXVolumeData() {
  console.log('\n📊 Collecting DEX Volume data (DefiLlama)...');
  
  try {
    const url = 'https://api.llama.fi/overview/dexs/ethereum?excludeTotalDataChart=false&excludeTotalDataChartBreakdown=true&dataType=dailyVolume';
    const response = await fetchWithRetry(url);
    const data = await response.json();
    
    if (!data.totalDataChart || !Array.isArray(data.totalDataChart)) {
      throw new Error('Invalid response format');
    }
    
    const volumeRecords = data.totalDataChart.map(([timestamp, volume]) => ({
      date: new Date(timestamp * 1000).toISOString().split('T')[0],
      daily_volume_usd: volume
    }));
    
    console.log(`Found ${volumeRecords.length} DEX volume records`);
    
    for (let i = 0; i < volumeRecords.length; i += 500) {
      const batch = volumeRecords.slice(i, i + 500);
      const { error } = await supabase
        .from('historical_dex_volume')
        .upsert(batch, { onConflict: 'date' });
      
      if (error) {
        console.error('Error upserting DEX volume batch:', error.message);
      }
    }
    
    console.log(`✅ Saved ${volumeRecords.length} DEX volume records`);
    return volumeRecords;
  } catch (error) {
    console.error('❌ DEX Volume collection failed:', error.message);
    return [];
  }
}

// ============================================
// 8. Fear & Greed (Alternative.me)
// ============================================
async function collectFearGreedData() {
  console.log('\n😱 Collecting Fear & Greed data (Alternative.me)...');
  
  try {
    const url = 'https://api.alternative.me/fng/?limit=1095&format=json';
    const response = await fetchWithRetry(url);
    const data = await response.json();
    
    if (!data.data || !Array.isArray(data.data)) {
      throw new Error('Invalid response format');
    }
    
    const fgRecords = data.data.map(item => ({
      date: new Date(parseInt(item.timestamp) * 1000).toISOString().split('T')[0],
      fear_greed_index: parseInt(item.value),
      classification: item.value_classification
    }));
    
    console.log(`Found ${fgRecords.length} Fear & Greed records`);
    
    for (let i = 0; i < fgRecords.length; i += 500) {
      const batch = fgRecords.slice(i, i + 500);
      const { error } = await supabase
        .from('historical_fear_greed')
        .upsert(batch, { onConflict: 'date' });
      
      if (error) {
        console.error('Error upserting Fear & Greed batch:', error.message);
      }
    }
    
    console.log(`✅ Saved ${fgRecords.length} Fear & Greed records`);
    return fgRecords;
  } catch (error) {
    console.error('❌ Fear & Greed collection failed:', error.message);
    return [];
  }
}

// ============================================
// 9. Stablecoins (DefiLlama)
// ============================================
async function collectStablecoinData() {
  console.log('\n💵 Collecting Stablecoin data (DefiLlama)...');
  
  try {
    const url = 'https://stablecoins.llama.fi/stablecoincharts/ethereum';
    const response = await fetchWithRetry(url);
    const data = await response.json();
    
    if (!Array.isArray(data)) {
      throw new Error('Invalid response format');
    }
    
    const stablecoinRecords = data.map(item => {
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
        total_stablecoin_mcap: totalMcap
      };
    });
    
    console.log(`Found ${stablecoinRecords.length} stablecoin records`);
    
    for (let i = 0; i < stablecoinRecords.length; i += 500) {
      const batch = stablecoinRecords.slice(i, i + 500);
      const { error } = await supabase
        .from('historical_stablecoins')
        .upsert(batch, { onConflict: 'date' });
      
      if (error) {
        console.error('Error upserting stablecoin batch:', error.message);
      }
    }
    
    console.log(`✅ Saved ${stablecoinRecords.length} stablecoin records`);
    return stablecoinRecords;
  } catch (error) {
    console.error('❌ Stablecoin collection failed:', error.message);
    return [];
  }
}

// ============================================
// 10. ETH/BTC Ratio (CryptoCompare) - CoinGecko 대체
// ============================================
async function collectETHBTCRatio() {
  console.log('\n📉 Collecting ETH/BTC Ratio (CryptoCompare)...');
  
  try {
    // CryptoCompare daily historical data (최대 2000일)
    const apiKey = CRYPTOCOMPARE_API_KEY ? `&api_key=${CRYPTOCOMPARE_API_KEY}` : '';
    const url = `https://min-api.cryptocompare.com/data/v2/histoday?fsym=ETH&tsym=BTC&limit=1095${apiKey}`;
    
    const response = await fetchWithRetry(url);
    const data = await response.json();
    
    if (data.Response !== 'Success' || !data.Data || !data.Data.Data) {
      throw new Error('Invalid response from CryptoCompare');
    }
    
    const ratioRecords = data.Data.Data.map(item => ({
      date: new Date(item.time * 1000).toISOString().split('T')[0],
      eth_btc_ratio: item.close
    }));
    
    console.log(`Found ${ratioRecords.length} ETH/BTC ratio records`);
    
    for (let i = 0; i < ratioRecords.length; i += 500) {
      const batch = ratioRecords.slice(i, i + 500);
      const { error } = await supabase
        .from('historical_eth_btc')
        .upsert(batch, { onConflict: 'date' });
      
      if (error) {
        console.error('Error upserting ETH/BTC batch:', error.message);
      }
    }
    
    console.log(`✅ Saved ${ratioRecords.length} ETH/BTC ratio records`);
    return ratioRecords;
  } catch (error) {
    console.error('❌ ETH/BTC Ratio collection failed:', error.message);
    return [];
  }
}

// ============================================
// Main execution
// ============================================
async function main() {
  console.log('🚀 ETHval Data Collector v3.0 Starting...');
  console.log(`📅 ${new Date().toISOString()}`);
  console.log('='.repeat(50));
  
  const results = {
    coinmetrics: null,
    staking: null,
    burn: null,
    tvl: null,
    l2tvl: null,
    fees: null,
    dexVolume: null,
    fearGreed: null,
    stablecoins: null,
    ethBtc: null
  };
  
  try {
    // 1. CoinMetrics (NVT 직접 계산)
    results.coinmetrics = await collectCoinMetricsData();
    await sleep(1000);
    
    // 2. Staking
    results.staking = await collectStakingData();
    await sleep(1000);
    
    // 3. Burn (Etherscan v2)
    results.burn = await collectBurnData();
    await sleep(1000);
    
    // 4. TVL
    results.tvl = await collectTVLData();
    await sleep(1000);
    
    // 5. L2 TVL
    results.l2tvl = await collectL2TVLData();
    await sleep(1000);
    
    // 6. Fees
    results.fees = await collectFeesData();
    await sleep(1000);
    
    // 7. DEX Volume
    results.dexVolume = await collectDEXVolumeData();
    await sleep(1000);
    
    // 8. Fear & Greed
    results.fearGreed = await collectFearGreedData();
    await sleep(1000);
    
    // 9. Stablecoins
    results.stablecoins = await collectStablecoinData();
    await sleep(1000);
    
    // 10. ETH/BTC (CryptoCompare)
    results.ethBtc = await collectETHBTCRatio();
    
  } catch (error) {
    console.error('\n❌ Critical error:', error.message);
  }
  
  // Summary
  console.log('\n' + '='.repeat(50));
  console.log('📊 Collection Summary:');
  console.log(`  CoinMetrics (NVT): ${results.coinmetrics?.nvtRecords?.length || 0} records`);
  console.log(`  Staking: ${results.staking?.length || 0} records`);
  console.log(`  Burn: ${results.burn?.length || 0} records`);
  console.log(`  TVL: ${results.tvl?.length || 0} records`);
  console.log(`  L2 TVL: ${results.l2tvl?.length || 0} records`);
  console.log(`  Fees: ${results.fees?.length || 0} records`);
  console.log(`  DEX Volume: ${results.dexVolume?.length || 0} records`);
  console.log(`  Fear & Greed: ${results.fearGreed?.length || 0} records`);
  console.log(`  Stablecoins: ${results.stablecoins?.length || 0} records`);
  console.log(`  ETH/BTC: ${results.ethBtc?.length || 0} records`);
  console.log('='.repeat(50));
  console.log('✅ Data collection completed!');
}

main().catch(console.error);