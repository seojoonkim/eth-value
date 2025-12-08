/**
 * ETHval Data Collector v2.0
 * 
 * 핵심 원칙: Rate limit 문제 해결
 * - 초기 데이터: CoinMetrics GitHub CSV에서 한 번에 다운로드 (전체 히스토리)
 * - 일일 업데이트: 각 API 1회씩만 호출하여 증분 추가
 * 
 * 데이터 소스:
 * - NVT, 가격, 온체인 데이터: CoinMetrics Community CSV
 * - Staked ETH: beaconcha.in 차트 데이터
 * - Staking APR: beaconcha.in ETH.STORE
 * - Daily Burn: Etherscan ethsupply2 누적 차이
 * - TVL, DEX Volume, Fees: DefiLlama (한 번 호출로 전체 반환)
 * - Fear & Greed: Alternative.me
 */

const { createClient } = require('@supabase/supabase-js');

// Environment variables
const SUPABASE_URL = process.env.SUPABASE_URL;
const SUPABASE_KEY = process.env.SUPABASE_SERVICE_KEY;
const ETHERSCAN_API_KEY = process.env.ETHERSCAN_API_KEY || '';

const supabase = createClient(SUPABASE_URL, SUPABASE_KEY);

// Utility functions
const sleep = (ms) => new Promise(resolve => setTimeout(resolve, ms));

async function fetchWithRetry(url, options = {}, retries = 3) {
  for (let i = 0; i < retries; i++) {
    try {
      const response = await fetch(url, {
        ...options,
        headers: {
          'User-Agent': 'ETHval-DataCollector/2.0',
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
// 한 번의 다운로드로 전체 히스토리 획득
// ============================================
async function collectCoinMetricsData() {
  console.log('\n📊 Collecting CoinMetrics data (single CSV download)...');
  
  try {
    // GitHub raw에서 ETH CSV 다운로드
    const csvUrl = 'https://raw.githubusercontent.com/coinmetrics/data/master/csv/eth.csv';
    const response = await fetchWithRetry(csvUrl);
    const csvText = await response.text();
    
    // CSV 파싱
    const lines = csvText.trim().split('\n');
    const headers = lines[0].split(',');
    
    // 필요한 컬럼 인덱스 찾기
    const timeIdx = headers.indexOf('time');
    const priceIdx = headers.indexOf('PriceUSD');
    const nvtIdx = headers.indexOf('NVTAdj');
    const nvt90Idx = headers.indexOf('NVTAdj90');
    const capMrktIdx = headers.indexOf('CapMrktCurUSD');
    const txVolIdx = headers.indexOf('TxTfrValAdjUSD');
    const activeAddrIdx = headers.indexOf('AdrActCnt');
    const splyCurIdx = headers.indexOf('SplyCur');
    
    console.log(`Found ${lines.length - 1} rows in CoinMetrics CSV`);
    
    // 최근 3년 데이터만 필터링
    const threeYearsAgo = new Date();
    threeYearsAgo.setFullYear(threeYearsAgo.getFullYear() - 3);
    
    const nvtRecords = [];
    const priceRecords = [];
    
    for (let i = 1; i < lines.length; i++) {
      const cols = lines[i].split(',');
      const dateStr = cols[timeIdx];
      const date = new Date(dateStr);
      
      if (date < threeYearsAgo) continue;
      
      // NVT 데이터
      const nvtValue = parseFloat(cols[nvtIdx]) || parseFloat(cols[nvt90Idx]);
      if (nvtValue && nvtValue > 0 && nvtValue < 500) {
        nvtRecords.push({
          date: dateStr,
          nvt_ratio: nvtValue,
          market_cap: parseFloat(cols[capMrktIdx]) || null,
          transaction_volume: parseFloat(cols[txVolIdx]) || null,
          source: 'coinmetrics'
        });
      }
      
      // 가격 데이터 (다른 용도로 활용 가능)
      const price = parseFloat(cols[priceIdx]);
      if (price && price > 0) {
        priceRecords.push({
          date: dateStr,
          price_usd: price,
          market_cap: parseFloat(cols[capMrktIdx]) || null,
          supply: parseFloat(cols[splyCurIdx]) || null
        });
      }
    }
    
    console.log(`Parsed ${nvtRecords.length} NVT records, ${priceRecords.length} price records`);
    
    // NVT 데이터 Supabase에 저장
    if (nvtRecords.length > 0) {
      // 배치로 upsert (500개씩)
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
// 2. Staking 데이터 수집 (beaconcha.in)
// 차트 JSON에서 전체 히스토리 한 번에 획득
// ============================================
async function collectStakingData() {
  console.log('\n🥩 Collecting Staking data (beaconcha.in)...');
  
  try {
    // beaconcha.in 차트 페이지에서 staked_ether 데이터 가져오기
    // 차트가 사용하는 JSON 엔드포인트
    const chartUrl = 'https://beaconcha.in/api/v1/chart/staked_ether';
    
    let stakingRecords = [];
    
    try {
      const response = await fetchWithRetry(chartUrl);
      const data = await response.json();
      
      if (data.status === 'OK' && Array.isArray(data.data)) {
        stakingRecords = data.data.map(item => ({
          date: new Date(item.ts * 1000).toISOString().split('T')[0],
          total_staked_eth: item.v, // staked ETH value
          validator_count: null, // 별도 API 필요
          staking_apr: null, // ETH.STORE에서 가져옴
          source: 'beaconchain'
        })).filter(r => r.total_staked_eth > 0);
      }
    } catch (e) {
      console.log('Chart API failed, trying alternative method...');
    }
    
    // 대안: 현재 epoch에서 validator 수 × 32 ETH 계산
    if (stakingRecords.length === 0) {
      try {
        const epochUrl = 'https://beaconcha.in/api/v1/epoch/latest';
        const response = await fetchWithRetry(epochUrl);
        const data = await response.json();
        
        if (data.status === 'OK' && data.data) {
          const validatorCount = data.data.validatorscount;
          const avgBalance = data.data.averagevalidatorbalance / 1e9; // Gwei to ETH
          const totalStaked = validatorCount * avgBalance;
          
          stakingRecords.push({
            date: new Date().toISOString().split('T')[0],
            total_staked_eth: totalStaked,
            validator_count: validatorCount,
            staking_apr: null,
            source: 'beaconchain'
          });
          
          console.log(`Current staking: ${(totalStaked / 1e6).toFixed(2)}M ETH, ${validatorCount.toLocaleString()} validators`);
        }
      } catch (e) {
        console.error('Epoch API also failed:', e.message);
      }
    }
    
    // ETH.STORE에서 APR 가져오기
    try {
      const ethstoreUrl = 'https://beaconcha.in/api/v1/ethstore/latest';
      const response = await fetchWithRetry(ethstoreUrl);
      const data = await response.json();
      
      if (data.status === 'OK' && data.data) {
        const apr = data.data.apr * 100; // Convert to percentage
        console.log(`Current staking APR: ${apr.toFixed(2)}%`);
        
        // 최신 레코드에 APR 추가
        if (stakingRecords.length > 0) {
          stakingRecords[stakingRecords.length - 1].staking_apr = apr;
        }
      }
    } catch (e) {
      console.error('ETH.STORE API failed:', e.message);
    }
    
    // Supabase에 저장
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
// 3. Daily Burn 데이터 (Etherscan)
// ethsupply2에서 누적 burn 차이 계산
// ============================================
async function collectBurnData() {
  console.log('\n🔥 Collecting Burn data (Etherscan)...');
  
  try {
    const apiKey = ETHERSCAN_API_KEY ? `&apikey=${ETHERSCAN_API_KEY}` : '';
    const url = `https://api.etherscan.io/api?module=stats&action=ethsupply2${apiKey}`;
    
    const response = await fetchWithRetry(url);
    const data = await response.json();
    
    if (data.status !== '1' || !data.result) {
      throw new Error('Etherscan API returned invalid response');
    }
    
    const currentBurntFees = parseFloat(data.result.BurntFees) / 1e18;
    const ethSupply = parseFloat(data.result.EthSupply) / 1e18;
    const eth2Staking = parseFloat(data.result.Eth2Staking) / 1e18;
    
    console.log(`Total burnt: ${currentBurntFees.toLocaleString()} ETH`);
    console.log(`ETH Supply: ${(ethSupply / 1e6).toFixed(2)}M`);
    
    const today = new Date().toISOString().split('T')[0];
    
    // 어제 데이터 조회하여 일일 burn 계산
    const { data: yesterdayData } = await supabase
      .from('historical_gas_burn')
      .select('cumulative_burn')
      .lt('date', today)
      .order('date', { ascending: false })
      .limit(1);
    
    let dailyBurn = null;
    if (yesterdayData && yesterdayData.length > 0) {
      dailyBurn = currentBurntFees - yesterdayData[0].cumulative_burn;
      if (dailyBurn < 0) dailyBurn = null; // 데이터 오류 방지
    }
    
    const burnRecord = {
      date: today,
      eth_burnt: dailyBurn,
      cumulative_burn: currentBurntFees,
      avg_gas_price: null, // 별도 API 필요
      total_transactions: null,
      source: 'etherscan'
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
// 4. TVL 데이터 (DefiLlama)
// 한 번 호출로 전체 히스토리 반환
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
    
    // 최근 3년만 필터링
    const threeYearsAgo = Date.now() / 1000 - (3 * 365 * 24 * 60 * 60);
    
    const tvlRecords = data
      .filter(item => item.date > threeYearsAgo)
      .map(item => ({
        date: new Date(item.date * 1000).toISOString().split('T')[0],
        total_tvl: item.tvl,
        source: 'defillama'
      }));
    
    console.log(`Found ${tvlRecords.length} TVL records`);
    
    // 배치 upsert
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
// 5. L2 TVL 데이터 (DefiLlama)
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
      
      // 최근 3년만
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
      
      await sleep(200); // Rate limit 방지
    } catch (error) {
      console.error(`  ${chain} failed:`, error.message);
    }
  }
  
  // 날짜별로 그룹핑하여 총 L2 TVL 계산
  const dateMap = new Map();
  for (const record of allRecords) {
    if (!dateMap.has(record.date)) {
      dateMap.set(record.date, { date: record.date, chains: {}, total: 0 });
    }
    const entry = dateMap.get(record.date);
    entry.chains[record.chain] = record.tvl;
    entry.total += record.tvl;
  }
  
  const l2Records = Array.from(dateMap.values()).map(entry => ({
    date: entry.date,
    total_l2_tvl: entry.total,
    arbitrum_tvl: entry.chains['arbitrum'] || 0,
    optimism_tvl: entry.chains['optimism'] || 0,
    base_tvl: entry.chains['base'] || 0,
    zksync_tvl: entry.chains['zksync_era'] || 0,
    source: 'defillama'
  }));
  
  // 배치 upsert
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
// 6. Fees 데이터 (DefiLlama)
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
      daily_fees_usd: fees,
      source: 'defillama'
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
// 7. DEX Volume 데이터 (DefiLlama)
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
      daily_volume_usd: volume,
      source: 'defillama'
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
// 8. Fear & Greed Index (Alternative.me)
// ============================================
async function collectFearGreedData() {
  console.log('\n😱 Collecting Fear & Greed data (Alternative.me)...');
  
  try {
    // 최대 1095일 (3년) 데이터 요청
    const url = 'https://api.alternative.me/fng/?limit=1095&format=json';
    const response = await fetchWithRetry(url);
    const data = await response.json();
    
    if (!data.data || !Array.isArray(data.data)) {
      throw new Error('Invalid response format');
    }
    
    const fgRecords = data.data.map(item => ({
      date: new Date(parseInt(item.timestamp) * 1000).toISOString().split('T')[0],
      fear_greed_index: parseInt(item.value),
      classification: item.value_classification,
      source: 'alternative_me'
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
// 9. Stablecoin 데이터 (DefiLlama)
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
    
    const stablecoinRecords = data.map(item => ({
      date: new Date(item.date * 1000).toISOString().split('T')[0],
      total_stablecoin_mcap: Object.values(item.totalCirculating || {})
        .reduce((sum, val) => sum + (val?.peggedUSD || 0), 0),
      source: 'defillama'
    }));
    
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
// 10. ETH/BTC Ratio (CoinGecko)
// ============================================
async function collectETHBTCRatio() {
  console.log('\n📉 Collecting ETH/BTC Ratio (CoinGecko)...');
  
  try {
    // CoinGecko market chart - 최대 365일
    const url = 'https://api.coingecko.com/api/v3/coins/ethereum/market_chart?vs_currency=btc&days=max&interval=daily';
    const response = await fetchWithRetry(url);
    const data = await response.json();
    
    if (!data.prices || !Array.isArray(data.prices)) {
      throw new Error('Invalid response format');
    }
    
    const ratioRecords = data.prices.map(([timestamp, price]) => ({
      date: new Date(timestamp).toISOString().split('T')[0],
      eth_btc_ratio: price,
      source: 'coingecko'
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
  console.log('🚀 ETHval Data Collector v2.0 Starting...');
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
    // 1. CoinMetrics (NVT, 가격 등) - 전체 히스토리 한 번에
    results.coinmetrics = await collectCoinMetricsData();
    await sleep(1000);
    
    // 2. Staking 데이터
    results.staking = await collectStakingData();
    await sleep(1000);
    
    // 3. Burn 데이터
    results.burn = await collectBurnData();
    await sleep(1000);
    
    // 4. TVL 데이터
    results.tvl = await collectTVLData();
    await sleep(1000);
    
    // 5. L2 TVL 데이터
    results.l2tvl = await collectL2TVLData();
    await sleep(1000);
    
    // 6. Fees 데이터
    results.fees = await collectFeesData();
    await sleep(1000);
    
    // 7. DEX Volume 데이터
    results.dexVolume = await collectDEXVolumeData();
    await sleep(1000);
    
    // 8. Fear & Greed Index
    results.fearGreed = await collectFearGreedData();
    await sleep(1000);
    
    // 9. Stablecoin 데이터
    results.stablecoins = await collectStablecoinData();
    await sleep(1000);
    
    // 10. ETH/BTC Ratio
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
