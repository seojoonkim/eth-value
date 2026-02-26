/**
 * ETHval Data Collector v7.4
 * 39개 전체 데이터셋 수집
 * + ETH Price, ETH/BTC: Dune API
 * + Funding Rate: CryptoQuant API (Binance 대체)
 * + AI 일간 해설 생성 (Claude Haiku)
 * + 병렬 처리로 속도 개선
 */

const { createClient } = require('@supabase/supabase-js');

const SUPABASE_URL = process.env.SUPABASE_URL;
const SUPABASE_KEY = process.env.SUPABASE_SERVICE_KEY;
const DUNE_API_KEY = process.env.DUNE_API_KEY;
const ANTHROPIC_API_KEY = process.env.ANTHROPIC_API_KEY;
const CRYPTOQUANT_API_KEY = process.env.CRYPTOQUANT_API_KEY;

if (!SUPABASE_URL || !SUPABASE_KEY) {
    console.error('❌ Missing SUPABASE_URL or SUPABASE_SERVICE_KEY');
    process.exit(1);
}

if (!DUNE_API_KEY) {
    console.warn('⚠️ Missing DUNE_API_KEY - Dune data collection will be skipped');
}

if (!ANTHROPIC_API_KEY) {
    console.warn('⚠️ Missing ANTHROPIC_API_KEY - AI commentary will be skipped');
}

if (!CRYPTOQUANT_API_KEY) {
    console.warn('⚠️ Missing CRYPTOQUANT_API_KEY - CryptoQuant data will be skipped');
}

const supabase = createClient(SUPABASE_URL, SUPABASE_KEY);
const sleep = (ms) => new Promise(r => setTimeout(r, ms));

// 결과 상태 헬퍼
const result = {
    ok: (count, msg = '') => ({ count, status: 'ok', msg }),
    skip: (msg = 'Already up to date') => ({ count: 0, status: 'skip', msg }),
    warn: (count, msg) => ({ count, status: 'warn', msg }),
    fail: (msg) => ({ count: 0, status: 'fail', msg })
};

// ============================================================
// AI Commentary Section Definitions
// ============================================================
const COMMENTARY_SECTIONS = {
    // 02.1 투자자 심리 - 7개 차트
    // Charts: Realized Price, MVRV Ratio, Fear & Greed, Funding Rate, Open Interest, Exchange ETH Reserve, Whale Transactions
    investor_sentiment: {
        title: 'Investor Sentiment',
        title_ko: '투자자 심리',
        charts: ['Realized Price', 'MVRV Ratio', 'Fear & Greed', 'Funding Rate', 'Open Interest', 'Exchange ETH Reserve', 'Whale Transactions'],
        tables: {
            mvrv: 'historical_mvrv',  // mvrv_ratio + realized_price
            fear_greed: 'historical_fear_greed',  // value
            funding_rate: 'historical_funding_rate',  // funding_rate
            open_interest: 'historical_open_interest',  // open_interest
            exchange_reserve: 'historical_exchange_reserve',  // reserve_eth
            whale_tx: 'historical_whale_tx'  // whale_tx_count
        }
    },
    // 02.2 시장 포지션 - 5개 차트
    // Charts: ETH/BTC Ratio, ETH Dominance, Stablecoin Mcap, Volatility, NVT Ratio
    market_position: {
        title: 'Market Position',
        title_ko: '시장 포지션',
        charts: ['ETH/BTC Ratio', 'ETH Dominance', 'Stablecoin Mcap', 'Volatility', 'NVT Ratio'],
        tables: {
            eth_btc: 'historical_eth_btc',  // ratio
            eth_dominance: 'historical_eth_dominance',  // eth_dominance
            stablecoins: 'historical_stablecoins',  // total_mcap (전체 스테이블코인)
            volatility: 'historical_volatility',  // volatility_30d
            nvt: 'historical_nvt'  // nvt_ratio
        }
    },
    // 02.3 공급 역학 - 6개 차트
    // Charts: Staking Yield (APR), Staked ETH, ETH Burned, ETH Issued, Net Supply, Effective Float
    supply_dynamics: {
        title: 'Supply Dynamics',
        title_ko: '공급 역학',
        charts: ['Staking Yield (APR)', 'Staked ETH', 'ETH Burned', 'ETH Issued', 'Net Supply', 'Effective Float'],
        tables: {
            staking_apr: 'historical_staking_apr',  // lido_apr
            staking: 'historical_staking',  // total_staked_eth
            eth_burnt: 'historical_gas_burn',  // eth_burnt
            eth_issued: 'historical_daily_issuance',  // daily_issuance
            eth_supply: 'historical_eth_supply'  // eth_supply (for Effective Float calc)
        },
        fieldOverrides: {
            eth_burnt: 'eth_burnt',
            eth_issued: 'daily_issuance'
        },
        context: `Net Supply와 Effective Float는 계산값입니다:
- Net Supply Change = (ETH Burned - ETH Issued) / Total Supply * 365 * 100 (연간 %)
- Effective Float = Total Supply - Staked ETH - Contract Balances
ETH Issued는 일 약 930 ETH (검증자 보상)
현재 ETH는 burn < issuance로 약 +0.5%/년 인플레이션 상태.`
    },
    // 02.4 네트워크 수요 - 5개 차트
    // Charts: Gas Price, Gas Utilization, Network Fees, Blob Fees, Blob Count
    network_demand: {
        title: 'Network Demand',
        title_ko: '네트워크 수요',
        charts: ['Gas Price', 'Gas Utilization', 'Network Fees', 'Blob Fees', 'Blob Count'],
        tables: {
            gas_price: 'historical_gas_burn',  // avg_gas_price_gwei (메인 필드)
            gas_utilization: 'historical_gas_burn',  // gas_utilization (별도 키로 분리)
            fees: 'historical_protocol_fees',  // fees
            blob_count: 'historical_blob_data',  // blob_count (메인 필드)
            blob_fees: 'historical_blob_data'  // blob_fee_eth (별도 키로 분리)
        },
        // 각 키별로 어떤 필드를 메인으로 사용할지 지정
        fieldOverrides: {
            gas_price: 'avg_gas_price_gwei',
            gas_utilization: 'gas_utilization',
            blob_count: 'blob_count',
            blob_fees: 'blob_fee_eth'
        }
    },
    // 02.5 사용자 활동 - 5개 차트
    // Charts: New Addresses, L1 Active Addresses, L2 Active Addresses, L1 Transactions, L2 Transactions
    user_activity: {
        title: 'User Activity',
        title_ko: '사용자 활동',
        charts: ['New Addresses', 'L1 Active Addresses', 'L2 Active Addresses', 'L1 Transactions', 'L2 Transactions'],
        tables: {
            new_addresses: 'historical_new_addresses',  // new_addresses
            active_addresses: 'historical_active_addresses',  // active_addresses
            l2_addresses: 'historical_l2_addresses',  // active_addresses (aggregate)
            transactions: 'historical_transactions',  // tx_count
            l2_transactions: 'historical_l2_transactions'  // tx_count (aggregate)
        }
    },
    // 02.6 예치 자본 - 6개 차트
    // Charts: L1 TVL, L2 TVL, DeFi Lending TVL, L1 Stablecoin Supply, L2 Stablecoin Supply, App Capital
    locked_capital: {
        title: 'Locked Capital',
        title_ko: '예치 자본',
        charts: ['L1 TVL', 'L2 TVL', 'DeFi Lending TVL', 'L1 Stablecoin Supply', 'L2 Stablecoin Supply', 'App Capital'],
        tables: {
            ethereum_tvl: 'historical_ethereum_tvl',  // tvl
            l2_tvl: 'historical_l2_tvl',  // tvl (aggregate)
            lending_tvl: 'historical_lending_tvl',  // total_tvl
            stablecoins_eth: 'historical_stablecoins_eth',  // total_mcap (L1 ETH 체인 스테이블코인)
            l2_stablecoins: 'historical_l2_stablecoin_daily',  // total (L2 스테이블코인 공급량)
            staking: 'historical_staking'  // total_staked_eth (App Capital용)
        },
        fieldOverrides: {
            l2_stablecoins: 'total'
        }
    },
    // 02.7 결제량 - 6개 차트
    // NOTE: L1/L2 ETH Transfer는 ETH 전송만, Total Volume은 모든 토큰 포함
    // ERC-20 토큰 전송, DEX 스왑 등은 별도 지표로 측정
    // Charts: L1 Total Volume, L1 Stablecoin Volume, L1 ETH Transfer, L2 Total Volume, L2 Stablecoin Volume, L2 ETH Transfer, L1 DEX Volume, Bridge Volume
    settlement_volume: {
        title: 'Settlement Volume',
        title_ko: '결제량',
        charts: ['L1 Total Volume', 'L1 Stablecoin Volume', 'L1 ETH Transfer', 'L2 Total Volume', 'L2 Stablecoin Volume', 'L2 ETH Transfer', 'L1 DEX Volume', 'Bridge Volume'],
        // AI에게 전달할 컨텍스트: 각 지표의 정확한 정의
        context: `⚠️ CRITICAL: 8 DIFFERENT METRICS - DO NOT CONFUSE ⚠️

=== SIZE REFERENCE (biggest to smallest) ===
1. L1 전체 볼륨 (L1 Total Volume): $100-600B/day ← BIGGEST
2. L2 전체 볼륨 (L2 Total Volume): $50-300B/day
3. L1 스테이블코인 볼륨: $80-200B/day
4. L2 스테이블코인 볼륨: $70-100B/day
5. L1 ETH 전송량 (L1 ETH Transfer): $5-10B/day ← MUCH SMALLER!
6. L1 DEX 볼륨: $1-3B/day
7. L2 ETH 전송량 (L2 ETH Transfer): $200-500M/day (Mantle 제외)
8. 브릿지 볼륨: $10-50M/day

=== THE KEY DISTINCTION ===
❌ WRONG: "L1 전체 볼륨이 $7B" (X)
✅ RIGHT: "L1 ETH 전송량이 $7B, L1 전체 볼륨은 $200B+" (O)

• "L1 전체 볼륨" = ETH + 모든 토큰 = $100B+ (큰 숫자)
• "L1 ETH 전송량" = ETH만 = $5-10B (작은 숫자)

이 두 지표의 차이는 10배~50배입니다!

=== WHAT EACH METRIC MEASURES ===
• L1 Total Volume: L1의 모든 온체인 전송 (ETH + 모든 ERC-20)
• L1 ETH Transfer: L1의 ETH 전송만 (토큰 제외)
• L1 Stablecoin Volume: L1의 스테이블코인 전송만
• L2 Total Volume: L2 8개 체인의 모든 전송
• L2 ETH Transfer: L2의 ETH 전송만 (Mantle MNT 제외, Settlement Layer용)
• L2 Stablecoin Volume: L2의 스테이블코인만
• L1 DEX Volume: L1 DEX 거래량 (주간 데이터)
• Bridge Volume: 브릿지 전송량

When you see data, check the VALUE RANGE to identify which metric it is!`,
        tables: {
            l1_eth_transfer: 'historical_l1_total_volume',  // eth_volume_usd (L1 ETH Transfer - ETH only, ~$7B)
            l1_total_volume: 'historical_l1_total_volume',  // total_volume_usd (ETH + all tokens, ~$200B)
            l2_native_transfer: 'historical_l2_total_volume',  // native_volume_usd (L2 ETH Transfer - excl Mantle, ~$300M)
            l2_total_volume: 'historical_l2_total_volume',  // total_volume_usd (ETH + all tokens, ~$100B)
            bridge_volume: 'historical_bridge_volume',  // bridge_volume_eth (aggregate)
            stablecoin_volume: 'historical_stablecoin_volume',  // daily_volume (~$80B)
            l2_stablecoin_volume: 'historical_l2_stablecoin_volume',  // total_volume (~$77B)
            dex_volume: 'historical_dex_volume'  // volume (~$2B weekly)
        }
    }
};

// ============================================================
// AI Commentary Generation Functions
// ============================================================

/**
 * Fetch historical statistics for a metric (3-year data for context)
 * Returns min, max, median, percentiles, and current percentile
 */
async function fetchHistoricalStats(tableName, fieldName) {
    try {
        // 3년치 데이터 가져오기 (약 1095일)
        const { data, error } = await supabase
            .from(tableName)
            .select(`date, ${fieldName}`)
            .order('date', { ascending: false })
            .limit(1100);
        
        if (error || !data || data.length < 30) return null;
        
        // 유효한 값만 추출
        const values = data
            .map(d => d[fieldName])
            .filter(v => v !== null && v !== undefined && !isNaN(v) && isFinite(v));
        
        if (values.length < 30) return null;
        
        // 정렬
        const sorted = [...values].sort((a, b) => a - b);
        const n = sorted.length;
        
        // 통계 계산
        const min = sorted[0];
        const max = sorted[n - 1];
        const median = sorted[Math.floor(n / 2)];
        const p10 = sorted[Math.floor(n * 0.1)];
        const p25 = sorted[Math.floor(n * 0.25)];
        const p75 = sorted[Math.floor(n * 0.75)];
        const p90 = sorted[Math.floor(n * 0.9)];
        
        // 현재값의 percentile 계산
        const currentVal = values[0]; // 최신값
        const belowCount = sorted.filter(v => v < currentVal).length;
        const currentPercentile = Math.round((belowCount / n) * 100);
        
        return {
            min,
            max,
            median,
            p10,
            p25,
            p75,
            p90,
            currentVal,
            currentPercentile,
            dataPoints: n
        };
    } catch (e) {
        console.error(`  Error fetching stats for ${tableName}.${fieldName}:`, e.message);
        return null;
    }
}

/**
 * Fetch latest metrics data for a section
 */
async function fetchSectionMetrics(sectionKey) {
    const section = COMMENTARY_SECTIONS[sectionKey];
    if (!section) return null;
    
    const metricsData = {};
    const today = new Date().toISOString().split('T')[0];
    const fourteenDaysAgo = new Date(Date.now() - 14 * 24 * 60 * 60 * 1000).toISOString().split('T')[0];
    const ninetyDaysAgo = new Date(Date.now() - 90 * 24 * 60 * 60 * 1000).toISOString().split('T')[0];
    const ninetyFiveDaysAgo = new Date(Date.now() - 95 * 24 * 60 * 60 * 1000).toISOString().split('T')[0];
    
    // ═══════════════════════════════════════════════════════════════════
    // 마지막 날 미취합 데이터 제외 함수 (화면과 동일 로직)
    // ═══════════════════════════════════════════════════════════════════
    // 미취합 데이터 제외: 마지막 데이터 항상 제외 (수집 중일 수 있음)
    // ═══════════════════════════════════════════════════════════════════
    function checkAndRemoveIncomplete(records, valueField) {
        if (!records || records.length < 3) return records;
        
        // 마지막 데이터 항상 제외
        return records.slice(1);
    }
    
    // 테이블별 값 필드 매핑 (DATASETS 기준 - 실제 DB 필드명)
    const valueFieldMap = {
        'historical_ethereum_tvl': 'tvl',
        'historical_staking': 'total_staked_eth',
        'historical_l2_tvl': 'tvl',
        'historical_protocol_fees': 'fees',
        'historical_dex_volume': 'volume',
        'historical_stablecoins': 'total_mcap',
        'historical_stablecoins_eth': 'total_mcap',
        'historical_eth_btc': 'ratio',
        'historical_funding_rate': 'funding_rate',
        'historical_eth_dominance': 'eth_dominance',
        'historical_lending_tvl': 'total_tvl',
        'historical_staking_apr': 'lido_apr',
        'historical_blob_data': 'blob_count',
        'historical_l2_transactions': 'tx_count',
        // REMOVED: 'historical_l2_tx_volume' - now using historical_l2_total_volume.native_volume_usd
        'historical_bridge_volume': 'bridge_volume_eth',
        'historical_whale_tx': 'whale_tx_count',
        'historical_mvrv': 'mvrv_ratio',
        'historical_stablecoin_volume': 'daily_volume',
        'historical_l2_stablecoin_volume': 'total_volume',
        'historical_l2_stablecoin_daily': 'total',  // L2 Stablecoin Supply
        'historical_daily_issuance': 'daily_issuance',  // ETH Issued
        'historical_new_addresses': 'new_addresses',
        'historical_gas_burn': 'avg_gas_price_gwei',
        'historical_transactions': 'tx_count',
        'historical_volatility': 'volatility_30d',
        'historical_exchange_reserve': 'reserve_eth',
        'historical_eth_supply': 'eth_supply',
        'historical_l2_addresses': 'active_addresses',
        'historical_active_addresses': 'active_addresses',
        'historical_fear_greed': 'value',
        'historical_nvt': 'nvt_ratio',
        'historical_open_interest': 'open_interest',
        'historical_l1_total_volume': 'total_volume_usd',  // Also has eth_volume_usd for L1 ETH Transfer
        'historical_l2_total_volume': 'total_volume_usd',  // Also has native_volume_usd for L2 ETH Transfer
    };
    
    for (const [metricKey, tableName] of Object.entries(section.tables)) {
        try {
            // Special handling for L1 Total Volume table (used by both l1_eth_transfer and l1_total_volume)
            // l1_eth_transfer uses eth_volume_usd, l1_total_volume uses total_volume_usd
            if (tableName === 'historical_l1_total_volume') {
                const fieldToUse = metricKey === 'l1_eth_transfer' ? 'eth_volume_usd' : 'total_volume_usd';
                const { data: recent } = await supabase
                    .from(tableName)
                    .select(`date, ${fieldToUse}`)
                    .gte('date', ninetyFiveDaysAgo)
                    .order('date', { ascending: false })
                    .limit(95);
                
                if (recent && recent.length > 0) {
                    // 마지막 데이터 제외 (수집 중일 수 있음)
                    let cleaned = recent.filter(r => r[fieldToUse] > 0);
                    if (cleaned.length > 2) {
                        cleaned = cleaned.slice(1);
                    }
                    
                    metricsData[metricKey] = {
                        latest: cleaned[0],
                        recent3d: cleaned.slice(0, 3),
                        recent7d: cleaned.slice(0, 7),
                        around90d: cleaned.slice(87, 94),
                        ninetyDaysAgo: cleaned.length > 90 ? cleaned[90] : null
                    };
                }
                continue;
            }
            
            // Special handling for L2 addresses (stored by chain)
            if (tableName === 'historical_l2_addresses') {
                const { data: recent } = await supabase
                    .from(tableName)
                    .select('date, active_addresses')
                    .gte('date', ninetyFiveDaysAgo)
                    .order('date', { ascending: false });
                
                if (recent && recent.length > 0) {
                    const byDate = {};
                    for (const r of recent) {
                        if (!byDate[r.date]) byDate[r.date] = 0;
                        byDate[r.date] += parseInt(r.active_addresses || 0);
                    }
                    let dates = Object.keys(byDate).sort().reverse();
                    
                    // 마지막 데이터 제외 (수집 중일 수 있음)
                    if (dates.length > 2) {
                        dates = dates.slice(1);
                    }
                    
                    const latestDate = dates[0];
                    
                    metricsData[metricKey] = {
                        latest: { date: latestDate, active_addresses: byDate[latestDate] },
                        recent3d: dates.slice(0, 3).map(d => ({ date: d, active_addresses: byDate[d] })),
                        recent7d: dates.slice(0, 7).map(d => ({ date: d, active_addresses: byDate[d] })),
                        around90d: dates.slice(87, 94).map(d => ({ date: d, active_addresses: byDate[d] })),
                        ninetyDaysAgo: dates.length > 90 ? { date: dates[90], active_addresses: byDate[dates[90]] } : null
                    };
                }
                continue;
            }
            
            // Special handling for L2 Transactions (stored by chain)
            if (tableName === 'historical_l2_transactions') {
                const { data: recent } = await supabase
                    .from(tableName)
                    .select('date, tx_count')
                    .gte('date', ninetyFiveDaysAgo)
                    .order('date', { ascending: false });
                
                if (recent && recent.length > 0) {
                    const byDate = {};
                    for (const r of recent) {
                        if (!byDate[r.date]) byDate[r.date] = 0;
                        byDate[r.date] += parseInt(r.tx_count || 0);
                    }
                    let dates = Object.keys(byDate).sort().reverse();
                    
                    // 마지막 데이터 제외 (수집 중일 수 있음)
                    if (dates.length > 2) {
                        dates = dates.slice(1);
                    }
                    
                    const latestDate = dates[0];
                    
                    metricsData[metricKey] = {
                        latest: { date: latestDate, tx_count: byDate[latestDate] },
                        recent3d: dates.slice(0, 3).map(d => ({ date: d, tx_count: byDate[d] })),
                        recent7d: dates.slice(0, 7).map(d => ({ date: d, tx_count: byDate[d] })),
                        around90d: dates.slice(87, 94).map(d => ({ date: d, tx_count: byDate[d] })),
                        ninetyDaysAgo: dates.length > 90 ? { date: dates[90], tx_count: byDate[dates[90]] } : null
                    };
                }
                continue;
            }
            
            // REMOVED: historical_l2_tx_volume handling
            // L2 ETH Transfer now uses historical_l2_total_volume.native_volume_usd
            
            // Special handling for Bridge Volume (stored by chain)
            if (tableName === 'historical_bridge_volume') {
                const { data: recent } = await supabase
                    .from(tableName)
                    .select('date, bridge_volume_eth')
                    .gte('date', ninetyFiveDaysAgo)
                    .order('date', { ascending: false });
                
                if (recent && recent.length > 0) {
                    const byDate = {};
                    for (const r of recent) {
                        if (!byDate[r.date]) byDate[r.date] = 0;
                        byDate[r.date] += parseFloat(r.bridge_volume_eth || 0);
                    }
                    let dates = Object.keys(byDate).sort().reverse();
                    
                    // 마지막 데이터 제외 (수집 중일 수 있음)
                    if (dates.length > 2) {
                        dates = dates.slice(1);
                    }
                    
                    const latestDate = dates[0];
                    
                    metricsData[metricKey] = {
                        latest: { date: latestDate, bridge_volume_eth: byDate[latestDate] },
                        recent3d: dates.slice(0, 3).map(d => ({ date: d, bridge_volume_eth: byDate[d] })),
                        recent7d: dates.slice(0, 7).map(d => ({ date: d, bridge_volume_eth: byDate[d] })),
                        around90d: dates.slice(87, 94).map(d => ({ date: d, bridge_volume_eth: byDate[d] })),
                        ninetyDaysAgo: dates.length > 90 ? { date: dates[90], bridge_volume_eth: byDate[dates[90]] } : null
                    };
                }
                continue;
            }
            
            // Special handling for L2 TVL (stored by chain)
            if (tableName === 'historical_l2_tvl') {
                const { data: recent } = await supabase
                    .from(tableName)
                    .select('date, tvl')
                    .gte('date', ninetyFiveDaysAgo)
                    .order('date', { ascending: false });
                
                if (recent && recent.length > 0) {
                    const byDate = {};
                    for (const r of recent) {
                        if (!byDate[r.date]) byDate[r.date] = 0;
                        byDate[r.date] += parseFloat(r.tvl || 0);
                    }
                    let dates = Object.keys(byDate).sort().reverse();
                    
                    // 마지막 데이터 제외 (수집 중일 수 있음)
                    if (dates.length > 2) {
                        dates = dates.slice(1);
                    }
                    
                    const latestDate = dates[0];
                    
                    metricsData[metricKey] = {
                        latest: { date: latestDate, tvl: byDate[latestDate] },
                        recent3d: dates.slice(0, 3).map(d => ({ date: d, tvl: byDate[d] })),
                        recent7d: dates.slice(0, 7).map(d => ({ date: d, tvl: byDate[d] })),
                        around90d: dates.slice(87, 94).map(d => ({ date: d, tvl: byDate[d] })),
                        ninetyDaysAgo: dates.length > 90 ? { date: dates[90], tvl: byDate[dates[90]] } : null
                    };
                }
                continue;
            }
            
            // Special handling for L2 Total Volume table (used by both l2_native_transfer and l2_total_volume)
            // l2_native_transfer (L2 ETH Transfer) uses native_volume_usd, l2_total_volume uses total_volume_usd
            if (tableName === 'historical_l2_total_volume') {
                const fieldToUse = metricKey === 'l2_native_transfer' ? 'native_volume_usd' : 'total_volume_usd';
                const { data: recent } = await supabase
                    .from(tableName)
                    .select(`date, ${fieldToUse}`)
                    .gte('date', ninetyFiveDaysAgo)
                    .order('date', { ascending: false });
                
                if (recent && recent.length > 0) {
                    const byDate = {};
                    for (const r of recent) {
                        if (!byDate[r.date]) byDate[r.date] = 0;
                        byDate[r.date] += parseFloat(r[fieldToUse] || 0);
                    }
                    let dates = Object.keys(byDate).sort().reverse();
                    
                    // 마지막 데이터 제외 (수집 중일 수 있음)
                    if (dates.length > 2) {
                        dates = dates.slice(1);
                    }
                    
                    const latestDate = dates[0];
                    
                    metricsData[metricKey] = {
                        latest: { date: latestDate, [fieldToUse]: byDate[latestDate] },
                        recent3d: dates.slice(0, 3).map(d => ({ date: d, [fieldToUse]: byDate[d] })),
                        recent7d: dates.slice(0, 7).map(d => ({ date: d, [fieldToUse]: byDate[d] })),
                        around90d: dates.slice(87, 94).map(d => ({ date: d, [fieldToUse]: byDate[d] })),
                        ninetyDaysAgo: dates.length > 90 ? { date: dates[90], [fieldToUse]: byDate[dates[90]] } : null
                    };
                }
                continue;
            }
            
            // Special handling for L2 Stablecoin Volume (stored by chain)
            if (tableName === 'historical_l2_stablecoin_volume') {
                const { data: recent } = await supabase
                    .from(tableName)
                    .select('date, total_volume')
                    .gte('date', ninetyFiveDaysAgo)
                    .order('date', { ascending: false });
                
                if (recent && recent.length > 0) {
                    const byDate = {};
                    for (const r of recent) {
                        if (!byDate[r.date]) byDate[r.date] = 0;
                        byDate[r.date] += parseFloat(r.total_volume || 0);
                    }
                    let dates = Object.keys(byDate).sort().reverse();
                    
                    // 마지막 데이터 제외 (수집 중일 수 있음)
                    if (dates.length > 2) {
                        dates = dates.slice(1);
                    }
                    
                    const latestDate = dates[0];
                    
                    metricsData[metricKey] = {
                        latest: { date: latestDate, total_volume: byDate[latestDate] },
                        recent3d: dates.slice(0, 3).map(d => ({ date: d, total_volume: byDate[d] })),
                        recent7d: dates.slice(0, 7).map(d => ({ date: d, total_volume: byDate[d] })),
                        around90d: dates.slice(87, 94).map(d => ({ date: d, total_volume: byDate[d] })),
                        ninetyDaysAgo: dates.length > 90 ? { date: dates[90], total_volume: byDate[dates[90]] } : null
                    };
                }
                continue;
            }
            
            // Get recent data (35 days for 30d trend analysis)
            const { data: recent } = await supabase
                .from(tableName)
                .select('*')
                .gte('date', ninetyFiveDaysAgo)
                .order('date', { ascending: false })
                .limit(95);
            
            // Get 30-day ago data for comparison (backup)
            const { data: older } = await supabase
                .from(tableName)
                .select('*')
                .lte('date', ninetyDaysAgo)
                .order('date', { ascending: false })
                .limit(1);
            
            if (recent && recent.length > 0) {
                // fieldOverrides가 있으면 해당 필드를 메인으로 사용
                let valueField = valueFieldMap[tableName];
                if (section.fieldOverrides && section.fieldOverrides[metricKey]) {
                    valueField = section.fieldOverrides[metricKey];
                }
                
                // 미취합 데이터 제외 (화면과 동일 로직)
                let cleanedRecent = recent;
                if (valueField) {
                    cleanedRecent = checkAndRemoveIncomplete(recent, valueField);
                }
                
                // 30일 전 ±3일 (27~33일 전) 데이터 찾기
                const around90d = cleanedRecent.filter(d => {
                    const daysDiff = Math.floor((new Date(today) - new Date(d.date)) / (24 * 60 * 60 * 1000));
                    return daysDiff >= 87 && daysDiff <= 93;
                });
                
                // fieldOverrides가 있으면 해당 필드만 추출해서 저장 (중복 테이블 문제 해결)
                if (section.fieldOverrides && section.fieldOverrides[metricKey]) {
                    const targetField = section.fieldOverrides[metricKey];
                    // 역사적 통계 가져오기
                    const historicalStats = await fetchHistoricalStats(tableName, targetField);
                    
                    metricsData[metricKey] = {
                        latest: { date: cleanedRecent[0]?.date, [targetField]: cleanedRecent[0]?.[targetField] },
                        recent3d: cleanedRecent.slice(0, 3).map(r => ({ date: r.date, [targetField]: r[targetField] })),
                        recent7d: cleanedRecent.slice(0, 7).map(r => ({ date: r.date, [targetField]: r[targetField] })),
                        around90d: around90d.map(r => ({ date: r.date, [targetField]: r[targetField] })),
                        ninetyDaysAgo: older?.[0] ? { date: older[0].date, [targetField]: older[0][targetField] } : null,
                        historicalStats: historicalStats
                    };
                } else {
                    // 역사적 통계 가져오기
                    const historicalStats = await fetchHistoricalStats(tableName, valueField);
                    
                    metricsData[metricKey] = {
                        latest: cleanedRecent[0],
                        recent3d: cleanedRecent.slice(0, 3),
                        recent7d: cleanedRecent.slice(0, 7),
                        around90d: around90d,
                        ninetyDaysAgo: older?.[0] || null,
                        historicalStats: historicalStats
                    };
                }
            }
        } catch (e) {
            console.error(`  Error fetching ${tableName}:`, e.message);
        }
    }
    
    // Also get current ETH price
    try {
        const { data: priceData } = await supabase
            .from('historical_eth_price')
            .select('*')
            .order('date', { ascending: false })
            .limit(2);
        
        if (priceData && priceData.length > 0) {
            metricsData.eth_price = {
                latest: priceData[0],
                previous: priceData[1] || null
            };
        }
    } catch (e) {
        console.error('  Error fetching ETH price:', e.message);
    }
    
    return metricsData;
}

/**
 * Format metrics data for AI prompt (using 3-day averages for 90d comparison)
 */
function formatMetricsForPrompt(sectionKey, metricsData) {
    const section = COMMENTARY_SECTIONS[sectionKey];
    const ethPrice = metricsData.eth_price?.latest?.close || 3900;  // fallback price
    
    let prompt = `Section: ${section.title} (${section.title_ko})\n`;
    prompt += `Charts in this section: ${section.charts.join(', ')}\n\n`;
    prompt += `Current ETH Price: $${ethPrice.toFixed(2)}\n\n`;
    prompt += `Key Metrics (Current = latest complete day, 90-Day Change = vs 3-day avg from 90 days ago):\n`;
    
    // 필드에서 값을 추출하는 헬퍼 함수 (DATASETS 기준)
    // 순서 중요: 구체적인 필드명이 먼저 와야 함
    const extractValue = (record) => {
        if (!record) return null;
        const fields = ['value', 'funding_rate', 'lido_apr', 'eth_dominance', 'ratio', 'reserve_eth',
            'mvrv_ratio', 'realized_price', 'nvt_ratio', 'volatility_30d', 'whale_tx_count',
            'blob_count', 'blob_fee_eth', 'new_addresses', 'active_addresses', 'tx_count',
            'eth_supply', 'total_staked_eth', 'avg_gas_price_gwei', 'gas_utilization', 'eth_burnt',
            'tx_volume_usd', 'daily_volume', 'bridge_volume_eth', 'open_interest',
            'volume', 'fees', 'tvl', 'total_tvl', 'total_mcap', 'total',
            'eth_volume_usd', 'total_volume_usd', 'native_volume_usd', 'total_volume', 'daily_issuance'];
        for (const f of fields) {
            if (record[f] !== undefined && record[f] !== null) {
                return { field: f, value: record[f] };
            }
        }
        return null;
    };
    
    // 차트에서 USD로 표시하는 ETH 볼륨 필드들 (ETH→USD 변환 필요)
    // Note: tx_volume_usd는 이미 USD로 저장되므로 변환 불필요
    const ethToUsdFields = ['bridge_volume_eth'];
    
    // 배열의 평균값 계산 (ETH 볼륨은 USD로 변환)
    const calcAvg = (records, fieldName) => {
        if (!records || records.length === 0) return null;
        const multiplier = ethToUsdFields.includes(fieldName) ? ethPrice : 1;
        const values = records.map(r => extractValue(r)?.value).filter(v => v !== null && v !== undefined);
        if (values.length === 0) return null;
        return (values.reduce((a, b) => a + b, 0) / values.length) * multiplier;
    };
    
    // 특정 필드의 90일 변화 계산 헬퍼
    const calc90dChangeForField = (data, fieldName) => {
        if (!data?.latest || !data?.around90d || data.around90d.length === 0) return null;
        const currentVal = data.latest[fieldName];
        if (currentVal === undefined || currentVal === null) return null;
        
        const oldValues = data.around90d.map(r => r[fieldName]).filter(v => v !== null && v !== undefined);
        if (oldValues.length === 0) return null;
        const oldAvg = oldValues.reduce((a, b) => a + b, 0) / oldValues.length;
        if (oldAvg === 0) return null;
        
        return ((currentVal - oldAvg) / oldAvg * 100).toFixed(1);
    };
    
    for (const [key, data] of Object.entries(metricsData)) {
        if (key === 'eth_price') continue;
        if (!data?.latest) continue;
        
        const extracted = extractValue(data.latest);
        if (!extracted) continue;
        
        const fieldName = extracted.field;
        
        // ETH→USD 변환 여부
        const needsUsdConversion = ethToUsdFields.includes(fieldName);
        const multiplier = needsUsdConversion ? ethPrice : 1;
        const currentVal = extracted.value * multiplier;  // 최신 완전한 날의 값 (차트와 동일)
        
        // 90일 전 3일 평균
        const around90dAvg = calcAvg(data.around90d, fieldName);
        
        // 단위 결정 (차트 표시 단위 기준)
        let unit = '';
        if (['tvl', 'total_tvl', 'realized_price', 'daily_volume', 'volume', 'tx_volume_usd', 'total_mcap', 'fees', 'eth_volume_usd', 'total_volume_usd', 'native_volume_usd', 'total_volume', 'open_interest', 'total'].includes(fieldName)) unit = ' USD';
        else if (ethToUsdFields.includes(fieldName)) unit = ' USD';  // ETH 볼륨 → 차트에서 USD로 표시
        else if (['total_staked_eth', 'reserve_eth', 'eth_burnt', 'eth_supply', 'blob_fee_eth', 'daily_issuance'].includes(fieldName)) unit = ' ETH';
        else if (['funding_rate', 'eth_dominance', 'volatility_30d', 'lido_apr', 'gas_utilization'].includes(fieldName)) unit = '%';
        else if (fieldName === 'avg_gas_price_gwei') unit = ' Gwei';
        
        // 90일 변화율 계산 (현재값 vs 90일 전 3일 평균)
        let changeStr = '';
        if (around90dAvg !== null && around90dAvg !== 0) {
            const change = ((currentVal - around90dAvg) / around90dAvg * 100).toFixed(1);
            changeStr = `(${change > 0 ? '+' : ''}${change}% vs 90d ago)`;
        }
        
        // 값 포맷팅
        let valStr;
        if (typeof currentVal === 'number') {
            if (currentVal >= 1e12) valStr = (currentVal / 1e12).toFixed(2) + 'T';
            else if (currentVal >= 1e9) valStr = (currentVal / 1e9).toFixed(2) + 'B';
            else if (currentVal >= 1e6) valStr = (currentVal / 1e6).toFixed(2) + 'M';
            else if (currentVal >= 1e3) valStr = (currentVal / 1e3).toFixed(2) + 'K';
            else valStr = currentVal.toFixed(2);
        } else {
            valStr = String(currentVal);
        }
        
        prompt += `- ${key}: ${valStr}${unit} ${changeStr}\n`;
        
        // 역사적 통계 추가 (AI가 맥락을 이해하도록)
        const stats = data.historicalStats;
        if (stats) {
            const formatStatVal = (v) => {
                if (v >= 1e12) return (v / 1e12).toFixed(2) + 'T';
                if (v >= 1e9) return (v / 1e9).toFixed(2) + 'B';
                if (v >= 1e6) return (v / 1e6).toFixed(2) + 'M';
                if (v >= 1e3) return (v / 1e3).toFixed(2) + 'K';
                if (Math.abs(v) < 1 && v !== 0) return v.toFixed(4);
                return v.toFixed(2);
            };
            prompt += `  └ 📊 Historical Context (${stats.dataPoints} days):\n`;
            prompt += `    • Current Percentile: ${stats.currentPercentile}% (${stats.currentPercentile < 25 ? 'LOW - bottom quartile' : stats.currentPercentile < 50 ? 'BELOW median' : stats.currentPercentile < 75 ? 'ABOVE median' : 'HIGH - top quartile'})\n`;
            prompt += `    • Range: ${formatStatVal(stats.min)}${unit} ~ ${formatStatVal(stats.max)}${unit}\n`;
            prompt += `    • Median: ${formatStatVal(stats.median)}${unit}, P25: ${formatStatVal(stats.p25)}${unit}, P75: ${formatStatVal(stats.p75)}${unit}\n`;
        }
        
        // 추가 필드들 (같은 테이블에 있는 관련 데이터)
        const latest = data.latest;
        
        // Staking APR
        if (latest.lido_apr !== undefined && fieldName !== 'lido_apr') {
            const change90d = calc90dChangeForField(data, 'lido_apr');
            const changeNote = change90d ? ` (${change90d > 0 ? '+' : ''}${change90d}% vs 90d)` : '';
            prompt += `  └ staking_apr: ${latest.lido_apr?.toFixed(2) || 'N/A'}%${changeNote}\n`;
        }
        
        // Gas Utilization
        if (latest.gas_utilization !== undefined && fieldName !== 'gas_utilization') {
            const change90d = calc90dChangeForField(data, 'gas_utilization');
            const changeNote = change90d ? ` (${change90d > 0 ? '+' : ''}${change90d}% vs 90d)` : '';
            prompt += `  └ gas_utilization: ${latest.gas_utilization?.toFixed(1) || 'N/A'}%${changeNote}\n`;
        }
        
        // Blob Fees
        if (latest.blob_fee_eth !== undefined && fieldName !== 'blob_fee_eth') {
            const change90d = calc90dChangeForField(data, 'blob_fee_eth');
            const changeNote = change90d ? ` (${change90d > 0 ? '+' : ''}${change90d}% vs 90d)` : '';
            prompt += `  └ blob_fees: ${latest.blob_fee_eth?.toFixed(4) || 'N/A'} ETH${changeNote}\n`;
        }
        
        // Realized Price
        if (latest.realized_price !== undefined && key !== 'mvrv' && fieldName !== 'realized_price') {
            const change90d = calc90dChangeForField(data, 'realized_price');
            const changeNote = change90d ? ` (${change90d > 0 ? '+' : ''}${change90d}% vs 90d)` : '';
            prompt += `  └ realized_price: $${latest.realized_price?.toFixed(2) || 'N/A'}${changeNote}\n`;
        }
        
        // MVRV Ratio (for mvrv table)
        if (latest.mvrv_ratio !== undefined && fieldName !== 'mvrv_ratio') {
            const change90d = calc90dChangeForField(data, 'mvrv_ratio');
            const changeNote = change90d ? ` (${change90d > 0 ? '+' : ''}${change90d}% vs 90d)` : '';
            prompt += `  └ mvrv_ratio: ${latest.mvrv_ratio?.toFixed(2) || 'N/A'}${changeNote}\n`;
        }
        
        // ETH Burnt (for gas_burn table)
        if (latest.eth_burnt !== undefined && fieldName !== 'eth_burnt') {
            const change90d = calc90dChangeForField(data, 'eth_burnt');
            const changeNote = change90d ? ` (${change90d > 0 ? '+' : ''}${change90d}% vs 90d)` : '';
            prompt += `  └ eth_burnt: ${latest.eth_burnt?.toFixed(2) || 'N/A'} ETH${changeNote}\n`;
        }
        
        // Open Interest
        if (latest.open_interest !== undefined && fieldName !== 'open_interest') {
            const change90d = calc90dChangeForField(data, 'open_interest');
            const changeNote = change90d ? ` (${change90d > 0 ? '+' : ''}${change90d}% vs 90d)` : '';
            const oiVal = latest.open_interest >= 1e9 ? (latest.open_interest / 1e9).toFixed(2) + 'B' : (latest.open_interest / 1e6).toFixed(2) + 'M';
            prompt += `  └ open_interest: $${oiVal}${changeNote}\n`;
        }
    }
    
    return prompt;
}

/**
 * Call Claude Haiku API to generate commentary
 * @param {string} lang - Language code: 'en', 'ko', 'zh', 'ja'
 */
async function generateCommentary(sectionKey, metricsData, lang = 'en', existingScores = null) {
    if (!ANTHROPIC_API_KEY) return null;
    
    const section = COMMENTARY_SECTIONS[sectionKey];
    const metricsPrompt = formatMetricsForPrompt(sectionKey, metricsData);
    
    const langConfig = {
        en: {
            instruction: 'Write in English.',
            headers: {
                current: '📊 Current Status',
                trend: '📈 90-Day Trend', 
                valuation: '💡 Valuation Insight'
            }
        },
        ko: {
            instruction: 'Write in Korean (한국어로 작성하세요). Use natural Korean financial terminology. IMPORTANT: For blockchain/crypto technical terms (TVL, MVRV, NVT, DeFi, Fear & Greed Index, Funding Rate, etc.), write the Korean translation first, then include the English term in parentheses. Example: 총 예치금(TVL), 시장가치 대 실현가치 비율(MVRV), 공포탐욕지수(Fear & Greed Index).',
            headers: {
                current: '📊 현재 상태',
                trend: '📈 90일 트렌드',
                valuation: '💡 밸류에이션 시사점'
            }
        },
        zh: {
            instruction: 'Write in Simplified Chinese (用简体中文写). Use standard Chinese financial terms. IMPORTANT: For blockchain/crypto technical terms (TVL, MVRV, NVT, DeFi, Fear & Greed Index, Funding Rate, etc.), write the Chinese translation first, then include the English term in parentheses. Example: 总锁定价值(TVL), 市值与实现价值比率(MVRV), 恐惧贪婪指数(Fear & Greed Index).',
            headers: {
                current: '📊 当前状态',
                trend: '📈 90天趋势',
                valuation: '💡 估值启示'
            }
        },
        ja: {
            instruction: 'Write in Japanese (日本語で書いてください). Use appropriate Japanese financial terminology. IMPORTANT: For blockchain/crypto technical terms (TVL, MVRV, NVT, DeFi, Fear & Greed Index, Funding Rate, etc.), write the Japanese translation first, then include the English term in parentheses. Example: 総預かり資産(TVL), 時価総額対実現価値比率(MVRV), 恐怖強欲指数(Fear & Greed Index).',
            headers: {
                current: '📊 現在の状況',
                trend: '📈 90日トレンド',
                valuation: '💡 バリュエーションの示唆'
            }
        }
    };
    
    const config = langConfig[lang] || langConfig.en;
    
    // 영어: 점수 + 본문 생성 / 다른 언어: 본문만 생성
    const isEnglish = (lang === 'en');
    
    let systemPrompt;
    let userPrompt;
    
    if (isEnglish) {
        // 영어: 점수 + 본문 + reasoning 생성
        systemPrompt = `You are an expert Ethereum market analyst. Write analysis for the "${section.title}" section.

STRICT OUTPUT FORMAT:
You must output a JSON object with scores, reasoning for each score, AND 3 paragraphs.

REQUIRED JSON FORMAT (output ONLY this, no markdown):
{"scores":[X,Y,Z],"reasoning":["reason for score1","reason for score2","reason for score3"],"text":"paragraph1|||paragraph2|||paragraph3"}

SCORE DEFINITIONS (0-100 scale, 50 is neutral):
- Score 1 (Current Status - Market Temperature): 0-44=cold/fear, 45-55=neutral, 56-100=hot/greed
  * Based on: Fear & Greed Index, Funding Rate, current percentile vs historical
  * Low score = market fear/cooling, High score = market greed/overheating
  
- Score 2 (90-Day Trend - Momentum): 0-44=downtrend, 45-55=sideways, 56-100=uptrend  
  * Based on: 90-day % changes in the data
  * Changes > +20% → high score (70-90), Changes < -20% → low score (10-30)
  
- Score 3 (Valuation - Bullish/Bearish Signal): 0-44=bearish, 45-55=neutral, 56-100=bullish
  * IMPORTANT: Use the "Current Percentile" data provided
  * Percentile < 25% (bottom quartile) = UNDERVALUED → Bullish score (70-90)
  * Percentile 25-50% = Slightly undervalued → Score 55-70
  * Percentile 50-75% = Slightly overvalued → Score 30-45
  * Percentile > 75% (top quartile) = OVERVALUED → Bearish score (10-30)

REASONING FORMAT:
- Each reasoning string should be 1-2 sentences explaining why you chose that score
- Include specific data points that influenced the score (percentiles, % changes)
- Example: "Score 65: Fear & Greed at 72 (75th percentile) indicates greed, Funding Rate positive at 0.01%"
- Example: "Score 28: 90d change of -35% across key metrics shows strong downtrend"
- Example: "Score 78: MVRV at 22nd percentile (historically low) suggests undervaluation"

SCORING GUIDELINES USING HISTORICAL CONTEXT:
- The data includes "Current Percentile" for each metric (0-100%)
- Percentile shows where current value sits in 3-year history
- For VALUATION score: Lower percentile = more undervalued = HIGHER score (inverted!)
- For SENTIMENT score: Use Fear & Greed percentile directly
- For TREND score: Focus on 90-day % change direction and magnitude

CRITICAL RULES:
- ${config.instruction}
- Output ONLY valid JSON, no markdown code blocks
- scores array must have exactly 3 integers between 0-100
- reasoning array must have exactly 3 strings explaining each score
- text field contains 3 paragraphs separated by |||
- EACH PARAGRAPH MUST HAVE EXACTLY 5 SENTENCES - this is mandatory
- ⚠️ ABSOLUTELY NO NUMBERS IN TEXT paragraphs - describe trends qualitatively only
- ⚠️ Use descriptive words: "historically low", "near all-time highs", "below median"
- Professional analyst tone, qualitative analysis only
- Minimum 180 words per paragraph`;

        userPrompt = `Analyze these ${section.title} metrics. Output JSON with scores, reasoning, and text.

${section.context ? `CRITICAL CONTEXT FOR THIS SECTION:\n${section.context}\n\n` : ''}${metricsPrompt}

IMPORTANT REQUIREMENTS:
1. Each paragraph MUST contain exactly 5 sentences
2. Provide clear reasoning for each score using the data above
3. Output format: {"scores":[X,Y,Z],"reasoning":["...","...","..."],"text":"para1|||para2|||para3"}

Remember: Score meanings
- Score 1 (Current Status): cold(0-44) / neutral(45-55) / hot(56-100)
- Score 2 (90-Day Trend): down(0-44) / sideways(45-55) / up(56-100)
- Score 3 (Valuation): bearish(0-44) / neutral(45-55) / bullish(56-100)`;
    } else {
        // 다른 언어: 본문만 생성 (점수는 영어에서 이미 생성됨)
        systemPrompt = `You are an expert Ethereum market analyst. Write analysis for the "${section.title}" section.

OUTPUT FORMAT:
Write exactly 3 paragraphs separated by ||| (three pipe characters).
Output ONLY the text, no JSON, no scores.

Example output format:
paragraph1 text here|||paragraph2 text here|||paragraph3 text here

CRITICAL RULES:
- ${config.instruction}
- EACH PARAGRAPH MUST HAVE EXACTLY 5 SENTENCES - this is mandatory
- Paragraph 1 (Current Status): Focus on TODAY's spot data primarily, with brief 7-day context
- Paragraph 2 (Trend): Focus on 90-DAY trends, medium-term direction
- Paragraph 3 (Valuation): Investment implications, bullish/bearish outlook
- ⚠️ ABSOLUTELY NO NUMBERS IN TEXT - describe trends qualitatively (상승/하락/횡보, 上涨/下跌/横盘, 上昇/下落/横ばい)
- ⚠️ DO NOT include any specific percentages, dollar amounts, ratios, or numerical values
- ⚠️ Use descriptive words only
- Professional analyst tone, qualitative analysis only
- Minimum 180 words per paragraph`;

        userPrompt = `Analyze these ${section.title} metrics. Output ONLY 3 paragraphs separated by |||

${section.context ? `CRITICAL CONTEXT FOR THIS SECTION:\n${section.context}\n\n` : ''}${metricsPrompt}

IMPORTANT: 
1. Each paragraph MUST contain exactly 5 sentences
2. Output format: para1|||para2|||para3 (no JSON, no scores)
3. NO NUMBERS in the text - use qualitative descriptions only`;
    }

    try {
        const response = await fetch('https://api.anthropic.com/v1/messages', {
            method: 'POST',
            headers: {
                'Content-Type': 'application/json',
                'x-api-key': ANTHROPIC_API_KEY,
                'anthropic-version': '2023-06-01'
            },
            body: JSON.stringify({
                model: 'claude-haiku-4-5-20251001',
                max_tokens: 3000,
                messages: [
                    { role: 'user', content: userPrompt }
                ],
                system: systemPrompt
            })
        });
        
        if (!response.ok) {
            const errorText = await response.text();
            console.error(`  Claude API error: ${response.status} - ${errorText}`);
            return null;
        }
        
        const result = await response.json();
        const content = result.content?.[0]?.text || null;
        
        if (!content) return null;
        
        if (isEnglish) {
            // 영어: JSON 파싱 (scores + reasoning + text)
            try {
                const parsed = JSON.parse(content);
                return {
                    scores: parsed.scores || [50, 50, 50],
                    reasoning: parsed.reasoning || ['No reasoning provided', 'No reasoning provided', 'No reasoning provided'],
                    text: parsed.text || content
                };
            } catch (e) {
                console.warn('  JSON parse failed, using text fallback');
                return {
                    scores: [50, 50, 50],
                    reasoning: ['Parse error', 'Parse error', 'Parse error'],
                    text: content
                };
            }
        } else {
            // 다른 언어: 텍스트만 반환 (기존 scores 사용)
            return {
                scores: existingScores || [50, 50, 50],
                reasoning: null, // 다른 언어는 reasoning 없음
                text: content
            };
        }
        
    } catch (e) {
        console.error(`  Claude API call failed:`, e.message);
        return null;
    }
}

/**
 * Save commentary to Supabase (with multilingual support, scores, and reasoning)
 */
async function saveCommentary(sectionKey, commentaries, scores, reasoning, metricsSnapshot) {
    const today = new Date().toISOString().split('T')[0];
    
    try {
        const { error } = await supabase
            .from('daily_commentary')
            .upsert({
                date: today,
                section_key: sectionKey,
                commentary: commentaries.en,
                commentary_ko: commentaries.ko || null,
                commentary_zh: commentaries.zh || null,
                commentary_ja: commentaries.ja || null,
                scores: scores || [50, 50, 50],
                score_reasoning: reasoning || null,
                metrics_snapshot: metricsSnapshot,
                created_at: new Date().toISOString()
            }, { onConflict: 'date,section_key' });
        
        if (error) {
            console.error(`  Error saving commentary for ${sectionKey}:`, error.message);
            return false;
        }
        return true;
    } catch (e) {
        console.error(`  Error saving commentary:`, e.message);
        return false;
    }
}

/**
 * Generate all section commentaries (4 languages: EN, KO, ZH, JA)
 */
async function generateAllCommentaries() {
    if (!ANTHROPIC_API_KEY) {
        console.log('\n⏭️ Skipping AI commentary - No ANTHROPIC_API_KEY');
        return { success: 0, failed: 0 };
    }
    
    console.log('\n' + '='.repeat(60));
    console.log('🤖 AI DAILY COMMENTARY GENERATION (Claude Haiku)');
    console.log('   Generating 4 languages: EN, KO, ZH, JA');
    console.log('='.repeat(60));
    
    const LANGUAGES = ['en', 'ko', 'zh', 'ja'];
    let success = 0, failed = 0;
    
    for (const sectionKey of Object.keys(COMMENTARY_SECTIONS)) {
        const section = COMMENTARY_SECTIONS[sectionKey];
        console.log(`\n📝 [${sectionKey}] ${section.title}...`);
        
        // Fetch metrics
        const metricsData = await fetchSectionMetrics(sectionKey);
        if (!metricsData || Object.keys(metricsData).length === 0) {
            console.log(`  ❌ No metrics data available`);
            failed++;
            continue;
        }
        
        console.log(`  ✓ Fetched ${Object.keys(metricsData).length} metric groups`);
        
        // Generate commentary for each language
        // 영어 먼저 생성해서 점수 확정, 다른 언어는 같은 점수 사용
        const commentaries = {};
        let scores = [50, 50, 50]; // Default scores
        let reasoning = null;
        
        // 1. 영어 먼저 생성 (점수 + reasoning 포함)
        const enResult = await generateCommentary(sectionKey, metricsData, 'en', null);
        if (enResult) {
            commentaries.en = enResult.text;
            scores = enResult.scores;
            reasoning = enResult.reasoning;
            console.log(`  ✓ EN: ${enResult.text.length} chars, scores: [${scores.join(',')}]`);
            if (reasoning) {
                console.log(`  📝 Reasoning:`);
                reasoning.forEach((r, i) => console.log(`     Score ${i+1}: ${r}`));
            }
        } else {
            console.log(`  ❌ Failed to generate English commentary`);
            failed++;
            continue;
        }
        await sleep(500);
        
        // 2. 다른 언어 생성 (영어 점수 전달, 본문만 생성)
        for (const lang of ['ko', 'zh', 'ja']) {
            const result = await generateCommentary(sectionKey, metricsData, lang, scores);
            if (result) {
                commentaries[lang] = result.text;
                console.log(`  ✓ ${lang.toUpperCase()}: ${result.text.length} chars`);
            } else {
                console.log(`  ⚠️ ${lang.toUpperCase()}: Failed`);
            }
            await sleep(500); // Rate limit between API calls
        }
        
        // Save to Supabase (with scores and reasoning)
        const saved = await saveCommentary(sectionKey, commentaries, scores, reasoning, metricsData);
        if (saved) {
            console.log(`  ✅ Saved to Supabase (${Object.keys(commentaries).length} languages)`);
            success++;
        } else {
            failed++;
        }
        
        // Rate limit: wait between sections
        await sleep(1000);
    }
    
    console.log('\n' + '-'.repeat(40));
    console.log(`📊 Commentary: ✅ ${success}/7  |  ❌ ${failed}/7`);
    
    return { success, failed };
}

// Dune Query IDs
const DUNE_QUERIES = {
    // Price Data (Binance/CoinGecko 대체)
    ETH_PRICE: 6390291,      // ETHval - ETH Daily Price
    ETH_BTC_RATIO: 6390302,  // ETHval - ETH BTC Ratio
    
    // Existing queries
    BLOB: 6350774,
    // TX_VOLUME: 6350858,  // ARCHIVED - Use L1_TOTAL_VOLUME instead
    ACTIVE_ADDR: 6352303,
    L2_ACTIVE_ADDR: 6352308,
    // L2_TX_VOLUME: 6352386,  // REMOVED - Use L2 Total Volume (6386591) native_volume_usd instead
    BRIDGE_VOLUME: 6352417,
    WHALE_TX: 6352498,
    NEW_ADDR: 6352513,
    MVRV: 6354057,
    STABLECOIN_VOL: 6353868,
    GAS_PRICE: 6354506,  // Daily average gas price
    
    // New queries
    L1_TOTAL_VOLUME: 6386589,     // L1 Total Volume (ETH + ERC-20) - for NVT
    L2_TOTAL_VOLUME: 6386591,     // L2 Total Volume (all chains)
    L2_DEX_VOLUME: 6395472,       // L2 DEX Volume (all chains)
    BRIDGE_TOTAL_VOLUME: 6395474  // Bridge Total Volume (ETH + ERC-20)
};

// ============================================================
// Helper Functions
// ============================================================
async function fetchJSON(url, retries = 3) {
    const urlShort = url.split('?')[0].replace('https://', '').substring(0, 50);
    
    for (let i = 0; i < retries; i++) {
        try {
            const controller = new AbortController();
            const timeout = setTimeout(() => controller.abort(), 30000);
            const res = await fetch(url, {
                signal: controller.signal,
                headers: { 'User-Agent': 'ETHval/7.2', 'Accept': 'application/json' }
            });
            clearTimeout(timeout);
            
            if (!res.ok) {
                console.error(`  ⚠️ HTTP ${res.status} from ${urlShort}`);
                if (res.status === 429) {
                    console.error(`  ⚠️ Rate limited! Waiting ${5 * (i + 1)}s...`);
                    await sleep(5000 * (i + 1)); // Rate limit: 더 긴 대기
                    continue;
                }
                throw new Error(`HTTP ${res.status}`);
            }
            return await res.json();
        } catch (e) {
            console.error(`  ⚠️ Fetch error (attempt ${i + 1}/${retries}): ${e.message}`);
            if (i < retries - 1) await sleep(2000 * (i + 1));
        }
    }
    console.error(`  ❌ Failed after ${retries} attempts: ${urlShort}`);
    return null;
}

// Dune API helper - fetch all results with pagination
// Note: Dune queries are scheduled to auto-refresh daily at 03:30-04:00 UTC
async function fetchDuneResults(queryId, maxRows = 10000, maxStaleDays = 2) {
    if (!DUNE_API_KEY) {
        console.log('  ⚠️ No DUNE_API_KEY');
        return null;
    }
    
    try {
        // 1. Check if cache is stale and needs refresh
        // First get metadata to know total rows
        const metaUrl = `https://api.dune.com/api/v1/query/${queryId}/results?limit=1`;
        const metaResponse = await fetch(metaUrl, { 
            headers: { 'X-Dune-API-Key': DUNE_API_KEY }
        });
        
        if (metaResponse.ok) {
            const metaData = await metaResponse.json();
            const totalRows = metaData?.result?.metadata?.total_row_count || 100;
            
            // Fetch last 20 rows to find latest date (handles ASC sorted results)
            const offset = Math.max(0, totalRows - 20);
            const checkUrl = `https://api.dune.com/api/v1/query/${queryId}/results?limit=20&offset=${offset}`;
            const checkResponse = await fetch(checkUrl, { 
                headers: { 'X-Dune-API-Key': DUNE_API_KEY }
            });
            
            if (checkResponse.ok) {
                const checkData = await checkResponse.json();
                const rows = checkData?.result?.rows || [];
                if (rows.length > 0) {
                    // Find the latest date
                    let latestDateStr = '';
                    let latestTime = 0;
                    for (const row of rows) {
                        const dateVal = row.block_date || row.date || row.day || '';
                        const dateStr = String(dateVal).split('T')[0].split(' ')[0];
                        const time = new Date(dateStr).getTime();
                        if (time > latestTime) {
                            latestTime = time;
                            latestDateStr = dateStr;
                        }
                    }
                    
                    const daysDiff = Math.floor((Date.now() - latestTime) / (24*60*60*1000));
                    
                    if (daysDiff > maxStaleDays) {
                        console.log(`  ⚠️ Data stale: ${latestDateStr} (${daysDiff}d ago), refreshing...`);
                        
                        // Execute fresh query
                        const execResponse = await fetch(`https://api.dune.com/api/v1/query/${queryId}/execute`, {
                            method: 'POST',
                            headers: { 
                                'X-Dune-API-Key': DUNE_API_KEY,
                                'Content-Type': 'application/json'
                            },
                            body: JSON.stringify({ performance: 'medium' })
                        });
                        
                        if (execResponse.ok) {
                            const execData = await execResponse.json();
                            const executionId = execData.execution_id;
                            console.log(`  ⏳ Execution started: ${executionId}`);
                            
                            // Poll for completion (max 2 min)
                            for (let i = 0; i < 24; i++) {
                                await sleep(5000);
                                const statusResponse = await fetch(`https://api.dune.com/api/v1/execution/${executionId}/status`, {
                                    headers: { 'X-Dune-API-Key': DUNE_API_KEY }
                                });
                                if (!statusResponse.ok) continue;
                                
                                const statusData = await statusResponse.json();
                                if (statusData.state === 'QUERY_STATE_COMPLETED') {
                                    console.log('  ✅ Query refresh completed');
                                    break;
                                } else if (statusData.state === 'QUERY_STATE_FAILED') {
                                    console.log('  ❌ Query refresh failed');
                                    break;
                                }
                                console.log(`  ⏳ Waiting... (${statusData.state})`);
                            }
                        }
                    } else {
                        console.log(`  📅 Latest: ${latestDateStr} (${daysDiff}d ago) ✓`);
                    }
                }
            }
        }
        
        // 2. Fetch all results
        const allRows = [];
        const pageSize = 1000;
        let offset = 0;
        
        while (offset < maxRows) {
            const url = `https://api.dune.com/api/v1/query/${queryId}/results?limit=${pageSize}&offset=${offset}`;
            const response = await fetch(url, { 
                headers: { 'X-Dune-API-Key': DUNE_API_KEY },
                timeout: 30000
            });
            
            if (!response.ok) {
                const errorText = await response.text().catch(() => 'no body');
                console.error(`  ❌ Dune API error: ${response.status} - ${errorText.slice(0, 200)}`);
                break;
            }
            
            const data = await response.json();
            
            if (offset === 0) {
                const state = data?.state || data?.execution_id ? 'has execution' : 'direct result';
                console.log(`  📡 Query ${queryId}: state=${state}, has_result=${!!data?.result}`);
                if (data?.result?.rows?.length > 0) {
                    console.log(`  📋 Columns: ${Object.keys(data.result.rows[0]).join(', ')}`);
                }
            }
            
            const rows = data?.result?.rows || [];
            
            if (rows.length === 0) {
                if (offset === 0) {
                    console.log(`  ⚠️ Query ${queryId} returned 0 rows (state: ${data?.state || 'unknown'})`);
                }
                break;
            }
            
            allRows.push(...rows);
            offset += pageSize;
            
            if (rows.length < pageSize) break;
            await sleep(500);
        }
        
        console.log(`  📊 Total rows fetched: ${allRows.length}`);
        return allRows;
    } catch (e) {
        console.error(`  ❌ Dune fetch error for query ${queryId}: ${e.message}`);
        return null;
    }
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
// 1. ETH Price (Dune API - 안정적)
// ============================================================
async function collect_eth_price() {
    if (!DUNE_API_KEY) {
        return result.skip('No Dune API key');
    }
    
    try {
        // Dune API로 ETH 가격 데이터 가져오기
        const response = await fetch(
            `https://api.dune.com/api/v1/query/${DUNE_QUERIES.ETH_PRICE}/results?limit=1500`,
            { headers: { 'X-Dune-API-Key': DUNE_API_KEY } }
        );
        
        if (!response.ok) {
            throw new Error(`Dune API error: ${response.status}`);
        }
        
        const data = await response.json();
        
        if (!data?.result?.rows || data.result.rows.length === 0) {
            throw new Error('No data from Dune');
        }
        
        const records = data.result.rows.map(row => {
            // Parse date and create timestamp
            const dateStr = typeof row.date === 'string' ? row.date.split(' ')[0] : row.date;
            const timestamp = new Date(dateStr).getTime();
            
            return {
                date: dateStr,
                timestamp: timestamp,
                open: parseFloat(row.open) || parseFloat(row.avg_price),
                high: parseFloat(row.high) || parseFloat(row.avg_price),
                low: parseFloat(row.low) || parseFloat(row.avg_price),
                close: parseFloat(row.close) || parseFloat(row.avg_price),
                volume: 0  // Dune에서 volume 없음
            };
        });
        
        if (records.length > 100) {
            const saved = await upsertBatch('historical_eth_price', records);
            return result.ok(saved);
        }
        
        throw new Error('Insufficient data');
    } catch (e) {
        // 실패 시 기존 데이터 유지
        const { data: existing } = await supabase
            .from('historical_eth_price')
            .select('date')
            .order('date', { ascending: false })
            .limit(1);
        
        if (existing?.length > 0) {
            return result.skip(`Dune error: ${e.message}`);
        }
        return result.fail(e.message);
    }
}

// ============================================================
// 2. Ethereum TVL (DefiLlama)
// ============================================================
async function collect_ethereum_tvl() {
    const data = await fetchJSON('https://api.llama.fi/v2/historicalChainTvl/Ethereum');
    if (!data) return 0;
    const records = data.filter(d => d.date > cutoff3Y() && d.tvl > 0).map(d => ({
        date: new Date(d.date * 1000).toISOString().split('T')[0],
        tvl: parseFloat(d.tvl.toFixed(2))
    }));
    return await upsertBatch('historical_ethereum_tvl', records);
}

// ============================================================
// 3. L2 TVL (DefiLlama)
// ============================================================
async function collect_l2_tvl() {
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
// 4. DeFi Protocol Revenue (DefiLlama) - Uniswap, Aave, Lido etc. NOT L1 gas fees
// ============================================================
async function collect_protocol_fees() {
    // Try new API endpoint first (overview/fees with chain filter)
    let data = await fetchJSON('https://api.llama.fi/overview/fees/ethereum?dataType=dailyFees');
    
    // Fallback to old endpoint if new one fails
    if (!data?.totalDataChart) {
        console.log('  ⚠️ New fees API failed, trying legacy endpoint...');
        data = await fetchJSON('https://api.llama.fi/summary/fees/ethereum?dataType=dailyFees');
    }
    
    if (!data?.totalDataChart) {
        console.log('  ❌ Both fees API endpoints failed');
        return result.fail('No fees data available from DefiLlama');
    }
    
    const records = data.totalDataChart.filter(d => d[1] > 0).map(d => ({
        date: new Date(d[0] * 1000).toISOString().split('T')[0],
        fees: parseFloat(d[1].toFixed(2))
    }));
    
    if (records.length === 0) {
        return result.fail('No valid fees records');
    }
    
    return await upsertBatch('historical_protocol_fees', records);
}

// ============================================================
// 5. Staking Data (DefiLlama Yields API - admin.html 방식)
// ============================================================
async function collect_staking() {
    
    // Primary: DefiLlama yields API (APR + TVL 동시에)
    const yieldData = await fetchJSON('https://yields.llama.fi/chart/747c1d2a-c668-4682-b9f9-296708a3dd90');
    
    if (!yieldData?.data || yieldData.data.length === 0) {
        console.log('  ⚠️ DefiLlama yields API failed, trying Lido protocol...');
        
        // Fallback: Lido protocol TVL
        const lidoData = await fetchJSON('https://api.llama.fi/protocol/lido');
        if (!lidoData?.tvl || lidoData.tvl.length === 0) {
            console.log('  ❌ DefiLlama & Lido APIs failed');
            return result.fail('No staking data available');
        }
        
        const { data: prices } = await supabase.from('historical_eth_price').select('date, close').order('date', { ascending: false }).limit(1100);
        const priceMap = new Map(prices?.map(p => [p.date, parseFloat(p.close)]) || []);
        
        const cutoff = Date.now() / 1000 - (1095 * 86400);
        const records = [];
        
        for (const point of lidoData.tvl) {
            if (point.date < cutoff) continue;
            
            const date = new Date(point.date * 1000).toISOString().split('T')[0];
            const lidoTvlUsd = point.totalLiquidityUSD || 0;
            const price = priceMap.get(date) || 3500;
            
            if (lidoTvlUsd <= 0) continue;
            
            const lidoStakedEth = lidoTvlUsd / price;
            const totalStakedEth = lidoStakedEth / 0.28; // Lido ~28% market share
            const totalValidators = Math.round(totalStakedEth / 32);
            
            records.push({
                date,
                total_staked_eth: Math.round(totalStakedEth),
                total_validators: totalValidators,
                avg_apr: 3.5, // Fallback APR
                source: 'defillama-lido'
            });
        }
        
        // Dedupe
        const seen = new Set();
        const uniqueRecords = records.filter(r => {
            if (seen.has(r.date)) return false;
            seen.add(r.date);
            return true;
        });
        
        console.log(`  📦 ${uniqueRecords.length} staking records (from Lido fallback)`);
        return await upsertBatch('historical_staking', uniqueRecords);
    }
    
    // Get ETH prices for TVL calculation
    const { data: prices } = await supabase.from('historical_eth_price').select('date, close').order('date', { ascending: false }).limit(1100);
    const priceMap = new Map(prices?.map(p => [p.date, parseFloat(p.close)]) || []);
    
    const cutoff = Date.now() - (1095 * 24 * 60 * 60 * 1000);
    const records = [];
    
    // Lido market share varies by year
    const getMarketShare = (date) => {
        const year = new Date(date).getFullYear();
        if (year <= 2022) return 0.30;
        if (year === 2023) return 0.32;
        return 0.28;
    };
    
    for (const point of yieldData.data) {
        const timestamp = new Date(point.timestamp).getTime();
        if (timestamp < cutoff) continue;
        
        const date = point.timestamp.split('T')[0];
        const lidoTvlUsd = point.tvlUsd || 0;
        const apr = point.apy || 0;
        const price = priceMap.get(date) || 3500;
        
        if (lidoTvlUsd <= 0) continue;
        
        const lidoStakedEth = lidoTvlUsd / price;
        const marketShare = getMarketShare(date);
        const totalStakedEth = lidoStakedEth / marketShare;
        const totalValidators = Math.round(totalStakedEth / 32);
        
        records.push({
            date,
            total_staked_eth: Math.round(totalStakedEth),
            total_validators: totalValidators,
            avg_apr: parseFloat(apr.toFixed(2)),
            source: 'defillama'
        });
    }
    
    // Dedupe
    const seen = new Set();
    const uniqueRecords = records.filter(r => {
        if (seen.has(r.date)) return false;
        seen.add(r.date);
        return true;
    });
    
    console.log(`  📦 ${uniqueRecords.length} staking records with APR`);
    return await upsertBatch('historical_staking', uniqueRecords);
}

// ============================================================
// 6. Gas & Burn (Etherscan API for gas utilization)
// ============================================================
async function collect_gas_burn() {
    
    const ETHERSCAN_API_KEY = process.env.ETHERSCAN_API_KEY;
    
    // 1. 먼저 기존 데이터에서 마지막 날짜 확인
    const { data: existing } = await supabase.from('historical_gas_burn')
        .select('date')
        .order('date', { ascending: false })
        .limit(1);
    
    const lastDate = existing?.[0]?.date || '2022-01-01';
    const startDate = new Date(lastDate);
    startDate.setDate(startDate.getDate() + 1);
    const endDate = new Date();
    endDate.setDate(endDate.getDate() - 1); // 어제까지
    
    if (startDate >= endDate) {
        console.log('  ✅ Already up to date');
        return result.skip('Already up to date');
    }
    
    const startStr = startDate.toISOString().split('T')[0];
    const endStr = endDate.toISOString().split('T')[0];
    console.log(`  📅 Fetching ${startStr} to ${endStr}`);
    
    // 2. Etherscan API로 Gas Utilization 가져오기
    let gasUtilData = [];
    let gasPriceData = [];
    if (ETHERSCAN_API_KEY) {
        // Gas Utilization
        const utilUrl = `https://api.etherscan.io/api?module=stats&action=dailynetutilization&startdate=${startStr}&enddate=${endStr}&sort=asc&apikey=${ETHERSCAN_API_KEY}`;
        const utilRes = await fetchJSON(utilUrl);
        if (utilRes?.status === '1' && utilRes.result) {
            gasUtilData = utilRes.result;
            console.log(`  📊 Got ${gasUtilData.length} days of gas utilization from Etherscan`);
        }
        
        // Daily Average Gas Price (Wei -> Gwei)
        await sleep(250); // Rate limit
        const gasPriceUrl = `https://api.etherscan.io/api?module=stats&action=dailyavggasprice&startdate=${startStr}&enddate=${endStr}&sort=asc&apikey=${ETHERSCAN_API_KEY}`;
        const gasPriceRes = await fetchJSON(gasPriceUrl);
        if (gasPriceRes?.status === '1' && gasPriceRes.result) {
            gasPriceData = gasPriceRes.result;
            console.log(`  ⛽ Got ${gasPriceData.length} days of gas price from Etherscan`);
        }
    } else {
        console.log('  ⚠️ ETHERSCAN_API_KEY not set, skipping gas data');
    }
    
    // 3. fees/price 데이터로 ETH burnt 계산
    const { data: fees } = await supabase.from('historical_protocol_fees').select('date, fees').order('date');
    const { data: prices } = await supabase.from('historical_eth_price').select('date, close').order('date');
    if (!fees || !prices) return 0;
    
    const priceMap = new Map();
    prices.forEach(p => priceMap.set(p.date, parseFloat(p.close)));
    
    const gasUtilMap = new Map();
    gasUtilData.forEach(d => {
        gasUtilMap.set(d.UTCDate, parseFloat(d.networkUtilization) * 100);
    });
    
    const gasPriceMap = new Map();
    gasPriceData.forEach(d => {
        // gasPrice is in Wei, convert to Gwei (1 Gwei = 1e9 Wei)
        const gasPriceWei = parseFloat(d.avgGasPrice_Wei || 0);
        const gasPriceGwei = gasPriceWei / 1e9;
        if (gasPriceGwei > 0 && gasPriceGwei < 1000) {
            gasPriceMap.set(d.UTCDate, parseFloat(gasPriceGwei.toFixed(2)));
        }
    });
    
    const records = [];
    for (const f of fees) {
        if (f.date < startStr || f.date > endStr) continue;
        
        const price = priceMap.get(f.date);
        if (!price || !f.fees) continue;
        
        const burn = (f.fees * 0.80) / price;
        if (burn >= 50 && burn <= 50000) {
            records.push({
                date: f.date,
                eth_burnt: parseFloat(burn.toFixed(2)),
                avg_gas_price_gwei: gasPriceMap.get(f.date) || null,
                gas_utilization: gasUtilMap.get(f.date) || null,
                transaction_count: null,
                source: gasPriceMap.has(f.date) ? 'etherscan' : 'calculated'
            });
        }
    }
    
    if (records.length === 0) {
        console.log('  ✅ Already up to date');
        return result.skip('No new data needed');
    }
    
    console.log(`  📦 Saving ${records.length} records (${gasPriceMap.size} with gas price)`);
    const saved = await upsertBatch('historical_gas_burn', records);
    return result.ok(saved);
}

// ============================================================
// 7. Active Addresses (Etherscan or estimate)
// ============================================================
async function collect_active_addresses() {
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
    const data = await fetchJSON('https://api.alternative.me/fng/?limit=1095&format=json');
    
    if (data?.data && data.data.length > 10) {
        console.log(`  📦 Got ${data.data.length} records from API`);
        const records = data.data.map(d => ({
            date: new Date(parseInt(d.timestamp) * 1000).toISOString().split('T')[0],
            value: parseInt(d.value),
            classification: d.value_classification,
            source: 'alternative_me'
        }));
        return await upsertBatch('historical_fear_greed', records);
    }
    
    // Fallback: ETH 가격 변동 기반 추정
    console.log('  ⚠️ API failed, generating price-based estimates...');
    const { data: prices } = await supabase.from('historical_eth_price')
        .select('date, close')
        .order('date', { ascending: true })
        .limit(1100);
    
    if (!prices || prices.length < 30) {
        console.log('  ❌ Not enough price data for fallback');
        return 0;
    }
    
    const records = [];
    for (let i = 30; i < prices.length; i++) {
        const current = prices[i].close;
        const prev30 = prices[i - 30].close;
        const change30d = ((current - prev30) / prev30) * 100;
        
        // 30일 변동률 기반 Fear & Greed 추정
        let value;
        if (change30d < -30) value = 10 + Math.random() * 10;
        else if (change30d < -15) value = 20 + (change30d + 30) / 15 * 20;
        else if (change30d < -5) value = 40 + (change30d + 15) / 10 * 10;
        else if (change30d < 5) value = 45 + (change30d + 5) / 10 * 10;
        else if (change30d < 15) value = 55 + (change30d - 5) / 10 * 10;
        else if (change30d < 30) value = 65 + (change30d - 15) / 15 * 15;
        else value = 80 + Math.min(15, (change30d - 30) / 20 * 15);
        
        value = Math.max(5, Math.min(95, Math.round(value)));
        
        let classification;
        if (value < 25) classification = 'Extreme Fear';
        else if (value < 40) classification = 'Fear';
        else if (value < 60) classification = 'Neutral';
        else if (value < 75) classification = 'Greed';
        else classification = 'Extreme Greed';
        
        records.push({
            date: prices[i].date,
            value,
            classification,
            source: 'estimated'
        });
    }
    
    console.log(`  📦 Generated ${records.length} estimated records`);
    return await upsertBatch('historical_fear_greed', records);
}

// ============================================================
// 10. DEX Volume (DefiLlama)
// ============================================================
async function collect_dex_volume() {
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
// 13. ETH/BTC Ratio (Dune API - 안정적)
// ============================================================
async function collect_eth_btc() {
    if (!DUNE_API_KEY) {
        return result.skip('No Dune API key');
    }
    
    try {
        // Dune API로 ETH/BTC 비율 데이터 가져오기
        const response = await fetch(
            `https://api.dune.com/api/v1/query/${DUNE_QUERIES.ETH_BTC_RATIO}/results?limit=1500`,
            { headers: { 'X-Dune-API-Key': DUNE_API_KEY } }
        );
        
        if (!response.ok) {
            throw new Error(`Dune API error: ${response.status}`);
        }
        
        const data = await response.json();
        
        if (!data?.result?.rows || data.result.rows.length === 0) {
            throw new Error('No data from Dune');
        }
        
        const records = data.result.rows.map(row => ({
            date: row.date,
            ratio: parseFloat(row.ratio),
            source: 'dune'
        }));
        
        if (records.length > 100) {
            const saved = await upsertBatch('historical_eth_btc', records);
            return result.ok(saved);
        }
        
        throw new Error('Insufficient data');
    } catch (e) {
        // 실패 시 기존 데이터 유지
        const { data: existing } = await supabase
            .from('historical_eth_btc')
            .select('date')
            .order('date', { ascending: false })
            .limit(1);
        
        if (existing?.length > 0) {
            return result.skip(`Dune error: ${e.message}`);
        }
        return result.fail(e.message);
    }
}

// ============================================================
// 14. Funding Rate (CryptoQuant API via Cloudflare Proxy)
// ============================================================
async function collect_funding_rate() {
    const PROXY_URL = 'https://cryptoquant-proxy.seojoon-kim.workers.dev';
    
    try {
        const response = await fetch(
            `${PROXY_URL}/?endpoint=/v1/eth/market-data/funding-rates&window=day&exchange=all_exchange&limit=1095`
        );
        
        if (!response.ok) {
            throw new Error(`Proxy error: ${response.status}`);
        }
        
        const data = await response.json();
        
        if (data.status?.code !== 200) {
            throw new Error(data.status?.message || 'API error');
        }
        
        const rows = data?.result?.data || [];
        
        if (rows.length === 0) {
            throw new Error('No data from CryptoQuant');
        }
        
        console.log(`  📦 Got ${rows.length} funding rate records from CryptoQuant`);
        
        const records = rows.map(row => ({
            date: row.date,
            funding_rate: parseFloat(row.funding_rates || 0),
            source: 'cryptoquant'
        })).filter(r => r.date && !isNaN(r.funding_rate));
        
        if (records.length > 100) {
            const saved = await upsertBatch('historical_funding_rate', records);
            return result.ok(saved);
        }
        
        throw new Error('Insufficient data');
    } catch (e) {
        console.log(`  ❌ funding_rate: ${e.message}`);
        return result.fail(e.message);
    }
}

// ============================================================
// 15. Exchange Reserve (CryptoQuant API via Cloudflare Proxy)
// ============================================================
async function collect_exchange_reserve() {
    const PROXY_URL = 'https://cryptoquant-proxy.seojoon-kim.workers.dev';
    
    try {
        const response = await fetch(
            `${PROXY_URL}/?endpoint=/v1/eth/exchange-flows/reserve&window=day&exchange=all_exchange&limit=1095`
        );
        
        if (!response.ok) {
            throw new Error(`Proxy error: ${response.status}`);
        }
        
        const data = await response.json();
        
        if (data.status?.code !== 200) {
            throw new Error(data.status?.message || 'API error');
        }
        
        const rows = data?.result?.data || [];
        
        if (rows.length === 0) {
            throw new Error('No data from CryptoQuant');
        }
        
        console.log(`  📦 Got ${rows.length} exchange reserve records from CryptoQuant`);
        
        const records = rows.map(row => ({
            date: row.date,
            reserve_eth: parseFloat(row.reserve || 0),
            source: 'cryptoquant'
        })).filter(r => r.date && !isNaN(r.reserve_eth) && r.reserve_eth > 0);
        
        if (records.length > 100) {
            const saved = await upsertBatch('historical_exchange_reserve', records);
            return result.ok(saved);
        }
        
        throw new Error('Insufficient data');
    } catch (e) {
        console.log(`  ❌ exchange_reserve: ${e.message}`);
        return result.fail(e.message);
    }
}

// ============================================================
// 16. ETH Dominance (CoinGecko - admin.html과 동일)
// ============================================================
async function collect_eth_dominance() {
    try {
        // CoinGecko API (admin.html과 동일)
        const [ethData, btcData, globalData] = await Promise.all([
            fetchJSON('https://api.coingecko.com/api/v3/coins/ethereum/market_chart?vs_currency=usd&days=365&interval=daily'),
            fetchJSON('https://api.coingecko.com/api/v3/coins/bitcoin/market_chart?vs_currency=usd&days=365&interval=daily'),
            fetchJSON('https://api.coingecko.com/api/v3/global')
        ]);
        
        if (!ethData?.market_caps || !btcData?.market_caps || !globalData?.data) {
            throw new Error('CoinGecko API failed');
        }
        
        const currentBtcDom = globalData.data.market_cap_percentage.btc;
        
        // BTC mcap을 날짜별 맵으로
        const btcMcapMap = new Map();
        for (const [ts, mcap] of btcData.market_caps) {
            const date = new Date(ts).toISOString().split('T')[0];
            btcMcapMap.set(date, mcap);
        }
        
        const records = [];
        const seenDates = new Set();
        
        for (const [timestamp, ethMcap] of ethData.market_caps) {
            const date = new Date(timestamp).toISOString().split('T')[0];
            if (seenDates.has(date)) continue;
            seenDates.add(date);
            
            const btcMcap = btcMcapMap.get(date);
            if (!btcMcap) continue;
            
            // Total market cap = BTC mcap / BTC dominance
            const totalMcap = btcMcap / (currentBtcDom / 100);
            const ethDominance = (ethMcap / totalMcap) * 100;
            const btcDominance = (btcMcap / totalMcap) * 100;
            
            records.push({
                date,
                timestamp,
                eth_dominance: parseFloat(Math.min(25, Math.max(8, ethDominance)).toFixed(2)),
                btc_dominance: parseFloat(Math.min(70, Math.max(35, btcDominance)).toFixed(2)),
                total_mcap: totalMcap,
                source: 'coingecko'
            });
        }
        
        if (records.length > 100) {
            const saved = await upsertBatch('historical_eth_dominance', records);
            return result.ok(saved);
        }
    } catch (e) {
        // API 차단됨
    }
    
    // 실패 시 기존 데이터 유지
    const { data: existing } = await supabase
        .from('historical_eth_dominance')
        .select('date')
        .order('date', { ascending: false })
        .limit(1);
    
    if (existing?.length > 0) {
        return result.skip('API blocked, using existing');
    }
    return result.fail('No data available');
}

// ============================================================
// 16-2. Open Interest (CryptoQuant API via Cloudflare Proxy)
// ============================================================
async function collect_open_interest() {
    const PROXY_URL = 'https://cryptoquant-proxy.seojoon-kim.workers.dev';
    
    try {
        const response = await fetch(
            `${PROXY_URL}/?endpoint=/v1/eth/market-data/open-interest&window=day&exchange=all_exchange&symbol=all_symbol&limit=1095`
        );
        
        if (!response.ok) {
            throw new Error(`Proxy error: ${response.status}`);
        }
        
        const data = await response.json();
        
        if (data.status?.code !== 200) {
            throw new Error(data.status?.message || 'API error');
        }
        
        const rows = data?.result?.data || [];
        
        if (rows.length === 0) {
            throw new Error('No data from CryptoQuant');
        }
        
        console.log(`  📦 Got ${rows.length} open interest records from CryptoQuant`);
        
        const records = rows.map(row => ({
            date: row.date,
            open_interest: parseFloat(row.open_interest || 0),
            source: 'cryptoquant'
        })).filter(r => r.date && !isNaN(r.open_interest) && r.open_interest > 0);
        
        if (records.length > 100) {
            const saved = await upsertBatch('historical_open_interest', records);
            return result.ok(saved);
        }
        
        throw new Error('Insufficient data');
    } catch (e) {
        console.log(`  ❌ open_interest: ${e.message}`);
        return result.fail(e.message);
    }
}

// ============================================================
// 17. Blob Data (beaconcha.in)
// ============================================================
async function collect_blob_data() {
    // Limited API access - using existing or estimate
    const { data: existing } = await supabase.from('historical_blob_data').select('*').order('date', { ascending: false }).limit(1);
    if (existing && existing.length > 0) {
        console.log('  ✓ Using existing data');
        return result.skip('Dune provides this');
    }
    return result.fail('No public API');
}

// ============================================================
// 18. Lending TVL (DefiLlama)
// ============================================================
async function collect_lending_tvl() {
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
    const { data: prices } = await supabase.from('historical_eth_price').select('date, close').order('date', { ascending: true });
    if (!prices || prices.length < 30) return 0;
    
    console.log(`  Got ${prices.length} price records`);
    
    const records = [];
    // i = 29부터 시작 (30일 윈도우 필요)
    for (let i = 29; i < prices.length; i++) {
        const window = prices.slice(i - 29, i + 1); // 30일 윈도우
        const returns = [];
        for (let j = 1; j < window.length; j++) {
            if (window[j-1].close > 0) {
                returns.push(Math.log(window[j].close / window[j-1].close));
            }
        }
        if (returns.length < 20) continue;
        
        const mean = returns.reduce((a, b) => a + b, 0) / returns.length;
        const variance = returns.reduce((a, b) => a + Math.pow(b - mean, 2), 0) / returns.length;
        const volatility = Math.sqrt(variance * 365) * 100; // Annualized
        
        if (volatility > 0 && volatility < 500) {
            records.push({
                date: prices[i].date,
                volatility_30d: parseFloat(volatility.toFixed(2)),
                source: 'calculated'
            });
        }
    }
    console.log(`  Latest: ${records[records.length-1]?.date} = ${records[records.length-1]?.volatility_30d}%`);
    return await upsertBatch('historical_volatility', records);
}

// ============================================================
// 20. NVT Ratio (from L1 Total Volume Dune query)
// NVT = Market Cap / Daily On-chain Volume
// ============================================================
async function collect_nvt() {
    if (!DUNE_API_KEY) {
        console.log('  ⚠️ No Dune API key for NVT');
        return 0;
    }
    
    // Get ETH prices first
    const { data: prices } = await supabase.from('historical_eth_price')
        .select('date, close')
        .order('date', { ascending: false })
        .limit(1100);
    
    if (!prices || prices.length === 0) {
        console.log('  ❌ No price data available');
        return 0;
    }
    
    const priceMap = new Map(prices.map(p => [p.date, parseFloat(p.close)]));
    const ETH_SUPPLY = 120400000;
    
    try {
        console.log('  📡 Fetching L1 Total Volume from Dune...');
        
        // Paginate through all results
        let allRows = [];
        let offset = 0;
        const pageSize = 1000;
        
        while (true) {
            const response = await fetch(`https://api.dune.com/api/v1/query/${DUNE_QUERIES.L1_TOTAL_VOLUME}/results?limit=${pageSize}&offset=${offset}`, {
                headers: { 'X-Dune-API-Key': DUNE_API_KEY }
            });
            
            if (!response.ok) break;
            
            const data = await response.json();
            const rows = data.result?.rows || [];
            
            if (rows.length === 0) break;
            
            allRows = allRows.concat(rows);
            console.log(`  📊 Fetched ${rows.length} rows (total: ${allRows.length})`);
            
            if (rows.length < pageSize) break;
            offset += pageSize;
        }
        
        if (allRows.length === 0) {
            console.log('  ⚠️ No data from Dune');
            return 0;
        }
        
        const records = [];
        for (const row of allRows) {
            let dateStr = row.date;
            if (typeof dateStr === 'string' && dateStr.includes(' ')) {
                dateStr = dateStr.split(' ')[0];
            }
            
            const price = priceMap.get(dateStr);
            if (!price) continue;
            
            // Use eth_volume_usd (native ETH transfers only) for proper NVT calculation
            // NVT should use native coin volume, not all token transfers
            const txVolumeUsd = parseFloat(row.eth_volume_usd) || 0;
            if (txVolumeUsd <= 0) continue;
            
            const mcap = price * ETH_SUPPLY;
            const nvt = mcap / txVolumeUsd;
            
            if (nvt > 0 && nvt < 500) {
                records.push({
                    date: dateStr,
                    timestamp: new Date(dateStr).getTime(),
                    nvt_ratio: parseFloat(nvt.toFixed(2)),
                    market_cap: parseFloat(mcap.toFixed(2)),
                    tx_volume_usd: parseFloat(txVolumeUsd.toFixed(2)),
                    source: 'dune'
                });
            }
        }
        
        // Sort by date descending and filter incomplete data
        records.sort((a, b) => b.date.localeCompare(a.date));
        
        if (records.length >= 8) {
            const latestVol = records[0].tx_volume_usd;
            const prev7Avg = records.slice(1, 8).reduce((sum, r) => sum + r.tx_volume_usd, 0) / 7;
            if (prev7Avg > 0 && latestVol < prev7Avg * 0.3) {
                console.log(`  ⚠️ Excluding incomplete: ${records[0].date}`);
                records.shift();
            }
        }
        
        console.log(`  📦 ${records.length} NVT records from Dune`);
        return await upsertBatch('historical_nvt', records);
        
    } catch (e) {
        console.log(`  ❌ Dune error: ${e.message}`);
        return 0;
    }
}

// ============================================================
// 21. Transactions (DefiLlama)
// ============================================================
async function collect_transactions() {
    
    // growthepie API - 실제 트랜잭션 수
    const data = await fetchJSON('https://api.growthepie.xyz/v1/export/txcount.json');
    if (!data || !Array.isArray(data)) {
        console.log('  ⚠️ growthepie API failed');
        return 0;
    }
    
    // Ethereum mainnet 데이터만 필터
    const ethRecords = data
        .filter(d => d.origin_key === 'ethereum' && d.metric_key === 'txcount')
        .map(d => ({
            date: d.date,
            tx_count: Math.floor(d.value),
            source: 'growthepie'
        }));
    
    console.log(`  📦 ${ethRecords.length} ETH mainnet tx records`);
    return await upsertBatch('historical_transactions', ethRecords);
}

// ============================================================
// 22. L2 Transactions (growthepie - 실제 데이터)
// ============================================================
async function collect_l2_transactions() {
    
    // growthepie API - 모든 체인의 실제 트랜잭션 수
    const data = await fetchJSON('https://api.growthepie.xyz/v1/export/txcount.json');
    if (!data || !Array.isArray(data)) {
        console.log('  ⚠️ growthepie API failed');
        return 0;
    }
    
    // L2 체인들 필터 (ethereum 제외)
    const l2Chains = ['arbitrum', 'optimism', 'base', 'zksync_era', 'linea', 'scroll', 'blast', 'manta', 'mode', 'zora', 'polygon_zkevm', 'starknet'];
    
    const l2Records = data
        .filter(d => l2Chains.includes(d.origin_key) && d.metric_key === 'txcount')
        .map(d => ({
            date: d.date,
            chain: d.origin_key,
            tx_count: Math.floor(d.value),
            source: 'growthepie'
        }));
    
    console.log(`  📦 ${l2Records.length} L2 tx records across ${l2Chains.length} chains`);
    return await upsertBatch('historical_l2_transactions', l2Records, 'date,chain');
}

// ============================================================
// 23. L2 Addresses (estimate)
// ============================================================
async function collect_l2_addresses() {
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
    // 중복 제거 (같은 date+protocol 조합)
    const seen = new Set();
    const deduped = all.filter(r => {
        const key = `${r.date}_${r.protocol}`;
        if (seen.has(key)) return false;
        seen.add(key);
        return true;
    });
    return await upsertBatch('historical_protocol_tvl', deduped, 'date,protocol');
}

// ============================================================
// 25. Staking APR (DefiLlama/Lido) - admin.html 방식
// ============================================================
async function collect_staking_apr() {
    const data = await fetchJSON('https://yields.llama.fi/chart/747c1d2a-c668-4682-b9f9-296708a3dd90');
    
    if (!data?.data || data.data.length === 0) {
        console.log('  ⚠️ DefiLlama yields API failed, using estimates');
        
        // Fallback: Generate estimated APR data (3-4% range)
        const today = new Date();
        const records = [];
        
        for (let i = 0; i < 1095; i++) {
            const date = new Date(today);
            date.setDate(date.getDate() - i);
            
            // APR 추세: 2022년 ~5% → 2025년 ~3.5%
            const daysFromStart = 1095 - i;
            const progress = daysFromStart / 1095;
            const baseApr = 5.0 - (1.5 * progress);
            const variation = Math.sin(daysFromStart * 0.05) * 0.3;
            
            records.push({
                date: date.toISOString().split('T')[0],
                lido_apr: parseFloat((baseApr + variation).toFixed(2)),
                source: 'estimated'
            });
        }
        
        const count = await upsertBatch('historical_staking_apr', records);
        return result.warn(count, 'Using estimated data');
    }
    
    console.log(`  📦 Got ${data.data.length} records from DefiLlama`);
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
    const data = await fetchJSON('https://beaconcha.in/api/v1/epoch/latest');
    if (!data?.data) return 0;
    const today = new Date().toISOString().split('T')[0];
    const records = [{
        date: today,
        block_count: 7200, // ~7200 blocks/day
        avg_block_time: 12
    }];
    return await upsertBatch('historical_network_stats', records);
}

// ============================================================
// DUNE API COLLECTIONS (30-39)
// ============================================================

// 30. Blob Data (Dune)
async function collect_dune_blob() {
    if (!DUNE_API_KEY) { console.log('  ⏭️ Skipped - No API key'); return result.skip('No API key'); }
    
    const rows = await fetchDuneResults(DUNE_QUERIES.BLOB, 1000);
    if (!rows) {
        console.log('  ⚠️ Query returned null - check query ID: ' + DUNE_QUERIES.BLOB);
        return result.warn(0, 'Query failed');
    }
    if (rows.length === 0) {
        console.log('  ⚠️ Query returned empty - check if scheduled');
        return result.warn(0, 'No data from Dune');
    }
    
    const records = rows.map(r => {
        let dateStr = r.block_date || r.date || '';
        if (dateStr.includes(' ')) dateStr = dateStr.split(' ')[0];
        if (dateStr.includes('T')) dateStr = dateStr.split('T')[0];
        return {
            date: dateStr,
            blob_count: parseInt(r.blob_count || r.blobs || 0),
            blob_gas_used: parseFloat(r.blob_gas_used || 0),
            blob_fee_eth: parseFloat(r.blob_fee_eth || 0),
            source: 'dune'
        };
    }).filter(r => r.date && r.blob_count > 0);
    
    console.log(`  ✓ ${records.length} records`);
    if (records.length > 0) console.log(`  📅 Latest: ${records[0].date}`);
    const saved = await upsertBatch('historical_blob_data', records);
    return result.ok(saved);
}

// 31. L1 TX Volume (Dune)
// REMOVED: collect_dune_l1_volume
// L1 ETH Transfer now uses historical_l1_total_volume.eth_volume_usd instead
// Query 6350858 is no longer needed

// 32. Active Addresses L1 (Dune)
async function collect_dune_active_addr() {
    if (!DUNE_API_KEY) { console.log('  ⏭️ Skipped - No API key'); return result.skip('No API key'); }
    
    const rows = await fetchDuneResults(DUNE_QUERIES.ACTIVE_ADDR, 5000);
    if (!rows) {
        console.log('  ⚠️ Query returned null - check query ID: ' + DUNE_QUERIES.ACTIVE_ADDR);
        return result.warn(0, 'Query failed');
    }
    if (rows.length === 0) {
        console.log('  ⚠️ Query returned empty - check if scheduled');
        return result.warn(0, 'No data from Dune');
    }
    
    const records = rows.map(r => {
        let dateStr = r.block_date || r.date || '';
        if (dateStr.includes(' ')) dateStr = dateStr.split(' ')[0];
        if (dateStr.includes('T')) dateStr = dateStr.split('T')[0];
        return {
            date: dateStr,
            active_addresses: parseInt(r.active_addresses || r.unique_addresses || 0)
        };
    }).filter(r => r.date && r.active_addresses > 0);
    
    console.log(`  ✓ ${records.length} records`);
    if (records.length > 0) console.log(`  📅 Latest: ${records[0].date}`);
    const saved = await upsertBatch('historical_active_addresses', records);
    return result.ok(saved);
}

// 33. L2 Active Addresses (Dune)
async function collect_dune_l2_addr() {
    if (!DUNE_API_KEY) { console.log('  ⏭️ Skipped - No API key'); return result.skip('No API key'); }
    
    const rows = await fetchDuneResults(DUNE_QUERIES.L2_ACTIVE_ADDR, 10000);
    if (!rows) {
        console.log('  ⚠️ Query returned null - check query ID: ' + DUNE_QUERIES.L2_ACTIVE_ADDR);
        return result.warn(0, 'Query failed');
    }
    if (rows.length === 0) {
        console.log('  ⚠️ Query returned empty - check if scheduled');
        return result.warn(0, 'No data from Dune');
    }
    
    const records = rows.map(r => {
        let dateStr = r.block_date || r.date || '';
        if (dateStr.includes(' ')) dateStr = dateStr.split(' ')[0];
        if (dateStr.includes('T')) dateStr = dateStr.split('T')[0];
        return {
            date: dateStr,
            chain: r.chain || r.l2_name || 'unknown',
            active_addresses: parseInt(r.active_addresses || r.unique_addresses || 0),
            source: 'dune'
        };
    }).filter(r => r.date && r.active_addresses > 0);
    
    console.log(`  ✓ ${records.length} records`);
    if (records.length > 0) console.log(`  📅 Latest: ${records[0].date}`);
    const saved = await upsertBatch('historical_l2_addresses', records, 'date,chain');
    return result.ok(saved);
}

// 34. L2 ETH Transfer (Dune) - ETH transfers only (excludes Mantle MNT)
// REMOVED: collect_dune_l2_native_transfer
// L2 ETH Transfer now uses historical_l2_total_volume.native_volume_usd instead
// Query 6352386 is no longer needed

// 34. Bridge Volume (Dune)
async function collect_dune_bridge() {
    if (!DUNE_API_KEY) { console.log('  ⏭️ Skipped - No API key'); return result.skip('No API key'); }
    
    const rows = await fetchDuneResults(DUNE_QUERIES.BRIDGE_VOLUME, 10000);
    if (!rows) {
        console.log('  ⚠️ Query returned null - check query ID: ' + DUNE_QUERIES.BRIDGE_VOLUME);
        return result.warn(0, 'Query failed');
    }
    if (rows.length === 0) {
        console.log('  ⚠️ Query returned empty - check if scheduled');
        return result.warn(0, 'No data from Dune');
    }
    
    const records = rows.map(r => {
        let dateStr = r.block_date || r.date || '';
        if (dateStr.includes(' ')) dateStr = dateStr.split(' ')[0];
        if (dateStr.includes('T')) dateStr = dateStr.split('T')[0];
        return {
            date: dateStr,
            chain: r.chain || r.l2_name || 'unknown',
            bridge_volume_eth: parseFloat(r.bridge_volume_eth || r.volume_eth || 0),
            source: 'dune'
        };
    }).filter(r => r.date && r.bridge_volume_eth > 0);
    
    console.log(`  ✓ ${records.length} records`);
    if (records.length > 0) console.log(`  📅 Latest: ${records[0].date}`);
    const saved = await upsertBatch('historical_bridge_volume', records, 'date,chain');
    return result.ok(saved);
}

// 35-1. L2 DEX Volume (Dune)
async function collect_dune_l2_dex_volume() {
    if (!DUNE_API_KEY) { console.log('  ⏭️ Skipped - No API key'); return result.skip('No API key'); }
    
    const rows = await fetchDuneResults(DUNE_QUERIES.L2_DEX_VOLUME, 15000);
    if (!rows) {
        console.log('  ⚠️ Query returned null - check query ID: ' + DUNE_QUERIES.L2_DEX_VOLUME);
        return result.warn(0, 'Query failed');
    }
    if (rows.length === 0) {
        console.log('  ⚠️ Query returned empty - check if scheduled');
        return result.warn(0, 'No data from Dune');
    }
    
    const records = rows.map(r => {
        let dateStr = r.date || r.block_date || '';
        if (dateStr.includes(' ')) dateStr = dateStr.split(' ')[0];
        if (dateStr.includes('T')) dateStr = dateStr.split('T')[0];
        return {
            date: dateStr,
            blockchain: r.blockchain || 'unknown',
            dex_volume_usd: parseFloat(r.dex_volume_usd || 0)
        };
    }).filter(r => r.date && r.dex_volume_usd > 0);
    
    console.log(`  ✓ ${records.length} records`);
    if (records.length > 0) console.log(`  📅 Latest: ${records[0].date}`);
    const saved = await upsertBatch('historical_l2_dex_volume', records, 'date,blockchain');
    return result.ok(saved);
}

// 35-2. Bridge Total Volume (Dune) - ETH + ERC-20 tokens
async function collect_dune_bridge_total_volume() {
    if (!DUNE_API_KEY) { console.log('  ⏭️ Skipped - No API key'); return result.skip('No API key'); }
    
    const rows = await fetchDuneResults(DUNE_QUERIES.BRIDGE_TOTAL_VOLUME, 15000);
    if (!rows) {
        console.log('  ⚠️ Query returned null - check query ID: ' + DUNE_QUERIES.BRIDGE_TOTAL_VOLUME);
        return result.warn(0, 'Query failed');
    }
    if (rows.length === 0) {
        console.log('  ⚠️ Query returned empty - check if scheduled');
        return result.warn(0, 'No data from Dune');
    }
    
    const records = rows.map(r => {
        let dateStr = r.date || r.block_date || '';
        if (dateStr.includes(' ')) dateStr = dateStr.split(' ')[0];
        if (dateStr.includes('T')) dateStr = dateStr.split('T')[0];
        return {
            date: dateStr,
            destination_chain: r.destination_chain || 'unknown',
            bridge_volume_usd: parseFloat(r.bridge_volume_usd || 0)
        };
    }).filter(r => r.date && r.bridge_volume_usd > 0);
    
    console.log(`  ✓ ${records.length} records`);
    if (records.length > 0) console.log(`  📅 Latest: ${records[0].date}`);
    const saved = await upsertBatch('historical_bridge_total_volume', records, 'date,destination_chain');
    return result.ok(saved);
}

// 35-3. L1 Total Volume (Dune) - ETH + all ERC-20 transfers
async function collect_dune_l1_total_volume() {
    if (!DUNE_API_KEY) { console.log('  ⏭️ Skipped - No API key'); return result.skip('No API key'); }

    const rows = await fetchDuneResults(DUNE_QUERIES.L1_TOTAL_VOLUME, 2000);
    if (!rows) {
        console.log('  ⚠️ Query returned null - check query ID: ' + DUNE_QUERIES.L1_TOTAL_VOLUME);
        return result.warn(0, 'Query failed');
    }
    if (rows.length === 0) {
        console.log('  ⚠️ Query returned empty - check if scheduled');
        return result.warn(0, 'No data from Dune');
    }

    const records = rows.map(r => {
        let dateStr = r.date || r.block_date || '';
        if (dateStr.includes(' ')) dateStr = dateStr.split(' ')[0];
        if (dateStr.includes('T')) dateStr = dateStr.split('T')[0];
        return {
            date: dateStr,
            total_volume_usd: parseFloat(r.total_volume_usd || 0),
            eth_volume_usd: parseFloat(r.eth_volume_usd || 0)
        };
    }).filter(r => r.date && (r.total_volume_usd > 0 || r.eth_volume_usd > 0));

    console.log(`  ✓ ${records.length} records`);
    if (records.length > 0) console.log(`  📅 Latest: ${records[0].date}`);
    const saved = await upsertBatch('historical_l1_total_volume', records);
    return result.ok(saved);
}

// 35-4. L2 Total Volume (Dune) - All L2 chains ETH + tokens
async function collect_dune_l2_total_volume() {
    if (!DUNE_API_KEY) { console.log('  ⏭️ Skipped - No API key'); return result.skip('No API key'); }

    const rows = await fetchDuneResults(DUNE_QUERIES.L2_TOTAL_VOLUME, 15000);
    if (!rows) {
        console.log('  ⚠️ Query returned null - check query ID: ' + DUNE_QUERIES.L2_TOTAL_VOLUME);
        return result.warn(0, 'Query failed');
    }
    if (rows.length === 0) {
        console.log('  ⚠️ Query returned empty - check if scheduled');
        return result.warn(0, 'No data from Dune');
    }

    const records = rows.map(r => {
        let dateStr = r.date || r.block_date || '';
        if (dateStr.includes(' ')) dateStr = dateStr.split(' ')[0];
        if (dateStr.includes('T')) dateStr = dateStr.split('T')[0];
        return {
            date: dateStr,
            chain: r.chain || r.blockchain || 'unknown',
            total_volume_usd: parseFloat(r.total_volume_usd || 0),
            native_volume_usd: parseFloat(r.native_volume_usd || r.eth_volume_usd || 0)
        };
    }).filter(r => r.date && (r.total_volume_usd > 0 || r.native_volume_usd > 0));

    console.log(`  ✓ ${records.length} records`);
    if (records.length > 0) console.log(`  📅 Latest: ${records[0].date}`);
    const saved = await upsertBatch('historical_l2_total_volume', records, 'date,chain');
    return result.ok(saved);
}

// 35-5. Daily Issuance (calculated from ETH supply changes)
async function collect_daily_issuance() {
    // Get ETH supply data (more records for better calculation)
    const { data: supplies } = await supabase
        .from('historical_eth_supply')
        .select('date, eth_supply')
        .order('date', { ascending: true });

    if (!supplies || supplies.length < 2) {
        console.log('  ⚠️ Not enough supply data');
        return result.warn(0, 'Not enough supply data');
    }

    console.log(`  📊 Got ${supplies.length} supply records`);

    // Calculate daily issuance from supply changes
    // Daily issuance = supply change + burned ETH (since supply = previous_supply + issued - burned)
    const { data: burnData } = await supabase
        .from('historical_gas_burn')
        .select('date, eth_burnt')
        .order('date', { ascending: true });

    const burnMap = new Map();
    if (burnData) {
        burnData.forEach(b => burnMap.set(b.date, parseFloat(b.eth_burnt || 0)));
        console.log(`  📊 Got ${burnData.length} burn records`);
    }

    const records = [];
    let skippedNegative = 0;
    let skippedTooHigh = 0;
    let skippedNoBurn = 0;

    for (let i = 1; i < supplies.length; i++) {
        const today = supplies[i];
        const yesterday = supplies[i - 1];

        const supplyChange = parseFloat(today.eth_supply) - parseFloat(yesterday.eth_supply);
        const burned = burnMap.get(today.date) || 0;

        // Daily Issuance = Supply Change + Burned
        // (because: new_supply = old_supply + issued - burned)
        const dailyIssuance = supplyChange + burned;

        // Validator rewards are roughly 930-2500 ETH/day depending on staking participation
        // Allow wider range: -500 to 10000 (negative can happen due to data timing issues)
        if (dailyIssuance >= -500 && dailyIssuance < 10000) {
            // Use absolute value for display, but store actual value
            const displayIssuance = Math.max(0, dailyIssuance);
            records.push({
                date: today.date,
                daily_issuance: parseFloat(displayIssuance.toFixed(2))
            });
        } else if (dailyIssuance < -500) {
            skippedNegative++;
        } else {
            skippedTooHigh++;
        }

        if (burned === 0) skippedNoBurn++;
    }

    console.log(`  📊 Skipped: ${skippedNegative} negative, ${skippedTooHigh} too high, ${skippedNoBurn} no burn data`);

    if (records.length === 0) {
        console.log('  ⚠️ No valid issuance records calculated');
        return result.warn(0, 'No valid records');
    }

    console.log(`  ✓ ${records.length} issuance records calculated`);
    if (records.length > 0) console.log(`  📅 Latest: ${records[records.length - 1].date}`);
    const saved = await upsertBatch('historical_daily_issuance', records);
    return result.ok(saved);
}

// 36. Whale Transactions (Dune)
async function collect_dune_whale() {
    if (!DUNE_API_KEY) { console.log('  ⏭️ Skipped - No API key'); return 0; }
    
    const rows = await fetchDuneResults(DUNE_QUERIES.WHALE_TX, 5000);
    if (!rows || rows.length === 0) return 0;
    
    const records = rows.map(r => ({
        date: r.block_date || r.date,
        whale_tx_count: parseInt(r.whale_tx_count || r.tx_count || 0),
        whale_volume_eth: parseFloat(r.whale_volume_eth || r.volume_eth || 0),
        source: 'dune'
    })).filter(r => r.date && r.whale_tx_count > 0);
    
    console.log(`  📊 Got ${records.length} records`);
    return await upsertBatch('historical_whale_tx', records);
}

// 37. New Addresses (Dune)
async function collect_dune_new_addr() {
    if (!DUNE_API_KEY) { console.log('  ⏭️ Skipped - No API key'); return 0; }
    
    const rows = await fetchDuneResults(DUNE_QUERIES.NEW_ADDR, 5000);
    if (!rows || rows.length === 0) return 0;
    
    const records = rows.map(r => ({
        date: r.block_date || r.date,
        new_addresses: parseInt(r.new_addresses || r.new_wallets || 0),
        source: 'dune'
    })).filter(r => r.date && r.new_addresses > 0);
    
    console.log(`  📊 Got ${records.length} records`);
    return await upsertBatch('historical_new_addresses', records);
}

// 38. MVRV Ratio (Dune)
async function collect_dune_mvrv() {
    if (!DUNE_API_KEY) { console.log('  ⏭️ Skipped - No API key'); return result.skip('No API key'); }
    
    const rows = await fetchDuneResults(DUNE_QUERIES.MVRV, 5000);
    if (!rows) {
        console.log('  ⚠️ Query returned null');
        return result.warn(0, 'Query failed');
    }
    if (rows.length === 0) {
        console.log('  ⚠️ Query returned empty');
        return result.warn(0, 'No data from Dune');
    }
    
    // Dune 컬럼명: day, spot_price, estimated_realized_price, mvrv_proxy_pct
    // mvrv_proxy_pct는 백분율로, 78 = "78% 프리미엄" = MVRV ratio 1.78
    const records = rows.map(r => {
        // 날짜 파싱: "2025-12-18 00:00:00" -> "2025-12-18"
        let dateStr = r.day || r.block_date || r.date || '';
        if (dateStr.includes(' ')) dateStr = dateStr.split(' ')[0];
        if (dateStr.includes('T')) dateStr = dateStr.split('T')[0];
        
        // mvrv_proxy_pct: 78 = 78% 프리미엄 = 1.78 ratio (admin.html 방식)
        const mvrvPct = parseFloat(r.mvrv_proxy_pct || 0);
        const mvrvRatio = 1 + (mvrvPct / 100); // 78 -> 1.78
        
        return {
            date: dateStr,
            spot_price: parseFloat(r.spot_price || 0),
            realized_price: parseFloat(r.estimated_realized_price || r.realized_price || 0),
            mvrv_ratio: parseFloat(mvrvRatio.toFixed(4)),
            mvrv_pct: mvrvPct,
            source: 'dune'
        };
    }).filter(r => r.date && r.realized_price > 0);
    
    console.log(`  ✓ ${records.length} records`);
    if (records.length > 0) {
        console.log(`  📅 Latest: ${records[0].date} = ${records[0].mvrv_ratio}x (realized: $${records[0].realized_price.toFixed(2)})`);
    }
    const saved = await upsertBatch('historical_mvrv', records);
    return result.ok(saved);
}

// 39. Stablecoin Volume (Dune)
async function collect_dune_stablecoin_vol() {
    if (!DUNE_API_KEY) { console.log('  ⏭️ Skipped - No API key'); return result.skip('No API key'); }
    
    const rows = await fetchDuneResults(DUNE_QUERIES.STABLECOIN_VOL, 5000);
    if (!rows) {
        console.log('  ⚠️ Query returned null');
        return result.warn(0, 'Query failed');
    }
    if (rows.length === 0) {
        console.log('  ⚠️ Query returned empty');
        return result.warn(0, 'No data from Dune');
    }
    
    // Dune 컬럼명: block_date, daily_volume_usd
    const records = rows.map(r => {
        // 날짜 파싱: "2025-12-18 00:00:00" -> "2025-12-18"
        let dateStr = r.block_date || r.date || '';
        if (dateStr.includes(' ')) dateStr = dateStr.split(' ')[0];
        if (dateStr.includes('T')) dateStr = dateStr.split('T')[0];
        
        return {
            date: dateStr,
            daily_volume: parseFloat(r.daily_volume_usd || r.daily_volume || r.volume || 0)
        };
    }).filter(r => r.date && r.daily_volume > 0);
    
    console.log(`  ✓ ${records.length} records`);
    if (records.length > 0) {
        console.log(`  📅 Latest: ${records[0].date} = $${(records[0].daily_volume / 1e9).toFixed(2)}B`);
    }
    const saved = await upsertBatch('historical_stablecoin_volume', records);
    return result.ok(saved);
}

// 40. Gas Price (Dune) - Daily average gas price
async function collect_dune_gas_price() {
    if (!DUNE_API_KEY) { console.log('  ⏭️ Skipped - No API key'); return 0; }
    if (DUNE_QUERIES.GAS_PRICE === 0) { 
        console.log('  ⏭️ Skipped - Query ID not set'); 
        return 0; 
    }
    
    const rows = await fetchDuneResults(DUNE_QUERIES.GAS_PRICE, 5000);
    if (!rows || rows.length === 0) return 0;
    
    // Update historical_gas_burn table with gas price, eth_burnt, fees_usd data
    const records = rows.map(r => {
        // Parse date: "2025-12-14 00:00" or "2025-12-14T00:00:00" -> "2025-12-14"
        let dateStr = r.block_date || r.date || '';
        if (dateStr.includes(' ')) {
            dateStr = dateStr.split(' ')[0];
        } else if (dateStr.includes('T')) {
            dateStr = dateStr.split('T')[0];
        }
        
        return {
            date: dateStr,
            avg_gas_price_gwei: parseFloat(r.avg_gas_price_gwei || r.gas_price_gwei || r.avg_gas_price || 0),
            eth_burnt: parseFloat(r.eth_burnt || 0),
            gas_utilization: parseFloat(r.gas_utilization || r.utilization || 0),
            transaction_count: parseInt(r.tx_count || r.transaction_count || 0),
            fees_usd: parseFloat(r.fees_usd || 0)
        };
    }).filter(r => r.date && r.avg_gas_price_gwei > 0);
    
    console.log(`  📊 Got ${records.length} records with gas price`);
    if (records.length > 0) {
        console.log(`  📅 Date range: ${records[records.length-1].date} to ${records[0].date}`);
        console.log(`  ⛽ Sample: ${records[0].date} = ${records[0].avg_gas_price_gwei.toFixed(2)} Gwei, ${records[0].eth_burnt.toFixed(2)} ETH burnt, $${(records[0].fees_usd/1000).toFixed(1)}K fees`);
    }

    // Upsert records to historical_gas_burn (insert if not exists, update if exists)
    let updated = 0;
    for (const record of records) {
        const upsertData = {
            date: record.date,
            avg_gas_price_gwei: record.avg_gas_price_gwei,
            eth_burnt: record.eth_burnt,
            fees_usd: record.fees_usd
        };
        if (record.gas_utilization > 0) {
            upsertData.gas_utilization = record.gas_utilization;
        }
        if (record.transaction_count > 0) {
            upsertData.transaction_count = record.transaction_count;
        }

        const { error } = await supabase
            .from('historical_gas_burn')
            .upsert(upsertData, { onConflict: 'date' });

        if (!error) updated++;
    }

    console.log(`  ✅ Upserted ${updated} records in historical_gas_burn`);
    return updated;
}

// ============================================================
// Main
// ============================================================
async function main() {
    console.log('═'.repeat(60));
    console.log('🚀 ETHval Data Collector v7.3');
    console.log(`📅 ${new Date().toISOString()}`);
    console.log('═'.repeat(60));
    
    if (DUNE_API_KEY) console.log('✓ Dune API Key detected');
    else console.log('⚠️ No Dune API Key - Dune collections will be skipped');
    
    const startTime = Date.now();
    const results = {};
    
    // 결과 래퍼 (기존 함수가 숫자를 반환하면 변환)
    const wrapResult = (res, isDune = false) => {
        if (typeof res === 'number') {
            if (res > 0) return result.ok(res);
            // Dune 쿼리는 0건이어도 warn (쿼리 확인 필요)
            if (isDune) return result.warn(0, 'Check Dune query');
            return result.fail('No data');
        }
        return res;
    };
    
    // 진행상황 출력 헬퍼
    const runCollector = async (name, fn, index, total) => {
        const prefix = `[${String(index).padStart(2, '0')}/${total}]`;
        try {
            const res = wrapResult(await fn());
            if (res.status === 'fail') {
                console.log(`❌ ${prefix} ${name}: ${res.msg}`);
            } else if (res.status === 'warn') {
                console.log(`⚠️ ${prefix} ${name}: ${res.count} (${res.msg})`);
            }
            // 성공은 로그 안 함 (Summary에서 표시)
            return res;
        } catch (e) {
            console.log(`❌ ${prefix} ${name}: ${e.message}`);
            return result.fail(e.message);
        }
    };
    
    // ============================================================
    // PHASE 1: DefiLlama APIs (순차 처리 - rate limit 방지)
    // ============================================================
    console.log('\n📦 Phase 1: DefiLlama APIs...');
    const defiLlamaStart = Date.now();
    
    results.ethereum_tvl = await runCollector('Ethereum TVL', collect_ethereum_tvl, 1, 38); await sleep(500);
    results.l2_tvl = await runCollector('L2 TVL', collect_l2_tvl, 2, 38); await sleep(500);
    results.protocol_fees = await runCollector('Protocol Fees', collect_protocol_fees, 3, 38); await sleep(500);
    results.lending_tvl = await runCollector('Lending TVL', collect_lending_tvl, 4, 38); await sleep(500);
    results.protocol_tvl = await runCollector('Protocol TVL', collect_protocol_tvl, 5, 38); await sleep(500);
    results.staking_apr = await runCollector('Staking APR', collect_staking_apr, 6, 38); await sleep(500);
    results.eth_in_defi = await runCollector('ETH in DeFi', collect_eth_in_defi, 7, 38); await sleep(500);
    results.dex_volume = await runCollector('DEX Volume', collect_dex_volume, 8, 38); await sleep(500);
    results.dex_by_protocol = await runCollector('DEX by Protocol', collect_dex_by_protocol, 9, 38); await sleep(500);
    results.staking = await runCollector('Staking Data', collect_staking, 10, 38);
    
    console.log(`  ✓ DefiLlama: ${((Date.now() - defiLlamaStart) / 1000).toFixed(1)}s`);
    
    // ============================================================
    // PHASE 2: Price APIs (DefiLlama primary)
    // ============================================================
    console.log('\n💰 Phase 2: Price & Market Data...');
    const priceStart = Date.now();
    
    results.eth_price = await runCollector('ETH Price', collect_eth_price, 11, 38); await sleep(500);
    results.eth_btc = await runCollector('ETH/BTC Ratio', collect_eth_btc, 12, 38); await sleep(500);
    results.eth_dominance = await runCollector('ETH Dominance', collect_eth_dominance, 13, 38); await sleep(500);
    results.global_mcap = await runCollector('Global MCap', collect_global_mcap, 14, 38);
    
    console.log(`  ✓ Price APIs: ${((Date.now() - priceStart) / 1000).toFixed(1)}s`);
    
    // ============================================================
    // PHASE 3: Other APIs (병렬)
    // ============================================================
    console.log('\n🔗 Phase 3: Other APIs (parallel)...');
    const otherStart = Date.now();
    
    const phase3Results = await Promise.all([
        collect_stablecoins(),
        collect_stablecoins_eth(),
        collect_fear_greed(),
        collect_eth_supply(),
        collect_volatility(),
        collect_nvt(),
        collect_transactions(),
        collect_l2_transactions(),
        collect_l2_addresses(),
        collect_funding_rate(),
        collect_exchange_reserve(),
        collect_open_interest(),
        collect_blob_data(),
        collect_active_addresses(),
        collect_network_stats(),
        collect_gas_burn()
    ]);
    
    const phase3Names = ['stablecoins', 'stablecoins_eth', 'fear_greed', 'eth_supply', 'volatility', 'nvt', 'transactions', 'l2_transactions', 'l2_addresses', 'funding_rate', 'exchange_reserve', 'open_interest', 'blob_data', 'active_addresses', 'network_stats', 'gas_burn'];
    phase3Results.forEach((res, i) => {
        results[phase3Names[i]] = wrapResult(res);
        const r = results[phase3Names[i]];
        if (r.status === 'fail') console.log(`  ❌ ${phase3Names[i]}: ${r.msg}`);
        else if (r.status === 'warn') console.log(`  ⚠️ ${phase3Names[i]}: ${r.msg}`);
    });
    
    console.log(`  ✓ Other APIs: ${((Date.now() - otherStart) / 1000).toFixed(1)}s`);
    
    // ============================================================
    // PHASE 4: Dune APIs (병렬)
    // ============================================================
    console.log('\n🔷 Phase 4: Dune APIs...');
    const duneStart = Date.now();
    
    if (DUNE_API_KEY) {
        const duneResults = await Promise.all([
            collect_dune_blob(),
            collect_dune_active_addr(),
            collect_dune_l2_addr(),
            collect_dune_bridge(),
            collect_dune_l2_dex_volume(),
            collect_dune_bridge_total_volume(),
            collect_dune_l1_total_volume(),
            collect_dune_l2_total_volume(),
            collect_dune_whale(),
            collect_dune_new_addr(),
            collect_dune_mvrv(),
            collect_dune_stablecoin_vol(),
            collect_dune_gas_price()
        ]);

        const duneNames = ['dune_blob', 'dune_active_addr', 'dune_l2_addr', 'dune_bridge', 'dune_l2_dex_volume', 'dune_bridge_total_volume', 'dune_l1_total_volume', 'dune_l2_total_volume', 'dune_whale', 'dune_new_addr', 'dune_mvrv', 'dune_stablecoin_vol', 'dune_gas_price'];
        duneResults.forEach((res, i) => {
            results[duneNames[i]] = wrapResult(res, true);
            const r = results[duneNames[i]];
            if (r.status === 'fail') console.log(`  ❌ ${duneNames[i]}: ${r.msg}`);
            else if (r.status === 'warn' && r.count === 0) console.log(`  ⚠️ ${duneNames[i]}: ${r.msg}`);
        });

        console.log(`  ✓ Dune: ${((Date.now() - duneStart) / 1000).toFixed(1)}s`);
    } else {
        console.log('  ⏭️ Skipped (no API key)');
    }

    // ============================================================
    // PHASE 5: Calculated metrics (depends on Phase 3 & 4)
    // ============================================================
    console.log('\n📐 Phase 5: Calculated Metrics...');
    const calcStart = Date.now();

    results.daily_issuance = await runCollector('Daily Issuance', collect_daily_issuance, 41, 41);

    console.log(`  ✓ Calculated: ${((Date.now() - calcStart) / 1000).toFixed(1)}s`);
    
    // ============================================================
    // Summary
    // ============================================================
    const totalTime = ((Date.now() - startTime) / 1000).toFixed(1);
    
    console.log('\n' + '='.repeat(60));
    console.log('📊 COLLECTION SUMMARY:');
    console.log('='.repeat(60));
    
    let success = 0, warned = 0, failed = 0;
    const failedDatasets = []; // 실패한 데이터셋 목록
    
    Object.entries(results).forEach(([key, res]) => {
        const { count, status, msg } = res;
        let icon, display;
        
        if (status === 'ok') {
            icon = '✅';
            display = count.toLocaleString();
            success++;
        } else if (status === 'skip') {
            icon = '⏭️';
            display = 'up-to-date';
            success++; // skip도 성공으로 카운트
        } else if (status === 'warn') {
            icon = '⚠️';
            display = `${count.toLocaleString()} (${msg})`;
            warned++;
            failedDatasets.push(key); // warn도 실패 목록에 추가
        } else {
            icon = '❌';
            display = msg || 'failed';
            failed++;
            failedDatasets.push(key);
        }
        
        console.log(`${icon} ${key.padEnd(22)} : ${display}`);
    });
    
    console.log('='.repeat(60));
    console.log(`✅ OK: ${success}  |  ⚠️ Warn: ${warned}  |  ❌ Fail: ${failed}  |  ⏱️ ${totalTime}s`);
    if (failedDatasets.length > 0) {
        console.log(`❌ Failed: ${failedDatasets.join(', ')}`);
    }
    console.log('='.repeat(60));
    
    // ============================================================
    // AI Daily Commentary Generation
    // ============================================================
    const commentaryResults = await generateAllCommentaries();
    
    // Save scheduler log to Supabase
    const endTime = Date.now();
    const duration = Math.round((endTime - startTime) / 1000);
    const logStatus = failed === 0 ? 'success' : (success > failed ? 'partial' : 'failed');
    
    // Detect trigger type from GitHub Actions environment
    const triggerType = process.env.GITHUB_EVENT_NAME === 'schedule' ? 'schedule' : 'manual';
    const runTimestamp = new Date().toISOString();
    
    try {
        const { error } = await supabase.from('scheduler_logs').insert({
            run_timestamp: runTimestamp,
            run_date: runTimestamp.split('T')[0],
            trigger_type: triggerType,
            status: logStatus,
            success_count: success,
            failed_count: failed,
            failed_datasets: JSON.stringify(failedDatasets),
            duration_seconds: duration,
            total_datasets: 41  // +3: l1_total_volume, l2_total_volume, daily_issuance
        });
        
        if (error) console.error('Failed to save scheduler log:', error.message);
        else console.log(`📝 Scheduler log saved (${triggerType})`);
    } catch (e) {
        console.error('Failed to save scheduler log:', e.message);
    }
    
    console.log('\n' + '='.repeat(60));
    console.log('🏁 COLLECTION COMPLETE');
    console.log(`⏱️ Total duration: ${duration} seconds`);
    console.log(`🤖 AI Commentary: ${commentaryResults.success}/7 generated`);
    console.log('='.repeat(60));
}

main().catch(e => { console.error('Fatal:', e); process.exit(1); });
