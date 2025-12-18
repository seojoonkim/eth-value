/**
 * ETHval Data Collector v7.1
 * 39개 전체 데이터셋 수집 (Dune API 포함)
 * + AI 일간 해설 생성 (Claude Haiku)
 */

const { createClient } = require('@supabase/supabase-js');

const SUPABASE_URL = process.env.SUPABASE_URL;
const SUPABASE_KEY = process.env.SUPABASE_SERVICE_KEY;
const DUNE_API_KEY = process.env.DUNE_API_KEY;
const ANTHROPIC_API_KEY = process.env.ANTHROPIC_API_KEY;

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

const supabase = createClient(SUPABASE_URL, SUPABASE_KEY);
const sleep = (ms) => new Promise(r => setTimeout(r, ms));

// ============================================================
// AI Commentary Section Definitions
// ============================================================
const COMMENTARY_SECTIONS = {
    investor_sentiment: {
        title: 'Investor Sentiment',
        title_ko: '투자자 심리',
        metrics: ['fear_greed', 'funding_rate', 'exchange_reserve', 'volatility'],
        tables: {
            fear_greed: 'historical_fear_greed',
            funding_rate: 'historical_funding_rate',
            exchange_reserve: 'historical_exchange_reserve',
            volatility: 'historical_volatility'
        }
    },
    market_position: {
        title: 'Market Position',
        title_ko: '시장 포지션',
        metrics: ['eth_dominance', 'eth_btc', 'mvrv', 'realized_price'],
        tables: {
            eth_dominance: 'historical_eth_dominance',
            eth_btc: 'historical_eth_btc',
            mvrv: 'historical_mvrv'
        }
    },
    supply_dynamics: {
        title: 'Supply Dynamics',
        title_ko: '공급 역학',
        metrics: ['staking', 'eth_burned', 'eth_issued', 'net_supply', 'effective_float'],
        tables: {
            staking: 'historical_staking',
            eth_supply: 'historical_eth_supply',
            gas_burn: 'historical_gas_burn'
        }
    },
    network_demand: {
        title: 'Network Demand',
        title_ko: '네트워크 수요',
        metrics: ['gas_price', 'gas_utilization', 'blob_fees', 'transactions'],
        tables: {
            gas_burn: 'historical_gas_burn',
            blob: 'historical_blob',
            transactions: 'historical_transactions'
        }
    },
    user_activity: {
        title: 'User Activity',
        title_ko: '사용자 활동',
        metrics: ['active_addresses', 'new_addresses', 'l2_addresses', 'whale_tx'],
        tables: {
            active_addresses: 'historical_active_addresses',
            l2_addresses: 'historical_l2_addresses',
            new_addresses: 'historical_new_addresses',
            whale_tx: 'historical_whale_transactions'
        }
    },
    locked_capital: {
        title: 'Locked Capital',
        title_ko: '잠긴 자본',
        metrics: ['ethereum_tvl', 'l2_tvl', 'stablecoins', 'lending_tvl', 'staked_eth'],
        tables: {
            ethereum_tvl: 'historical_ethereum_tvl',
            l2_tvl: 'historical_l2_tvl',
            stablecoins: 'historical_stablecoins',
            lending_tvl: 'historical_lending_tvl',
            staking: 'historical_staking'
        }
    },
    settlement_volume: {
        title: 'Settlement Volume',
        title_ko: '결제량',
        metrics: ['l1_volume', 'l2_volume', 'bridge_volume', 'dex_volume', 'stablecoin_volume'],
        tables: {
            l1_volume: 'historical_l1_volume',
            l2_volume: 'historical_l2_volume',
            bridge_volume: 'historical_bridge_volume',
            dex_volume: 'historical_dex_volume',
            stablecoin_volume: 'historical_stablecoin_volume'
        }
    }
};

// ============================================================
// AI Commentary Generation Functions
// ============================================================

/**
 * Fetch latest metrics data for a section
 */
async function fetchSectionMetrics(sectionKey) {
    const section = COMMENTARY_SECTIONS[sectionKey];
    if (!section) return null;
    
    const metricsData = {};
    const today = new Date().toISOString().split('T')[0];
    const sevenDaysAgo = new Date(Date.now() - 7 * 24 * 60 * 60 * 1000).toISOString().split('T')[0];
    const thirtyDaysAgo = new Date(Date.now() - 30 * 24 * 60 * 60 * 1000).toISOString().split('T')[0];
    
    for (const [metricKey, tableName] of Object.entries(section.tables)) {
        try {
            // Get recent data (last 7 days)
            const { data: recent } = await supabase
                .from(tableName)
                .select('*')
                .gte('date', sevenDaysAgo)
                .order('date', { ascending: false })
                .limit(7);
            
            // Get 30-day ago data for comparison
            const { data: older } = await supabase
                .from(tableName)
                .select('*')
                .lte('date', thirtyDaysAgo)
                .order('date', { ascending: false })
                .limit(1);
            
            if (recent && recent.length > 0) {
                metricsData[metricKey] = {
                    latest: recent[0],
                    recent: recent,
                    thirtyDaysAgo: older?.[0] || null
                };
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
 * Format metrics data for AI prompt
 */
function formatMetricsForPrompt(sectionKey, metricsData) {
    const section = COMMENTARY_SECTIONS[sectionKey];
    let prompt = `Section: ${section.title}\n\n`;
    prompt += `Current ETH Price: $${metricsData.eth_price?.latest?.close?.toFixed(2) || 'N/A'}\n\n`;
    prompt += `Key Metrics (Latest vs 30 days ago):\n`;
    
    for (const [key, data] of Object.entries(metricsData)) {
        if (key === 'eth_price') continue;
        if (!data?.latest) continue;
        
        const latest = data.latest;
        const older = data.thirtyDaysAgo;
        
        // Extract numeric values based on table structure
        let currentVal, oldVal, unit = '';
        
        if (latest.value !== undefined) {
            currentVal = latest.value;
            oldVal = older?.value;
        } else if (latest.tvl !== undefined) {
            currentVal = latest.tvl;
            oldVal = older?.tvl;
            unit = ' USD';
        } else if (latest.index !== undefined) {
            currentVal = latest.index;
            oldVal = older?.index;
        } else if (latest.rate !== undefined) {
            currentVal = latest.rate;
            oldVal = older?.rate;
            unit = '%';
        } else if (latest.staked_eth !== undefined) {
            currentVal = latest.staked_eth;
            oldVal = older?.staked_eth;
            unit = ' ETH';
        } else if (latest.dominance !== undefined) {
            currentVal = latest.dominance;
            oldVal = older?.dominance;
            unit = '%';
        } else if (latest.ratio !== undefined) {
            currentVal = latest.ratio;
            oldVal = older?.ratio;
        } else if (latest.reserve !== undefined) {
            currentVal = latest.reserve;
            oldVal = older?.reserve;
            unit = ' ETH';
        } else if (latest.avg_gas_price_gwei !== undefined) {
            currentVal = latest.avg_gas_price_gwei;
            oldVal = older?.avg_gas_price_gwei;
            unit = ' Gwei';
        } else if (latest.mvrv_ratio !== undefined) {
            currentVal = latest.mvrv_ratio;
            oldVal = older?.mvrv_ratio;
        } else if (latest.daily_volume !== undefined) {
            currentVal = latest.daily_volume;
            oldVal = older?.daily_volume;
            unit = ' USD';
        } else if (latest.blob_count !== undefined) {
            currentVal = latest.blob_count;
            oldVal = older?.blob_count;
        } else if (latest.active_addresses !== undefined) {
            currentVal = latest.active_addresses;
            oldVal = older?.active_addresses;
        } else if (latest.volatility !== undefined) {
            currentVal = latest.volatility;
            oldVal = older?.volatility;
            unit = '%';
        }
        
        if (currentVal !== undefined) {
            const change = oldVal ? ((currentVal - oldVal) / oldVal * 100).toFixed(1) : 'N/A';
            const changeStr = oldVal ? `(${change > 0 ? '+' : ''}${change}% vs 30d ago)` : '';
            
            // Format large numbers
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
        }
    }
    
    // Add 7-day trend
    prompt += `\n7-day trend:\n`;
    for (const [key, data] of Object.entries(metricsData)) {
        if (key === 'eth_price') continue;
        if (!data?.recent || data.recent.length < 2) continue;
        
        const first = data.recent[data.recent.length - 1];
        const last = data.recent[0];
        
        // Get comparable values
        let firstVal, lastVal;
        if (first.value !== undefined) { firstVal = first.value; lastVal = last.value; }
        else if (first.tvl !== undefined) { firstVal = first.tvl; lastVal = last.tvl; }
        else if (first.index !== undefined) { firstVal = first.index; lastVal = last.index; }
        else if (first.rate !== undefined) { firstVal = first.rate; lastVal = last.rate; }
        else if (first.dominance !== undefined) { firstVal = first.dominance; lastVal = last.dominance; }
        else if (first.mvrv_ratio !== undefined) { firstVal = first.mvrv_ratio; lastVal = last.mvrv_ratio; }
        
        if (firstVal && lastVal) {
            const weekChange = ((lastVal - firstVal) / firstVal * 100).toFixed(1);
            const trend = weekChange > 1 ? '📈 Rising' : weekChange < -1 ? '📉 Falling' : '➡️ Stable';
            prompt += `- ${key}: ${trend} (${weekChange > 0 ? '+' : ''}${weekChange}% this week)\n`;
        }
    }
    
    return prompt;
}

/**
 * Call Claude Haiku API to generate commentary
 * @param {string} lang - Language code: 'en', 'ko', 'zh', 'ja'
 */
async function generateCommentary(sectionKey, metricsData, lang = 'en') {
    if (!ANTHROPIC_API_KEY) return null;
    
    const section = COMMENTARY_SECTIONS[sectionKey];
    const metricsPrompt = formatMetricsForPrompt(sectionKey, metricsData);
    
    const langInstructions = {
        en: 'Write in English.',
        ko: 'Write in Korean (한국어로 작성하세요).',
        zh: 'Write in Simplified Chinese (用简体中文写).',
        ja: 'Write in Japanese (日本語で書いてください).'
    };
    
    const systemPrompt = `You are an expert Ethereum market analyst providing daily commentary for the ETHval dashboard. 
Your analysis should be:
- Objective and data-driven
- Concise (maximum 8 sentences)
- Focus on what the metrics indicate about market conditions
- Include brief outlook on potential price/valuation implications
- No disclaimers or investment advice warnings
- Write in a professional, analytical tone

${langInstructions[lang] || langInstructions.en}

Format: Write 6-8 sentences as a single paragraph. Start with the current state, then trends, then implications.`;

    const userPrompt = `Based on the following Ethereum ${section.title} metrics data, provide a brief daily analysis:

${metricsPrompt}

Write a 6-8 sentence analysis covering:
1. Current state of these metrics
2. Notable changes in the past 7-30 days  
3. What this suggests for ETH price direction and valuation

${langInstructions[lang] || ''}`;

    try {
        const response = await fetch('https://api.anthropic.com/v1/messages', {
            method: 'POST',
            headers: {
                'Content-Type': 'application/json',
                'x-api-key': ANTHROPIC_API_KEY,
                'anthropic-version': '2023-06-01'
            },
            body: JSON.stringify({
                model: 'claude-3-5-haiku-20241022',
                max_tokens: 500,
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
        return result.content?.[0]?.text || null;
        
    } catch (e) {
        console.error(`  Claude API call failed:`, e.message);
        return null;
    }
}

/**
 * Save commentary to Supabase (with multilingual support)
 */
async function saveCommentary(sectionKey, commentaries, metricsSnapshot) {
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
        const commentaries = {};
        for (const lang of LANGUAGES) {
            const commentary = await generateCommentary(sectionKey, metricsData, lang);
            if (commentary) {
                commentaries[lang] = commentary;
                console.log(`  ✓ ${lang.toUpperCase()}: ${commentary.length} chars`);
            } else {
                console.log(`  ⚠️ ${lang.toUpperCase()}: Failed`);
            }
            await sleep(500); // Rate limit between API calls
        }
        
        // Need at least English version
        if (!commentaries.en) {
            console.log(`  ❌ Failed to generate English commentary`);
            failed++;
            continue;
        }
        
        // Save to Supabase
        const saved = await saveCommentary(sectionKey, commentaries, metricsData);
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
    BLOB: 6350774,
    TX_VOLUME: 6350858,
    ACTIVE_ADDR: 6352303,
    L2_ACTIVE_ADDR: 6352308,
    L2_TX_VOLUME: 6352386,
    BRIDGE_VOLUME: 6352417,
    WHALE_TX: 6352498,
    NEW_ADDR: 6352513,
    MVRV: 6354057,
    STABLECOIN_VOL: 6353868,
    GAS_PRICE: 6354506  // Daily average gas price
};

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
                headers: { 'User-Agent': 'ETHval/7.0', 'Accept': 'application/json' }
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

// Dune API helper - fetch all results with pagination
// Note: Dune queries are scheduled to auto-refresh daily at 03:30-04:00 UTC
async function fetchDuneResults(queryId, maxRows = 10000) {
    if (!DUNE_API_KEY) return null;
    
    const allRows = [];
    const pageSize = 1000;
    let offset = 0;
    
    try {
        while (offset < maxRows) {
            const response = await fetch(
                `https://api.dune.com/api/v1/query/${queryId}/results?limit=${pageSize}&offset=${offset}`,
                { headers: { 'X-Dune-API-Key': DUNE_API_KEY } }
            );
            
            if (!response.ok) {
                console.error(`  Dune API error: ${response.status}`);
                break;
            }
            
            const data = await response.json();
            const rows = data?.result?.rows || [];
            
            if (rows.length === 0) break;
            
            allRows.push(...rows);
            offset += pageSize;
            
            if (rows.length < pageSize) break;
            await sleep(500); // Rate limit
        }
        
        return allRows;
    } catch (e) {
        console.error(`  Dune fetch error: ${e.message}`);
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
// 1. ETH Price (Binance)
// ============================================================
async function collect_eth_price() {
    console.log('\n📈 [1/29] ETH Price (Binance) + Volume (CoinGecko)...');
    
    // ═══════════════════════════════════════════════════════════════════
    // Step 1: Fetch price data from Binance (OHLC - more accurate)
    // ═══════════════════════════════════════════════════════════════════
    const data = await fetchJSON('https://api.binance.com/api/v3/klines?symbol=ETHUSDT&interval=1d&limit=1100');
    if (!data) return 0;
    
    const records = data.map(k => ({
        date: new Date(k[0]).toISOString().split('T')[0],
        open: parseFloat(k[1]), high: parseFloat(k[2]), low: parseFloat(k[3]),
        close: parseFloat(k[4]), volume: 0  // Volume will be from CoinGecko
    }));
    
    console.log(`  ✓ Binance price: ${records.length} days`);
    
    // ═══════════════════════════════════════════════════════════════════
    // Step 2: Fetch total market volume from CoinGecko
    // ═══════════════════════════════════════════════════════════════════
    try {
        const cgData = await fetchJSON('https://api.coingecko.com/api/v3/coins/ethereum/market_chart?vs_currency=usd&days=1100&interval=daily');
        
        if (cgData && cgData.total_volumes) {
            // Build date -> volume map
            const volumeMap = new Map();
            for (const [ts, vol] of cgData.total_volumes) {
                const date = new Date(ts).toISOString().split('T')[0];
                volumeMap.set(date, vol);
            }
            
            console.log(`  ✓ CoinGecko volume: ${volumeMap.size} days`);
            
            // Merge volume into price records
            let matched = 0;
            for (const record of records) {
                if (volumeMap.has(record.date)) {
                    record.volume = volumeMap.get(record.date);
                    matched++;
                }
            }
            console.log(`  ✓ Volume matched: ${matched}/${records.length} days`);
        } else {
            console.warn('  ⚠️ CoinGecko volume fetch failed');
        }
    } catch (e) {
        console.warn(`  ⚠️ CoinGecko error: ${e.message}`);
    }
    
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
        tvl: parseFloat(d.tvl.toFixed(2))
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
        fees: parseFloat(d[1].toFixed(2))
    }));
    return await upsertBatch('historical_protocol_fees', records);
}

// ============================================================
// 5. Staking Data (beaconcha.in)
// ============================================================
async function collect_staking() {
    console.log('\n🥩 [5/29] Staking Data...');
    const records = [];
    
    // beaconcha.in staked_ether 차트 (전체 Effective Balance 합계)
    const chart = await fetchJSON('https://beaconcha.in/api/v1/chart/staked_ether');
    if (chart?.status === 'OK' && chart.data) {
        console.log(`  📊 Beaconcha.in chart: ${chart.data.length} points`);
        
        // 날짜순 정렬
        const sortedData = chart.data
            .filter(item => Array.isArray(item) && item[1] > 0)
            .sort((a, b) => a[0] - b[0]);
        
        let prevValue = null;
        for (const item of sortedData) {
            const stakedEth = parseFloat(item[1]);
            const date = new Date(item[0]).toISOString().split('T')[0];
            
            // 기본 범위 검증 (15M ~ 40M)
            if (stakedEth < 15000000 || stakedEth > 40000000) {
                console.log(`  ⚠️ Skip ${date}: ${(stakedEth/1e6).toFixed(2)}M out of range`);
                continue;
            }
            
            // 일일 변동폭 검증 (전날 대비 2% 초과 변동 시 스킵)
            if (prevValue !== null) {
                const changePercent = Math.abs((stakedEth - prevValue) / prevValue * 100);
                if (changePercent > 2) {
                    console.log(`  ⚠️ Skip ${date}: ${changePercent.toFixed(2)}% daily change (abnormal)`);
                    continue;
                }
            }
            
            records.push({
                date: date,
                total_staked_eth: stakedEth,
                total_validators: Math.floor(stakedEth / 32),
                avg_apr: null
            });
            
            prevValue = stakedEth;
        }
        console.log(`  ✅ Valid records after filtering: ${records.length}`);
    }
    
    // APR from Lido
    const lido = await fetchJSON('https://eth-api.lido.fi/v1/protocol/steth/apr/sma');
    if (lido?.data?.smaApr) {
        const today = new Date().toISOString().split('T')[0];
        const idx = records.findIndex(r => r.date === today);
        if (idx >= 0) records[idx].avg_apr = parseFloat(lido.data.smaApr.toFixed(2));
    }
    
    // 최근 1095일만 유지
    const cutoff = new Date();
    cutoff.setDate(cutoff.getDate() - 1095);
    const filtered = records.filter(r => new Date(r.date) >= cutoff);
    
    // Dedupe (같은 날짜 중복 제거)
    const unique = new Map();
    filtered.forEach(r => unique.set(r.date, r));
    
    console.log(`  📦 ${unique.size} staking records to save`);
    return await upsertBatch('historical_staking', Array.from(unique.values()));
}

// ============================================================
// 6. Gas & Burn (Etherscan API for gas utilization)
// ============================================================
async function collect_gas_burn() {
    console.log('\n🔥 [6/29] Gas & Burn...');
    
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
        return 0;
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
        console.log('  ✅ No new records to add');
        return 0;
    }
    
    console.log(`  📦 Saving ${records.length} records (${gasPriceMap.size} with gas price)`);
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
    // ⚠️ 무료 API 없음 - CryptoQuant/Glassnode/CoinGlass 모두 유료
    // 실제 트렌드 기반 추정: 2022년 ~24M → 2025년 ~15M (지속적 감소)
    
    const today = new Date();
    const startDate = new Date('2022-01-01');
    const records = [];
    
    for (let i = 0; i < 1095; i++) {
        const date = new Date(today);
        date.setDate(date.getDate() - i);
        const dateStr = date.toISOString().split('T')[0];
        
        // 2022년: ~24M ETH → 2025년: ~15M ETH (꾸준한 감소)
        // FTX 붕괴 (2022.11) 이후 급격한 감소 → 이후 완만한 감소
        let baseTrend;
        if (date < new Date('2022-11-01')) {
            baseTrend = 24000000; // FTX 전
        } else if (date < new Date('2023-06-01')) {
            // FTX 붕괴 후 급감 (24M → 18M)
            const ftxProgress = (date - new Date('2022-11-01')) / (new Date('2023-06-01') - new Date('2022-11-01'));
            baseTrend = 24000000 - (6000000 * Math.min(1, ftxProgress));
        } else {
            // 2023년 중반 이후 완만한 감소 (18M → 15M)
            const postFtxProgress = (date - new Date('2023-06-01')) / (today - new Date('2023-06-01'));
            baseTrend = 18000000 - (3000000 * Math.min(1, postFtxProgress));
        }
        
        // 소폭 변동 (±1%)
        const noise = (Math.sin(i * 0.3) * 0.005 + Math.sin(i * 0.07) * 0.005) * baseTrend;
        const reserve = Math.max(14000000, baseTrend + noise);
        
        records.push({
            date: dateStr,
            reserve_eth: Math.round(reserve),
            source: 'estimated'
        });
    }
    
    console.log(`  📦 Generated ${records.length} estimated records (24M→15M trend)`);
    return await upsertBatch('historical_exchange_reserve', records);
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
                market_cap: mcap
            });
        }
    }
    return await upsertBatch('historical_nvt', records);
}

// ============================================================
// 21. Transactions (DefiLlama)
// ============================================================
async function collect_transactions() {
    console.log('\n📝 [21/29] Transactions (growthepie)...');
    
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
    console.log('\n🔗 [22/29] L2 Transactions (growthepie)...');
    
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
    console.log('\n🫧 [30/39] Blob Data (Dune)...');
    if (!DUNE_API_KEY) { console.log('  ⏭️ Skipped - No API key'); return 0; }
    
    const rows = await fetchDuneResults(DUNE_QUERIES.BLOB, 1000);
    if (!rows || rows.length === 0) return 0;
    
    const records = rows.map(r => ({
        date: r.block_date || r.date,
        blob_count: parseInt(r.blob_count || r.blobs || 0),
        blob_gas_used: parseFloat(r.blob_gas_used || 0),
        blob_fee_eth: parseFloat(r.blob_fee_eth || 0),
        source: 'dune'
    })).filter(r => r.date && r.blob_count > 0);
    
    console.log(`  📊 Got ${records.length} records`);
    return await upsertBatch('historical_blob_data', records);
}

// 31. L1 TX Volume (Dune)
async function collect_dune_l1_volume() {
    console.log('\n💸 [31/39] L1 TX Volume (Dune)...');
    if (!DUNE_API_KEY) { console.log('  ⏭️ Skipped - No API key'); return 0; }
    
    const rows = await fetchDuneResults(DUNE_QUERIES.TX_VOLUME, 5000);
    if (!rows || rows.length === 0) return 0;
    
    const records = rows.map(r => ({
        date: r.block_date || r.date,
        tx_volume_eth: parseFloat(r.tx_volume_eth || r.volume_eth || 0),
        tx_volume_usd: parseFloat(r.tx_volume_usd || r.volume_usd || 0),
        source: 'dune'
    })).filter(r => r.date && r.tx_volume_eth > 0);
    
    console.log(`  📊 Got ${records.length} records`);
    return await upsertBatch('historical_l1_volume', records);
}

// 32. Active Addresses L1 (Dune)
async function collect_dune_active_addr() {
    console.log('\n👥 [32/39] Active Addresses L1 (Dune)...');
    if (!DUNE_API_KEY) { console.log('  ⏭️ Skipped - No API key'); return 0; }
    
    const rows = await fetchDuneResults(DUNE_QUERIES.ACTIVE_ADDR, 5000);
    if (!rows || rows.length === 0) return 0;
    
    const records = rows.map(r => ({
        date: r.block_date || r.date,
        active_addresses: parseInt(r.active_addresses || r.unique_addresses || 0),
        source: 'dune'
    })).filter(r => r.date && r.active_addresses > 0);
    
    console.log(`  📊 Got ${records.length} records`);
    return await upsertBatch('historical_active_addresses', records);
}

// 33. L2 Active Addresses (Dune)
async function collect_dune_l2_addr() {
    console.log('\n👤 [33/39] L2 Active Addresses (Dune)...');
    if (!DUNE_API_KEY) { console.log('  ⏭️ Skipped - No API key'); return 0; }
    
    const rows = await fetchDuneResults(DUNE_QUERIES.L2_ACTIVE_ADDR, 10000);
    if (!rows || rows.length === 0) return 0;
    
    const records = rows.map(r => ({
        date: r.block_date || r.date,
        chain: r.chain || r.l2_name || 'unknown',
        active_addresses: parseInt(r.active_addresses || r.unique_addresses || 0),
        source: 'dune'
    })).filter(r => r.date && r.active_addresses > 0);
    
    console.log(`  📊 Got ${records.length} records`);
    return await upsertBatch('historical_l2_addresses', records, 'date,chain');
}

// 34. L2 TX Volume (Dune)
async function collect_dune_l2_volume() {
    console.log('\n🔗 [34/39] L2 TX Volume (Dune)...');
    if (!DUNE_API_KEY) { console.log('  ⏭️ Skipped - No API key'); return 0; }
    
    const rows = await fetchDuneResults(DUNE_QUERIES.L2_TX_VOLUME, 10000);
    if (!rows || rows.length === 0) return 0;
    
    const records = rows.map(r => ({
        date: r.block_date || r.date,
        chain: r.chain || r.l2_name || 'unknown',
        tx_volume_eth: parseFloat(r.tx_volume_eth || r.volume_eth || 0),
        source: 'dune'
    })).filter(r => r.date && r.tx_volume_eth > 0);
    
    console.log(`  📊 Got ${records.length} records`);
    return await upsertBatch('historical_l2_tx_volume', records, 'date,chain');
}

// 35. Bridge Volume (Dune)
async function collect_dune_bridge() {
    console.log('\n🌉 [35/39] Bridge Volume (Dune)...');
    if (!DUNE_API_KEY) { console.log('  ⏭️ Skipped - No API key'); return 0; }
    
    const rows = await fetchDuneResults(DUNE_QUERIES.BRIDGE_VOLUME, 10000);
    if (!rows || rows.length === 0) return 0;
    
    const records = rows.map(r => ({
        date: r.block_date || r.date,
        chain: r.chain || r.l2_name || 'unknown',
        bridge_volume_eth: parseFloat(r.bridge_volume_eth || r.volume_eth || 0),
        source: 'dune'
    })).filter(r => r.date && r.bridge_volume_eth > 0);
    
    console.log(`  📊 Got ${records.length} records`);
    return await upsertBatch('historical_bridge_volume', records, 'date,chain');
}

// 36. Whale Transactions (Dune)
async function collect_dune_whale() {
    console.log('\n🐋 [36/39] Whale Transactions (Dune)...');
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
    console.log('\n🆕 [37/39] New Addresses (Dune)...');
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
    console.log('\n📊 [38/39] MVRV Ratio (Dune)...');
    if (!DUNE_API_KEY) { console.log('  ⏭️ Skipped - No API key'); return 0; }
    
    const rows = await fetchDuneResults(DUNE_QUERIES.MVRV, 5000);
    if (!rows || rows.length === 0) return 0;
    
    const records = rows.map(r => ({
        date: r.block_date || r.date,
        mvrv_ratio: parseFloat(r.mvrv_ratio || r.mvrv || 0),
        realized_price: parseFloat(r.realized_price || r.realised_price || 0),
        market_cap: parseFloat(r.market_cap || 0),
        realized_cap: parseFloat(r.realized_cap || r.realised_cap || 0),
        source: 'dune'
    })).filter(r => r.date && r.mvrv_ratio > 0);
    
    console.log(`  📊 Got ${records.length} records`);
    return await upsertBatch('historical_mvrv', records);
}

// 39. Stablecoin Volume (Dune)
async function collect_dune_stablecoin_vol() {
    console.log('\n💵 [39/40] Stablecoin Volume (Dune)...');
    if (!DUNE_API_KEY) { console.log('  ⏭️ Skipped - No API key'); return 0; }
    
    const rows = await fetchDuneResults(DUNE_QUERIES.STABLECOIN_VOL, 5000);
    if (!rows || rows.length === 0) return 0;
    
    const records = rows.map(r => ({
        date: r.block_date || r.date,
        daily_volume: parseFloat(r.daily_volume || r.volume || 0),
        tx_count: parseInt(r.tx_count || 0),
        source: 'dune'
    })).filter(r => r.date && r.daily_volume > 0);
    
    console.log(`  📊 Got ${records.length} records`);
    return await upsertBatch('historical_stablecoin_volume', records);
}

// 40. Gas Price (Dune) - Daily average gas price
async function collect_dune_gas_price() {
    console.log('\n⛽ [40/40] Gas Price (Dune)...');
    if (!DUNE_API_KEY) { console.log('  ⏭️ Skipped - No API key'); return 0; }
    if (DUNE_QUERIES.GAS_PRICE === 0) { 
        console.log('  ⏭️ Skipped - Query ID not set'); 
        return 0; 
    }
    
    const rows = await fetchDuneResults(DUNE_QUERIES.GAS_PRICE, 5000);
    if (!rows || rows.length === 0) return 0;
    
    // Update historical_gas_burn table with gas price data
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
            gas_utilization: parseFloat(r.gas_utilization || r.utilization || 0),
            transaction_count: parseInt(r.tx_count || r.transaction_count || 0),
            source: 'dune'
        };
    }).filter(r => r.date && r.avg_gas_price_gwei > 0);
    
    console.log(`  📊 Got ${records.length} records with gas price`);
    if (records.length > 0) {
        console.log(`  📅 Date range: ${records[records.length-1].date} to ${records[0].date}`);
        console.log(`  ⛽ Sample: ${records[0].date} = ${records[0].avg_gas_price_gwei.toFixed(2)} Gwei`);
    }
    
    // Update existing records in historical_gas_burn
    let updated = 0;
    for (const record of records) {
        const { error } = await supabase
            .from('historical_gas_burn')
            .update({ 
                avg_gas_price_gwei: record.avg_gas_price_gwei,
                gas_utilization: record.gas_utilization > 0 ? record.gas_utilization : null,
                transaction_count: record.transaction_count || null,
                source: 'dune'
            })
            .eq('date', record.date);
        
        if (!error) updated++;
    }
    
    console.log(`  ✅ Updated ${updated} records in historical_gas_burn`);
    return updated;
}

// ============================================================
// Main
// ============================================================
async function main() {
    console.log('🚀 ETHval Data Collector v7.0');
    console.log(`📅 ${new Date().toISOString()}`);
    console.log('='.repeat(60));
    console.log('Collecting 40 datasets (29 API + 11 Dune)...\n');
    if (DUNE_API_KEY) {
        console.log('✅ Dune API Key detected');
        console.log('📌 Note: Dune queries auto-refresh daily at 03:30-04:00 UTC');
    } else {
        console.log('⚠️ No Dune API Key - Dune collections will be skipped');
    }
    
    const startTime = Date.now();
    const results = {};
    
    // Collect API data
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
    
    // Dune API Collections (fetch pre-scheduled results)
    console.log('\n' + '='.repeat(60));
    console.log('🔷 DUNE API COLLECTIONS (fetching scheduled results)');
    console.log('='.repeat(60));
    
    results.dune_blob = await collect_dune_blob(); await sleep(1000);
    // results.dune_l1_volume = await collect_dune_l1_volume(); await sleep(1000); // 테이블 없음 - 스킵
    results.dune_active_addr = await collect_dune_active_addr(); await sleep(1000);
    results.dune_l2_addr = await collect_dune_l2_addr(); await sleep(1000);
    results.dune_l2_volume = await collect_dune_l2_volume(); await sleep(1000);
    results.dune_bridge = await collect_dune_bridge(); await sleep(1000);
    results.dune_whale = await collect_dune_whale(); await sleep(1000);
    results.dune_new_addr = await collect_dune_new_addr(); await sleep(1000);
    results.dune_mvrv = await collect_dune_mvrv(); await sleep(1000);
    results.dune_stablecoin_vol = await collect_dune_stablecoin_vol(); await sleep(1000);
    results.dune_gas_price = await collect_dune_gas_price();
    
    // Summary
    console.log('\n' + '='.repeat(60));
    console.log('📊 COLLECTION SUMMARY:');
    console.log('='.repeat(60));
    
    let success = 0, failed = 0;
    const failedDatasets = [];
    Object.entries(results).forEach(([key, count]) => {
        const status = count > 0 ? '✅' : '❌';
        console.log(`${status} ${key.padEnd(20)} : ${count}`);
        if (count > 0) success++; 
        else {
            failed++;
            failedDatasets.push(key);
        }
    });
    
    console.log('='.repeat(60));
    console.log(`✅ Success: ${success}/39  |  ❌ Failed: ${failed}/39`);
    console.log('='.repeat(60));
    
    // ============================================================
    // AI Daily Commentary Generation
    // ============================================================
    const commentaryResults = await generateAllCommentaries();
    
    // Save scheduler log to Supabase
    const endTime = Date.now();
    const duration = Math.round((endTime - startTime) / 1000);
    const logStatus = failed === 0 ? 'success' : (success > failed ? 'partial' : 'failed');
    
    try {
        const { error } = await supabase.from('scheduler_logs').upsert({
            run_date: new Date().toISOString().split('T')[0],
            status: logStatus,
            success_count: success,
            failed_count: failed,
            failed_datasets: JSON.stringify(failedDatasets),
            duration_seconds: duration,
            total_datasets: 39  // l1_volume 제외
        }, { onConflict: 'run_date' });
        
        if (error) console.error('Failed to save scheduler log:', error.message);
        else console.log('📝 Scheduler log saved to Supabase');
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
