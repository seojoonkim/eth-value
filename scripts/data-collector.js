/**
 * ETHval Historical Data Collector
 * Supabase + GitHub Actions 버전
 * 
 * 환경변수:
 * - SUPABASE_URL: Supabase 프로젝트 URL
 * - SUPABASE_SERVICE_KEY: Supabase service_role 키 (anon 키 아님!)
 * - CRYPTOCOMPARE_API_KEY: CryptoCompare API 키 (선택)
 * - ETHERSCAN_API_KEY: Etherscan API 키 (선택)
 */

const https = require('https');
const http = require('http');

// ============================================
// Configuration
// ============================================
const CONFIG = {
    SUPABASE_URL: process.env.SUPABASE_URL,
    SUPABASE_SERVICE_KEY: process.env.SUPABASE_SERVICE_KEY,
    CRYPTOCOMPARE_API_KEY: process.env.CRYPTOCOMPARE_API_KEY || '',
    ETHERSCAN_API_KEY: process.env.ETHERSCAN_API_KEY || '',
    
    // 수집 기간 (일)
    DAYS_TO_FETCH: 1095, // 3년
    
    // Rate limiting (ms)
    RATE_LIMIT_DELAY: 300,
};

// Validate config
if (!CONFIG.SUPABASE_URL || !CONFIG.SUPABASE_SERVICE_KEY) {
    console.error('❌ SUPABASE_URL and SUPABASE_SERVICE_KEY are required');
    process.exit(1);
}

// ============================================
// Supabase Client (Native Node.js)
// ============================================
class SupabaseClient {
    constructor(url, key) {
        this.url = url.replace(/\/$/, '');
        this.key = key;
    }

    async query(table, method = 'GET', body = null, queryParams = '') {
        const url = `${this.url}/rest/v1/${table}${queryParams}`;
        
        return new Promise((resolve, reject) => {
            const urlObj = new URL(url);
            const options = {
                hostname: urlObj.hostname,
                path: urlObj.pathname + urlObj.search,
                method: method,
                headers: {
                    'apikey': this.key,
                    'Authorization': `Bearer ${this.key}`,
                    'Content-Type': 'application/json',
                    'Prefer': method === 'POST' ? 'resolution=merge-duplicates,return=minimal' : 'return=representation'
                }
            };

            const req = https.request(options, (res) => {
                let data = '';
                res.on('data', chunk => data += chunk);
                res.on('end', () => {
                    try {
                        const result = data ? JSON.parse(data) : null;
                        if (res.statusCode >= 200 && res.statusCode < 300) {
                            resolve(result);
                        } else {
                            reject(new Error(`HTTP ${res.statusCode}: ${JSON.stringify(result)}`));
                        }
                    } catch (e) {
                        if (res.statusCode >= 200 && res.statusCode < 300) {
                            resolve(null);
                        } else {
                            reject(new Error(`HTTP ${res.statusCode}: ${data}`));
                        }
                    }
                });
            });

            req.on('error', reject);
            
            if (body) {
                req.write(JSON.stringify(body));
            }
            req.end();
        });
    }

    async select(table, columns = '*', filters = {}) {
        let query = `?select=${columns}`;
        for (const [key, value] of Object.entries(filters)) {
            query += `&${key}=${encodeURIComponent(value)}`;
        }
        return this.query(table, 'GET', null, query);
    }

    async upsert(table, data) {
        // Supabase upsert via POST with Prefer header
        return this.query(table, 'POST', data, '?on_conflict=date');
    }

    async upsertWithConflict(table, data, conflictColumn) {
        return this.query(table, 'POST', data, `?on_conflict=${conflictColumn}`);
    }
}

const supabase = new SupabaseClient(CONFIG.SUPABASE_URL, CONFIG.SUPABASE_SERVICE_KEY);

// ============================================
// Utility Functions
// ============================================
function fetch(url) {
    return new Promise((resolve, reject) => {
        const client = url.startsWith('https') ? https : http;
        
        client.get(url, (res) => {
            let data = '';
            res.on('data', chunk => data += chunk);
            res.on('end', () => {
                try {
                    resolve(JSON.parse(data));
                } catch (e) {
                    reject(new Error(`Failed to parse JSON: ${data.substring(0, 200)}`));
                }
            });
        }).on('error', reject);
    });
}

function sleep(ms) {
    return new Promise(resolve => setTimeout(resolve, ms));
}

function formatDate(date) {
    return date.toISOString().split('T')[0];
}

function log(level, dataset, message) {
    const icons = { info: 'ℹ️', success: '✅', warning: '⚠️', error: '❌' };
    console.log(`${icons[level] || '•'} [${dataset}] ${message}`);
}

async function logToSupabase(dataset, type, message, details = null) {
    try {
        await supabase.query('data_collection_logs', 'POST', {
            dataset_name: dataset,
            log_type: type,
            message: message,
            details: details
        });
    } catch (e) {
        // Ignore logging errors
    }
}

async function updateStatus(dataset, status, extra = {}) {
    try {
        await supabase.query('data_collection_status', 'PATCH', {
            status,
            last_run_at: new Date().toISOString(),
            updated_at: new Date().toISOString(),
            ...extra
        }, `?dataset_name=eq.${dataset}`);
    } catch (e) {
        console.error(`Failed to update status for ${dataset}:`, e.message);
    }
}

// ============================================
// Data Collectors
// ============================================

/**
 * 1. ETH Price (CryptoCompare)
 */
async function collectETHPrice() {
    const dataset = 'eth_price';
    log('info', dataset, 'Starting collection...');
    
    try {
        const allData = [];
        const batchSize = 2000;
        let toTs = Math.floor(Date.now() / 1000);
        
        while (allData.length < CONFIG.DAYS_TO_FETCH) {
            const apiKey = CONFIG.CRYPTOCOMPARE_API_KEY ? `&api_key=${CONFIG.CRYPTOCOMPARE_API_KEY}` : '';
            const url = `https://min-api.cryptocompare.com/data/v2/histoday?fsym=ETH&tsym=USD&limit=${batchSize}&toTs=${toTs}${apiKey}`;
            
            const response = await fetch(url);
            
            if (response.Response === 'Error') {
                throw new Error(response.Message);
            }
            
            const data = response.Data?.Data || [];
            if (data.length === 0) break;
            
            allData.push(...data);
            toTs = data[0].time - 86400;
            
            log('info', dataset, `Fetched ${allData.length} days...`);
            await sleep(CONFIG.RATE_LIMIT_DELAY);
        }
        
        // Transform and save
        const records = allData
            .filter(d => d.close > 0)
            .map(d => ({
                date: formatDate(new Date(d.time * 1000)),
                timestamp: d.time,
                open: d.open,
                high: d.high,
                low: d.low,
                close: d.close,
                volume: d.volumeto
            }));
        
        // Batch upsert (500 at a time)
        for (let i = 0; i < records.length; i += 500) {
            const batch = records.slice(i, i + 500);
            await supabase.upsert('historical_eth_price', batch);
            log('info', dataset, `Saved ${Math.min(i + 500, records.length)}/${records.length} records`);
        }
        
        await updateStatus(dataset, 'success', {
            record_count: records.length,
            date_from: records[0]?.date,
            date_to: records[records.length - 1]?.date
        });
        
        log('success', dataset, `Completed: ${records.length} records`);
        return true;
    } catch (error) {
        log('error', dataset, error.message);
        await updateStatus(dataset, 'failed', { last_error: error.message });
        await logToSupabase(dataset, 'error', error.message);
        return false;
    }
}

/**
 * 2. Ethereum TVL (DefiLlama)
 */
async function collectEthereumTVL() {
    const dataset = 'ethereum_tvl';
    log('info', dataset, 'Starting collection...');
    
    try {
        const url = 'https://api.llama.fi/v2/historicalChainTvl/Ethereum';
        const data = await fetch(url);
        
        if (!Array.isArray(data)) {
            throw new Error('Invalid response format');
        }
        
        const records = data.map(d => ({
            date: formatDate(new Date(d.date * 1000)),
            timestamp: d.date,
            tvl: d.tvl
        }));
        
        // Batch upsert
        for (let i = 0; i < records.length; i += 500) {
            const batch = records.slice(i, i + 500);
            await supabase.upsert('historical_ethereum_tvl', batch);
        }
        
        await updateStatus(dataset, 'success', {
            record_count: records.length,
            date_from: records[0]?.date,
            date_to: records[records.length - 1]?.date
        });
        
        log('success', dataset, `Completed: ${records.length} records`);
        return true;
    } catch (error) {
        log('error', dataset, error.message);
        await updateStatus(dataset, 'failed', { last_error: error.message });
        return false;
    }
}

/**
 * 3. L2 TVL (DefiLlama)
 */
async function collectL2TVL() {
    const dataset = 'l2_tvl';
    log('info', dataset, 'Starting collection...');
    
    const chains = [
        'Arbitrum', 'Optimism', 'Base', 'Polygon zkEVM',
        'zkSync Era', 'Linea', 'Scroll', 'Blast', 'Mantle', 'Starknet'
    ];
    
    let totalRecords = 0;
    let successChains = 0;
    
    try {
        for (const chain of chains) {
            try {
                const url = `https://api.llama.fi/v2/historicalChainTvl/${encodeURIComponent(chain)}`;
                const data = await fetch(url);
                
                if (!Array.isArray(data) || data.length === 0) {
                    log('warning', dataset, `No data for ${chain}`);
                    continue;
                }
                
                const records = data.map(d => ({
                    date: formatDate(new Date(d.date * 1000)),
                    timestamp: d.date,
                    chain: chain,
                    tvl: d.tvl
                }));
                
                // Batch upsert with composite key
                for (let i = 0; i < records.length; i += 500) {
                    const batch = records.slice(i, i + 500);
                    await supabase.upsertWithConflict('historical_l2_tvl', batch, 'date,chain');
                }
                
                totalRecords += records.length;
                successChains++;
                log('info', dataset, `${chain}: ${records.length} records`);
                
                await sleep(CONFIG.RATE_LIMIT_DELAY);
            } catch (e) {
                log('warning', dataset, `Failed ${chain}: ${e.message}`);
            }
        }
        
        const status = successChains === chains.length ? 'success' : 'partial';
        await updateStatus(dataset, status, {
            record_count: totalRecords,
            last_warning: successChains < chains.length ? `${chains.length - successChains} chains failed` : null
        });
        
        log('success', dataset, `Completed: ${totalRecords} records from ${successChains}/${chains.length} chains`);
        return true;
    } catch (error) {
        log('error', dataset, error.message);
        await updateStatus(dataset, 'failed', { last_error: error.message });
        return false;
    }
}

/**
 * 4. Protocol Fees (DefiLlama)
 */
async function collectProtocolFees() {
    const dataset = 'protocol_fees';
    log('info', dataset, 'Starting collection...');
    
    try {
        const url = 'https://api.llama.fi/summary/fees/ethereum?dataType=dailyFees';
        const data = await fetch(url);
        
        if (!data.totalDataChart) {
            throw new Error('Invalid response format');
        }
        
        const records = data.totalDataChart.map(([timestamp, fees]) => ({
            date: formatDate(new Date(timestamp * 1000)),
            timestamp: timestamp,
            fees: fees
        }));
        
        // Batch upsert
        for (let i = 0; i < records.length; i += 500) {
            const batch = records.slice(i, i + 500);
            await supabase.upsert('historical_protocol_fees', batch);
        }
        
        await updateStatus(dataset, 'success', {
            record_count: records.length,
            date_from: records[0]?.date,
            date_to: records[records.length - 1]?.date
        });
        
        log('success', dataset, `Completed: ${records.length} records`);
        return true;
    } catch (error) {
        log('error', dataset, error.message);
        await updateStatus(dataset, 'failed', { last_error: error.message });
        return false;
    }
}

/**
 * 5. Staking Data (beaconcha.in)
 */
async function collectStakingData() {
    const dataset = 'staking_data';
    log('info', dataset, 'Starting collection...');
    
    try {
        // beaconcha.in의 차트 데이터는 직접 접근이 어려움
        // 대안: rated.network API 또는 수동 입력 안내
        
        log('warning', dataset, 'Automated collection not available. Manual input required.');
        await updateStatus(dataset, 'partial', {
            last_warning: 'Requires manual data from beaconcha.in/charts'
        });
        await logToSupabase(dataset, 'warning', 'Manual collection required from beaconcha.in');
        
        return false;
    } catch (error) {
        log('error', dataset, error.message);
        await updateStatus(dataset, 'failed', { last_error: error.message });
        return false;
    }
}

/**
 * 6. Gas & Burn Data (Etherscan)
 */
async function collectGasBurn() {
    const dataset = 'gas_burn';
    log('info', dataset, 'Starting collection...');
    
    if (!CONFIG.ETHERSCAN_API_KEY) {
        log('warning', dataset, 'ETHERSCAN_API_KEY not set. Limited data available.');
    }
    
    try {
        const endDate = formatDate(new Date());
        const startDate = formatDate(new Date(Date.now() - CONFIG.DAYS_TO_FETCH * 24 * 60 * 60 * 1000));
        const apiKey = CONFIG.ETHERSCAN_API_KEY || 'YourApiKeyToken';
        
        // Fetch daily gas price
        const gasPriceUrl = `https://api.etherscan.io/api?module=stats&action=dailyavggasprice&startdate=${startDate}&enddate=${endDate}&sort=asc&apikey=${apiKey}`;
        const gasPriceData = await fetch(gasPriceUrl);
        
        await sleep(250); // Etherscan rate limit
        
        // Fetch daily tx count
        const txCountUrl = `https://api.etherscan.io/api?module=stats&action=dailytx&startdate=${startDate}&enddate=${endDate}&sort=asc&apikey=${apiKey}`;
        const txCountData = await fetch(txCountUrl);
        
        // Check for errors
        if (gasPriceData.status !== '1' || txCountData.status !== '1') {
            const msg = gasPriceData.message || txCountData.message || 'API error';
            if (msg.includes('API Key')) {
                throw new Error('Valid Etherscan API key required');
            }
            throw new Error(msg);
        }
        
        // Merge data by date
        const gasPriceMap = new Map();
        (gasPriceData.result || []).forEach(d => {
            gasPriceMap.set(d.UTCDate, {
                avg_gas_price_gwei: d.avgGasPrice_Wei ? d.avgGasPrice_Wei / 1e9 : null
            });
        });
        
        const records = (txCountData.result || []).map(d => ({
            date: d.UTCDate,
            timestamp: parseInt(d.unixTimeStamp),
            avg_gas_price_gwei: gasPriceMap.get(d.UTCDate)?.avg_gas_price_gwei || null,
            transaction_count: parseInt(d.transactionCount) || null
        }));
        
        // Batch upsert
        for (let i = 0; i < records.length; i += 500) {
            const batch = records.slice(i, i + 500);
            await supabase.upsert('historical_gas_burn', batch);
        }
        
        await updateStatus(dataset, 'success', {
            record_count: records.length,
            date_from: records[0]?.date,
            date_to: records[records.length - 1]?.date
        });
        
        log('success', dataset, `Completed: ${records.length} records`);
        return true;
    } catch (error) {
        log('error', dataset, error.message);
        await updateStatus(dataset, 'failed', { last_error: error.message });
        return false;
    }
}

/**
 * 7. Active Addresses
 */
async function collectActiveAddresses() {
    const dataset = 'active_addresses';
    log('info', dataset, 'Starting collection...');
    
    try {
        // Etherscan free API doesn't provide active addresses
        // Requires Dune Analytics or Etherscan Pro
        
        log('warning', dataset, 'Requires Dune Analytics or Etherscan Pro. Manual input required.');
        await updateStatus(dataset, 'partial', {
            last_warning: 'Use Dune Analytics: https://dune.com/browse/dashboards?q=ethereum%20active%20addresses'
        });
        
        return false;
    } catch (error) {
        log('error', dataset, error.message);
        await updateStatus(dataset, 'failed', { last_error: error.message });
        return false;
    }
}

/**
 * 8. ETH Supply (Etherscan)
 */
async function collectETHSupply() {
    const dataset = 'eth_supply';
    log('info', dataset, 'Starting collection...');
    
    try {
        const apiKey = CONFIG.ETHERSCAN_API_KEY || 'YourApiKeyToken';
        const url = `https://api.etherscan.io/api?module=stats&action=ethsupply2&apikey=${apiKey}`;
        const data = await fetch(url);
        
        if (data.status !== '1' || !data.result) {
            throw new Error(data.message || 'Failed to fetch supply');
        }
        
        const record = {
            date: formatDate(new Date()),
            eth_supply: parseFloat(data.result.EthSupply) / 1e18,
            eth2_staking: parseFloat(data.result.Eth2Staking) / 1e18,
            burnt_fees: parseFloat(data.result.BurntFees) / 1e18,
            withdrawn_total: parseFloat(data.result.WithdrawnTotal) / 1e18
        };
        
        await supabase.upsert('historical_eth_supply', [record]);
        
        await updateStatus(dataset, 'success', {
            record_count: 1,
            date_from: record.date,
            date_to: record.date
        });
        
        log('success', dataset, `Completed: Supply data saved`);
        return true;
    } catch (error) {
        log('error', dataset, error.message);
        await updateStatus(dataset, 'failed', { last_error: error.message });
        return false;
    }
}

// ============================================
// Main Execution
// ============================================
async function main() {
    console.log('═'.repeat(60));
    console.log('🚀 ETHval Historical Data Collector');
    console.log('═'.repeat(60));
    console.log(`📅 Target: ${CONFIG.DAYS_TO_FETCH} days (3 years)`);
    console.log(`🗄️ Database: ${CONFIG.SUPABASE_URL}`);
    console.log('═'.repeat(60));
    console.log('');
    
    const collectors = [
        { name: 'ETH Price', fn: collectETHPrice },
        { name: 'Ethereum TVL', fn: collectEthereumTVL },
        { name: 'L2 TVL', fn: collectL2TVL },
        { name: 'Protocol Fees', fn: collectProtocolFees },
        { name: 'Staking Data', fn: collectStakingData },
        { name: 'Gas & Burn', fn: collectGasBurn },
        { name: 'Active Addresses', fn: collectActiveAddresses },
        { name: 'ETH Supply', fn: collectETHSupply },
    ];
    
    let successCount = 0;
    let partialCount = 0;
    let failedCount = 0;
    
    for (const collector of collectors) {
        console.log(`\n${'─'.repeat(50)}`);
        console.log(`📦 ${collector.name}`);
        console.log('─'.repeat(50));
        
        try {
            const result = await collector.fn();
            if (result) {
                successCount++;
            } else {
                partialCount++;
            }
        } catch (error) {
            console.error(`❌ ${collector.name} failed:`, error.message);
            failedCount++;
        }
        
        await sleep(500);
    }
    
    // Summary
    console.log('\n' + '═'.repeat(60));
    console.log('📊 COLLECTION SUMMARY');
    console.log('═'.repeat(60));
    console.log(`✅ Success: ${successCount}`);
    console.log(`⚠️ Partial: ${partialCount}`);
    console.log(`❌ Failed: ${failedCount}`);
    console.log('═'.repeat(60));
    
    // Exit with error code if any failures
    if (failedCount > 0) {
        process.exit(1);
    }
}

main().catch(error => {
    console.error('Fatal error:', error);
    process.exit(1);
});
