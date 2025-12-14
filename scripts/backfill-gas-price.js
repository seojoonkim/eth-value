/**
 * Backfill Gas Price Data
 * 
 * This script fetches historical daily average gas price from Etherscan
 * and updates the historical_gas_burn table where avg_gas_price_gwei is null.
 * 
 * Usage:
 *   SUPABASE_URL=xxx SUPABASE_KEY=xxx ETHERSCAN_API_KEY=xxx node backfill-gas-price.js
 */

import { createClient } from '@supabase/supabase-js';

const SUPABASE_URL = process.env.SUPABASE_URL;
const SUPABASE_KEY = process.env.SUPABASE_KEY;
const ETHERSCAN_API_KEY = process.env.ETHERSCAN_API_KEY;

if (!SUPABASE_URL || !SUPABASE_KEY || !ETHERSCAN_API_KEY) {
    console.error('Missing required environment variables: SUPABASE_URL, SUPABASE_KEY, ETHERSCAN_API_KEY');
    process.exit(1);
}

const supabase = createClient(SUPABASE_URL, SUPABASE_KEY);

async function fetchJSON(url) {
    try {
        const res = await fetch(url);
        if (!res.ok) return null;
        return await res.json();
    } catch (e) {
        console.error('Fetch error:', e.message);
        return null;
    }
}

async function sleep(ms) {
    return new Promise(resolve => setTimeout(resolve, ms));
}

async function main() {
    console.log('🔄 Backfilling Gas Price Data...\n');
    
    // 1. Get date range from historical_gas_burn where avg_gas_price_gwei is null
    const { data: nullRecords, error } = await supabase
        .from('historical_gas_burn')
        .select('date')
        .is('avg_gas_price_gwei', null)
        .order('date', { ascending: true });
    
    if (error) {
        console.error('Error fetching null records:', error);
        return;
    }
    
    if (!nullRecords || nullRecords.length === 0) {
        console.log('✅ No records need backfilling');
        return;
    }
    
    console.log(`📊 Found ${nullRecords.length} records with null gas price`);
    
    const startDate = nullRecords[0].date;
    const endDate = nullRecords[nullRecords.length - 1].date;
    console.log(`📅 Date range: ${startDate} to ${endDate}`);
    
    // 2. Fetch gas price from Etherscan (API has max 10000 days per call)
    // Split into chunks if needed
    const chunkSize = 365; // 1 year at a time
    const allGasPrices = new Map();
    
    let currentStart = new Date(startDate);
    const finalEnd = new Date(endDate);
    
    while (currentStart <= finalEnd) {
        const chunkEnd = new Date(currentStart);
        chunkEnd.setDate(chunkEnd.getDate() + chunkSize);
        if (chunkEnd > finalEnd) chunkEnd.setTime(finalEnd.getTime());
        
        const startStr = currentStart.toISOString().split('T')[0];
        const endStr = chunkEnd.toISOString().split('T')[0];
        
        console.log(`\n⛽ Fetching gas price: ${startStr} to ${endStr}...`);
        
        const url = `https://api.etherscan.io/api?module=stats&action=dailyavggasprice&startdate=${startStr}&enddate=${endStr}&sort=asc&apikey=${ETHERSCAN_API_KEY}`;
        const data = await fetchJSON(url);
        
        if (data?.status === '1' && data.result) {
            console.log(`   Got ${data.result.length} records`);
            data.result.forEach(d => {
                const gasPriceWei = parseFloat(d.avgGasPrice_Wei || 0);
                const gasPriceGwei = gasPriceWei / 1e9;
                if (gasPriceGwei > 0 && gasPriceGwei < 1000) {
                    allGasPrices.set(d.UTCDate, parseFloat(gasPriceGwei.toFixed(2)));
                }
            });
        } else {
            console.log(`   ⚠️ No data or error: ${data?.message || 'unknown'}`);
        }
        
        // Rate limit: 5 calls per second
        await sleep(250);
        
        currentStart = new Date(chunkEnd);
        currentStart.setDate(currentStart.getDate() + 1);
    }
    
    console.log(`\n📦 Total gas prices collected: ${allGasPrices.size}`);
    
    if (allGasPrices.size === 0) {
        console.log('⚠️ No gas prices fetched, exiting');
        return;
    }
    
    // 3. Update Supabase records
    console.log('\n💾 Updating Supabase...');
    
    let updated = 0;
    let notFound = 0;
    
    for (const record of nullRecords) {
        const gasPrice = allGasPrices.get(record.date);
        if (gasPrice) {
            const { error: updateError } = await supabase
                .from('historical_gas_burn')
                .update({ 
                    avg_gas_price_gwei: gasPrice,
                    source: 'etherscan'
                })
                .eq('date', record.date);
            
            if (updateError) {
                console.error(`Error updating ${record.date}:`, updateError);
            } else {
                updated++;
            }
        } else {
            notFound++;
        }
        
        // Log progress every 100 records
        if ((updated + notFound) % 100 === 0) {
            console.log(`   Progress: ${updated + notFound}/${nullRecords.length} (${updated} updated, ${notFound} not found)`);
        }
    }
    
    console.log(`\n✅ Done! Updated ${updated} records, ${notFound} dates not found in Etherscan data`);
}

main().catch(console.error);
