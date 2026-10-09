#!/usr/bin/env node
// Data freshness gate. Fails (exit 1) when any table the site reads stops advancing.
// Why: CryptoQuant/Dune feeds silently froze for 164–188 days while collection runs stayed green.
// Uses the public anon key from index.html (read-only).
const fs = require('fs');
const path = require('path');

// table → max allowed age in days (daily feeds publish D-1; allow a 2-day slack for provider lag)
const EXPECT_SOURCE = {
    historical_active_addresses: 'coinmetrics',
    historical_l2_addresses: 'growthepie',
};

const TABLES = {
    historical_eth_price: 3,
    historical_funding_rate: 3,
    historical_exchange_reserve: 4,
    historical_open_interest: 3,
    historical_l2_total_volume: 4,
    historical_l2_dex_volume: 4,
    historical_whale_tx: 3,
    historical_eth_supply: 4,
    historical_active_addresses: 4,
    historical_l2_addresses: 4,
    historical_staking_apr: 4,
    historical_fear_greed: 3,
    daily_commentary: 2,
};

(async () => {
    const html = fs.readFileSync(path.join(__dirname, '../index.html'), 'utf8');
    const url = html.match(/https:\/\/[a-z0-9]+\.supabase\.co/)[0];
    const key = html.match(/eyJ[A-Za-z0-9_\-]+\.[A-Za-z0-9_\-]+\.[A-Za-z0-9_\-]+/)[0];
    const H = { apikey: key, Authorization: `Bearer ${key}` };
    let bad = 0;
    for (const [t, maxAge] of Object.entries(TABLES)) {
        try {
            const r = await fetch(`${url}/rest/v1/${t}?select=*&order=date.desc&limit=1`, { headers: H });
            if (!r.ok) { console.log(`SKIP  ${t} (HTTP ${r.status})`); continue; }
            const [row] = await r.json();
            if (!row) { console.log(`STALE ${t} empty`); bad++; continue; }
            const age = Math.floor((Date.now() - Date.parse(String(row.date).slice(0, 10))) / 864e5);
            const synthetic = row.source === 'estimated';   // placeholder values must never be the latest row
            // Single source of record: a different (or missing) source on the latest row means two collectors
            // with different definitions are overwriting each other (seen 2026-10: Dune vs Coin Metrics).
            const want = EXPECT_SOURCE[t];
            const mixed = want && row.source !== want;
            const ok = age <= maxAge && !synthetic && !mixed;
            if (!ok) bad++;
            console.log(`${ok ? 'ok   ' : synthetic ? 'SYNTH' : mixed ? 'MIXED' : 'STALE'} ${t.padEnd(30)} ${String(row.date).slice(0, 10)} ${age}d (max ${maxAge}d)${row.source ? ' ' + row.source : ''}`);
        } catch (e) { console.log(`SKIP  ${t} (${e.message})`); }
    }
    console.log(bad ? `FAIL: ${bad} stale, synthetic or mixed-source table(s)` : 'PASS');
    process.exit(bad ? 1 : 0);
})();
