// 일회성: historical_eth_price.volume의 2020-06-14..2022-12-20 legacy 값(출처 불명, CoinGecko와 다른 계열)을 null로 바꾼다.
// 값은 먼저 data/archive/eth_price_volume_legacy_2020-2022.json 에 보관됨. 보관본과 DB 값이 정확히 같은 행만 바꾼다.
// DRY_RUN=1 이면 확인만 한다.
const fs = require('fs'), path = require('path');
const { createClient } = require('@supabase/supabase-js');
const sb = createClient(process.env.SUPABASE_URL, process.env.SUPABASE_SERVICE_KEY);
const DRY = process.env.DRY_RUN === '1';
(async () => {
    const arch = JSON.parse(fs.readFileSync(path.join(__dirname, '../data/archive/eth_price_volume_legacy_2020-2022.json'), 'utf8')).rows;
    const want = new Map(arch.map(r => [r.date, Number(r.volume)]));
    const first = arch[0].date, last = arch[arch.length - 1].date;
    const { data: rows, error } = await sb.from('historical_eth_price').select('date, volume').gte('date', first).lte('date', last).order('date');
    if (error) throw new Error(error.message);
    const match = [], mismatch = [];
    for (const r of rows) {
        if (!want.has(r.date)) continue;
        if (r.volume === null) continue; // 이미 처리됨
        (Number(r.volume) === want.get(r.date) ? match : mismatch).push(r);
    }
    console.log(`archive ${arch.length} rows ${first}..${last}; DB rows in range ${rows.length}; match ${match.length}; mismatch ${mismatch.length}; already null ${rows.filter(r => r.volume === null).length}`);
    if (mismatch.length) { console.log('mismatch sample', JSON.stringify(mismatch.slice(0, 5))); process.exit(1); }
    if (DRY || !match.length) return;
    const dates = match.map(r => r.date);
    for (let i = 0; i < dates.length; i += 200) {
        const { error: e } = await sb.from('historical_eth_price').update({ volume: null }).in('date', dates.slice(i, i + 200));
        if (e) throw new Error(e.message);
    }
    const { data: after } = await sb.from('historical_eth_price').select('date, volume').gte('date', first).lte('date', last);
    const left = after.filter(r => r.volume !== null && Number(r.volume) > 0).length;
    console.log(`after: ${after.length} rows in range, non-null volume left ${left}`);
    if (left) process.exit(1);
})().catch(e => { console.error(e); process.exit(1); });
