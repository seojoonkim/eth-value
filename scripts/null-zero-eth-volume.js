// 일회성: historical_eth_price.volume 이 정확히 0인 placeholder 행(2022-12-21..2025-10-10)을 NULL로 바꾼다.
// 날짜 목록은 먼저 data/archive/eth_price_volume_zero_placeholders_2022-2025.json 에 보관됨.
// 보관본에 있는 날짜이면서 DB 값이 정확히 0인 행만 바꾼다 (OHLC 미변경). DRY_RUN=1 이면 확인만.
const fs = require('fs'), path = require('path');
const { createClient } = require('@supabase/supabase-js');
const sb = createClient(process.env.SUPABASE_URL, process.env.SUPABASE_SERVICE_KEY);
const DRY = process.env.DRY_RUN === '1';
(async () => {
    const arch = JSON.parse(fs.readFileSync(path.join(__dirname, '../data/archive/eth_price_volume_zero_placeholders_2022-2025.json'), 'utf8')).rows;
    const want = new Set(arch.map(r => r.date));
    const first = arch[0].date, last = arch[arch.length - 1].date;
    const rows = [];
    for (let off = 0; ; off += 1000) {
        const { data, error } = await sb.from('historical_eth_price').select('date, volume').gte('date', first).lte('date', last).order('date').range(off, off + 999);
        if (error) throw new Error(error.message);
        rows.push(...data);
        if (data.length < 1000) break;
    }
    const zero = rows.filter(r => want.has(r.date) && r.volume !== null && Number(r.volume) === 0);
    const other = rows.filter(r => want.has(r.date) && r.volume !== null && Number(r.volume) !== 0);
    console.log(`archive ${arch.length} dates ${first}..${last}; DB rows ${rows.length}; zero ${zero.length}; non-zero ${other.length}; null ${rows.filter(r => r.volume === null).length}`);
    if (other.length) console.log('non-zero (left untouched)', JSON.stringify(other.slice(0, 5)));
    if (DRY || !zero.length) return;
    const dates = zero.map(r => r.date);
    for (let i = 0; i < dates.length; i += 200) {
        const { error: e } = await sb.from('historical_eth_price').update({ volume: null }).in('date', dates.slice(i, i + 200)).eq('volume', 0);
        if (e) throw new Error(e.message);
    }
    const { count } = await sb.from('historical_eth_price').select('date', { count: 'exact', head: true }).eq('volume', 0);
    console.log(`after: rows with volume = 0 in whole table: ${count}`);
    if (count) process.exit(1);
})().catch(e => { console.error(e); process.exit(1); });
