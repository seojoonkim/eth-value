// Check data-collector alignWithCard on live rows: NVT 90D change must match the card (~-4%), staking latest = 33.32M not 23.37M.
const fs = require('fs');
const src = fs.readFileSync(__dirname + '/../scripts/data-collector.js', 'utf8');
const fnSrc = src.slice(src.indexOf('function alignWithCard'), src.indexOf('function checkAndRemoveIncomplete'));
const alignWithCard = new Function(fnSrc + '; return alignWithCard;')();
const html = fs.readFileSync(__dirname + '/../index.html', 'utf8');
const URL = html.match(/SUPABASE_URL\s*[:=]\s*['"]([^'"]+)/)[1];
const KEY = html.match(/SUPABASE_ANON_KEY\s*[:=]\s*['"]([^'"]+)/)[1];
const get = (q) => fetch(`${URL}/rest/v1/${q}`, { headers: { apikey: KEY, Authorization: 'Bearer ' + KEY } }).then(r => r.json());
let fail = 0; const ok = (c, m) => { console.log((c ? 'ok ' : 'FAIL ') + m); if (!c) fail++; };
(async () => {
  // Collector side: rows the AI sees (latest + days 87-93), after alignWithCard.
  const nvt = (await get('historical_nvt?select=date,nvt_ratio&order=date.desc&limit=110')).slice(1); // collector drops newest row
  const a = alignWithCard('historical_nvt', nvt);
  const today = new Date().toISOString().slice(0, 10);
  const dd = (d) => Math.floor((new Date(today) - new Date(d)) / 864e5);
  const near = a.filter(r => dd(r.date) >= 87 && dd(r.date) <= 93);
  const cur = a[0].nvt_ratio, old = near.reduce((x, r) => x + r.nvt_ratio, 0) / near.length, chg = (cur - old) / old * 100;
  // Card side: same as index.html — drop today/yesterday, 7-day MA, first row >= now-90d vs last.
  const all = (await get('historical_nvt?select=date,nvt_ratio&order=date.desc&limit=400'));
  const t0 = new Date(); t0.setHours(0, 0, 0, 0); const two = new Date(t0); two.setDate(two.getDate() - 2);
  const raw = all.map(d => ({ date: new Date(d.date), value: +d.nvt_ratio })).filter(d => d.date <= two).reverse();
  const ma = raw.map((d, i, arr) => i < 6 ? d : { date: d.date, value: arr.slice(i - 6, i + 1).reduce((s, x) => s + x.value, 0) / 7 });
  const cut = new Date(); cut.setDate(cut.getDate() - 90);
  const win = ma.filter(d => d.date >= cut);
  const card = (win[win.length - 1].value - win[0].value) / Math.abs(win[0].value) * 100;
  ok(Math.sign(chg) === Math.sign(card) && Math.abs(chg - card) < 5, // AI base = mean of days 87-93, card base = day-90 point
     `NVT 90D: AI input ${chg.toFixed(1)}% vs card ${card.toFixed(1)}% (latest ${a[0].date})`);
  const st = (await get('historical_staking?select=date,total_staked_eth&order=date.desc&limit=20'));
  const s = alignWithCard('historical_staking', st);
  ok(s[0].total_staked_eth > 3e7, `staking latest after filter ${(s[0].total_staked_eth / 1e6).toFixed(2)}M on ${s[0].date}`);
  ok(alignWithCard('historical_tvl', st) === st, 'other tables unchanged');
  console.log(fail ? 'ALIGN FAIL' : 'ALIGN ALL PASS'); process.exit(fail ? 1 : 0);
})();
