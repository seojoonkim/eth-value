// Read-only: pull recent news_events with the public anon key the page itself uses, flag
// (a) mechanism final direction vs event direction, (b) en/ko arrow mismatch, (c) likely duplicates.
const fs = require('fs');
const html = fs.readFileSync(__dirname + '/../index.html', 'utf8');
const URL = (html.match(/SUPABASE_URL\s*=\s*'([^']+)'/) || [])[1];
const KEY = (html.match(/SUPABASE_ANON_KEY\s*=\s*'([^']+)'/) || html.match(/SUPABASE_KEY\s*=\s*'([^']+)'/) || [])[1];
if (!URL || !KEY) { console.log('no url/key found'); process.exit(1); }
const since = process.argv[2] || '2026-09-25';
(async () => {
  const r = await fetch(`${URL}/rest/v1/news_events?select=id,date,category,direction,impact,title,mechanism&date=gte.${since}&order=date.desc&limit=500`, { headers: { apikey: KEY, Authorization: 'Bearer ' + KEY } });
  const rows = await r.json();
  console.log('rows', rows.length);
  const want = /outflow streak|Bitget|비트겟/i;
  for (const e of rows) if (want.test(e.title?.en || '') || want.test(e.title?.ko || ''))
    console.log(JSON.stringify({ id: e.id, d: e.date, dir: e.direction, imp: e.impact, en: e.title.en, ko: e.title.ko, mEn: e.mechanism?.en, mKo: e.mechanism?.ko }));
  fs.writeFileSync('/tmp/news_events.json', JSON.stringify(rows));
})();
