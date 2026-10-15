// Print sources + summaries of given news_events ids (read-only, anon key).
const fs = require('fs');
const html = fs.readFileSync(__dirname + '/../index.html', 'utf8');
const URL = html.match(/SUPABASE_URL\s*=\s*'([^']+)'/)[1];
const KEY = (html.match(/SUPABASE_ANON_KEY\s*=\s*'([^']+)'/) || html.match(/SUPABASE_KEY\s*=\s*'([^']+)'/))[1];
(async () => {
  for (const id of process.argv.slice(2)) {
    const r = await fetch(`${URL}/rest/v1/news_events?select=id,date,published_at,summary,sources&id=eq.${encodeURIComponent(id)}`, { headers: { apikey: KEY, Authorization: 'Bearer ' + KEY } });
    const [e] = await r.json();
    console.log('==', id, e?.published_at); console.log(' summary:', e?.summary?.en);
    for (const s of e?.sources || []) console.log('  -', s.date, s.source, '|', s.title, '|', s.url);
  }
})();
