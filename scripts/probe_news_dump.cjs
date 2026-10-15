// Dump full rows (all columns) for given ids to stdout as JSON — backup before a data fix. Read-only, anon key.
const fs = require('fs');
const html = fs.readFileSync(__dirname + '/../index.html', 'utf8');
const URL = html.match(/SUPABASE_URL\s*=\s*'([^']+)'/)[1];
const KEY = (html.match(/SUPABASE_ANON_KEY\s*=\s*'([^']+)'/) || html.match(/SUPABASE_KEY\s*=\s*'([^']+)'/))[1];
(async () => {
  const ids = process.argv.slice(2).map(encodeURIComponent).join(',');
  const r = await fetch(`${URL}/rest/v1/news_events?select=*&id=in.(${ids})`, { headers: { apikey: KEY, Authorization: 'Bearer ' + KEY } });
  console.log(JSON.stringify(await r.json(), null, 1));
})();
