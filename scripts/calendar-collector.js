#!/usr/bin/env node
/**
 * ETHval calendar collector — first-party schedules only (no estimated dates).
 *   FOMC     https://www.federalreserve.gov/monetarypolicy/fomccalendars.htm
 *   CPI      https://www.bls.gov/schedule/news_release/bls.ics  (SUMMARY "Consumer Price Index")
 *   Upgrades https://blog.ethereum.org/feed.xml -> "<Name> Mainnet Announcement" posts -> activation time (UTC)
 * Writes future rows (today-1 .. today+400d) to Supabase `calendar_events`.
 * Usage: node scripts/calendar-collector.js [--dry-run]
 * A source that fails or returns nothing makes the run exit 1 (so a silent parser break is visible),
 * but rows from the other sources are still written.
 */
const SUPABASE_URL = process.env.SUPABASE_URL;
const SUPABASE_KEY = process.env.SUPABASE_SERVICE_KEY;
const DRY = process.argv.includes('--dry-run');
const UA = 'ETHval-calendar/1.0 (+https://ethval.com; simon@hashed.com)';

const FOMC_URL = 'https://www.federalreserve.gov/monetarypolicy/fomccalendars.htm';
const BLS_ICS = 'https://www.bls.gov/schedule/news_release/bls.ics';
const BLS_CPI_PAGE = 'https://www.bls.gov/schedule/news_release/cpi.htm';
const EF_FEED = 'https://blog.ethereum.org/feed.xml';

const MONTHS = ['january', 'february', 'march', 'april', 'may', 'june', 'july', 'august', 'september', 'october', 'november', 'december'];
const pad = (n) => String(n).padStart(2, '0');
const ymd = (y, m, d) => `${y}-${pad(m)}-${pad(d)}`;

async function get(url) {
    const r = await fetch(url, { headers: { 'User-Agent': UA, Accept: 'text/html,text/calendar,application/xml,*/*' }, signal: AbortSignal.timeout(30000) });
    if (!r.ok) throw new Error(`HTTP ${r.status} ${url}`);
    return r.text();
}
const strip = (s) => s.replace(/<script[\s\S]*?<\/script>|<style[\s\S]*?<\/style>/g, ' ').replace(/<[^>]+>/g, ' ')
    .replace(/&nbsp;/g, ' ').replace(/&amp;/g, '&').replace(/&#39;|&rsquo;/g, "'").replace(/\s+/g, ' ').trim();

// FOMC: each "<year> FOMC Meetings" panel lists month + "27-28" / "8-9*" / "31-1" (month may be "January/February").
function parseFomc(html) {
    const out = [];
    const heads = [...html.matchAll(/(\d{4}) FOMC Meetings/g)].map(m => ({ y: +m[1], i: m.index }));
    heads.forEach((h, k) => {
        const seg = html.slice(h.i, k + 1 < heads.length ? heads[k + 1].i : html.length);
        for (const m of seg.matchAll(/fomc-meeting__month[^>]*>\s*<strong>([^<]+)<\/strong>[\s\S]*?fomc-meeting__date[^>]*>([^<]+)/g)) {
            const mon = m[1].trim().toLowerCase().split(/[\/-]/).map(s => MONTHS.indexOf(s.trim()) + 1);
            const dm = m[2].trim().match(/^(\d{1,2})\s*-\s*(\d{1,2})(\*)?/);
            if (!dm || mon.some(x => x < 1)) continue; // e.g. "notation vote" / unscheduled rows
            const m1 = mon[0], m2 = mon[mon.length - 1];
            const d1 = +dm[1], d2 = +dm[2];
            const endM = (m2 === m1 && d2 < d1) ? m1 + 1 : m2;
            const yEnd = endM > 12 ? h.y + 1 : h.y;
            const end = ymd(yEnd, ((endM - 1) % 12) + 1, d2);
            out.push({ id: `fomc:${end}`, kind: 'fomc', name: 'FOMC', date: end, date_start: ymd(h.y, m1, d1), starts_at: null, sep: !!dm[3], source: 'Federal Reserve', source_url: FOMC_URL });
        }
    });
    return out;
}

// CPI: BLS iCal, DTSTART;TZID=US-Eastern:20261014T083000. Convert ET -> UTC with the US DST rule.
function etToUtc(y, mo, d, hh, mm) {
    const nthSun = (yy, month, n) => { const f = new Date(Date.UTC(yy, month - 1, 1)).getUTCDay(); return 1 + ((7 - f) % 7) + (n - 1) * 7; };
    const dstStart = Date.UTC(y, 2, nthSun(y, 3, 2), 7);   // 2:00 EST = 07:00 UTC
    const dstEnd = Date.UTC(y, 10, nthSun(y, 11, 1), 6);    // 2:00 EDT = 06:00 UTC
    const asEst = Date.UTC(y, mo - 1, d, hh + 5, mm);
    const dst = asEst - 3600000 >= dstStart && asEst - 3600000 < dstEnd;
    return new Date(dst ? asEst - 3600000 : asEst).toISOString();
}
function parseCpi(ics) {
    const out = [];
    const txt = ics.replace(/\r\n[ \t]/g, '');
    for (const ev of txt.split('BEGIN:VEVENT').slice(1)) {
        const s = (ev.match(/\nSUMMARY[^:]*:(.*)/) || [])[1];
        if (!s || s.trim() !== 'Consumer Price Index') continue;
        const t = ev.match(/\nDTSTART[^:]*:(\d{4})(\d{2})(\d{2})T(\d{2})(\d{2})/);
        if (!t) continue;
        const date = `${t[1]}-${t[2]}-${t[3]}`;
        out.push({ id: `cpi:${date}`, kind: 'cpi', name: 'CPI', date, date_start: null, starts_at: etToUtc(+t[1], +t[2], +t[3], +t[4], +t[5]), sep: false, source: 'U.S. Bureau of Labor Statistics', source_url: BLS_CPI_PAGE });
    }
    return out;
}

// Upgrades: only "<Name> Mainnet Announcement" posts, which state the mainnet activation time.
async function collectUpgrades() {
    const feed = await get(EF_FEED);
    const items = [...feed.matchAll(/<item>([\s\S]*?)<\/item>/g)].map(m => m[1]);
    const out = [];
    for (const it of items) {
        // Unwrap CDATA before stripping tags — "<![CDATA[...]]>" itself looks like a tag.
        const title = strip(((it.match(/<title>([\s\S]*?)<\/title>/) || [])[1] || '').replace(/<!\[CDATA\[|\]\]>/g, ''));
        const m = title.match(/^(\w+) Mainnet Announcement$/i);
        if (!m) continue;
        // Old announcements describe past activations — skip them instead of fetching every post.
        const pub = Date.parse(((it.match(/<pubDate>([\s\S]*?)<\/pubDate>/) || [])[1] || '').trim());
        if (pub && Date.now() - pub > 400 * 86400000) continue;
        const url = ((it.match(/<link>([\s\S]*?)<\/link>/) || [])[1] || '').trim();
        if (!url) continue;
        const text = strip(await get(url));
        const a = text.match(/activate on the Ethereum mainnet[^.]*?\(?(?:on\s+)?([A-Z][a-z]+ \d{1,2}, \d{4})(?:,| at)\s*(\d{2}:\d{2}:\d{2}) UTC/);
        if (!a) { console.log(`  ⚠️ upgrade ${m[1]}: no mainnet activation time in post`); continue; }
        const dt = new Date(`${a[1]} ${a[2]} UTC`);
        if (isNaN(dt)) continue;
        const date = dt.toISOString().slice(0, 10);
        out.push({ id: `upgrade:${date}:${m[1].toLowerCase()}`, kind: 'upgrade', name: m[1], date, date_start: null, starts_at: dt.toISOString(), sep: false, source: 'Ethereum Foundation Blog', source_url: url });
    }
    return out;
}

async function sb(path, opts = {}) {
    const r = await fetch(`${SUPABASE_URL}/rest/v1/${path}`, { ...opts, headers: { apikey: SUPABASE_KEY, Authorization: `Bearer ${SUPABASE_KEY}`, 'Content-Type': 'application/json', ...(opts.headers || {}) } });
    if (!r.ok) throw new Error(`supabase ${path} HTTP ${r.status} ${(await r.text()).slice(0, 200)}`);
    return r.status === 204 ? null : r.json().catch(() => null);
}

async function main() {
    const lo = new Date(Date.now() - 86400000).toISOString().slice(0, 10);
    const hi = new Date(Date.now() + 400 * 86400000).toISOString().slice(0, 10);
    const rows = [];
    const steps = [
        ['fomc', async () => parseFomc(await get(FOMC_URL))],
        ['cpi', async () => parseCpi(await get(BLS_ICS))],
        ['upgrade', collectUpgrades]
    ];
    for (const [k, fn] of steps) {
        try {
            const all = await fn();
            const fut = all.filter(r => r.date >= lo && r.date <= hi);
            console.log(`  ${k}: parsed ${all.length}, upcoming ${fut.length}`);
            // FOMC and CPI publish a schedule every year — zero parsed rows means the page changed.
            if (k !== 'upgrade' && !all.length) { console.log(`  ❌ ${k}: parser found nothing`); process.exitCode = 1; }
            rows.push(...fut);
        } catch (e) { console.log(`  ❌ ${k}: ${e.message}`); process.exitCode = 1; }
    }
    const fetched = new Date().toISOString();
    rows.forEach(r => { r.fetched_at = fetched; });
    rows.sort((a, b) => a.date.localeCompare(b.date));
    if (DRY || !SUPABASE_URL || !SUPABASE_KEY) { console.log(JSON.stringify(rows, null, 1)); return; }
    if (rows.length) await sb('calendar_events?on_conflict=id', { method: 'POST', headers: { Prefer: 'resolution=merge-duplicates,return=minimal' }, body: JSON.stringify(rows) });
    console.log(`✅ calendar events upserted: ${rows.length}`);
}

if (require.main === module) main().catch(e => { console.error(e); process.exit(1); });
module.exports = { parseFomc, parseCpi, etToUtc };
