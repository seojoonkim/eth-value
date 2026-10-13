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

// Upgrades: EF "<Name> Mainnet Announcement" posts state the mainnet activation time. Wording varies by post:
//   Fusaka  "scheduled to activate on the Ethereum mainnet at slot 13,164,544 (December 3, 2025, 21:49:11 UTC)"
//   Pectra  "activate on the Ethereum mainnet on May 07, 2025 at epoch 364032 (10:05:11 UTC)"
//   Dencun  "activate on the Ethereum mainnet at epoch 269568 , occurring on March 13, 2024 at 13:55 UTC"
//   Shapella "will activate on the Ethereum network at epoch 194048 , scheduled for 22:27:35 UTC on April 12, 2023"
//   Glamsterdam-style schedule table row "Mainnet <epoch> <slot> 2026-10-06 13:53:36 <unix>" (TBD rows = not scheduled)
// The schedule table also appears in "<Name> Testnet Announcement" posts, so a Mainnet row there that is no
// longer TBD is picked up too. Only explicit first-party dates; anything ambiguous returns null (no guess).
const MON = '(Jan(?:uary)?|Feb(?:ruary)?|Mar(?:ch)?|Apr(?:il)?|May|June?|July?|Aug(?:ust)?|Sep(?:t(?:ember)?)?|Oct(?:ober)?|Nov(?:ember)?|Dec(?:ember)?)';
function parseMainnetActivation(text) {
    const t = text.replace(/\s+/g, ' ');
    const iso = (d, tm) => { const x = new Date(`${d} ${tm.length === 5 ? tm + ':00' : tm} UTC`); return isNaN(x) ? null : x; };
    // 1) schedule table row: "Mainnet 411,392 13,164,544 2026-12-03 21:49:11 1796333351"
    const row = t.match(/\bMainnet\s+[\d,]+\s+[\d,]+\s+(\d{4}-\d{2}-\d{2})\s+(\d{2}:\d{2}(?::\d{2})?)\b/);
    if (row) { const x = new Date(`${row[1]}T${row[2].length === 5 ? row[2] + ':00' : row[2]}Z`); if (!isNaN(x)) return x; }
    // 2) prose: look only at the ~240 chars after "activate on the Ethereum mainnet/network" (the post date sits
    //    before it, e.g. "Posted ... on April 23, 2025 ... activate on the Ethereum mainnet on May 07, 2025").
    //    Month may be abbreviated with a dot ("Apr. 12, 2023"); a testnet name inside the window = not mainnet.
    for (const m of t.matchAll(/activat\w* on (?:the )?(?:Ethereum )?(?:mainnet|network)\b/gi)) {
        if (/^activat\w* on (?:the )?network/i.test(m[0])) continue;
        const win = t.slice(m.index + m[0].length, m.index + m[0].length + 240);
        const date = win.match(new RegExp(MON + '\\.?\\s+(\\d{1,2}),\\s*(\\d{4})', 'i'));
        const time = win.match(/(\d{2}:\d{2}(?::\d{2})?)\s*UTC/);
        if (!date || !time) continue;
        const upto = win.slice(0, Math.max(date.index, time.index));
        if (/\b(Sepolia|Holesky|Hole\u0161ky|Hoodi|Goerli|testnet)\b/i.test(upto)) continue;
        const mo = MONTHS.findIndex(x => x.startsWith(date[1].toLowerCase()));
        if (mo < 0) continue;
        const x = iso(`${MONTHS[mo]} ${date[2]}, ${date[3]}`, time[1]);
        if (x) return x;
    }
    return null;
}
function upgradeTitle(title) {
    let m = title.match(/^(\w+) Mainnet Announcement$/i) || title.match(/^Mainnet (\w+) Announcement$/i);
    if (m) return { name: m[1], kind: 'mainnet' };
    m = title.match(/^(\w+) Testnets? Announcement$/i);
    return m ? { name: m[1], kind: 'testnet' } : null;
}
async function collectUpgrades() {
    const feed = await get(EF_FEED);
    const items = [...feed.matchAll(/<item>([\s\S]*?)<\/item>/g)].map(m => m[1]);
    const out = new Map();
    for (const it of items) {
        // Unwrap CDATA before stripping tags — "<![CDATA[...]]>" itself looks like a tag.
        const title = strip(((it.match(/<title>([\s\S]*?)<\/title>/) || [])[1] || '').replace(/<!\[CDATA\[|\]\]>/g, ''));
        const u = upgradeTitle(title);
        if (!u) continue;
        // Old announcements describe past activations — skip them instead of fetching every post.
        const pub = Date.parse(((it.match(/<pubDate>([\s\S]*?)<\/pubDate>/) || [])[1] || '').trim());
        if (pub && Date.now() - pub > 400 * 86400000) continue;
        const url = ((it.match(/<link>([\s\S]*?)<\/link>/) || [])[1] || '').trim();
        if (!url) continue;
        const dt = parseMainnetActivation(strip(await get(url)));
        if (!dt) { console.log(`  ${u.kind === 'mainnet' ? '⚠️' : 'ℹ️'} upgrade ${u.name} (${u.kind} post): no mainnet activation time${u.kind === 'testnet' ? ' yet (TBD)' : ' in post'}`); continue; }
        const key = u.name.toLowerCase();
        // A mainnet announcement wins over a testnet post's schedule table for the same upgrade.
        if (out.has(key) && out.get(key)._kind === 'mainnet' && u.kind !== 'mainnet') continue;
        const date = dt.toISOString().slice(0, 10);
        out.set(key, { _kind: u.kind, id: `upgrade:${date}:${key}`, kind: 'upgrade', name: u.name, date, date_start: null, starts_at: dt.toISOString(), sep: false, source: 'Ethereum Foundation Blog', source_url: url });
    }
    return [...out.values()].map(({ _kind, ...r }) => r);
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
module.exports = { parseFomc, parseCpi, etToUtc, parseMainnetActivation, upgradeTitle };
