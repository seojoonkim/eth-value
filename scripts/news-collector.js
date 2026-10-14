#!/usr/bin/env node
/**
 * ETHval News Impact collector.
 * Collects Ethereum-relevant news (RSS + Google News search), asks an LLM to cluster headlines into
 * material events and tag each with category / direction / impact / horizon / affected ETHval metrics,
 * then upserts into Supabase table `news_events`.
 *
 * Usage:
 *   node scripts/news-collector.js                 # daily: last 3 days
 *   node scripts/news-collector.js --backfill 90   # weekly windows over the last 90 days
 *   add --dry-run to print JSON instead of writing
 */
const crypto = require('crypto');

const SUPABASE_URL = process.env.SUPABASE_URL;
const SUPABASE_KEY = process.env.SUPABASE_SERVICE_KEY;
const OPENAI_API_KEY = process.env.OPENAI_API_KEY;
const ANTHROPIC_API_KEY = process.env.ANTHROPIC_API_KEY;
const MODEL = process.env.NEWS_MODEL || process.env.COMMENTARY_MODEL || 'gpt-6-luna';
const HAIKU = 'claude-haiku-4-5-20251001';

const args = process.argv.slice(2);
const DRY = args.includes('--dry-run');
const bfIdx = args.indexOf('--backfill');
const BACKFILL_DAYS = bfIdx >= 0 ? parseInt(args[bfIdx + 1] || '90', 10) : 0;

const CATEGORIES = ['macro', 'flows', 'regulation', 'protocol', 'security', 'competitors', 'ecosystem', 'market'];
const METRICS = [
    'price', 'fear_greed', 'funding_rate', 'open_interest', 'exchange_reserve', 'etf_flows',
    'staking', 'burn', 'supply', 'fees', 'tvl', 'stablecoins', 'l2_activity', 'dominance',
    'rates', 'dxy', 'btc', 'volatility', 'active_addresses'
];

const RSS_FEEDS = [
    ['CoinDesk', 'https://www.coindesk.com/arc/outboundfeeds/rss/'],
    ['The Block', 'https://www.theblock.co/rss.xml'],
    ['Decrypt', 'https://decrypt.co/feed'],
    ['Cointelegraph', 'https://cointelegraph.com/rss/tag/ethereum'],
    ['Ethereum Foundation', 'https://blog.ethereum.org/feed.xml'],
    ['Federal Reserve', 'https://www.federalreserve.gov/feeds/press_all.xml'],
    ['SEC', 'https://www.sec.gov/news/pressreleases.rss'],
];

// Google News search queries (one per category family). Window is added per call.
const GN_QUERIES = [
    'ethereum',
    'ether ETF OR "ethereum ETF" OR "ETH treasury"',
    'SEC OR CFTC crypto regulation OR stablecoin bill OR crypto tax',
    '"Federal Reserve" rates OR CPI OR "Treasury yields"',
    'solana OR "competing blockchain" upgrade OR ETF',
    'crypto hack OR exploit OR liquidations',
];

const RELEVANT = /\b(eth|ether|ethereum|crypto|bitcoin|btc|stablecoin|defi|layer.?2|l2|rollup|solana|sec|cftc|fomc|fed|federal reserve|rate|cpi|inflation|yield|treasur|tariff|etf|staking|blob|hack|exploit|liquidat|token|blockchain|vitalik|base|arbitrum|optimism)\b/i;

function decode(s) {
    return (s || '').replace(/<!\[CDATA\[([\s\S]*?)\]\]>/g, '$1').replace(/<[^>]+>/g, '')
        .replace(/&amp;/g, '&').replace(/&lt;/g, '<').replace(/&gt;/g, '>').replace(/&quot;/g, '"')
        .replace(/&#39;|&#x27;|&apos;/g, "'").replace(/&#(\d+);/g, (_, n) => String.fromCharCode(+n)).trim();
}

function parseRss(xml, sourceName) {
    const items = [];
    const re = /<(item|entry)[\s>][\s\S]*?<\/\1>/g;
    let m;
    while ((m = re.exec(xml))) {
        const blk = m[0];
        const get = (tag) => { const r = blk.match(new RegExp(`<${tag}[^>]*>([\\s\\S]*?)<\\/${tag}>`)); return r ? decode(r[1]) : ''; };
        let link = get('link');
        if (!link) { const l = blk.match(/<link[^>]*href="([^"]+)"/); link = l ? l[1] : ''; }
        const dateStr = get('pubDate') || get('published') || get('updated') || get('dc:date');
        const d = new Date(dateStr);
        let title = get('title');
        let src = sourceName;
        const gs = blk.match(/<source[^>]*>([\s\S]*?)<\/source>/);
        if (gs) { src = decode(gs[1]); title = title.replace(new RegExp(`\\s+-\\s+${src.replace(/[.*+?^${}()|[\]\\]/g, '\\$&')}$`), ''); }
        if (!title || isNaN(d)) continue;
        items.push({ title, link, date: d.toISOString().slice(0, 10), ts: d.getTime(), source: src });
    }
    return items;
}

async function fetchText(url) {
    const r = await fetch(url, { headers: { 'User-Agent': 'Mozilla/5.0 (ethval.com news collector)' }, signal: AbortSignal.timeout(20000) });
    if (!r.ok) throw new Error(`HTTP ${r.status}`);
    return r.text();
}

async function collectHeadlines(fromDate, toDate, useRss) {
    const all = [];
    if (useRss) {
        for (const [name, url] of RSS_FEEDS) {
            try { const it = parseRss(await fetchText(url), name); all.push(...it); console.log(`  rss ${name}: ${it.length}`); }
            catch (e) { console.log(`  ⚠️ rss ${name}: ${e.message}`); }
        }
    }
    const after = fromDate, before = new Date(new Date(toDate).getTime() + 86400000).toISOString().slice(0, 10);
    for (const q of GN_QUERIES) {
        const url = `https://news.google.com/rss/search?q=${encodeURIComponent(`${q} after:${after} before:${before}`)}&hl=en-US&gl=US&ceid=US:en`;
        try { const it = parseRss(await fetchText(url), 'Google News'); all.push(...it); console.log(`  gnews "${q.slice(0, 30)}": ${it.length}`); }
        catch (e) { console.log(`  ⚠️ gnews "${q.slice(0, 30)}": ${e.message}`); }
        await new Promise(r => setTimeout(r, 800));
    }
    const seen = new Set();
    return all
        .filter(h => h.date >= fromDate && h.date <= toDate && RELEVANT.test(h.title))
        .filter(h => { const k = h.title.toLowerCase().replace(/[^a-z0-9]+/g, ' ').trim().slice(0, 80); if (seen.has(k)) return false; seen.add(k); return true; })
        .sort((a, b) => a.ts - b.ts);
}

const SYSTEM = `You are a senior crypto macro analyst for ETHval (Ethereum valuation dashboard).
From raw headlines, identify MATERIAL events that could move the ETH price. Merge headlines about the same event.
Ignore price-recap articles ("ETH price today", "price prediction", "could ETH hit $X") and opinion fluff unless they report a concrete new fact.
Cover ALL relevant categories, not only Ethereum-specific news:
- macro: Fed/FOMC, CPI/jobs, Treasury yields, dollar, tariffs, risk-off/on in equities
- flows: spot ETH ETF flows/approvals, corporate ETH treasuries, large fund buying/selling
- regulation: SEC/CFTC actions, laws (stablecoins, market structure), taxes, court rulings, non-US regulators
- protocol: Ethereum upgrades/hard forks, EIPs, gas limit, client releases, roadmap
- security: hacks, exploits, bridge failures, client bugs, outages
- competitors: Solana and other L1s (upgrades, ETFs, market share shifts)
- ecosystem: L2s, DeFi, stablecoin issuance, RWA tokenization on Ethereum
- market: liquidations, exchange issues, BTC moves, token unlocks, large whale moves
For each event output:
 id_hint: short stable English slug of the event (lowercase-hyphen, <=60 chars)
 date: YYYY-MM-DD of first report
 category: one of ${CATEGORIES.join('|')}
 direction: 1 bullish for ETH, -1 bearish, 0 mixed/neutral
 impact: 3 high (market-wide, likely moves ETH >3% or structural), 2 medium, 1 low
 horizon: immediate|short|structural
 metrics: 1-3 of ${METRICS.join('|')} that this event moves first
 confidence: 0..1 your confidence in the direction call
 title: {en,ko,zh,ja} concise headline (<=70 chars each; Korean in natural plain language, Hangul only — never Japanese kana or Chinese characters in "ko")
 summary: {en,ko,zh,ja} one sentence of what happened
 mechanism: {en,ko,zh,ja} one short causal chain from event -> metric -> ETH price, using arrows (e.g. "ETF inflow ↑ → exchange reserve ↓ → sell pressure ↓ → price +")
 source_idx: array of indices of the headlines used
Only include events with real price relevance. Quality over quantity. Return JSON {"events":[...]}.`;

async function llm(user, system = SYSTEM) {
    if (OPENAI_API_KEY) {
        try {
            const r = await fetch('https://api.openai.com/v1/chat/completions', {
                method: 'POST',
                headers: { 'Content-Type': 'application/json', Authorization: `Bearer ${OPENAI_API_KEY}` },
                body: JSON.stringify({ model: MODEL, max_completion_tokens: 16000, response_format: { type: 'json_object' }, messages: [{ role: 'system', content: system }, { role: 'user', content: user }] }),
                signal: AbortSignal.timeout(300000)
            });
            if (!r.ok) throw new Error(`HTTP ${r.status} ${(await r.text()).slice(0, 200)}`);
            const j = await r.json();
            const t = j.choices?.[0]?.message?.content;
            if (t) { console.log(`  🤖 ${MODEL} ${j.usage?.prompt_tokens}/${j.usage?.completion_tokens}`); return { text: t, model: MODEL }; }
            throw new Error('empty');
        } catch (e) { console.log(`  ⚠️ ${MODEL}: ${e.message}`); }
    }
    if (!ANTHROPIC_API_KEY) throw new Error('no LLM key');
    const r = await fetch('https://api.anthropic.com/v1/messages', {
        method: 'POST',
        headers: { 'Content-Type': 'application/json', 'x-api-key': ANTHROPIC_API_KEY, 'anthropic-version': '2023-06-01' },
        body: JSON.stringify({ model: HAIKU, max_tokens: 16000, system, messages: [{ role: 'user', content: user + '\nReturn only the JSON object.' }] }),
        signal: AbortSignal.timeout(300000)
    });
    if (!r.ok) throw new Error(`Haiku HTTP ${r.status}`);
    const j = await r.json();
    return { text: j.content?.[0]?.text || '', model: HAIKU };
}

function parseJson(t) {
    const s = t.indexOf('{'), e = t.lastIndexOf('}');
    return JSON.parse(t.slice(s, e + 1));
}

// Each language field must actually be written in that script (10-14: Japanese text landed in title.ko, and 清算 inside a Korean chain).
const KANA = /[\u3040-\u30ff]/;
const HAN = /[\u4e00-\u9fff]/;
const HANGUL = /[\uac00-\ud7a3]/;
function langOk(o) {
    if (!o || typeof o !== 'object' || !o.en || !o.ko) return false;
    if (!HANGUL.test(o.ko) || KANA.test(o.ko) || HAN.test(o.ko)) return false;
    if (KANA.test(o.en) || HAN.test(o.en) || HANGUL.test(o.en)) return false;
    if (o.zh && (KANA.test(o.zh) || HANGUL.test(o.zh))) return false;
    if (o.ja && HANGUL.test(o.ja)) return false;
    return true;
}

function validEvent(ev) {
    return ev && /^\d{4}-\d{2}-\d{2}$/.test(ev.date) && CATEGORIES.includes(ev.category)
        && [-1, 0, 1].includes(+ev.direction) && [1, 2, 3].includes(+ev.impact) && langOk(ev.title) && langOk(ev.mechanism)
        && (!ev.summary || langOk(ev.summary));
}
if (typeof module !== 'undefined') module.exports = { langOk, validEvent };

async function classifyWindow(heads, existing) {
    if (!heads.length) return [];
    const lines = heads.slice(0, 400).map((h, i) => `${i}. [${h.date}] ${h.title} (${h.source})`).join('\n');
    const ex = existing.length ? `\nAlready recorded events (do NOT output these again; output only NEW events):\n${existing.map(e => `- ${e.date} ${e.id}: ${e.title}`).join('\n')}\n` : '';
    const { text, model } = await llm(`Headlines:\n${lines}\n${ex}`);
    const out = parseJson(text).events || [];
    const res = [];
    for (const ev of out) {
        if (!validEvent(ev)) { console.log(`  skip invalid: ${JSON.stringify(ev).slice(0, 120)}`); continue; }
        const srcs = (ev.source_idx || []).map(i => heads[i]).filter(Boolean);
        if (!srcs.length) { console.log(`  skip unsourced: ${ev.title.en}`); continue; }
        const slug = String(ev.id_hint || ev.title.en).toLowerCase().replace(/[^a-z0-9]+/g, '-').replace(/^-|-$/g, '').slice(0, 60);
        const date = srcs.map(s => s.date).sort()[0];
        res.push({
            id: `${date}-${slug}`.slice(0, 80) + '-' + crypto.createHash('sha1').update(date + slug).digest('hex').slice(0, 6),
            date,
            published_at: new Date(Math.min(...srcs.map(s => s.ts))).toISOString(),
            category: ev.category,
            direction: +ev.direction,
            impact: +ev.impact,
            horizon: ['immediate', 'short', 'structural'].includes(ev.horizon) ? ev.horizon : 'short',
            metrics: (ev.metrics || []).filter(m => METRICS.includes(m)).slice(0, 3),
            title: ev.title, summary: ev.summary || null, mechanism: ev.mechanism,
            sources: srcs.slice(0, 5).map(s => ({ title: s.title, url: s.link, source: s.source, date: s.date })),
            confidence: Math.max(0, Math.min(1, +ev.confidence || 0.5)),
            model, updated_at: new Date().toISOString()
        });
    }
    return res;
}

async function sb(path, opts = {}) {
    const r = await fetch(`${SUPABASE_URL}/rest/v1/${path}`, {
        ...opts,
        headers: { apikey: SUPABASE_KEY, Authorization: `Bearer ${SUPABASE_KEY}`, 'Content-Type': 'application/json', ...(opts.headers || {}) }
    });
    if (!r.ok) throw new Error(`supabase ${path} HTTP ${r.status} ${(await r.text()).slice(0, 200)}`);
    return r.status === 204 ? null : r.json().catch(() => null);
}

async function existingEvents(from, to) {
    if (!SUPABASE_URL || !SUPABASE_KEY) return [];
    const rows = await sb(`news_events?select=id,date,title&date=gte.${from}&date=lte.${to}&order=date.asc&limit=1000`);
    return rows.map(r => ({ id: r.id, date: r.date, title: r.title?.en || '' }));
}

const iso = (d) => d.toISOString().slice(0, 10);
const addDays = (s, n) => iso(new Date(new Date(s).getTime() + n * 86400000));

// --repair-lang: re-translate stored events whose language fields are in the wrong script.
const REPAIR_SYSTEM = `You fix multilingual fields of crypto news events. For each item you get the English text of each field.
Return JSON {"items":[{"n":<number>,"title":{"en","ko","zh","ja"},"summary":{...}|null,"mechanism":{...}}]}.
Rules: keep "en" exactly as given. "ko" must be natural plain Korean in Hangul only (no Japanese kana, no Chinese characters; Latin tickers like ETH/ETF/L1 allowed).
"zh" Simplified Chinese, "ja" Japanese. Keep arrows (→ ↑ ↓) and the same causal chain in mechanism. title <= 70 chars.`;

async function repairLang() {
    const since = addDays(iso(new Date()), -400);
    const rows = await sb(`news_events?select=id,title,summary,mechanism&date=gte.${since}&limit=2000`);
    const bad = rows.filter(e => !langOk(e.title) || !langOk(e.mechanism) || (e.summary && !langOk(e.summary)));
    console.log(`🔧 mixed-language events: ${bad.length}/${rows.length}`);
    let fixed = 0;
    for (let i = 0; i < bad.length; i += 10) {
        const chunk = bad.slice(i, i + 10);
        const user = chunk.map((e, k) => `${k + 1}. title.en: ${e.title?.en}\n   summary.en: ${e.summary?.en || '(none)'}\n   mechanism.en: ${e.mechanism?.en}`).join('\n');
        let items = [];
        try { items = parseJson((await llm(user, REPAIR_SYSTEM)).text).items || []; } catch (err) { console.log(`  ⚠️ repair chunk ${i}: ${err.message}`); process.exitCode = 1; continue; }
        for (const it of items) {
            const e = chunk[+it.n - 1];
            if (!e) continue;
            const patch = { title: { ...it.title, en: e.title.en }, mechanism: { ...it.mechanism, en: e.mechanism.en } };
            if (e.summary) patch.summary = it.summary ? { ...it.summary, en: e.summary.en } : e.summary;
            if (!langOk(patch.title) || !langOk(patch.mechanism) || (patch.summary && !langOk(patch.summary))) { console.log(`  ✗ still mixed: ${e.id}`); process.exitCode = 1; continue; }
            if (DRY) { console.log(`  [dry] ${e.id} ko="${patch.title.ko}"`); fixed++; continue; }
            await sb(`news_events?id=eq.${encodeURIComponent(e.id)}`, { method: 'PATCH', headers: { Prefer: 'return=minimal' }, body: JSON.stringify(patch) });
            console.log(`  ✓ ${e.id} ko="${patch.title.ko}"`);
            fixed++;
        }
        const missed = chunk.filter((e, k) => !items.some(it => +it.n === k + 1));
        if (missed.length) { console.log(`  ⚠️ model skipped ${missed.length}: ${missed.map(e => e.id).join(', ')}`); process.exitCode = 1; }
    }
    console.log(`✅ repaired ${fixed}/${bad.length}`);
}

async function main() {
    if (args.includes('--repair-lang')) return repairLang();
    const today = iso(new Date());
    const windows = [];
    if (BACKFILL_DAYS > 0) {
        for (let s = addDays(today, -BACKFILL_DAYS); s <= today; s = addDays(s, 7)) windows.push([s, addDays(s, 6) > today ? today : addDays(s, 6), false]);
    } else {
        windows.push([addDays(today, -3), today, true]);
    }
    let total = 0;
    const dump = [];
    for (const [from, to, rss] of windows) {
        console.log(`📰 window ${from}..${to}`);
        try {
            const heads = await collectHeadlines(from, to, rss);
            console.log(`  headlines: ${heads.length}`);
            const existing = await existingEvents(addDays(from, -3), to);
            const events = await classifyWindow(heads, existing);
            const fresh = events.filter(e => !existing.some(x => x.id === e.id));
            console.log(`  events: ${events.length} (new ${fresh.length})`);
            if (DRY) { dump.push(...fresh); continue; }
            if (fresh.length) await sb('news_events?on_conflict=id', { method: 'POST', headers: { Prefer: 'resolution=merge-duplicates,return=minimal' }, body: JSON.stringify(fresh) });
            total += fresh.length;
        } catch (e) {
            console.log(`  ❌ window ${from}: ${e.message}`);
            process.exitCode = 1;
        }
    }
    if (DRY) console.log(JSON.stringify(dump, null, 1));
    console.log(`✅ news events written: ${total}`);
}

if (require.main === module) main().catch(e => { console.error(e); process.exit(1); });
