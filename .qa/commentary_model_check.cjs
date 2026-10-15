// Gate (offline): commentary model = Claude Haiku 5.5 first, Luna fallback, text blocks read by type,
// model recorded per row, footer shows the recorded model (never a hard-coded "Claude Haiku").
const fs = require('fs');
const path = require('path');
const root = path.join(__dirname, '..');
const dc = fs.readFileSync(path.join(root, 'scripts/data-collector.js'), 'utf8');
const nc = fs.readFileSync(path.join(root, 'scripts/news-collector.js'), 'utf8');
const html = fs.readFileSync(path.join(root, 'index.html'), 'utf8');
let fail = 0;
const check = (name, ok) => { console.log((ok ? 'PASS ' : 'FAIL ') + name); if (!ok) fail++; };

check('commentary default model is claude-haiku-5-5', /HAIKU_MODEL = process\.env\.HAIKU_MODEL \|\| 'claude-haiku-5-5'/.test(dc));
check('no Haiku 4.5 id left in collectors', !/claude-haiku-4-5-2025/.test(dc.replace(/MODEL_LABELS = \{[^}]*\}/, '')) && !/claude-haiku-4-5-2025/.test(nc));
const call = dc.slice(dc.indexOf('async function callCommentaryLLM'), dc.indexOf('async function generateCommentary'));
check('Haiku tried before Luna', call.indexOf('callHaiku') > -1 && call.indexOf('callHaiku') < call.indexOf('callLuna'));
const haiku = dc.slice(dc.indexOf('async function callHaiku'), dc.indexOf('async function callLuna'));
check('Haiku reads text blocks by type', /filter\(b => b\.type === 'text'\)/.test(haiku) && !/content\?\.\[0\]/.test(haiku));
check('Haiku request has no temperature/top_p/top_k', !/temperature|top_p|top_k/.test(haiku.replace(/\/\/.*$/gm, '')));
check('Haiku handles refusal stop_reason', /stop_reason === 'refusal'/.test(haiku));
check('row stores _model and _model_label', /_model: sectionModel/.test(dc) && /_model_label:/.test(dc));
check('news Haiku reads text blocks by type', /filter\(x => x\.type === 'text'\)/.test(nc));
check('page selects model_label', /model_label:metrics_snapshot->>_model_label/.test(html));
check('footer has no hard-coded "Claude Haiku"', !/i18nText\.by\} Claude Haiku/.test(html));
check('footer renders commentary.model_label', /commentary\.model_label \?/.test(html));

// Runtime: simulate fetch for the fallback order with stubbed globals.
(async () => {
  const src = dc.slice(dc.indexOf('async function callHaiku'), dc.indexOf('\nasync function generateCommentary'));
  const run = async (anthropicOk) => {
    const calls = [];
    const fetch = async (url) => {
      calls.push(url.includes('anthropic') ? 'haiku' : 'luna');
      if (url.includes('anthropic')) {
        if (!anthropicOk) return { ok: false, status: 401, text: async () => 'invalid x-api-key' };
        return { ok: true, json: async () => ({ model: 'claude-haiku-5-5', stop_reason: 'end_turn', content: [{ type: 'thinking', thinking: '...' }, { type: 'text', text: 'A|||B|||C' }], usage: {} }) };
      }
      return { ok: true, json: async () => ({ choices: [{ message: { content: 'L|||M|||N' } }], usage: {} }) };
    };
    const f = new Function('fetch', 'console', `const ANTHROPIC_API_KEY='x', OPENAI_API_KEY='y', HAIKU_MODEL='claude-haiku-5-5', LUNA_MODEL='gpt-6-luna'; let lastCommentaryModel=null;
      ${src}
      return async () => { const t = await callCommentaryLLM('s','u',false); return { t, m: lastCommentaryModel }; };`);
    const r = await f(fetch, { log() {}, error() {} })();
    return { ...r, calls };
  };
  const a = await run(true);
  check('runtime: Haiku answers, thinking block skipped', a.t === 'A|||B|||C' && a.m === 'claude-haiku-5-5' && a.calls.join() === 'haiku');
  const b = await run(false);
  check('runtime: Haiku 401 -> Luna fallback, model recorded', b.t === 'L|||M|||N' && b.m === 'gpt-6-luna' && b.calls.join() === 'haiku,luna');
  console.log(fail ? `COMMENTARY MODEL FAIL (${fail})` : 'COMMENTARY MODEL ALL PASS');
  process.exit(fail ? 1 : 0);
})();
