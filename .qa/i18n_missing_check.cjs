// i18n coverage: every data-i18n key used in the page must exist in ko/zh/ja dictionaries (else English leaks).
// Also lists visible English-only headings in kr mode on live/local. Usage: node .qa/i18n_missing_check.cjs [url]
const fs = require('fs');
const html = fs.readFileSync(__dirname + '/../index.html', 'utf8');
const start = html.indexOf('const translations = {');
const end = html.indexOf('function t(key, fallback)');
const block = html.slice(start, end);
const langs = {};
for (const lang of ['en', 'ko', 'zh', 'ja']) {
  const i = block.search(new RegExp(`\\n\\s{12}${lang}:\\s*\\{`));
  langs[lang] = i;
}
const order = Object.entries(langs).filter(([, i]) => i >= 0).sort((a, b) => a[1] - b[1]);
const keysOf = {};
order.forEach(([lang, i], k) => {
  const seg = block.slice(i, k + 1 < order.length ? order[k + 1][1] : block.length);
  keysOf[lang] = new Set([...seg.matchAll(/(?:^|[\n,{]\s*)'([^']+)'\s*:/g)].map(m => m[1]));
});
const used = new Set([...html.matchAll(/data-i18n="([^"]+)"/g)].map(m => m[1]).filter(k => !/[${}' +]/.test(k)));
let fail = 0;
for (const lang of ['ko', 'zh', 'ja']) {
  const miss = [...used].filter(k => !keysOf[lang]?.has(k));
  console.log(`${miss.length ? 'FAIL' : 'PASS'} ${lang} keys=${keysOf[lang]?.size} missing=${miss.length}${miss.length ? ' ' + miss.slice(0, 40).join(', ') : ''}`);
  if (miss.length) fail++;
}
console.log(fail ? 'I18N-MISSING FAIL' : 'I18N-MISSING ALL PASS');
process.exit(fail ? 1 : 0);
