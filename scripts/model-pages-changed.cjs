#!/usr/bin/env node
// 모델 페이지 재빌드 후 "실제로 바뀐 파일" 수를 센다 (model-pages.yml 커밋 여부 판단용).
// 비교 전에 가리는 것: 빌드 시각("YYYY-MM-DD HH:MM"), <lastmod>, 부동소수 끝자리 잡음.
// 끝자리 잡음: 같은 입력이라도 합산 순서에 따라 13.704895992050789 ↔ 13.704895992050787 처럼
// 1e-14 수준만 달라짐(2026-10-15 d06d96e 실측, 최대 상대오차 2.3e-14). 소수 6자리 이상 숫자는
// 상대오차 1e-9 이내면 같다고 본다 — 화면에 보이는 값($2,854·$13.76 등)과 차트 좌표 변화는 그대로 잡힌다.
// 빌더(build-model-pages.cjs)도 값을 유효숫자 10자리로 저장해 차트 좌표 0.1px 뒤집힘을 원천에서 막는다.
// 페이지 내용 자체는 바꾸지 않고 비교에만 쓴다.
//   node scripts/model-pages-changed.cjs                 → HEAD vs 스테이징된 파일(작업트리)
//   node scripts/model-pages-changed.cjs --commit <sha>  → <sha>~1 vs <sha> (검증용)
const { execSync } = require('child_process');
const fs = require('fs');

const sh = (c) => execSync(c, { maxBuffer: 1 << 28 }).toString();
const show = (ref, f) => { try { return sh(`git show ${ref}:${f}`); } catch (e) { return ''; } };

const LONG = /-?\d+\.\d{6,}(?:e[-+]?\d+)?/gi;
const mask = (s) => s.replace(/\d{4}-\d{2}-\d{2} \d{2}:\d{2}/g, 'T').replace(/<lastmod>[^<]*<\/lastmod>/g, '');
// 같은 구조 + 긴 소수는 상대오차 1e-9 이내면 같다고 본다(반올림 경계 뒤집힘에도 안전)
function same(a, b) {
  a = mask(a); b = mask(b);
  if (a === b) return true;
  if (a.replace(LONG, '#') !== b.replace(LONG, '#')) return false;
  const na = a.match(LONG) || [], nb = b.match(LONG) || [];
  if (na.length !== nb.length) return false;
  for (let i = 0; i < na.length; i++) {
    const x = Number(na[i]), y = Number(nb[i]);
    if (Math.abs(x - y) > 1e-9 * Math.max(Math.abs(x), Math.abs(y))) return false;
  }
  return true;
}

module.exports = { same };
if (require.main === module) main();
function main() {
const PATH_RE = /^(models|reports)\/|^(sitemap\.xml|robots\.txt)$/;
const args = process.argv.slice(2);
let files, oldOf, newOf;
if (args[0] === '--commit') {
  const c = args[1];
  files = sh(`git show --name-only --format= ${c}`).split('\n').filter((f) => PATH_RE.test(f));
  oldOf = (f) => show(`${c}~1`, f);
  newOf = (f) => show(c, f);
} else {
  files = sh('git diff --cached --name-only').split('\n').filter(Boolean);
  oldOf = (f) => show('HEAD', f);
  newOf = (f) => (fs.existsSync(f) ? fs.readFileSync(f, 'utf8') : '');
}

let n = 0;
const changed = [];
for (const f of files) {
  if (!same(oldOf(f), newOf(f))) { n++; if (changed.length < 5) changed.push(f); }
}
if (args.includes('--verbose')) console.error(`checked ${files.length} files; real changes ${n}${changed.length ? ' e.g. ' + changed.join(', ') : ''}`);
console.log(n);
}
