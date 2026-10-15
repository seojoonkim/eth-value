#!/bin/bash
# One-shot mobile/desktop QA gate. Run in background (takes ~3-5 min):
#   bash .qa/qa_all.sh [url]   -> /tmp/ethval-qa/summary.txt ; exit 1 on any failure
# Why a file: long inline heredocs get blocked by the terminal parser, and foreground runs >600s time out.
set -u
# Local runs: `node .qa/with_server.cjs qa_all.sh` (free port, auto stop) or .qa/qa_snapshot.sh.
# A bare default URL may hit another project's stale server; the target guard below then fails loudly.
cd "$(dirname "$0")"
# 2026-10-15: no URL → re-run itself under with_server.cjs (own server on a free port, stopped on exit).
# The old fixed default 127.0.0.1:8765 was often held by another project's server (8765/8791 both
# non-ETHval on 2026-10-15) and the run failed at the target guard before checking anything.
if [ -z "${1:-}" ]; then exec node with_server.cjs qa_all.sh; fi
URL="$1"
# 2026-10-15: one private result folder per run. A fixed /tmp/ethval-qa made two concurrent runs
# (auto-dev worker + a manual check) write into one summary.txt (2026-10-14 23:11: two URLs/ports in one
# file -> 4 false FAILs). At exit, /tmp/ethval-qa/latest points at this run and /tmp/ethval-qa/summary.txt
# is replaced atomically with this run's complete summary (old readers keep working). Path printed first.
BASE=/tmp/ethval-qa; mkdir -p "$BASE"
OUT="$(mktemp -d /tmp/ethval-qa.run.XXXXXX)"; : > "$OUT/summary.txt"
publish() {
  # -n: replace the old link itself (mv onto a dir symlink would move INTO the old run folder)
  ln -sfn "$OUT" "$BASE/latest"
  cp "$OUT/summary.txt" "$BASE/summary.txt.tmp.$$" && mv -f "$BASE/summary.txt.tmp.$$" "$BASE/summary.txt"
  # keep the 12 newest run folders
  ls -1dt /tmp/ethval-qa.run.* 2>/dev/null | tail -n +13 | xargs rm -rf 2>/dev/null
}
trap publish EXIT
echo "QA_OUT $OUT"
fail=0
log() { echo "$*" | tee -a "$OUT/summary.txt"; }

log "URL $URL  $(date '+%F %T')"
# Guard: the default port can be held by another project's server (2026-10-13: 8765 = resonance-observatory),
# which silently makes every check fail/pass against the wrong site. Verify the page is ETHval first.
if ! curl -s -m 10 "$URL" | grep -q 'ETHval'; then
  log "target: FAIL — $URL is not serving ETHval (check: lsof -iTCP:${URL##*:} ; start: python3 -m http.server <free port> --bind 127.0.0.1)"
  log "RESULT FAIL"; exit 1
fi
if node geom_check.cjs "$URL" > "$OUT/geom.txt" 2>&1; then log "geom: PASS"; else log "geom: FAIL"; fail=1; fi
cut -c1-200 "$OUT/geom.txt" >> "$OUT/summary.txt"

if [[ "$URL" == http://127.0.0.1* ]]; then
  QA_URL="$URL" node dom3.cjs > "$OUT/height.txt" 2>&1
  grep -E "^doc|^==" "$OUT/height.txt" >> "$OUT/summary.txt"
  doc=$(awk '/^doc/{print $2}' "$OUT/height.txt")
  # Mobile page-height bloat guard. 2026-10-13: 27500 -> 28000 after intentional legibility pass
  # (min text 10px -> 11px added ~309px in Fundamentals/Ratings). Raise only with a measured, intended reason.
  # 2026-10-13: 28000 -> 28500 for the News Impact weekly matrix (backlog item; measured 27790 -> 28308, +518px at 390).
  # 2026-10-13: 28500 -> 28800. Measured: HEAD 5b2e808 (upcoming-events line) already 28511; US 10Y card +45 -> 28556.
  # 2026-10-13: 28800 -> 29300 for the News Impact call scorecard (backlog item). Measured at 390: HEAD dcce79a 28556 -> 29080 (+524, all in #section-valuation).
  # 2026-10-13: 29300 -> 29600 for the Broad Dollar Index card (backlog item). Measured at 390: HEAD ddb2a1c 29056 -> 29368 (+312, all in #fundamentals).
  # 2026-10-13: 29600 -> 29900 for per-category vs-BTC line in the call scorecard (backlog item). Measured at 390: HEAD a16d2b8 29473 -> 29615 (+142).
  # 2026-10-13: 29900 -> 30700 for Simon's "raise the smallest font + one font scale" request (min 11 -> 12px, 18 sizes -> 10).
  #   Measured at 390 (en): HEAD 29816 -> 30317 (+501): #fundamentals +352 (metric-desc 11->12px, line-height already 1.35),
  #   #section-valuation +150 (news-impact labels), ratings +32. Not bloat; budget keeps ~380px headroom.
  # 2026-10-14: 30700 -> 32000 for user-requested section 06 Research Library (feedback doc) + title/box-title
  #   size steps + ELI5 buttons. Measured at 390 (en): 31696 total; #research alone 1111 (8 sources, compact rows on phones).
  # 2026-10-14: 32000 -> 32700 for the 01.3 fair value band chart (backlog item). Measured at 390 (en): HEAD 31723 -> 32328
  #   (+605, all in #section-valuation: chart 230px + current-zone row + 7-zone legend + published band-edge note). ~370px headroom.
  # 2026-10-14: 32700 -> 33500 for the 01.3 model track record (backtest) panel (backlog item). Measured at 390 (en):
  #   HEAD 32380 -> 33162 (+782; #fv-backtest 762px = 5-row table + period/method note + required overfitting warning). ~340px headroom.
  # 2026-10-14: 33500 -> 33700 for the 01.4 simulator report presets row (backlog #6). Measured at 390 (en), 2 runs each:
  #   HEAD 33452 -> 33614 (+162; all in #simulator-section 1876 -> 2038 = title + one chip row + one-line hint, idle state). ~86px headroom.
  # 2026-10-14: 33700 -> 34000 for the 03 Triple Point breakdown (backlog #7, closed <details> like the other 03 boxes).
  #   Measured at 390 (en), 2 runs each: HEAD 33680 -> 33815 (+135 = #triple-point closed: 16px gap + summary title + one meta line). ~185px headroom.
  # 2026-10-14: 34000 -> 34400 for the 03 relative value ETH/SOL/BTC box (backlog #8, closed <details> like the other 03 boxes).
  #   Measured at 390 (en), 2 runs each: HEAD 462e92a 33971 -> 34066 (+95 = #relval closed: 16px gap + summary title + one meta line).
  #   HEAD had already grown to 33971 (29px headroom) from the phone title-size commit 8195844. ~334px headroom.
  # 2026-10-15: 34400 -> 35800 for the design refresh be90904 (13px reading text, 24px figures, roomier card padding).
  #   Measured at 390 (en), 2 runs each: 4ced6ff 34211 -> be90904 35474 (+1263): #fundamentals +975 (metric cards),
  #   #section-valuation +170, #ratings +92, #research +25. Card radius 10->6px (design rule) changes no height. ~326px headroom.
  if [ -n "$doc" ] && [ "$doc" -lt 35800 ]; then log "height: PASS ($doc px < 35800)"; else log "height: FAIL ($doc)"; fail=1; fi
fi

node mobile_audit.cjs "$URL" "$OUT/audit" 390 > "$OUT/audit.txt" 2>&1
python3 - "$OUT" <<'PY' >> "$OUT/summary.txt"
import json, sys
r = json.load(open(sys.argv[1] + '/audit/report.json'))
ov = [x for x in r.get('overflow', []) if 'share' not in str(x[1])]
print('audit: overflow(non-share)=%d tiny=%d smallTaps=%d' % (len(ov), len(r.get('tiny', [])), len(r.get('taps', []))))
PY
grep -q "overflow(non-share)=0" "$OUT/summary.txt" || { log "audit: FAIL overflow"; fail=1; }

# Independent checks below run concurrently via xargs -P (each owns its browser + output file;
# macOS bash 3.2 has no `wait -n`). 2026-10-13 (zeon): the serial chain had grown to ~10 of the
# ~11 min gate, mostly fixed waitForTimeout settles. QA_JOBS=1 = serial. PASS/FAIL lines keep this order.
# 2026-10-13: readability gates — WCAG AA tokens both themes; no English leftovers / zh 反馈 mistranslation in ko/zh/ja
# 2026-10-13: simulator must use the same model formulas as the cards (Metcalfe/P/S/Fee Yield had drifted);
# typography: no coding-mono numbers / JetBrains download, slider handle >= 20px
# 2026-10-13: weekly matrix cells must carry a count unit (N건/news/条/件) + legend, 4 langs, no overflow at 390
# 2026-10-13: Fundamentals "news that moved this metric" badges: count == events (30d), click filters panel, no overlap at 390/1280
# 2026-10-13: News Impact call scorecard: rendered k/n == in-page recompute (7d close), n<10 -> hold label, 4 langs, no overflow/mid-word break
# 2026-10-13: design consistency (card radius, CTA flat + contrast, control font, card surfaces),
# dark-mode colour aliases follow dark tokens, Korean never wraps mid-word
# 2026-10-13 (web balance): 01 Valuation columns end on one line, header/body share one container,
# 2026-10-14: 03 constants table == MODEL_CONSTANTS/formula source, changelog latest == code, downloads row count == REST
# 2026-10-14: 03 composite sensitivity: mean/median/trimmed/leave-one-out == independent recompute (cards + stored history), headline median = true median
# 2026-10-14: 03 Triple Point breakdown: 3 axis contributions == cards sum/12, add up to headline, waterfall contiguous, 12 reasons in 4 langs, login card never on the table
# 2026-10-14: 03 relative value ETH/SOL/BTC: P/Fees, P/Revenue == independent recompute from Supabase REST, one source per table, BTC revenue 0 -> '—', 4 langs, no overflow
# 2026-10-14: constants-history (git only): every commit that changed MODEL_CONSTANTS has its own dated CHANGELOG entry with that sha
# 2026-10-15: share card + 'Post to X' tweet text in 4 langs, values == page, link keeps the language
# one font scale (min 12px), numbered section boundaries, nothing leaves its card, no text-on-text overlap
QA_JOBS="${QA_JOBS:-4}"
CHECKS=(
  "commentary-prompt|commentary_check|commentary|"
  "oi-single-source|oi_single_source_check|oi_src|"
  "no-invented-history|flat_fallback_check|flat|"
  "avatar-src|avatar_src_check|avatar|"
  "coingecko-cache|cg_calls_check|cg|$URL"
  "coingecko-blocked-fallback|cg_blocked_check|cgblocked|$URL"
  "contrast|contrast_check|contrast|$URL"
  "i18n-polish|polish_check|polish|$URL"
  "sim-vs-cards|sim_vs_cards|sim|$URL"
  "type|type_check|type|$URL"
  "matrix|matrix_check|matrix|$URL"
  "news-badge|news_badge_check|badge|$URL $OUT/nimb"
  "news-score|news_score_check|score|$URL $OUT/nisc"
  "news-score-rel-geom|rel_geom_check|relgeom|$URL"
  "design_consistency_check|design_consistency_check|design_consistency_check|$URL"
  "metric_value_overlap_check|metric_value_overlap_check|metric_value_overlap_check|$URL"
  "token_theme_check|token_theme_check|token_theme_check|$URL"
  "ko_wordbreak_check|ko_wordbreak_check|ko_wordbreak_check|$URL"
  "val_balance_check|val_balance_check|val_balance_check|$URL"
  "container_align_check|container_align_check|container_align_check|$URL"
  "font_scale_check|font_scale_check|font_scale_check|$URL"
  "section_boundary_check|section_boundary_check|section_boundary_check|$URL"
  "card_containment_check|card_containment_check|card_containment_check|$URL"
  "text_overlap_check|text_overlap_check|text_overlap_check|$URL"
  "nav_landing_check|nav_landing_check|nav_landing_check|$URL"
  "nav_scroll_active_check|nav_scroll_active_check|nav_scroll_active_check|$URL"
  "price_consistency_check|price_consistency_check|price_consistency_check|$URL"
  "extras_check|extras_check|extras_check|$URL"
  "fv_band_check|fv_band_check|fv_band_check|$URL"
  "fv_backtest_check|fv_backtest_check|fv_backtest_check|$URL"
  "constants_check|constants_check|constants_check|$URL"
  "composite_sens|composite_sens_check|composite_sens|$URL"
  "triple_point|triple_point_check|triple_point|$URL"
  "relval|relval_check|relval|$URL"
  "burn_board|burn_board_check|burn_board|$URL"
  "sim_presets|sim_presets_check|sim_presets|$URL"
  "constants-history|constants_history_check|constants_history|"
  "embed_check|embed_check|embed_check|$URL"
  "lang_url_check|lang_url_check|lang_url_check|$URL"
  "mobile_audit|mobile_audit|mobile_audit|$URL"
  "community_badge|community_badge_check|community_badge|$URL"
  "forum_mobile_date|forum_mobile_date_check|forum_mobile_date|$URL"
  "forum_header|forum_header_check|forum_header|$URL"
  "forum_author|forum_author_check|forum_author|$URL"
  "forum_mobile_title|forum_mobile_title_check|forum_mobile_title|$URL"
  "menu_gap|menu_gap_check|menu_gap|$URL"
  "subtitle|subtitle_size_check|subtitle|$URL"
  "rating_title|rating_title_check|rating_title|$URL"
  "model_badge|model_badge_check|model_badge|$URL"
  "commentary_fetch|commentary_fetch_check|commentary_fetch|$URL"
  "argon_bugs|argon_bugs_check|argon_bugs|$URL"
  "fee_label|fee_label_check|fee_label|$URL"
  "sidebar|sidebar_check|sidebar|$URL"
  "news_lang|news_lang_check|news_lang|"
  "model_pages|model_pages_check|model_pages|$URL"
  "reports|reports_check|reports|$URL"
  "report_rebuild|report_rebuild_check|report_rebuild|"
  "share_card_i18n|share_card_i18n_check|share_card_i18n|$URL $OUT/share"
  # 2026-10-15: ETH->USD conversions use that day's close (no $3,500 fallback); same values on every load
  "price_fallback|price_fallback_check|price_fallback|$URL"
  "float_consistency|float_consistency_check|float_consistency|$URL"
  # 2026-10-15: keep-all text must stay in its box (ja/zh no-space sentences ran out at 390; ko legend under badge)
  "keep_all_overflow|keep_all_overflow_check|keep_all_overflow|$URL 390,1280"
  # 2026-10-15: chart legend "All model lines" toggle label on one line (390 ko broke as "모델 선 / 전체")
  "lines_toggle|lines_toggle_check|lines_toggle|$URL 360,390,1280 en,ko,ja,zh $OUT/lines"
  # 2026-10-15: login/rate CTA docked in header/sidebar — never over body text, never overlapping header controls
  "fab_cover|fab_cover_check|fab_cover|$URL $OUT/fab"
)
for spec in "${CHECKS[@]}"; do IFS="|" read -r label script out args <<< "$spec"; rm -f "$OUT/$out.rc"; done
printf '%s\n' "${CHECKS[@]}" | xargs -P "$QA_JOBS" -I{} bash -c '
  IFS="|" read -r label script out args <<< "$1"
  if node "$script.cjs" $args > "'$OUT'/$out.txt" 2>&1; then echo 0; else echo 1; fi > "'$OUT'/$out.rc"
' _ {}
for spec in "${CHECKS[@]}"; do
  IFS="|" read -r label script out args <<< "$spec"
  if [ "$(cat "$OUT/$out.rc" 2>/dev/null)" = 0 ]; then log "$label: PASS"; else
    log "$label: FAIL"; grep -E "FAIL|^==|SECOND" "$OUT/$out.txt" | head -8 | cut -c1-300 | tee -a "$OUT/summary.txt"; fail=1
  fi
done
log "RESULT $([ $fail = 0 ] && echo PASS || echo FAIL)"
exit $fail
