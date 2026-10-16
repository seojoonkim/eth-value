# Mobile spacing and delegation recovery — 2026-10-17

## Observed
- Child deleg_f024703e / sa-0-8647f3fe, reported model claude-opus-5-5, hard deadline 600s, duration 600.09s, one API call. Transcript only contains kickoff and timeout, no tool execution evidence. Backend cause remains unknown.
- Transcript: /Users/gimseojun/.hermes/profiles/sano/cache/delegation/live/deleg_f024703e/task-0.log
- Existing worktree had index.html mobile spacing changes and qa_all.sh registration on recovery; no attribution to the timed-out child. Preserved and exercised instead of overwriting.
- Live baseline browser measurements (390/430, both themes): community→signoff 46px; signoff bottom whitespace 42px. Desktop: 16px / 48px. Saved /tmp/mobile-spacing-before.json.
- Local spacing regression passed before bottom-padding refinement. Original signoff CTA carried 24px bottom margin in addition to 18px mobile padding. Removed the mobile-only margin.
- One recovery QA chain exited 143 with browser closed while navigating. Termination origin unknown; retried only the bounded audit, not the delegated task.

## Orchestration prevention
The parent violated the existing short-deadline scope rule by combining discovery, implementation, QA and deployment, and ended without a bounded progress checkpoint. This does not establish the cause of model latency. Retained recovery in parent, added offline scope policy tests and linked them into the failure skill. Scope tests 3/3 pass; existing prerequisite tests 3/3 and isolated Git ignored-file test 1/1 pass. These are decision/regression checks, not a global tool interception hook or proof of backend repair. Generic terminal error counts cannot be uniquely mapped to historical calls.

- Wider mobile 430px had #valuation margin-bottom 42px versus 28px at 390px, producing 40px versus 26px to the news title. Mobile-only margin normalized to 28px; regression retains 22–30px target rather than relaxing it.
- Local lesson recall audit returned 72/73 with an unrelated pre-existing dispatch-timeout-live-child miss; new short-worker-scope was not reported missing. First invocation mistakenly supplied a positional ID unsupported by the CLI; corrected to the documented full check. No claim that the full recall suite passes.

## Boundaries
No shared service restart, routing modification or permissions expansion. Full-site QA is not claimed by targeted spacing tests. Live release evidence is recorded separately after publication.
