# Diagnostic logging fixes — 2026-08-03 evening

Shipped for tomorrow’s session. No change to SQ criteria or live take-enable logic.

## 1. Consistency log coverage for SQ-only

`ensure_sq_consistency_rows` runs after `apply_sq_ready_promotions`. SQ-only promotes get a stub row in `consistency_rows` (flag `inputs.sq_appended_post_promote`), then the existing finalize pass fills `trade_take_enabled`, entry/SL, scores, and `promoted_via_structural_score` the same as organic READY.

## 2. Badge log: write on `trade_state` change

`should_log_badge_audit` now returns True when `trade_state` differs from the previous badge row, even inside the 4-minute debounce. Unchanged state+badges still throttle.

## 3. Take re-eval on subsequent polls (confirmation)

`trade_take_enabled` is **not** latched forever at promote. Every enrich:

1. FSM / dwell / window / structure recompute take for the current `trade_state`
2. SQ may force READY + `take=True` again **only if** still Top-6 + grade A/B (from universe snapshot) + SQ ≥ threshold
3. If SQ no longer eligible, the stock stays on the FSM state for that cycle (often WAIT/BLOCKED) with that cycle’s take flag

So a stale take=true does not persist across polls after conditions fail — unless SQ keeps re-qualifying on snapshot grade while the card shows something else (see ASHOKLEY dual-source note).

## Refs

- `ASHOKLEY_GRADE_CONTRADICTION.md`
- `TIMING_AND_ENTRY_SL.md`
