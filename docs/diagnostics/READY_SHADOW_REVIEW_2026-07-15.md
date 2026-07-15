# READY shadow review — 2026-07-15

Shadow-log / research only. No live READY or gate behavior changed by this note.

## 1. Classifications applied (prod)

Table: `kavach_ready_consistency_review` (joined to `kavach_ready_consistency_log` by `log_id`).

| log_id | symbol   | logged_at (IST)     | outcome_classification           |
|--------|----------|---------------------|----------------------------------|
| 36     | DIVISLAB | 2026-07-15 09:51:39 | `LOSS_CONFIRMED_QUALITY_FAIL`    |
| 199    | HYUNDAI  | 2026-07-15 10:44:40 | `LOSS_CONFIRMED_QUALITY_FAIL`    |
| 239    | CHOLAFIN | 2026-07-15 11:02:59 | `LOSS_CONFIRMED_QUALITY_FAIL`    |

Rollup after update (`/api/ready-shadow-review?session_date=2026-07-15`):

- `total_rows`: 574  
- `needs_classification_count`: 565  
- `classified_count`: **3**

Source notes/`reviewed_at` copied from `ready-shadow-review-2026-07-15-annotated.json`.

## 2. BANKINDIA shadow-log capture gap — root cause

**Verdict: expected scoping, not a per-symbol logger bug.**

### How the logger decides to write

In `enrich_trade_states` → `log_ready_consistency`, a row is written only when:

```text
pre_gate READY/READY(RECHECK)  OR  lock_mismatch  OR  vwap_gate_applied
```

It does **not** sample every locked / top-5 symbol every poll. Pine Confidence A+ / Trade Score is a separate panel; it does not imply TradeManthan `trade_state=READY`.

Enrich runs over checklist **display** stocks (locked Top-5 when snapshot is locked), but **logging** still requires the READY (or mismatch/gate) condition above.

### BANKINDIA timeline (2026-07-15, IST)

| Time   | Evidence |
|--------|----------|
| 12:50  | Lock **entry** (`intraday_2scan`, BULL rank 3) — `rs_lock_membership_audit` |
| 13:20–13:25 | Only **2** consistency-log rows; both `rendered_state=READY`, `in_lock=true`, rank 2, `vwap_slope_score=0`, `quality_pass=false` |
| 13:35  | Lock **remove** R2 (`rank_outside_band`) — same cycle as POLICYBZR remove |
| 14:20  | Lock **re-entry** (`intraday_2scan`, BULL rank 5); still on `daily_snapshot` EOD (rank 2) |
| after 13:25 | **0** further consistency-log rows for BANKINDIA |
| ~15:51 | TradingView Kavach showed A+ / 95 — outside this logger’s trigger |

So: after 13:25 the symbol left **pre-gate READY** (and was briefly off-lock 13:35–14:20). After re-lock at 14:20 it remained on the board but never re-hit READY, so silence is by design.

### Cohort pattern (same day)

| Symbol     | Last consistency log | Notes |
|------------|----------------------|--------|
| BANKINDIA  | 13:25 | READY then silence; lock remove 13:35, re-entry 14:20 without READY |
| POLICYBZR  | 13:25 | Same last poll; lock remove 13:35; no re-entry |
| MANKIND    | 11:30 | READY window ended; not a logger stall |
| ADANIPOWER | 10:47 | Same pattern |
| DLF / POWERINDIA / DELHIVERY / ETERNAL | 14:20 | Last READY-class poll cluster |
| IDEA       | 15:30 | Kept logging while pre-gate READY persisted |

Other symbols continued into the 14:00 hour → logger kept writing; BANKINDIA specifically was no longer READY-eligible for the shadow insert.

### Implication for research

Absence from the shadow log after time T means “no pre-gate READY (or mismatch/gate) event,” **not** “symbol was unscored / unseen by Pine.” For late-session Pine strength that never became TradeManthan READY, this table will stay empty by design. If future research needs continuous lock-membership sampling, that would be a **new** shadow stream (explicitly not requested here).

## 3. Flag for 22-Jul — `STEEP_OK_THRESHOLD_CHECK` (no code)

DIVISLAB 09:51: `vwap_slope_score=25.94`, `steep_ok=False`. Day-wide, `steep_ok=True` only at slope ≥ ~50.34 (BAJFINANCE / DALBHARAT / POLYCAB). Code path: `rs_vwap_quality.vwap_slope_steepening` / `THRESHOLD_VWAP_SLOPE`. Confirm at checkpoint that ~48–50 is intended calibration, not a wiring bug. **Do not change live gate yet.**

## 4. Flag for 22-Jul / 4-week shadow — `VWAP_EXTENSION_METRIC_MISSING` (no code)

Schema logs slope / `steep_ok` only. Propose shadow-only `vwap_extension_pct = (close - vwap) / vwap` on `kavach_ready_consistency_log` to separate extension-without-slope from slope-steepening (2026-07-07 research). Zero live effect; same backtest bar before any READY wiring.

---

## Phase 2 (2026-07-15 evening) — raw VWAP capture widened

**Problem:** READY-triggered consistency rows systematically land late in the VWAP move (2–4h after chart steep onset). `steep_ok` on those rows measures the **tail**, understating early-window slope ≥50 frequency.

**Change (shadow-only):** new append-only table `kavach_vwap_raw_log`, written every enrich poll for **every `daily_snapshot` lock member**, independent of `pre_gate_state`. Fields: `symbol`, `logged_at`, `vwap_slope_score`, `steep_ok`, `lock_rank`, `lock_direction`, `direction`, `vwap_extension_pct`.

- `kavach_ready_consistency_log` write rules **unchanged**.
- No live READY / gate / lock changes.
- No backfill of 2026-07-15; live forward from deploy for 22-Jul comparison.

---

## Phase 3 (2026-07-15 evening) — Option C full-universe VWAP slope sweep

**Question:** Do non-lock symbols ever show `vwap_slope_score >= 50` during the day?

**Change (shadow-only):**
- Table `kavach_universe_vwap_scan` — full ~200 F&O universe every 5m RTH
- Live path: **cache-only** (rides `centralized_market_data_5m` candle_cache; no Upstox storm)
- Scheduler: Mon–Fri `:01/:06/…` IST (offset +1m after market-data refresh)
- Columns include `in_lock_at_time`, `source` (`live`|`backfill`), `vwap_extension_pct`
- Backfill script: `scripts/backfill_universe_vwap_scan.py` via Upstox historical 5m (`range_end_date`) for 13/14/15-Jul
- `shadow.html` → **Export for Analysis** bundles consistency + universe scan + raw log + R1/R2 + snapshot
