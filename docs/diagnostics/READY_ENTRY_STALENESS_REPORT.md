# READY NOW entry-price staleness (shadow diagnostic)

**Generated:** 2026-07-29T10:51:37.406779+05:30

Instrumentation only — no live entry/gating/countdown changes.

## Coverage

| Metric | Value |
|---|---|
| Rows | 2140 (backfill 2137, live 3) |
| Window | 2026-07-15 03:53:20.190516+00:00 → 2026-07-29 05:21:04.420310+00:00 |
| Days / symbols | 11 / 104 |
| Initial promotions | 259 |
| Rechecks | 1881 (attempt≥2: 498) |

## gap_pct distribution

| Cohort | n | p50 | p90 | ≥1% | ≥2% | ≥5% | max |
|---|---|---|---|---|---|---|---|
| All events | 1266 | 0.111 | 0.443 | 2.8% | 1.5% | 1.1% | 5.721 |
| Recheck | 1099 | 0.113 | 0.482 | 3.1% | 1.6% | 1.2% | 5.721 |
| Recheck attempt≥2 | 493 | 0.119 | 0.526 | 4.1% | 2.0% | 1.0% | 5.465 |

## Entry not recalculated across recheck

**44.0%** (828/1881) of recheck events carried the same `entry_price_last_computed_ts` as the prior event (or entry unchanged while `entry_matches_ema5=false`).

READY cards with entry ≠ live EMA5: 474 / 1262.

## Flagged symbol-days (|gap_pct| ≥ 2.0% while READY/visible)

Count: **2**

| Date | Symbol | Max |gap|% | Entry | LTP | EMA5 | Match EMA5? | Attempt | Grade | Take? |
|---|---|---|---|---|---|---|---|---|---|
| 2026-07-29 | KAYNES | 5.721 | 3245.71 | 3431.4 | 3365.0257 | False | 1 | A+ | False |
| 2026-07-28 | SUZLON | 4.157 | 51.96 | 49.8 | 50.778 | False | 2 | D! | False |

## Definitions

{
  "gap_pct": "(ltp - entry_price) / entry_price \u00d7 100",
  "entry_price_last_computed_ts": "set to event time when entry\u2248EMA5 (\u00b10.02); else carried from prior same entry value (sticky)",
  "attempt_number": "1 + 10m IST slots crossed since ready_visible_since (frontend Enter-within attempt proxy)",
  "event_type": "initial_promotion | recheck",
  "flag_threshold_pct": 2.0,
  "note": "Frontend 'Recheck confirmed \u00b7 attempt N' is sessionStorage-based; backend attempt_number approximates that via 10m slot crossings. Live dwell overrides trade_entry to live EMA5 when available \u2014 large gap with entry_matches_ema5=true means price ran away from EMA5 while card still READY/soft-held."
}

## Artifacts

- `docs/diagnostics/ready_entry_staleness_report.json`
- Table: `kavach_ready_entry_staleness_log`
- Runner: `scripts/analyze_ready_entry_staleness.py`

