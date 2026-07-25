# WIPRO `atr14_pct = 0.0` on 2026-07-24 — root cause

**Status:** read-only investigation complete. **No live fix.**  
**Priority:** **HIGH** — systemic false zeros, not WIPRO-only / not truncation.

**Question:** Was WIPRO `rs_scanner_history.atr14_pct = 0.0` on 2026-07-24 genuine missing ATR, or a truncation/cast bug (e.g. 0.45 → 0)?

**Prior context:** `../atr_progression_promote_backtest_20260720_24/` — WIPRO atr_consumed undefined because `atr14_pct = 0.0` filtered by `atr14_pct > 0`.

---

## Verdict

| | Answer |
|---|---|
| **(a)** | **Persistence / fetch-failure bug (false zero)** — not genuine ATR≈0, **not** integer truncation/rounding of a small %. Market ATR was ~2%. Writer stored sentinel `0.0` when daily candles were unavailable at enrich time. |
| **(b)** | Mechanism: `enrich_ranked_with_maturity` defaults metrics to `0.0`, swallows Upstox daily-candle failures → empty list → `compute_yesterday_range_metrics` early-returns `(0,0,0)`, upserts that into DOUBLE PRECISION. |
| **(c)** | **458 / 792** symbol-days in 2026-07-20→07-24 have `atr14_pct = 0` (57.8%). **All 458** belong to symbols that also have a **positive** `atr14_pct` somewhere in July 2026. |
| **(d)** | Re-run any analysis that builds `atr_consumed` / distance / suppress from `rs_scanner_history.atr14_pct` on this window (list below). |

---

## Evidence summary

### 1. Raw DB row (literal 0, not NULL)

| Field | WIPRO 2026-07-24 |
|---|---|
| Column type | `double precision` (not int) |
| `atr14_pct` | **0** (`is_null=false`, `atr_text='0'`) |
| `daily_range_pct` | **0** |
| `range_vs_atr_ratio` | **0** |
| `maturity_tag` | CONTINUING (consec=2) |
| `rs_pct` | 1.28 |

Backtest scripts filter `atr14_pct IS NOT NULL AND atr14_pct > 0` — so this literal **0.0** is dropped (not coalesced from NULL).

### 2. Not truncation / cast

- Schema: `DOUBLE PRECISION` (`backend/database.py` ~881).
- Write path: `round(atr14_pct, 4)` only (`rs_scanner_maturity.py:193`).
- In 07-20→07-24: **334/334** positive values have fractional parts; **0** integer-looking positives; min positive = **1.3173** (nothing in (0, 1) that could floor to 0).
- Same-day peers with decimals: ICICIGI **2.8465**, PRESTIGE **2.8013**.

### 3. Recomputed ATR (data was available)

Live Upstox daily FO (`NSE_FO|58419`) + same helpers as production:

| as_of | bars before strip | ATR(14) | y_close | **atr14_pct** |
|---|---:|---:|---:|---:|
| **2026-07-24** | 28 | 3.3634 | 170.45 (07-23) | **1.9732%** |
| EQ cross-check | 28 | 3.4065 | 174.82 | **1.9486%** |

WIPRO historical **successful** rows: 07-03 **2.3692**, 07-13 **2.2378**, 07-15 **2.3339**, 07-16 **2.3544** — same ~2% regime. Window zeros: 07-20, 21, 23, 24 (not a one-day glitch for WIPRO).

### 4. Failure-mode fingerprint

In 07-20→07-24, **every** zero ATR row also has `daily_range_pct = 0` and `range_vs_atr_ratio = 0` (458/458).

That matches the **early** `(0,0,0)` returns (empty / &lt;2 candles / bad closes / missing `instrument_key`), **not** the path that returns `(daily_range_pct, 0, 0)` when ATR history is short but OHLC exists (`atr0_but_range_pos = 0`).

Instrument key was present in master (`NSE_FO|58419`); RS snapshots show WIPRO BULL #7 @ 12:50 / 13:20 IST on 07-24 — enrichment ran and still wrote zeros → candle fetch returned empty (or exception → `[]`).

### 5. Scope — systemic

| Day | rows | zero/null | positive | % bad |
|---|---:|---:|---:|---:|
| 07-20 | 167 | 96 | 71 | 57.5 |
| 07-21 | 145 | 92 | 53 | 63.4 |
| 07-22 | 164 | 100 | 64 | 61.0 |
| 07-23 | 154 | 67 | 87 | 43.5 |
| 07-24 | 162 | 103 | 59 | 63.6 |
| **Total** | **792** | **458** | **334** | **57.8** |

On 07-24 alone: COFORGE / INFY / LTM / MPHASIS / TCS / RBLBANK / WIPRO = 0; ICICIGI / PRESTIGE = healthy decimals. **WIPRO-specific? No.**

Matches prior note in `../never_ready_fsm_gates_20260720_24/` (458/792 zeros).

---

## Exact mechanism (files:lines)

`backend/services/rs_scanner_maturity.py`:

1. **277** — `daily_range_pct = atr14_pct = range_vs_atr_ratio = 0.0` default (also if `instrument_key` missing).
2. **280–286** — `get_historical_candles_by_instrument_key(..., interval="days/1", days_back=30)` on exception → log debug → **`[]`**.
3. **`compute_yesterday_range_metrics` 132–133, 141–142, 150–151** — insufficient / invalid candles → **`(0.0, 0.0, 0.0)`**.
4. **159–164** — ATR None/≤0 → `(daily_range_pct, 0.0, 0.0)` (not the fingerprint seen in this window).
5. **193 + upsert 227** — `round(..., 4)` then persist sentinel **0.0** (not NULL).

Called from `relative_strength_scanner.py` ~687–689 after each Top-10 rank build. Silent fetch failure under Upstox rate pressure (documented 429 storm elsewhere) is the most plausible live trigger; we do not have the original HTTP response from 07-24.

**Not a bug:** `int()` / `FLOOR` / integer column on write.  
**Is a bug / design defect:** treating unavailable metrics as **0.0**, which consumers interpret as “no usable ATR” or (worse) falsy → fallback **1.2%**.

---

## Prior analyses that used this field / may need re-run

Checkpoint-window / ATR-consumed dependent (highest priority):

| Artifact / script | Why |
|---|---|
| `../atr_progression_promote_backtest_20260720_24/` · `scripts/backtest_atr_progression_promote_20260720_24.py` | Direct WIPRO miss; filters `atr14_pct > 0` |
| `../scoring_gate_backtest_20260720_24/` · `scripts/backtest_scoring_gate_20260720_24.py` | Part 1 ATR suppress / atr_consumed replay |
| `../scoring_gate_backtest_v2/` · `scripts/backtest_scoring_gate_v2.py` | Same atr_consumed approx |
| `../never_ready_fsm_gates_20260720_24/` · `scripts/never_ready_fsm_gates_20260720_24.py` | EMA5 distance ATR; already noted 458 zeros + fallback |
| `../daily_symbol_audit_20260720_24/` · `scripts/daily_symbol_audit_20260720_24.py` | ATR-consumed replay from atr14_pct |

Also uses field (fallback or join — review if window overlaps):

- `scripts/analyze_rs_selection_quality.py`, `analyze_rs_feature_profile.py`, `analyze_rs_early_move_hypothesis.py`, `analyze_rs_vwap_gate_backtest.py` (`or 1.2`)
- `backend/services/oi_heatmap_buildup_backtest.py` (`DEFAULT_ATR_PCT = 1.2`)
- `scripts/backtest_go_board_validation.py`, `scripts/diagnose_plus4_extension_outliers.py`
- Live: `rs_conviction_candles.py`, `kavach_universe_vwap_scan.py`, `rs_setup_radar.py`

---

## Caveats

1. Root cause of empty candles at **scan time** is inferred (fingerprint + successful recompute today), not proven via archived Upstox payloads.
2. Local `upstox_ws_intraday_1m` for WIPRO FO only covers ~07-21→24 — cannot rebuild ATR(14) from DB 1m alone; recompute used live Upstox daily history.
3. `compute_yesterday_range_metrics(as_of=historical)` does not drop bars **after** `as_of` (only strips bar equal to `as_of`); live scans use `as_of=today` so this does not explain the stored zero.
4. Fix direction (out of scope): persist NULL on failure; retry/cache daily ATR; or backfill from last good symbol ATR — **do not** treat 0 as real.

---

## Artifacts

| File | Contents |
|---|---|
| `README.md` | This report |
| `evidence.json` | Raw queries, recompute, counts, mechanism lines |
| `00_manifest.json` | One-page summary for parent handoff |
