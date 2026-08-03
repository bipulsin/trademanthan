# Architectural Fixes — VWAP Side / SQ Direct / Direction Flip

**Session validated:** 2026-08-03 (Day-1 live)  
**Gate semantics:** last *closed* session-paired 10m bar (`bar_end` ≤ now) close vs cumulative session VWAP on those closed 10m bars. Env `READY_VWAP_SIDE_GATE` default **ON**.

## Fix 1 — Replay of 12 READY cards

| Symbol | Path | Dir | Promote | Close | VWAP | Gate |
|--------|------|-----|---------|-------|------|------|
| CHOLAFIN | organic | LONG | 09:45:19 | 1900.8 | 1905.26 | **BLOCK** |
| INOXWIND | organic | LONG | 09:45:19 | 80.91 | 80.52 | PASS at promote* |
| BAJAJFINSV | organic | LONG | 09:45:19 | 2073.4 | 2075.68 | **BLOCK** |
| FORTIS | organic | LONG | 10:16:28 | 970.15 | 969.40 | PASS |
| DIVISLAB | SQ | LONG | 11:10:32 | 8468 | 8355.8 | PASS |
| PNBHOUSING | SQ | LONG | 11:16:24 | 1091.1 | 1082.3 | PASS |
| JUBLFOOD | SQ | LONG | 12:05:40 | 463.7 | 458.7 | PASS |
| PAYTM | SQ | LONG | 12:16:28 | 1424.8 | 1407.8 | PASS |
| ASHOKLEY | SQ | LONG | 12:25:35 | 175.1 | 172.9 | PASS |
| APLAPOLLO | SQ | LONG | 13:06:36 | 1937 | 1906.4 | PASS |
| LTM | SQ | LONG | 13:26:34 | 4684.1 | 4566.3 | PASS |
| MCX | other | SHORT | 15:48:46 | 2632 | 2649.4 | PASS |

\* **INOXWIND nuance:** Live audit used the *forming* 09:45 tip (5m close 80.12 **below** VWAP). Closed-bar rule correctly ignores forming tips. Last closed 10m at 09:45:19 is 09:35+09:40 (close 80.91 still above). Gate **rejects at 09:55:01** when the next 10m closes (close 80.4 < VWAP 80.51) — ≤10m after the tip went wrong. CHOLAFIN + BAJAJFINSV blocked immediately at promote.

## Fix 2 — SQ earliest vs actual (grade A/B + Top-6 LOCF + VWAP-side)

| Symbol | Actual SQ | Earliest inputs | Lateness closed (min) |
|--------|-----------|-----------------|------------------------|
| DIVISLAB | 11:10 | 10:55 | ~15 |
| PNBHOUSING | 11:16 | 10:45 | ~31 |
| JUBLFOOD | 12:05 | 11:35 | ~31 |
| PAYTM | 12:16 | 11:55 | ~21 |
| ASHOKLEY | 12:25 | 10:45† | ~100† |
| APLAPOLLO | 13:06 | 11:55 | ~71 |
| LTM | 13:26 | 13:05 | ~21 |

† ASHOKLEY earliest row used a SHORT Garuda LOCF while the live card was LONG — treat as upper bound; real LONG-aligned earliest needs side filter. Full SQ Total≥75 may lag the proxy by a bar or two; lock admission was the dominant delay.

## Fix 3 — FORTIS opposite-side

- LONG thesis break (first closed 10m below VWAP after READY): **~10:46** (bar 10:40–10:45, close 966.95 vs VWAP 969.38).
- At break: Garuda still **LONG** Top-6; RS `ranking_type` null; SQ Total recomputed ~**68** (&lt;75).
- **`would_qualify_short_ready = false`** — system would *evaluate* SHORT but not promote (no BEAR Top-6 / BEARISH RS + Total≥75). Manual TV SHORT ~11:15 would still need those inputs to fire a flip READY.

## Code

- `backend/services/vwap_side_gate.py` — hard gate + demote
- `backend/services/structural_quality_ready.py` — `sq_direct` inject, VWAP before SQ, `direction_flip_promotion`
- `backend/services/daily_checklist_trade_state.py` — gate every READY cycle; post-SQ re-gate + flip
- `backend/test_vwap_side_gate.py` — unit tests
- Replay: `docs/diagnostics/day1_live_20260803/_replay_vwap_sq_fixes.py`
