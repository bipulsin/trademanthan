# Day-1 follow-up — READY vs TAKE timing + Entry/SL (2026-08-03)

**No code changes.** Clarifies the 09:45 vs 09:50 timing puzzle and fills entry/SL for all 12 READY cards.  
Artifacts: `ready_take_entry_sl_20260803.{csv,json}`

---

## Direct answer: what is dashboard-authoritative for READY NOW?

**Neither log table.** The checklist UI (`frontend/public/dailyRSchecklist.js`) polls:

`GET /api/dashboard/daily-checklist/data` **every 60 seconds**

and renders READY NOW from live `stock.trade_state` (`READY` / `READY(RECHECK)`). It does **not** read `kavach_badge_input_log` or `kavach_ready_consistency_log`.

| Source | Role | Cadence |
|---|---|---|
| Live checklist API | **Authoritative for trader UI** | Server enrich on request / background refresh; UI poll **60s** |
| `kavach_ready_consistency_log` | Shadow diagnostic | Written only when pre-SQ gate says “interesting” (`is_ready_pre` / lock mismatch / VWAP gate) — then finalized after stack+SQ |
| `kavach_badge_input_log` | Shadow badge audit | Written after enrich, but **throttled**: min **4 minutes** between rows unless badge *active set* changes — **does not key off `trade_state` changes** |

So the Day-1 report’s “first READY @ 09:50” from the badge log was a **diagnostic sampling artifact**, not when the card appeared on screen.

---

## Timing puzzle (CHOLAFIN / INOXWIND / BAJAJFINSV)

### What actually happened (same enrich cycle)

| Time (IST) | Event |
|---|---|
| **09:44:33** | Badge log wrote pre-READY state (CHOLAFIN/INOXWIND: `SCANNING`; BAJAJFINSV: `BLOCKED`) with `REGIME UNSTABLE` still active |
| **09:45:19** | Server enrich: organic **READY** + `trade_take_enabled=true` + entry/SL. Written to **consistency** + **entry_staleness** (`card_visible=true`). |
| **09:45:19** | Badge log **skipped** this cycle: previous write ~46s earlier and badge active set unchanged → `_LOG_MIN_INTERVAL = 4 minutes` blocks the write |
| **09:50:22** | Next enrich that both consistency *and* badge record READY (badge throttle elapsed) |

**Confirmed write sequences (organic trio):**

```
CHOLAFIN
  badge:  09:44:33 SCANNING → 09:50:22 READY   (missed 09:45)
  cons:   09:45:19 READY take=true entry=1894.11 sl=1880.91
          09:50:22 READY take=true (same prices)
  stale:  09:45:19 READY take=true card_visible=true

INOXWIND / BAJAJFINSV — same pattern (badge miss at 09:45, catch-up 09:50)
```

### Can TAKE be true before READY is visible?

**Not as two independent states on the live API.** Take is computed on the same stock object as READY (`trade_state in READY* AND entry window AND structural OK`). When consistency logged take=true at 09:45:19, `trade_state` was already READY in that response.

**Residual UX lag (real, but seconds not five minutes):** a trader whose browser last polled at 09:44:xx could wait up to **~60s** for the next `/data` poll to show the card. That is UI poll lag, not badge-log lag.

**Flag (diagnostic, not trader-facing):** badge audit systematically under-samples READY transitions when gate badges don’t change — systemic today (all three morning organics), not a one-off.

---

## Combined table — all 12 Day-1 READY cards

Prices from `kavach_ready_entry_staleness_log` at first READY (= `trade_entry` / `ema10_value`); organic also match `kavach_ready_consistency_log.inputs`. SL rule: **EMA10**.

| Symbol | Path | Server READY | Consistency TAKE | Badge first READY | Entry | SL (EMA10) | Pts risk | Take@READY | Notes |
|---|---|---|---|---|---:|---:|---:|:---:|---|
| CHOLAFIN | organic | 09:45:19 | 09:45:19 | 09:50:22 | 1894.11 | 1880.91 | 13.20 | Y | badge throttle miss |
| INOXWIND | organic | 09:45:19 | 09:45:19 | 09:50:22 | 80.16 | 79.58 | 0.58 | Y | badge throttle miss |
| BAJAJFINSV | organic | 09:45:19 | 09:45:19 | 09:50:22 | 2064.62 | 2053.15 | 11.47 | Y | badge throttle miss |
| FORTIS | organic (+SQ 10:35) | 10:16:28 | 10:16:28 | 10:16:28 | 969.33 | 963.78 | 5.55 | Y | badge aligned |
| DIVISLAB | SQ-only | 11:10:32 | — | 11:10:32 | 8458.43 | 8411.21 | 47.22 | Y* | no cons READY row |
| PNBHOUSING | SQ-only | 11:16:24 | — | 11:16:24 | 1093.15 | 1088.03 | 5.12 | Y* | 0 cons rows all day |
| JUBLFOOD | SQ-only | 12:05:40 | — | 12:05:40 | 464.57 | 462.90 | 1.67 | Y* | cons later WAIT only |
| PAYTM | SQ-only | 12:16:28 | — | 12:16:28 | 1421.99 | 1415.91 | 6.08 | Y* | cons never READY |
| ASHOKLEY | SQ-only | 12:25:35 | — | 12:25:35 | 175.28 | 174.48 | 0.80 | Y* | staleness grade **D!** / TS35 (SQ log was A) |
| APLAPOLLO | SQ-only | 13:06:36 | — | 13:06:36 | 1938.81 | 1926.73 | 12.08 | Y* | cons later WAIT only |
| LTM | SQ-only | 13:26:34 | — | 13:26:34 | 4678.66 | 4647.86 | 30.80 | Y* | 0 cons rows |
| MCX | late / other | 15:48:46 | — | 15:48:46 | 2631.86 | 2632.99 | 1.13 | N | after window; SL above entry |

\*Take=true on **staleness** at promote (live stock had it). Day-1 report’s “SQ never take-enabled” was wrong for live state — it was a **consistency-log coverage hole**: SQ runs *after* `consistency_rows` are collected, and finalize only updates rows already queued. SQ-only promotes never get a consistency READY/take row.

`card_visible=false` on several SQ rows is dwell telemetry; UI does **not** filter on it (renders by `trade_state`).

---

## Write-sequence pattern across all 12

| Class | Pattern today |
|---|---|
| Organic morning (3) | Cons+stale READY@09:45; badge lag ~5m (4m throttle) — **systemic for that throttle rule** |
| FORTIS organic | Cons = badge = stale @10:16 (aligned) |
| SQ-only (7) | SQ log ≈ badge READY ≈ stale READY (same second); **no** consistency READY |
| MCX | Badge+stale READY late; take=false |

---

## Implications before any SQ take-wiring fix

1. **Do not use badge first-READY as “when the trader saw the card.”** Use consistency (organic) or staleness/SQ log (SQ), or ideally a dedicated UI-facing event.
2. **Day-1 “SQ take never fired” needs reframing:** take *was* set on the live object at promote (`staleness.trade_take_enabled=true`); consistency simply never recorded those promotes. Any fix should decide whether to (a) append consistency rows post-SQ for new READY, and/or (b) ensure take remains true on subsequent polls (separate question — badge/cons still showed take=false later for SQ names that left READY).
3. Entry/SL for the 12 are now in `ready_take_entry_sl_20260803.csv`.
