# TWCTO Vajra — Futures Rating (User Guide)

In the app, click the **?** icon next to the section title to open this guide.

## What Vajra is

Vajra is a **discretionary trade operating system** for current-month F&O futures. It helps you:

1. Find **early transitions** before trends are obvious  
2. **Validate** entries with a structured checklist  
3. **Track** open trades with lifecycle and health  
4. **Journal** closed trades with reasons  

It does **not** place broker orders. You execute in your terminal; Vajra scores, checks, and monitors.

## Architecture at a glance

| Stage | Timeframe | What happens |
|--------|-----------|--------------|
| Discovery | 30m | TPS + ECS computed for ~200 symbols; list sorted by **TPS (high → low)** |
| Shortlist validation | 5m | Top names get extra execution-structure checks |
| Your workflow | — | ENTER → checklist → ACTIVATE |
| Management | 5m refresh | Lifecycle, health, alerts while trade is open |

## TPS vs ECS

- **TPS (Transition Potential Score)** — early discovery: reclaim, compression, momentum, shallow pullback, low extension. Does not require LONG A+ or structure PASS.  
- **ECS (Expansion Confirmation Score)** — mature Vajra logic: structure, breakout, OBV, volume, trend. Use for continuation context.

## Discovery table

Top **8** symbols by TPS. **ENTER** opens validation (no auto-trade).

## Entry checklist (summary)

### Section A
Symbol, direction, entry price, lots, entry time.

### Section B — Structure (5m)
Auto-checked where noted; psychology items are **manual only**.

**Structure:** VWAP/EMA reclaim, Hilega-Milega, shallow pullback, no exhaustion, healthy candles, not into major level, strong reclaim close.

**Market:** NIFTY/BankNIFTY alignment, sector movers, volume > 1.2× average, not extended from VWAP.

**Psychology:** Not FOMO, risk accepted, not revenge, comfortable exit, structure valid after pullback.

### Section C
Read-only: TPS, ECS, extension risk, pullback quality, phase, trend.

### Section D
Warnings for extension, VWAP distance, vertical move, levels, weak reclaim, deep pullback.

### ACTIVATE
Saves to **Running order** (Daily Futures) or **Open Positions** (Smart Futures) as “Vajra managed”.

## Running cockpit

- **Lifecycle:** Early Transition → Expansion → Consolidation / Rotation → Exhaustion → Breakdown Risk → Failed Structure  
- **Health 0–100:** Strong / Healthy / Weakening / High Risk / Failure Risk  
- **Alerts:** Interpretation only (not buy/sell)  
- **CLOSE TRADE:** Exit price + reasons → journal  

## Recommended workflow

**Scan by TPS → ENTER → tick psychology → ACTIVATE → monitor Running → CLOSE with reasons → review journal.**
