# TWCTO Vajra — Futures Rating (User Guide)

In the app, open **Vajra Futures** from the left menu and click the **?** icon next to **Screen — Futures Rating**.

## What Vajra is

Vajra is a **discretionary trade operating system** for current-month F&O futures. It helps you:

1. Find **early transitions** on 30m bars (TPS)  
2. Time entries on 5m bars (EES / Entry State)  
3. **Validate** entries with a structured checklist  
4. **Track** open trades (Open Position) and **journal** closes (Closed Trades)  

It does **not** place broker orders. You execute in your terminal; Vajra scores, checks, and monitors.

## Page layout

| Section | Purpose |
|--------|---------|
| Screen — Futures Rating | Top 8 table + **more…** for full list |
| Open Position | Activated trades — lifecycle, health, close |
| Closed Trades | Square-offs this session with reasons |

## How the engine runs

| Stage | Timeframe | What happens |
|--------|-----------|--------------|
| Discovery | 30m | TPS + ECS + transition for ~200 symbols; every 5 min (9:30–15:00 IST) |
| Shortlist validation | 5m | Top TPS names get extra execution-structure checks |
| EES refresh | 5m | Executable Entry Score + Entry State on each run |
| Your workflow | — | ENTER → checklist → ACTIVATE |

The screen **auto-refreshes shortly after each scheduled run** (see **Updated** in the meta line).

## TPS vs EES vs ECS

- **TPS (Transition Potential Score, 30m)** — early discovery: reclaim, compression, momentum, shallow pullback, low extension. Does not require LONG A+.  
- **EES (Executable Entry Score, 5m)** — “Can I enter right now?” Timing, extension, reclaim quality. Refreshes every 5 minutes.  
- **ECS (Expansion Confirmation Score, 30m + 1hr)** — mature Vajra: structure, breakout, OBV, volume, trend.

Table sort: **TPS + EES + ECS** (highest combined first).

## Discovery table (top 8)

- **Status → Entry State band** — Status, TPS, EES, Entry State; second line = transition detail  
- **ECS, VWAP, Pullback, Extension** — context and risk  
- **Action** — ENTER workflow  

### Entry State (EES)

| EES | State | Meaning |
|-----|--------|---------|
| ≥ 75 | EXECUTABLE | Good 5m timing when TPS supports entry |
| 60–74 | PULLBACK | Prefer shallow pullback |
| 45–59 | WATCHLIST | Monitor; don’t chase |
| < 45 | AVOID | Extended — avoid chasing |

### Action buttons

- **ENTER** — TPS ≥ 52 and EES ≥ 65  
- **WAIT PULLBACK / WATCH / EXTENDED** — disabled (hover for reason)

## Telegram (optional)

Settings → Telegram ON → **Vajra ENTER alerts (Futures)**. One message per symbol per session when ENTER first becomes available.

## Trade Validation & Entry

### Step A
Symbol, direction, entry price, lots, entry time. **Cancel** closes the modal.

### Step B — Checklist
Structure + Market (automated from 5m; verify yourself). Psychology (**manual only**).

**ACTIVATE TRADE** is disabled if more than **70%** of Structure + Market checks are not PASS.

### ACTIVATE
Saves to **Vajra Futures → Open Position**. 5m monitoring while open. No broker execution.

## Open Position

- **Lifecycle:** Early Transition → Expansion → Consolidation / Rotation → Exhaustion → Breakdown Risk → Failed Structure  
- **Health 0–100:** Strong / Healthy / Weakening / High Risk / Failure Risk  
- **CLOSE TRADE** → Closed Trades  

## Recommended workflow

**Watch Updated → scan TPS+EES+ECS → ENTER → psychology → ACTIVATE → manage Open Position → CLOSE → review Closed Trades.**
