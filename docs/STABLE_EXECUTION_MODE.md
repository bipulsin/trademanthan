# Stable Execution Mode (Vajra Futures)

Overlay on the existing dynamic Vajra scanner. **Backend scoring still runs every 5 minutes** until 15:25 IST; the trader-facing experience prioritizes **focus and stability**.

## Philosophy

| Before | After (Stable Mode) |
|--------|---------------------|
| Latest momentum spike wins attention | **ESS** (Execution Stability Score) + sticky Top 3 |
| Top 8 rotates every 5 min | Leaders **locked** 15–60 min unless material change |
| Forced reshuffle | **Suggested rotation** only |
| Maximum opportunity exposure | **Execution quality under uncertainty** |

## Features

### 1. Execution Stability Score (ESS)

Computed in `backend/services/vajra/execution_stability.py`:

| Component | Weight |
|-----------|--------|
| Structure persistence | 30% |
| VWAP acceptance stability | 20% |
| Low noise movement | 15% |
| Trend continuation quality | 20% |
| Relative strength persistence | 15% |

Shown in the advanced row as **ESS** (separate from TPS / EES / ECS).

### 2. Sticky Top 3

- Default persistence: **30 minutes** (15 / 30 / 60 configurable per user).
- Replacement requires **+11** composite rank points (e.g. 82 → 84 does **not** swap; 82 → 93 can).
- Also requires **+15 ESS** on challenger when suggesting rotation.
- Incumbent dropped if score deteriorates by **≥12** or REJECT.

State stored in `vajra_stable_execution_state` (per user, per session).

### 3. Watchlist freeze (9:20–9:45 IST)

Trader clicks **Freeze Top 3 focus** to lock the current sticky list for the session. Backend keeps scoring; UI does not aggressively reshuffle the focus list.

### 4. Focus Mode

When enabled, the main table shows **only Sticky Top 3**. Banner: *“Trade selected setups. Ignore market noise.”*

### 5. Qualification smoothing

If a sticky leader was **EXECUTABLE** and scores dip slightly, display may hold EXECUTABLE with `qualification_decay` until conviction falls **≥12** points (buffer against 5m flicker).

### 6. UI controls (`vajrafutures.html`)

- **Stable Execution Mode** toggle (default ON for new sessions).
- **Focus Mode** toggle.
- **Sticky window** selector (15 / 30 / 60 min).
- **Freeze Top 3 focus** button.
- **Sticky Top 3 — Stable Execution** section in the table.

## API

| Method | Path | Purpose |
|--------|------|---------|
| GET | `/vajra-futures/ratings` | Includes `stable_execution` object + ESS on rows |
| GET | `/vajra-futures/stable-execution/state` | User prefs |
| PUT | `/vajra-futures/stable-execution/state` | Update toggles / persist minutes |
| POST | `/vajra-futures/stable-execution/freeze-focus` | `{ "stocks": ["A", "B", "C"] }` |

## Code map

| Module | Path |
|--------|------|
| ESS | `backend/services/vajra/execution_stability_score.py` |
| Sticky / freeze / overlay | `backend/services/vajra/stable_execution.py` |
| DDL | `backend/services/vajra/stable_execution_tables.py` |
| Router | `backend/routers/vajra_futures.py` |
| UI | `frontend/public/vajra-stable-execution.js` |

## Session interaction

Stable mode respects existing cutoffs:

- **15:25 IST** — screener snapshot frozen (no new persists).
- **15:30 IST** — ENTER disabled.

---

See also: `docs/HOW_VAJRA_FUTURES_WORKS.md`, `docs/VAJRA_QUALIFICATION_ARCHITECTURE_V2.md`.
