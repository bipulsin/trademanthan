# Sector-Integrated Stable Execution (Vajra Futures)

Overlay on the existing dynamic scanner — does **not** replace DISCOVERY / ARMED / EXECUTABLE or the 5m rating job.

## Workflow

1. **Dashboard** — sector movers + persistence heatmap (`persistence_heatmap` on `/scan/dashboard-sector-movers`).
2. **Discovery window** — 9:15–10:00 IST (`discovery_window` in API).
3. **Vajra ratings** — each row gets sector name, strength %, SSS, persistence minutes, status, trade badge.
4. **Execution window** — from 10:00 IST; freeze Top 3 enabled (also 9:20–9:45 early freeze).
5. **Stable Execution** — sticky Top 3, Focus Mode, sector-weighted rank (ESS + SSS boost/penalty).

## Metrics

| Field | Meaning |
|-------|---------|
| **SSS** | Sector Stability Score 0–100 (breadth, persistence, vol stability, leadership continuity, vs Nifty) |
| **sector_tag** | `TOP_SECTOR`, `PERSISTENT_LEADER`, `ROTATIONAL`, `WEAK_SECTOR` |
| **sector_trade_badge** | `SECTOR_ALIGNED`, `SECTOR_CONTRADICTION`, `SECTOR_CONFIRMED_WEAKNESS` |

## API

- `GET /vajra-futures/ratings` — rows enriched; `stable_execution.sector_heatmap`, workflow fields.
- `GET /vajra-futures/sector-persistence` — heatmap only.
- `GET /scan/dashboard-sector-movers` — includes `persistence_heatmap` after first poll.

## Code

- `backend/services/vajra/sector_intelligence.py` — persistence tracker + SSS + row enrichment.
- `backend/services/vajra/stable_execution.py` — sector-weighted `_rank_score`, overlay hook.
- `backend/services/vajra/session_window.py` — discovery / execution windows.

Persistence state is in-memory per IST session date (rebuilt from sector mover polls).
