# Chart choppiness validation — 2026-07-31

Retro run of `backend.services.chart_choppiness` (Condition A body-cross + Condition B EMA5/VWAP) on 10m session bars.

**Lookback fix (1-Aug):** rolling Condition A window never looks into bootstrap bars 0–3, so same-direction bootstrap crosses cannot flip A ON immediately after bootstrap.

## Sanity table (session end, post-fix)

| symbol | boot both-dir | body crosses | EMA5×VWAP | A end | B end | **combined** | A ON bars | toggles |
|--------|---------------|--------------|-----------|-------|-------|--------------|-----------|---------|
| APLAPOLLO | no (2 bearish) | 10 | 4 | OFF | ON | **CHOPPY** | 19/37 (51%) | 1 |
| HYUNDAI | no | 1 | 0 | OFF | OFF | ok | **0**/37 | 0 |
| ASHOKLEY | no (2 bullish) | 2 | 0 | OFF | OFF | ok | **0**/37 | 0 |
| BAJAJFINSV | no | 1 | 0 | OFF | OFF | ok | **0**/37 | 0 |
| KALYANKJIL | **yes** | 6 | 3 | OFF | ON | **CHOPPY** | 17/37 (46%) | 3 |
| SWIGGY | **yes** | 3 | 1 | OFF | OFF | ok | 10/37 | 4 |
| BAJFINANCE | no | 1 | 0 | OFF | OFF | ok | **0**/37 | 0 |
| BOSCHLTD | no | 1 | 1 | OFF | OFF | ok | **0**/37 | 0 |
| ETERNAL | no | 1 | 1 | OFF | OFF | ok | **0**/37 | 0 |
| PREMIERENE | no (3 bullish) | 3 | 2 | OFF | OFF | ok | **0**/37 | 0 |
| WIPRO | **yes** | 3 | 1 | OFF | OFF | ok | 10/37 | 4 |

ASHOKLEY / PREMIERENE false ~3-bar ON flip: **gone**. APLAPOLLO / KALYANKJIL still CHOPPY. Clean names still OFF.

Full timelines: `evidence.json`.

## Re-run

```bash
python -m backend.scripts.validate_chart_choppiness_20260731
```
