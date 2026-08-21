# Kavach BT Checkpoint — 22-Aug-2026

**Status:** Framework shipped (research-only). Run `scripts/run_kavach_bt_checkpoint.py` to populate tables and refresh this file with live cohort numbers.

## Scope

| Module | Topic | Live impact |
|--------|--------|-------------|
| BT-1 | Pullback v2 (dual VWAP+EMA10 reset; EMA5 increment; ≥5 hard-block display) | None (live flip deferred) |
| BT-2 | Resistance confluence ±0.2% pivot zone | Warning only |
| BT-3 | Exit A baseline EMA vs B 2R dynamic trail vs C actual | Research compare |
| BT-4 | Garuda MATCH / NO_MATCH / NOT_AVAILABLE | Shadow only |

## How to run

```bash
cd /Users/bipulsahay/TradeManthan
python3 scripts/run_kavach_bt_checkpoint.py --from 2026-07-22 --to 2026-08-21
```

Writes:

- DB: `bt_checkpoint_trade_detail`, `bt_checkpoint_summary`, `bt_checkpoint_pullback_bars`
- `docs/diagnostics/KAVACH_BT_CHECKPOINT_22AUG2026.md` (this file, overwritten with results)
- CSV under `~/Downloads/` when available

## Dashboard

- Page: `/kavach-bt-checkpoint.html` (admin nav)
- API: `/api/kavach-bt-checkpoint/summary`, `/trades`, `/export.csv`

## TradingView

- Additive Pine: `docs/diagnostics/TWCTO_Kavach_v3_1_checkpoint.txt`
- Base remains `TWCTO_Kavach_v3_0.txt`

## Rules (research tagging)

- Rule 15 = entry only
- Rule 24 Garuda = shadow
- Rule 25 Resistance = warning
- Rule 26 = dynamic trail candidate (Exit B)
- Rule 27 = 15:15 force in sims
- Rule 28 = PB≥5 hard-block display (live deferred)

## Recommendations

_Run the script to fill auto recommendations from trade_log cohorts._
