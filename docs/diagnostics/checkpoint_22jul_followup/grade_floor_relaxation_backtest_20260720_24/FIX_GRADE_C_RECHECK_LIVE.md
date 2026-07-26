# FIX: Variant A grade-C → READY(RECHECK) LIVE

**Shipped:** 2026-07-26  
**Status:** ROLLED BACK (live off) — 2026-07-26  
**Backtest basis:** week 2026-07-20→24 · n=69 · WR 49.3% · total **+22.52R** · verdict was `PROMISING_SHADOW_FIRST`

## Follow-up: live rollback (2026-07-26)

Trader requested restore of **pre-change / EOD 2026-07-24** grade-C behavior after chart-verified backtest errors:

| Symbol | Issue |
|---|---|
| MAZDOCK | Wrong direction |
| HCLTECH | Mistimed |
| PRESTIGE | Mistimed |

**Action:** set `GRADE_C_RECHECK_LIVE=0` on paperclip `.env` + `docker compose up -d --force-recreate app` (env-only; code path kept).

| Still true after rollback | Detail |
|---|---|
| Live gating | Plain non-stretch `C` hard-blocked from READY-family (not `READY(RECHECK)`) |
| Shadow logging | `grade_c_recheck_would_apply` / related `inputs` keys still populate when plain C would have been the path |
| Stretch / D | `C!` / `D` still blocked |
| A/B | Still can reach READY when other gates pass |
| Part 1 ATR-suppress | `ATR_READY_SUPPRESS_LIVE` unchanged (default on) |

Historical go-live record below is retained for audit.

## Explicit decision (historical go-live)

Backtest recommended **shadow-first**. Trader explicitly chose **go-live without further shadow** for Variant A only (plain non-stretch `C` → `READY(RECHECK)`). Variant B (stretch softening) remains **NO_GO** and was not shipped.

## What shipped

| Item | Detail |
|---|---|
| Rule | Plain `"C"` (non-stretch) qualifies for **READY(RECHECK)** only — same tier as ADX 20–25. Does **not** unlock full READY. |
| Still blocked | `D`, any stretch `!` (`C!`, `B!`, …) |
| Untouched | Rank/promotion, ATR-suppress Part 1, stretch softening (Variant B) |
| Flag | `GRADE_C_RECHECK_LIVE` (env, default **on** / `"1"`) |
| Shadow | `kavach_ready_consistency_log.inputs` keys below |
| Monitor | `scripts/monitor_grade_c_recheck_daily.py` |

## Flag / rollback (no code redeploy)

```bash
# Disable instantly (match ATR_READY_SUPPRESS_LIVE pattern):
# In twcto compose env (or container env), set:
GRADE_C_RECHECK_LIVE=0
# then recreate/restart app (no image rebuild required if env-only):
cd /home/ubuntu/twcto && docker compose up -d --force-recreate app
```

Re-enable: `GRADE_C_RECHECK_LIVE=1` (or unset — default is on).

## Shadow field keys (`inputs`)

| Key | Meaning |
|---|---|
| `grade_c_recheck_path` | Live C-path applied and FSM reached READY-family at compute time |
| `grade_c_recheck_would_apply` | Plain non-stretch C candidate (logged even when flag off) |
| `grade_c_recheck_live_enabled` | Flag snapshot at compute |
| `grade_c_recheck_grade` | Normalized grade string (e.g. `C`) |

## Monitoring

```bash
./scripts/paperclip-ssh.sh 'cd /home/ubuntu/twcto && docker compose exec -T app bash -lc \
  "PYTHONPATH=/app /opt/venv/bin/python /app/scripts/monitor_grade_c_recheck_daily.py"'
```

## ATR-suppress layering

Confirmed: Part 1 `ATR_READY_SUPPRESS_LIVE` still runs **after** FSM on READY-family (including grade-C `READY(RECHECK)`). ATR≥85% + not progressing → display `WATCHING` as before.

## Checklist (go-live)

- [x] Plain C → READY(RECHECK); C! / D still BLOCKED
- [x] Feature flag default on; flip-off via env
- [x] Shadow fields in consistency log
- [x] Daily monitor script
- [x] ATR-suppress still layers
- [x] Commit + push + paperclip rebuild deploy
- [x] Health check

## Checklist (rollback 2026-07-26)

- [x] `GRADE_C_RECHECK_LIVE=0` on paperclip `.env` + app recreate
- [x] Plain C → BLOCKED (not READY(RECHECK)); shadow `would_apply=true`
- [x] C! / D still BLOCKED; B/A still READY when gates pass
- [x] `ATR_READY_SUPPRESS_LIVE` still on (default); suppress probe → WATCHING
- [x] Health OK; no code redeploy required for flag-off

## Code

- `backend/services/daily_checklist_trade_state.py` — `_is_plain_c_grade`, `grade_c_recheck_live_enabled`, FSM gate + return/log fields
- `backend/test_daily_checklist_trade_state.py` — Variant A + ATR layering tests
- `scripts/monitor_grade_c_recheck_daily.py`
