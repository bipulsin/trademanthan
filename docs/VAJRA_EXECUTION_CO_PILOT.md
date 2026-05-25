# Vajra Execution Co-Pilot

AI-assisted **discretionary** layer on top of the dynamic scanner. Does not place orders or issue blind BUY/SELL calls.

## Pipeline

```
Market Context → Sector (SSS) → Stable Top 3 → Co-Pilot enrich → Trader confirms → ENTER
```

## Workflow states (UI)

| State | Maps from | Meaning |
|-------|-----------|---------|
| WAIT | DISCOVERY / REJECT | Monitor only |
| PREPARE | ARMED | Approaching trigger |
| EXECUTABLE | EXECUTABLE | Conditional plan ready |
| ACTIVE | Open `vajra_discretionary_trade` | Position live |
| EXIT RISK | Weak structure / health | Thesis weakening |

Qualification v2 (`DISCOVERY` / `ARMED` / `EXECUTABLE`) remains on the backend for scoring and hysteresis.

## Modules

| File | Role |
|------|------|
| `market_context_engine.py` | Session market bias |
| `setup_classifier.py` | Setup type + workflow state + A+/A/B/C |
| `trade_plan_generator.py` | Conditional entry/stop/targets |
| `invalidation_monitor.py` | Thesis weakening signals |
| `execution_events.py` | PREPARE / EXECUTION / RISK alerts |
| `execution_co_pilot.py` | Overlay orchestrator |

## API

- `GET /vajra-futures/ratings` — `co_pilot.market_context`, `co_pilot.execution_events`, per-row `trade_plan`, `execution_workflow_state`, `quality_grade`
- `POST /vajra-futures/trades/validate-preview` — includes `trade_plan`

## Trader workflow

1. Read **Market** line (bias + conviction).
2. Use **Stability** toolbar + sticky Top 3.
3. Watch **WAIT → PREPARE → EXECUTABLE** pills and **S1–S3 / W1–W3** sector badges.
4. On ENTER, review **conditional trade plan** before activation.

See also: `SECTOR_STABLE_EXECUTION.md`, `STABLE_EXECUTION_MODE.md`.
