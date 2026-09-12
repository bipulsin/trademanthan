# divtest (FastAPI)

MACD **Divergence + Histogram Flip** backtester integrated into TradeWithCTO’s FastAPI + Docker stack.

## URLs (production)

- UI: `https://www.tradewithcto.com/divtest.html`
- API: `https://www.tradewithcto.com/api/divtest/...`

## Layout

```
backend/services/divtest/     # strategy, runner, instruments, candles, results
backend/routers/divtest.py    # /api/divtest routes
frontend/public/divtest.*     # static UI
data/divtest/{SYM}/{tf}/{YYYY-MM}.json
```

The standalone Node app under `divtest/` is **deprecated** (kept for reference only). Use this FastAPI path.

## Behaviour

- Period: 6 or 12 calendar months (current month backwards)
- Timeframes: 10min, 15min, 1hr
- Instrument resolution: Upstox complete master → futures for month, else cash
- Idempotent month JSON cache; **Force Refresh** re-runs
- Live job progress via SSE `/api/divtest/jobs/{id}/events`
- Demo mode: set `DIVTEST_DEMO_MODE=1` (synthetic candles, no Upstox)

## Local

```bash
# with the normal backend venv
DIVTEST_DEMO_MODE=1 uvicorn backend.main:app --reload --port 8000
# open http://localhost:8000/divtest.html  (or nginx static mapping)
```

Production uses the paperclip Docker deploy (`./scripts/trigger-paperclip-deploy.sh`); Upstox token comes from the existing app token storage.
