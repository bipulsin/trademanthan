# AGENTS.md

## Cursor Cloud specific instructions

TradeManthan is a **FastAPI** backend (`backend/`, entry `main:app`) serving a **static
vanilla JS/HTML frontend** (`frontend/public/`, no build step). Production deploy (Docker +
nginx) lives in the separate `bipulsin/twcto_docker` repo; in the cloud dev VM we run the
backend and frontend directly.

### Environment already provisioned in the VM snapshot
- **Python 3.11** (from the deadsnakes PPA). The repo pins `numpy==1.24.3` / `pandas==2.0.3`
  which have no wheels for Python 3.12, so the venv MUST use 3.11. The venv lives at `.venv`.
- **PostgreSQL 16** with a `trademanthan` role/db matching the code's default
  `DATABASE_URL` (`postgresql://trademanthan:trademanthan123@localhost/trademanthan`).
- Root `.env` (gitignored) with dev config (`ENVIRONMENT=development`, dev `SECRET_KEY`).
  Not required for the DB to work — the code's built-in default DB URL already points at the
  local Postgres above.

### The update script installs deps; you still need to start services yourself.
The update script (venv + pip deps) runs automatically on VM startup. It does **not** start
Postgres or the app.

**Start PostgreSQL** (needed before the backend; not auto-started, no systemd in the VM):
```bash
sudo pg_ctlcluster 16 main start
```

**Run the backend** (dev, from repo root):
```bash
.venv/bin/python -m uvicorn main:app --host 0.0.0.0 --port 8000
```
Health: `curl -s http://localhost:8000/health`. Interactive API docs at `/docs`.

**Serve the frontend** (static; the JS auto-targets `http://localhost:8000` when the page
host is `localhost`, and the backend CORS allowlist already includes `http://localhost:3000`,
so serve it on port 3000):
```bash
cd frontend/public && /workspace/.venv/bin/python -m http.server 3000
```

### Non-obvious gotchas
- **`pandas-ta==0.3.14b0` was wiped from PyPI** (project went private May 2025). It is
  installed from the `MerlinR/Pandas-ta-fork` mirror with `--no-deps` (the fork's metadata
  wrongly pins `numpy==1.26.4`; `--no-deps` lets the repo's `numpy==1.24.3` win). The
  requirements install therefore filters out the `pandas-ta` line and installs the fork
  separately. Handled by the update script.
- **`setuptools` must be `<81`**: `pandas_ta` imports `pkg_resources`, removed in setuptools
  81+. The update script pins it; don't upgrade setuptools past 80.x in the venv.
- **Startup schema migrations partially fail on an empty DB** — you'll see
  `startup schema migration failed: ... UndefinedTable ... arbitrage_master`. This is
  expected: the raw-SQL migration block seeds `car_nifty200 FROM arbitrage_master`, but
  `arbitrage_master` is populated at runtime by a live-data scheduler (Upstox/Dhan) that
  needs credentials, so it doesn't exist on a fresh DB. The failure aborts that one
  transaction, so several raw-SQL-only tables (e.g. `daily_checklist`,
  `relative_strength_snapshot`, `smart_futures_daily`) are **not** created. The 24
  SQLAlchemy-model tables (`users`, `strategies`, `brokers`, `iron_condor_*`, …) are created
  fine via `Base.metadata.create_all` and the app starts normally.
- **Live market data / trading needs Upstox creds** (`UPSTOX_API_KEY`/`UPSTOX_API_SECRET` +
  an OAuth token). Without them, scanners/dashboards that call Upstox log warnings and return
  empty/errored data — model-backed CRUD endpoints (e.g. `/strategy/`) still work offline.
- **Auth is Google-OAuth only** (no password signup). For offline testing, insert a `users`
  row directly, or use endpoints that don't depend on `get_current_user`.
- **Redis and Celery are in `requirements.txt` but unused** (no client/worker is
  instantiated) — you do not need to run them.

### Tests / lint
- No lint tooling is configured in this repo (no ruff/flake8/black config).
- Tests are pytest files at the repo root and under `backend/` (`test_*.py`). Run e.g.
  `PYTHONPATH=. .venv/bin/python -m pytest test_trade_quality.py -q`. Many top-level
  `test_*` files are pure-logic and run offline; scanner/live-data ones may need Upstox.
- Offline scanner harness (core algo, no API): `PYTHONPATH=. .venv/bin/python
  test_premkt_scanner.py --sample` (see `README_TESTING.md`).
