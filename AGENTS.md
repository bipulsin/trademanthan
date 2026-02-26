# AGENTS.md

## Cursor Cloud specific instructions

### Project overview

TradeManthan (Tradentical) is a professional algorithmic trading platform for the Indian stock market. It consists of a **FastAPI backend** (the main service), a **static HTML/JS frontend** (no build step), and an optional standalone **SuperTrend Bitcoin strategy** under `algos/`.

### Running the backend (dev mode)

```bash
cd /workspace
DATABASE_URL="sqlite:///./trademanthan.db" ENVIRONMENT=development \
  python3 -m uvicorn backend.main:app --host 0.0.0.0 --port 8000 --reload
```

- The `.env` file at `backend/.env` (copied from `backend/env.example`) configures SQLite for local dev — no PostgreSQL required.
- `pydantic-settings` is a required dependency not listed in `backend/requirements.txt`; the update script installs it separately.
- `pandas==2.0.3` and `numpy==1.24.3` pinned in `requirements.txt` have no Python 3.12 wheels; the update script installs latest compatible versions instead.
- Swagger docs are available at `http://localhost:8000/docs`.

### Key gotchas

- **No tests exist** in the repository. `pytest` collects 0 items. The `algos/tests/` directory contains only an empty `__init__.py`.
- **No linter config** exists. `flake8` can be run manually: `python3 -m flake8 backend/ --select=E9,F63,F7,F82`.
- **Startup warnings are normal**: expect warnings about missing Upstox token, missing instruments file, and strategy import errors — these relate to external API integrations not available in local dev.
- **Frontend** is static HTML/CSS/JS in `frontend/public/`; no build step is needed. In production it is served by Nginx.
- **Logging** goes to `logs/trademanthan.log` (not stdout), created automatically on startup.
- The app creates a SQLite database file `trademanthan.db` in the working directory on first run.

### Testing core functionality

The scan webhook endpoint is the core feature. Test it with:

```bash
curl -X POST http://localhost:8000/scan/chartink-webhook \
  -H "Content-Type: application/json" \
  -d '{"scan_name": "test", "stocks": "RELIANCE,TCS", "triggered_at": "2026-01-01 10:00:00"}'
```

Other useful endpoints: `GET /health`, `GET /scan/health`, `GET /scan/scheduler-status`.
