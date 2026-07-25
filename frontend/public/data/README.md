# Public diagnostic data feeds

| File | Page | API |
|---|---|---|
| `top10-vs-ready-now.json` | `/top10-vs-ready-now.html` | `GET /scan/diagnostics/top10-vs-ready-now` |

Seed JSON is a static fallback. Live sessions are served from DB via the API (5‑minute cache). To merge a day into the seed:

```bash
START=YYYY-MM-DD END=YYYY-MM-DD WRITE_PUBLIC_SEED=1 \
  PYTHONPATH=/app /opt/venv/bin/python scripts/top10_vs_ready_now_20260720_24.py
```
