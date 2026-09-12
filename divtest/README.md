# divtest

MACD **Divergence + Histogram Flip** backtesting web app for [TradeWithCTO.com](https://www.tradewithcto.com). Fetches OHLC via Upstox API v3, stitches futures/cash month-by-month, runs the strategy, and appends results to local JSON files.

## Features

- Regular bullish/bearish MACD divergence detection + histogram flip confirmation
- Exits: opposite flip, ATR R-multiple stop/target, time-based (configurable; default flip **or** stop)
- Timeframes: 10min, 15min, 1hr
- Period: 6 or 12 calendar months (current month backwards)
- Incremental results: each instrument/timeframe/month written immediately; UI updates live
- Idempotent cache with **Force Refresh**
- Rate-limited Upstox client with 429 exponential backoff
- Optional HTTP Basic Auth + IP allowlist
- Demo mode for offline UI/strategy testing (`DIVTEST_DEMO_MODE=1`)

## Project layout

```
divtest/
  server.js                 # Express API + static UI
  lib/
    upstoxClient.js         # Auth, throttle, instrument master, candles
    strategyEngine.js       # MACD, divergence, flip, entries/exits
    backtestRunner.js       # Month-by-month orchestration
    resultsStore.js         # JSON persistence + analysis helpers
    config.js               # Defaults + runtime settings
  public/
    divtest.html
    divtest.css
    divtest.js
  data/{instrument}/{timeframe}/{YYYY-MM}.json
  cache/complete.json       # Instrument master (daily refresh)
  cache/upstox_token.json   # OAuth token (mode 0600)
```

## Quick start (local)

```bash
cd divtest
cp .env.example .env
npm install

# Offline demo (no Upstox token required)
DIVTEST_DEMO_MODE=1 npm start
# open http://localhost:3847/divtest.html
```

### Upstox setup

1. Create an app at [Upstox Developer](https://account.upstox.com/developer/apps).
2. Set Redirect URI to match `UPSTOX_REDIRECT_URI` (e.g. `https://your.domain/divtest/api/upstox/callback` or local callback).
3. Put credentials in `.env`:

```env
UPSTOX_API_KEY=...
UPSTOX_API_SECRET=...
UPSTOX_ACCESS_TOKEN=...   # optional if you complete browser OAuth
UPSTOX_REDIRECT_URI=https://your.domain/divtest/api/upstox/callback
DIVTEST_DEMO_MODE=0
DIVTEST_PORT=3847
DIVTEST_BASIC_AUTH_USER=divtest
DIVTEST_BASIC_AUTH_PASS=choose-a-strong-password
```

4. Start the server: `npm start`
5. If you do not have a long-lived access token, open `GET /api/upstox/login` → follow `url` → token is saved under `cache/upstox_token.json` (never exposed to the browser beyond a masked preview).

> Upstox does **not** support silent refresh without a browser auth code. On 401 the server reloads the token file/env. Re-run OAuth when the token expires.

### API

| Method | Path | Description |
|--------|------|-------------|
| POST | `/api/run-backtest` | Start job `{ instruments, periodMonths, forceRefresh, settings }` |
| GET | `/api/jobs/:id` | Job status + recent logs |
| GET | `/api/jobs/:id/events` | SSE progress/log stream |
| GET | `/api/results` | Filtered/paginated trades + analysis |
| GET | `/api/instruments` | Stored instruments + resolve `?q=RELIANCE` |
| GET/POST | `/api/settings` | Strategy parameters |
| GET | `/api/upstox/login` | OAuth dialog URL |
| GET | `/api/upstox/callback` | OAuth redirect handler |
| GET | `/api/health` | Liveness |

### Tests

```bash
npm test
```

## Public deployment (Nginx + PM2 + SSL)

### 1. Process manager (PM2)

```bash
cd /var/www/divtest   # or your deploy path
npm install --omit=dev
cp .env.example .env  # fill secrets
pm2 start server.js --name divtest
pm2 save
pm2 startup
```

### 2. Nginx reverse proxy

```nginx
# /etc/nginx/sites-available/divtest
server {
    listen 80;
    server_name divtest.tradewithcto.com;  # or path on main host

    location /divtest/ {
        proxy_pass http://127.0.0.1:3847/;
        proxy_http_version 1.1;
        proxy_set_header Host $host;
        proxy_set_header X-Real-IP $remote_addr;
        proxy_set_header X-Forwarded-For $proxy_add_x_forwarded_for;
        proxy_set_header X-Forwarded-Proto $scheme;
        # SSE
        proxy_buffering off;
        proxy_read_timeout 3600s;
    }
}
```

Enable + reload:

```bash
sudo ln -s /etc/nginx/sites-available/divtest /etc/nginx/sites-enabled/
sudo nginx -t && sudo systemctl reload nginx
```

### 3. TLS (Certbot)

```bash
sudo apt install certbot python3-certbot-nginx
sudo certbot --nginx -d divtest.tradewithcto.com
```

### 4. Abuse protection (required for public hosts)

**Basic auth** (app-level, already built-in):

```env
DIVTEST_BASIC_AUTH_USER=divtest
DIVTEST_BASIC_AUTH_PASS=long-random-secret
```

**Or Nginx basic auth:**

```bash
sudo apt install apache2-utils
sudo htpasswd -c /etc/nginx/.htpasswd_divtest divtest
```

```nginx
location /divtest/ {
    auth_basic "divtest";
    auth_basic_user_file /etc/nginx/.htpasswd_divtest;
    proxy_pass http://127.0.0.1:3847/;
    # ...same proxy headers as above
}
```

**IP allowlist** (app-level):

```env
DIVTEST_IP_ALLOWLIST=203.0.113.10,198.51.100.7
```

Never put `UPSTOX_ACCESS_TOKEN` in frontend code, git, or public env dumps.

## Data model

Each month file:

```json
{
  "instrument": "RELIANCE",
  "timeframe": "15min",
  "month": "2026-08",
  "source": "future:RELIANCE25AUGFUT",
  "instrument_key": "NSE_FO|…",
  "candles": 1200,
  "notes": [],
  "trades": [
    {
      "instrument": "RELIANCE",
      "timeframe": "15min",
      "entry_datetime": "…",
      "entry_price": 0,
      "side": "LONG",
      "exit_datetime": "…",
      "exit_price": 0,
      "qty_per_lot": 250,
      "pnl_inr": 0,
      "divergence_type": "bullish",
      "exit_reason": "opposite_flip"
    }
  ]
}
```

Re-running the same instrument **appends** new months/trades; already-processed months are skipped unless Force Refresh is checked.

## Instrument resolution

1. Download/cache Upstox `complete.json.gz` daily.
2. For each calendar month (newest first): prefer the monthly **future** active in that month.
3. If futures data is missing → fall back to **NSE_EQ / cash**.
4. Commodities (MCX): stitch historical futures month-by-month; gaps are logged in the live console.

## License

UNLICENSED — internal TradeWithCTO tool.
