#!/usr/bin/env bash
# Public GET — confirms Daily Futures ChartInk route is reachable (no auth).
# For a full POST test, use curl with the webhook secret (do not commit secrets).
set -e
BASE="${1:-https://www.tradewithcto.com}"
echo "GET $BASE/api/daily-futures/webhook/chartink/ping"
curl -sS --max-time 15 "$BASE/api/daily-futures/webhook/chartink/ping" | head -c 2000
echo
echo "GET $BASE/daily-futures/webhook/chartink/ping (same JSON if nginx proxies both)"
curl -sS --max-time 15 "$BASE/daily-futures/webhook/chartink/ping" | head -c 2000
echo
