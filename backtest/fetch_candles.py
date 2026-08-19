# TEMP: Upstox data fetch for backtest — not for live trading.
"""Fetch 15-minute current-month futures candles for the HA Momentum backtest."""
from __future__ import annotations

import json
import logging
import sys
import time
from datetime import date, datetime, timedelta
from pathlib import Path
from typing import Any, Dict, List, Optional, Tuple
from urllib.parse import quote

import requests
from sqlalchemy import text

ROOT = Path(__file__).resolve().parents[1]
if str(ROOT) not in sys.path:
    sys.path.insert(0, str(ROOT))

BACKTEST_FROM = "2026-07-17"
BACKTEST_TO = "2026-08-19"
CANDLE_INTERVAL = "15minute"
SLIPPAGE_PCT = 0.001
MAX_SL_RS = 5000
RR_T1 = 2
RR_T2 = 3
LARGE_CANDLE_THRESHOLD_PCT = 0.3
FORCED_EXIT_TIME = "15:00"

CANDLE_DIR = ROOT / "data" / "candles"
LOG_DIR = ROOT / "logs"
SLEEP_BETWEEN_CALLS = 0.12
BATCH_SIZE = 50
BATCH_PAUSE = 2.0
CHUNK_DAYS = 28

logger = logging.getLogger("ha_fetch")


def _setup_log() -> None:
    LOG_DIR.mkdir(parents=True, exist_ok=True)
    logging.basicConfig(
        level=logging.INFO,
        format="%(asctime)s %(levelname)s %(message)s",
        handlers=[
            logging.FileHandler(LOG_DIR / "fetch.log"),
            logging.StreamHandler(sys.stdout),
        ],
    )


def _date_chunks(start: date, end: date) -> List[Tuple[date, date]]:
    chunks: List[Tuple[date, date]] = []
    cur = start
    while cur <= end:
        nxt = min(cur + timedelta(days=CHUNK_DAYS - 1), end)
        chunks.append((cur, nxt))
        cur = nxt + timedelta(days=1)
    return chunks


def load_universe() -> List[Dict[str, str]]:
    from backend.database import SessionLocal

    if SessionLocal is None:
        raise RuntimeError("database not available")
    db = SessionLocal()
    try:
        rows = db.execute(
            text(
                """
                SELECT TRIM(stock) AS stock,
                       NULLIF(TRIM(currmth_future_symbol), '') AS fut_sym,
                       NULLIF(TRIM(currmth_future_instrument_key), '') AS ikey
                FROM arbitrage_master
                WHERE currmth_future_instrument_key IS NOT NULL
                  AND TRIM(currmth_future_instrument_key) <> ''
                ORDER BY stock
                """
            )
        ).mappings().all()
        return [dict(r) for r in rows]
    finally:
        db.close()


def _cache_path(symbol: str) -> Path:
    safe = "".join(ch if ch.isalnum() or ch in "-_" else "_" for ch in symbol.upper())
    return CANDLE_DIR / f"{safe}_15min.json"


def cache_ok(symbol: str) -> bool:
    p = _cache_path(symbol)
    return p.exists() and p.stat().st_size > 20


def _headers() -> Dict[str, str]:
    from backend.services.upstox_service import upstox_service

    return upstox_service.get_headers()


def fetch_chunk(instrument_key: str, to_d: date, from_d: date) -> List[Dict[str, Any]]:
    # TEMP: Upstox data fetch for backtest — not for live trading.
    rows = _http_candles(instrument_key, CANDLE_INTERVAL, to_d, from_d)
    if rows:
        return rows
    raw_1m = _http_candles(instrument_key, "1minute", to_d, from_d)
    if not raw_1m:
        return []
    from backend.services.upstox_service import _aggregate_1m_to_n_minute

    return _aggregate_1m_to_n_minute(raw_1m, 15)


def _http_candles(instrument_key: str, interval: str, to_d: date, from_d: date) -> List[Dict[str, Any]]:
    key_enc = quote(instrument_key, safe="")
    url = (
        "https://api.upstox.com/v2/historical-candle/"
        f"{key_enc}/{interval}/{to_d.isoformat()}/{from_d.isoformat()}"
    )
    wait = 5.0
    for attempt in range(4):
        time.sleep(SLEEP_BETWEEN_CALLS)
        try:
            resp = requests.get(url, headers=_headers(), timeout=30)
        except requests.RequestException as exc:
            logger.warning("request error %s: %s", instrument_key, exc)
            time.sleep(wait)
            wait *= 2
            continue
        if resp.status_code == 429:
            logger.warning("HTTP 429 for %s — backoff %.0fs", instrument_key, wait)
            time.sleep(wait)
            wait *= 2
            continue
        if resp.status_code != 200:
            logger.warning("HTTP %s for %s: %s", resp.status_code, instrument_key, resp.text[:200])
            return []
        body = resp.json()
        if body.get("status") != "success":
            logger.warning("no data %s: %s", instrument_key, body.get("message") or body.get("status"))
            return []
        raw = (body.get("data") or {}).get("candles") or []
        rows: List[Dict[str, Any]] = []
        for c in raw:
            if not isinstance(c, (list, tuple)) or len(c) < 6:
                continue
            rows.append(
                {
                    "timestamp": c[0],
                    "open": float(c[1]),
                    "high": float(c[2]),
                    "low": float(c[3]),
                    "close": float(c[4]),
                    "volume": float(c[5]),
                }
            )
        return rows
    logger.warning("gave up fetching %s", instrument_key)
    return []


def fetch_symbol(symbol: str, instrument_key: str, from_d: date, to_d: date) -> int:
    if cache_ok(symbol):
        return json.loads(_cache_path(symbol).read_text()).get("count") or 0
    warmup_from = from_d - timedelta(days=14)
    merged: Dict[str, Dict[str, Any]] = {}
    for a, b in _date_chunks(warmup_from, to_d):
        for row in fetch_chunk(instrument_key, b, a):
            merged[str(row["timestamp"])] = row
    candles = sorted(merged.values(), key=lambda r: r["timestamp"])
    CANDLE_DIR.mkdir(parents=True, exist_ok=True)
    payload = {
        "symbol": symbol,
        "instrument_key": instrument_key,
        "from": from_d.isoformat(),
        "to": to_d.isoformat(),
        "interval": CANDLE_INTERVAL,
        "count": len(candles),
        "candles": candles,
        "fetched_at": datetime.now().isoformat(),
    }
    _cache_path(symbol).write_text(json.dumps(payload), encoding="utf-8")
    if not candles:
        logger.warning("empty candles for %s (%s)", symbol, instrument_key)
    return len(candles)


def load_cached(symbol: str) -> Optional[Dict[str, Any]]:
    p = _cache_path(symbol)
    if not p.exists():
        return None
    try:
        return json.loads(p.read_text(encoding="utf-8"))
    except json.JSONDecodeError:
        return None


def main() -> None:
    _setup_log()
    from_d = date.fromisoformat(BACKTEST_FROM)
    to_d = date.fromisoformat(BACKTEST_TO)
    universe = load_universe()
    logger.info("universe %s symbols  %s → %s", len(universe), from_d, to_d)
    for i, row in enumerate(universe, 1):
        sym = row["stock"]
        ikey = row["ikey"]
        try:
            n = fetch_symbol(sym, ikey, from_d, to_d)
            logger.info("[%s/%s] %s %s bars", i, len(universe), sym, n)
        except Exception as exc:
            logger.exception("fetch failed %s: %s", sym, exc)
        if i % BATCH_SIZE == 0:
            logger.info("batch pause after %s symbols", i)
            time.sleep(BATCH_PAUSE)


if __name__ == "__main__":
    main()
