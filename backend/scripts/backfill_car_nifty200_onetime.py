#!/usr/bin/env python3
"""
One-time backfill for car_nifty200:
1. Set CHOLAFIN stock_ltp = 1514
2. For listed symbols, fill date_52weekhigh, last10daycummavg, signal, last_updated_date (and stock_ltp)
   using Yahoo Finance first, then Upstox if Yahoo fails. All API calls and outcomes logged to cargpt.log
"""
import sys
import os
from datetime import datetime, timedelta
from pathlib import Path

project_root = Path(__file__).resolve().parent.parent.parent
sys.path.insert(0, str(project_root))
os.chdir(project_root)

# Setup cargpt.log before other imports that might log
log_dir = project_root / "logs"
log_dir.mkdir(exist_ok=True)
cargpt_log_file = log_dir / "cargpt.log"

import logging
cargpt_logger = logging.getLogger("cargpt_backfill")
cargpt_logger.setLevel(logging.INFO)
cargpt_logger.handlers.clear()
fh = logging.FileHandler(cargpt_log_file, mode="a", encoding="utf-8")
fh.setLevel(logging.INFO)
fh.setFormatter(logging.Formatter("%(asctime)s - %(levelname)s - %(message)s", datefmt="%Y-%m-%d %H:%M:%S"))
cargpt_logger.addHandler(fh)
cargpt_logger.propagate = False

def log(msg: str):
    cargpt_logger.info(msg)
    print(msg)

# Symbols to backfill (CAR fields). CHOLAFIN also gets stock_ltp=1514 first.
BACKFILL_SYMBOLS = [
    "ABB", "ASTRAL", "AUROPHARMA", "COALINDIA", "CHOLAFIN", "GLENMARK", "LUPIN",
    "NTPC", "ONGC", "POWERINDIA", "TORNTPHARMA", "KEI", "BEL", "BHARATFORG",
]

YAHOO_BASE = "https://query1.finance.yahoo.com/v8/finance/chart"
UPSTOX_BASE = "https://api.upstox.com/v3"


def _is_valid_car_data(data: dict) -> bool:
    """Check that CAR result has enough to update DB: stock_ltp, date_52weekhigh, signal (last10daycummavg may be blank for partial)."""
    if not data:
        return False
    stock_ltp = data.get("stock_ltp")
    date_52 = data.get("date_52weekhigh")
    signal = data.get("signal")
    if stock_ltp is None:
        return False
    if date_52 is None or (isinstance(date_52, str) and not date_52.strip()):
        return False
    if signal is None or (isinstance(signal, str) and not signal.strip()):
        return False
    return True


def _date_52_to_str(v) -> str:
    """Normalize date_52weekhigh to YYYY-MM-DD string (handles pandas/numpy types from Yahoo)."""
    if v is None:
        return ""
    if isinstance(v, str):
        return v.strip()[:10]
    try:
        if hasattr(v, "strftime"):
            return v.strftime("%Y-%m-%d")
        if hasattr(v, "isoformat"):
            return str(v).split("T")[0][:10]
    except Exception:
        pass
    return str(v)[:10]


def _fetch_cholafin_nse_direct():
    """Fetch CHOLAFIN.NS directly from Yahoo chart API (same URL as user); return list of {date, close} or None."""
    import requests
    url = f"{YAHOO_BASE}/CHOLAFIN.NS?range=1y&interval=1d&includePrePost=false"
    try:
        headers = {"User-Agent": "Mozilla/5.0 (Windows NT 10.0; rv:91.0) Gecko/20100101 Firefox/91.0"}
        r = requests.get(url, headers=headers, timeout=30)
        if r.status_code != 200:
            return None
        data = r.json()
        if not data or "chart" not in data or "result" not in data.get("chart", {}) or not data["chart"]["result"]:
            return None
        result = data["chart"]["result"][0]
        timestamps = result.get("timestamp") or []
        indicators = result.get("indicators") or {}
        # Prefer quote[0].close; fallback to adjclose[0].adjclose (Yahoo sometimes returns only adjclose)
        closes = []
        quote_list = indicators.get("quote", [{}])
        if quote_list:
            closes = quote_list[0].get("close") or []
        if not closes and indicators.get("adjclose"):
            adj_list = indicators["adjclose"]
            if adj_list and isinstance(adj_list[0], dict) and "adjclose" in adj_list[0]:
                closes = adj_list[0]["adjclose"] or []
        if not timestamps or not closes or len(timestamps) != len(closes):
            return None
        ist = __import__("pytz").timezone("Asia/Kolkata")
        rows = []
        for i in range(len(timestamps)):
            ts = timestamps[i]
            close = closes[i] if i < len(closes) else None
            if close is None:
                continue
            try:
                from datetime import datetime as dt
                d = dt.fromtimestamp(ts, tz=__import__("pytz").UTC).astimezone(ist)
                rows.append({"date": d.strftime("%Y-%m-%d"), "close": float(close)})
            except (TypeError, ValueError):
                continue
        if not rows or len(rows) < 10:
            return None
        rows.sort(key=lambda x: x["date"])
        return rows
    except Exception:
        return None


def _run_car_from_rows(rows):
    """Run CAR logic on pre-fetched rows; return same dict as run_car_for_symbol_yahoo or None (incl. dma50, dma100, dma200)."""
    import pandas as pd
    from backend.services.car_nifty200_updater import (
        _compute_cumulative_avg,
        _compute_buy_signal,
        _compute_buy_signal_from_values,
        _compute_dma,
    )
    if not rows or len(rows) < 10:
        return None
    try:
        df = pd.DataFrame(rows).sort_values("date").reset_index(drop=True)
        dma50, dma100, dma200 = _compute_dma(df)
        idx_max = df["close"].idxmax()
        week_52_date = df.loc[idx_max, "date"]
        df_from_high = df[df["date"] >= week_52_date].copy().sort_values("date").reset_index(drop=True)
        last_close = float(df["close"].iloc[-1])
        df_from_high = _compute_cumulative_avg(df_from_high)
        if df_from_high is None or len(df_from_high) < 1:
            return None
        def _round_dma(x):
            return round(x, 4) if x is not None else None
        if len(df_from_high) < 10:
            cumm_vals = df_from_high["cumulative_avg"].tolist()
            last10_str = ",".join(f"{round(float(x), 2)}" for x in cumm_vals)
            signal = _compute_buy_signal_from_values([float(x) for x in cumm_vals])
            return {
                "stock_ltp": round(last_close, 4),
                "date_52weekhigh": week_52_date,
                "last10daycummavg": last10_str,
                "signal": signal,
                "dma50": _round_dma(dma50), "dma100": _round_dma(dma100), "dma200": _round_dma(dma200),
            }
        last_10 = df_from_high["cumulative_avg"].tail(10).tolist()
        last10_str = ",".join(f"{round(float(x), 2)}" for x in last_10)
        signal = _compute_buy_signal(df_from_high)
        return {
            "stock_ltp": round(last_close, 4),
            "date_52weekhigh": week_52_date,
            "last10daycummavg": last10_str,
            "signal": signal,
            "dma50": _round_dma(dma50), "dma100": _round_dma(dma100), "dma200": _round_dma(dma200),
        }
    except Exception:
        return None


def main():
    from backend.database import engine
    from sqlalchemy import text
    from backend.services.car_nifty200_updater import run_car_for_symbol_yahoo, run_car_for_symbol_upstox
    ist = __import__("pytz").timezone("Asia/Kolkata")
    today_str = datetime.now(ist).date().isoformat()

    # Clear cargpt.log so this run is the only content (latest execution log only)
    try:
        with open(cargpt_log_file, "w", encoding="utf-8"):
            pass
    except Exception as e:
        print(f"Warning: could not clear {cargpt_log_file}: {e}")

    log("=" * 60)
    log("CAR NIFTY200 one-time backfill started")
    log(f"Log file: {cargpt_log_file}")

    # 1. Update CHOLAFIN stock_ltp to 1514
    try:
        with engine.begin() as conn:
            conn.execute(
                text("UPDATE car_nifty200 SET stock_ltp = 1514 WHERE stock = 'CHOLAFIN'"),
            )
        log("Updated CHOLAFIN stock_ltp to 1514")
    except Exception as e:
        log(f"ERROR updating CHOLAFIN stock_ltp: {e}")
        cargpt_logger.exception("CHOLAFIN update failed")

    # 2. Get instrument_key for each symbol from car_nifty200
    with engine.begin() as conn:
        rows = conn.execute(
            text(
                "SELECT stock, stock_instrument_key FROM car_nifty200 WHERE stock = ANY(:symbols)"
            ),
            {"symbols": BACKFILL_SYMBOLS},
        ).mappings().all()
    key_by_stock = {r["stock"]: (r["stock_instrument_key"] or "").strip() for r in rows}

    for symbol in BACKFILL_SYMBOLS:
        symbol = (symbol or "").strip().upper()
        if not symbol:
            continue
        log(f"--- Processing {symbol} ---")
        instrument_key = key_by_stock.get(symbol, "")

        # Try Yahoo first
        yahoo_url_nse = f"{YAHOO_BASE}/{symbol}.NS?range=1y&interval=1d&includePrePost=false"
        log(f"Yahoo Finance API call (NSE): GET {yahoo_url_nse}")
        data = run_car_for_symbol_yahoo(symbol)
        if data and _is_valid_car_data(data):
            log(f"Yahoo Finance outcome for {symbol}: SUCCESS (source: NSE or BSE fallback)")
            log(f"  date_52weekhigh={data.get('date_52weekhigh')}, signal={data.get('signal')}, stock_ltp={data.get('stock_ltp')}")
        else:
            log(f"Yahoo Finance outcome for {symbol}: FAILED or incomplete (no/invalid data)")
            data = None
            # For CHOLAFIN, try direct fetch of CHOLAFIN.NS (same URL) in case server env differs
            if symbol == "CHOLAFIN":
                log(f"CHOLAFIN: trying direct Yahoo NSE fetch GET {YAHOO_BASE}/CHOLAFIN.NS?range=1y&interval=1d&includePrePost=false")
                rows = _fetch_cholafin_nse_direct()
                if rows:
                    data = _run_car_from_rows(rows)
                    if data and _is_valid_car_data(data):
                        log(f"CHOLAFIN: SUCCESS via direct Yahoo NSE fetch")
                        log(f"  date_52weekhigh={data.get('date_52weekhigh')}, signal={data.get('signal')}, stock_ltp={data.get('stock_ltp')}")
            if not data and instrument_key:
                # Try Upstox
                end_d = datetime.now(ist)
                start_d = end_d - timedelta(days=400)
                to_date = end_d.strftime("%Y-%m-%d")
                from_date = start_d.strftime("%Y-%m-%d")
                upstox_url = f"{UPSTOX_BASE}/historical-candle/{instrument_key}/days/1/{to_date}/{from_date}"
                log(f"Upstox API call: GET {upstox_url}")
                data = run_car_for_symbol_upstox(symbol, instrument_key)
                if data and _is_valid_car_data(data):
                    log(f"Upstox outcome for {symbol}: SUCCESS")
                    log(f"  date_52weekhigh={data.get('date_52weekhigh')}, signal={data.get('signal')}, stock_ltp={data.get('stock_ltp')}")
                else:
                    log(f"Upstox outcome for {symbol}: FAILED or incomplete")

        if data and _is_valid_car_data(data):
            try:
                stock_ltp_val = 1514.0 if symbol == "CHOLAFIN" else data["stock_ltp"]
                last10_val = data.get("last10daycummavg")
                if last10_val is None:
                    last10_val = ""
                dma50 = data.get("dma50")
                dma100 = data.get("dma100")
                dma200 = data.get("dma200")
                with engine.begin() as conn:
                    conn.execute(
                        text(
                            """
                            UPDATE car_nifty200
                            SET stock_ltp = :stock_ltp, date_52weekhigh = :date_52weekhigh,
                                last10daycummavg = :last10daycummavg, signal = :signal,
                                last_updated_date = :last_updated_date,
                                dma50 = :dma50, dma100 = :dma100, dma200 = :dma200
                            WHERE stock = :stock
                            """
                        ),
                        {
                            "stock": symbol,
                            "stock_ltp": stock_ltp_val,
                            "date_52weekhigh": _date_52_to_str(data["date_52weekhigh"]),
                            "last10daycummavg": last10_val,
                            "signal": data["signal"],
                            "last_updated_date": today_str,
                            "dma50": dma50, "dma100": dma100, "dma200": dma200,
                        },
                    )
                log(f"DB updated for {symbol}")
            except Exception as e:
                log(f"ERROR updating DB for {symbol}: {e}")
                cargpt_logger.exception("DB update failed")
        else:
            log(f"Skipped DB update for {symbol} (no valid data)")

    log("CAR NIFTY200 one-time backfill finished")
    log("=" * 60)


if __name__ == "__main__":
    main()
