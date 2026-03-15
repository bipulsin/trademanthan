"""
CAR NIFTY200 Updater - Updates car_nifty200 table using Yahoo Finance (primary) and Upstox (fallback).
Uses same CAR logic as cargpt: 52-week high date, cumulative average from that date, last 10 values, BUY/AVOID signal.
Runs every 3 hours; processes rows where last_updated_date is not current date (IST).
"""
import logging
from datetime import datetime, date
from typing import Optional, List, Dict, Any
import pytz
import pandas as pd

logger = logging.getLogger(__name__)

IST = pytz.timezone("Asia/Kolkata")
BATCH_SIZE = 10


def _today_ist() -> date:
    return datetime.now(IST).date()


def _compute_cumulative_avg(df: pd.DataFrame) -> pd.DataFrame:
    if df is None or df.empty:
        return df
    df = df.copy()
    df["cumulative_avg"] = df["close"].expanding().mean()
    return df


def _compute_buy_signal(df: pd.DataFrame) -> str:
    """Same as car_service: strictly increasing last 10 (latest first) -> BUY / AVERAGE OUT else AVOID / HOLD."""
    if df is None or len(df) < 10:
        return "AVOID / HOLD"
    last_10 = df["cumulative_avg"].tail(10).values
    rev = last_10[::-1]
    for i in range(9):
        if rev[i] <= rev[i + 1]:
            return "AVOID / HOLD"
    return "BUY / AVERAGE OUT"


# When 52w high is within last 10 trading days we cannot compute cumulative avg; use this signal and blank last10.
PARTIAL_SIGNAL = "AVOID/HOLD"


def _is_valid_car_result(data: Optional[Dict[str, Any]]) -> bool:
    """True if we have enough to update DB: stock_ltp, date_52weekhigh, signal (last10daycummavg may be blank for partial)."""
    if not data:
        return False
    if data.get("stock_ltp") is None:
        return False
    date_52 = data.get("date_52weekhigh")
    if date_52 is None or (isinstance(date_52, str) and not date_52.strip()):
        return False
    signal = data.get("signal")
    if signal is None or (isinstance(signal, str) and not signal.strip()):
        return False
    return True

def run_car_for_symbol_yahoo(symbol: str) -> Optional[Dict[str, Any]]:
    """
    Run CAR logic using Yahoo Finance (symbol.NS). No instrument_key needed.
    Returns dict with stock_ltp, date_52weekhigh, last10daycummavg (comma-separated or "" if <10 days), signal; or None on failure.
    When 52w high is within last 10 trading days: still returns date_52weekhigh, stock_ltp, last10daycummavg="", signal=PARTIAL_SIGNAL.
    """
    try:
        from backend.services.yahoo_vwap_service import yahoo_vwap_service
        rows = yahoo_vwap_service.get_historical_daily_1y(symbol)
        if not rows or len(rows) < 10:
            return None
        df = pd.DataFrame(rows)
        # 52-week high date = date where close was maximum
        idx_max = df["close"].idxmax()
        week_52_date = df.loc[idx_max, "date"]
        df_from_high = df[df["date"] >= week_52_date].copy().sort_values("date").reset_index(drop=True)
        last_close = float(df["close"].iloc[-1])
        if len(df_from_high) < 10:
            return {
                "stock_ltp": round(last_close, 4),
                "date_52weekhigh": week_52_date,
                "last10daycummavg": "",
                "signal": PARTIAL_SIGNAL,
            }
        df_from_high = _compute_cumulative_avg(df_from_high)
        if df_from_high is None or len(df_from_high) < 10:
            return {
                "stock_ltp": round(last_close, 4),
                "date_52weekhigh": week_52_date,
                "last10daycummavg": "",
                "signal": PARTIAL_SIGNAL,
            }
        last_10 = df_from_high["cumulative_avg"].tail(10).tolist()
        last10_str = ",".join(f"{round(float(x), 2)}" for x in last_10)
        signal = _compute_buy_signal(df_from_high)
        return {
            "stock_ltp": round(last_close, 4),
            "date_52weekhigh": week_52_date,
            "last10daycummavg": last10_str,
            "signal": signal,
        }
    except Exception as e:
        logger.debug(f"Yahoo CAR for {symbol}: {e}")
        return None


def run_car_for_symbol_upstox(stock: str, instrument_key: str) -> Optional[Dict[str, Any]]:
    """
    Run CAR logic using Upstox (existing car_service). Returns same shape as run_car_for_symbol_yahoo.
    When 52w high is within last 10 trading days: returns date_52weekhigh, stock_ltp, last10daycummavg="", signal=PARTIAL_SIGNAL.
    """
    try:
        from backend.services.car_service import (
            get_upstox_service,
            get_52_week_high_date,
            get_historical_close_from_date,
            compute_cumulative_avg,
            compute_buy_signal,
        )
        upstox = get_upstox_service()
        week_52_date = get_52_week_high_date(stock, instrument_key, upstox)
        if not week_52_date:
            return None
        df = get_historical_close_from_date(instrument_key, week_52_date, upstox)
        if df is None or df.empty:
            return None
        if len(df) < 10:
            last_close = float(df["close"].iloc[-1])
            return {
                "stock_ltp": round(last_close, 4),
                "date_52weekhigh": week_52_date,
                "last10daycummavg": "",
                "signal": PARTIAL_SIGNAL,
            }
        df = compute_cumulative_avg(df)
        last_close = float(df["close"].iloc[-1])
        last_10 = df["cumulative_avg"].tail(10).tolist()
        last10_str = ",".join(f"{round(float(x), 2)}" for x in last_10)
        signal = compute_buy_signal(df)
        return {
            "stock_ltp": round(last_close, 4),
            "date_52weekhigh": week_52_date,
            "last10daycummavg": last10_str,
            "signal": signal,
        }
    except Exception as e:
        logger.debug(f"Upstox CAR for {stock}: {e}")
        return None


def update_car_nifty200_batch() -> Dict[str, int]:
    """
    Update car_nifty200 rows where last_updated_date is not today (IST).
    For rows with NULL signal (never filled): try Upstox first, then Yahoo (to backfill).
    For rows already having data: try Yahoo first, then Upstox fallback.
    Yahoo tries .NS then .BO (BSE) when NSE returns no data.
    Returns {"updated": n, "failed": m}.
    """
    from backend.database import engine
    from sqlalchemy import text

    today = _today_ist()
    today_str = today.isoformat()
    updated = 0
    failed = 0

    with engine.begin() as conn:
        rows = conn.execute(
            text(
                """
                SELECT stock, stock_instrument_key, signal
                FROM car_nifty200
                WHERE last_updated_date IS NULL OR last_updated_date < :today
                ORDER BY stock ASC
                """
            ),
            {"today": today_str},
        ).mappings().all()

    if not rows:
        logger.info("car_nifty200: no rows to update (all up to date)")
        return {"updated": 0, "failed": 0}

    logger.info(f"car_nifty200: updating {len(rows)} rows (Upstox first for NULL signal, else Yahoo then Upstox)")

    for i in range(0, len(rows), BATCH_SIZE):
        batch = rows[i : i + BATCH_SIZE]
        for row in batch:
            stock = (row["stock"] or "").strip()
            instrument_key = (row["stock_instrument_key"] or "").strip()
            signal_null = (row.get("signal") or "").strip() == ""
            if not stock:
                failed += 1
                continue
            data = None
            # Rows with NULL signal: try Upstox first to backfill (Yahoo often has no .NS data for some symbols)
            if signal_null and instrument_key:
                data = run_car_for_symbol_upstox(stock, instrument_key)
                if data and _is_valid_car_result(data):
                    logger.info(f"car_nifty200: {stock} backfilled via Upstox (was NULL signal)")
            # If still no valid data, try Yahoo (NSE then BSE .BO)
            if data is None or not _is_valid_car_result(data):
                data = run_car_for_symbol_yahoo(stock)
            # If Yahoo failed, try Upstox (for rows we didn't try Upstox first, or retry)
            if data is None or not _is_valid_car_result(data):
                if instrument_key:
                    logger.info(f"car_nifty200: Yahoo no/incomplete data for {stock}, trying Upstox")
                    data = run_car_for_symbol_upstox(stock, instrument_key)
                else:
                    data = None
            if data and _is_valid_car_result(data):
                try:
                    last10 = data.get("last10daycummavg")
                    if last10 is None:
                        last10 = ""
                    with engine.begin() as conn:
                        conn.execute(
                            text(
                                """
                                UPDATE car_nifty200
                                SET stock_ltp = :stock_ltp, date_52weekhigh = :date_52weekhigh,
                                    last10daycummavg = :last10daycummavg, signal = :signal,
                                    last_updated_date = :last_updated_date
                                WHERE stock = :stock
                                """
                            ),
                            {
                                "stock": stock,
                                "stock_ltp": data["stock_ltp"],
                                "date_52weekhigh": data["date_52weekhigh"],
                                "last10daycummavg": last10,
                                "signal": data["signal"],
                                "last_updated_date": today_str,
                            },
                        )
                    updated += 1
                except Exception as e:
                    logger.warning(f"car_nifty200 update DB for {stock}: {e}")
                    failed += 1
            else:
                if instrument_key:
                    logger.warning(f"car_nifty200: both Yahoo and Upstox failed for {stock}")
                else:
                    logger.warning(f"car_nifty200: no instrument_key for {stock}, Yahoo failed")
                failed += 1

    logger.info(f"car_nifty200: updated={updated}, failed={failed}")
    return {"updated": updated, "failed": failed}


def run_car_nifty200_update_job():
    """Entry point for scheduler: run update and log result."""
    try:
        result = update_car_nifty200_batch()
        logger.info(f"CAR NIFTY200 job completed: {result}")
    except Exception as e:
        logger.error(f"CAR NIFTY200 job failed: {e}", exc_info=True)
