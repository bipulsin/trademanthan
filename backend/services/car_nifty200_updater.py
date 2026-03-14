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


def run_car_for_symbol_yahoo(symbol: str) -> Optional[Dict[str, Any]]:
    """
    Run CAR logic using Yahoo Finance (symbol.NS). No instrument_key needed.
    Returns dict with stock_ltp, date_52weekhigh, last10daycummavg (comma-separated), signal; or None on failure.
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
        if len(df_from_high) < 10:
            return None
        df_from_high = _compute_cumulative_avg(df_from_high)
        if df_from_high is None or len(df_from_high) < 10:
            return None
        last_close = float(df_from_high["close"].iloc[-1])
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
        if df is None or df.empty or len(df) < 10:
            return None
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
    Try Yahoo first; for any row that fails or stays unupdated, use Upstox.
    Process in batches of BATCH_SIZE. Returns {"updated": n, "failed": m}.
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
                SELECT stock, stock_instrument_key
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

    logger.info(f"car_nifty200: updating {len(rows)} rows (Yahoo first, Upstox fallback)")

    for i in range(0, len(rows), BATCH_SIZE):
        batch = rows[i : i + BATCH_SIZE]
        for row in batch:
            stock = (row["stock"] or "").strip()
            instrument_key = (row["stock_instrument_key"] or "").strip()
            if not stock:
                failed += 1
                continue
            data = run_car_for_symbol_yahoo(stock)
            if data is None or not all([data.get("stock_ltp"), data.get("date_52weekhigh"), data.get("last10daycummavg"), data.get("signal")]):
                if instrument_key:
                    data = run_car_for_symbol_upstox(stock, instrument_key)
                else:
                    data = None
            if data and all([data.get("stock_ltp"), data.get("date_52weekhigh"), data.get("last10daycummavg"), data.get("signal")]):
                try:
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
                                "last10daycummavg": data["last10daycummavg"],
                                "signal": data["signal"],
                                "last_updated_date": today_str,
                            },
                        )
                    updated += 1
                except Exception as e:
                    logger.warning(f"car_nifty200 update DB for {stock}: {e}")
                    failed += 1
            else:
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
