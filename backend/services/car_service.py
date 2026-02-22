"""
CAR GPT Service - Cumulative Average Return analysis logic.
Business logic for 52-week high, historical close, cumulative average, and BUY signal.
"""
import logging
from datetime import datetime, timedelta
from typing import Optional, List, Dict, Any
import pandas as pd
import pytz

logger = logging.getLogger(__name__)


def get_upstox_service():
    """Get Upstox service instance (lazy import to avoid circular deps)."""
    from backend.services.upstox_service import upstox_service
    return upstox_service


def get_stock_name(symbol: str) -> str:
    """Get stock name from instruments file."""
    try:
        from backend.services.symbol_isin_mapping import get_stock_name as _get_name
        name = _get_name(symbol)
        return name or symbol.upper()
    except Exception as e:
        logger.warning(f"Could not get stock name for {symbol}: {e}")
        return symbol.upper()


def get_52_week_high_date(symbol: str, instrument_key: str, upstox_service) -> Optional[str]:
    """
    Fetch last 365 days of daily close data and return the date where CLOSE was maximum.
    
    Returns:
        Date string in YYYY-MM-DD format, or None if no data
    """
    try:
        ist = pytz.timezone('Asia/Kolkata')
        end_date = datetime.now(ist)
        start_date = end_date - timedelta(days=365)
        to_date = end_date.strftime("%Y-%m-%d")
        from_date = start_date.strftime("%Y-%m-%d")

        candles = upstox_service.get_historical_candles_by_instrument_key(
            instrument_key=instrument_key,
            interval="days/1",
            days_back=365
        )
        if not candles or len(candles) == 0:
            return None

        # Find candle with max close
        max_close = -1.0
        max_date_str = None
        for c in candles:
            close = c.get('close', 0)
            ts = c.get('timestamp')
            if ts and close > max_close:
                max_close = close
                # Convert timestamp to date string
                if isinstance(ts, str) and 'T' in ts:
                    max_date_str = ts.split('T')[0]
                elif isinstance(ts, (int, float)):
                    dt = datetime.fromtimestamp(ts / 1000.0 if ts > 1e12 else ts, tz=ist)
                    max_date_str = dt.strftime("%Y-%m-%d")
                else:
                    max_date_str = str(ts)[:10]
        return max_date_str
    except Exception as e:
        logger.error(f"Error getting 52-week high date for {symbol}: {e}")
        return None


def get_historical_close_from_date(
    instrument_key: str,
    start_date_str: str,
    upstox_service
) -> Optional[pd.DataFrame]:
    """
    Fetch historical close data from start_date to today.
    Returns DataFrame with columns: date, close
    """
    try:
        candles = upstox_service.get_historical_candles_by_instrument_key(
            instrument_key=instrument_key,
            interval="days/1",
            days_back=400  # Enough to cover from 52-week high to today
        )
        if not candles or len(candles) == 0:
            return None

        rows = []
        for c in candles:
            ts = c.get('timestamp')
            close = c.get('close')
            if ts is None or close is None:
                continue
            if isinstance(ts, (int, float)):
                ist = pytz.timezone('Asia/Kolkata')
                dt = datetime.fromtimestamp(ts / 1000.0 if ts > 1e12 else ts, tz=ist)
                date_str = dt.strftime("%Y-%m-%d")
            elif isinstance(ts, str):
                date_str = ts.split('T')[0] if 'T' in ts else ts[:10]
            else:
                continue
            if date_str >= start_date_str:
                rows.append({"date": date_str, "close": float(close)})

        if not rows:
            return None
        df = pd.DataFrame(rows)
        df = df.sort_values("date").reset_index(drop=True)
        return df
    except Exception as e:
        logger.error(f"Error fetching historical close: {e}")
        return None


def compute_cumulative_avg(df: pd.DataFrame) -> pd.DataFrame:
    """Add cumulative_avg column using expanding().mean() on close."""
    if df is None or df.empty:
        return df
    df = df.copy()
    df["cumulative_avg"] = df["close"].expanding().mean()
    return df


def compute_buy_signal(df: pd.DataFrame) -> str:
    """
    Take last 10 rows of cumulative_avg, reverse so latest is first.
    Check strictly increasing: cumulative_avg[i] > cumulative_avg[i+1] for 9 consecutive pairs.
    If TRUE: "BUY / AVERAGE OUT", else "AVOID / HOLD"
    """
    if df is None or len(df) < 10:
        return "AVOID / HOLD"
    last_10 = df["cumulative_avg"].tail(10).values
    reversed_vals = last_10[::-1]  # Latest first
    for i in range(9):
        if reversed_vals[i] <= reversed_vals[i + 1]:
            return "AVOID / HOLD"
    return "BUY / AVERAGE OUT"


def run_car_analysis_for_symbol(
    symbol: str,
    upstox_service,
    number_of_weeks: int = 52
) -> Dict[str, Any]:
    """
    Run full CAR analysis for a single symbol.
    Returns dict with: symbol, stock_name, week_52_high_date, current_price,
    last_10_cumulative_avg, signal, error (if any)
    """
    result = {
        "symbol": symbol.upper(),
        "stock_name": get_stock_name(symbol),
        "week_52_high_date": None,
        "current_price": None,
        "last_10_cumulative_avg": [],
        "signal": "AVOID / HOLD",
        "error": None
    }
    try:
        instrument_key = upstox_service.get_instrument_key(symbol)
        if not instrument_key:
            result["error"] = f"Invalid symbol or instrument key not found for {symbol}"
            return result

        # Step 2: 52-week high date
        week_52_date = get_52_week_high_date(symbol, instrument_key, upstox_service)
        if not week_52_date:
            result["error"] = f"No historical data for {symbol}"
            return result
        result["week_52_high_date"] = week_52_date

        # Step 3: Historical close from 52-week high to today
        df = get_historical_close_from_date(instrument_key, week_52_date, upstox_service)
        if df is None or df.empty:
            result["error"] = f"Empty data from 52-week high for {symbol}"
            return result

        # Step 4: Cumulative average
        df = compute_cumulative_avg(df)
        if df is None or len(df) < 10:
            result["error"] = f"Insufficient data for cumulative avg for {symbol}"
            return result

        # Current price = last close
        result["current_price"] = round(float(df["close"].iloc[-1]), 2)

        # Last 10 cumulative averages (oldest to newest for display)
        last_10 = df["cumulative_avg"].tail(10).tolist()
        result["last_10_cumulative_avg"] = [round(float(x), 2) for x in last_10]

        # Step 5: BUY signal
        result["signal"] = compute_buy_signal(df)
        return result
    except Exception as e:
        logger.error(f"CAR analysis error for {symbol}: {e}", exc_info=True)
        result["error"] = str(e)
        return result


def run_car_analysis_for_symbols(
    symbols: List[str],
    upstox_service,
    number_of_weeks: int = 52
) -> List[Dict[str, Any]]:
    """Run CAR analysis for multiple symbols."""
    results = []
    for sym in symbols:
        sym = (sym or "").strip().upper()
        if not sym:
            continue
        r = run_car_analysis_for_symbol(sym, upstox_service, number_of_weeks)
        results.append(r)
    return results
