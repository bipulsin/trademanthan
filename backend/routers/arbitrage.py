import json
from datetime import datetime, timedelta
from pathlib import Path
from typing import Dict
from zoneinfo import ZoneInfo

from fastapi import APIRouter, HTTPException, Query
from fastapi.responses import StreamingResponse
from sqlalchemy import text

from backend.config import get_instruments_file_path
from backend.database import engine
from backend.services.upstox_service import UpstoxService
from backend.config import settings

from backend.services.arbitrage_daily_setup_scheduler import (
    arbitrage_daily_setup_scheduler,
    run_arbitrage_daily_setup_now,
)

router = APIRouter(prefix="/scan/arbitrage", tags=["arbitrage"])


@router.get("/version")
async def get_arbitrage_version():
    """Return API version for deployment verification."""
    return {"arbitrage_api": "v2", "pivot_ltp_source": "arbitrage_master"}


def _ensure_arbitrage_order_table() -> None:
    with engine.begin() as conn:
        conn.execute(
            text(
                """
                DO $$
                BEGIN
                    IF to_regclass('public.arbiitrage_order') IS NOT NULL
                       AND to_regclass('public.arbitrage_order') IS NULL THEN
                        ALTER TABLE arbiitrage_order RENAME TO arbitrage_order;
                    END IF;
                END
                $$;
                """
            )
        )
        conn.execute(
            text(
                """
                CREATE TABLE IF NOT EXISTS arbitrage_order (
                    id BIGSERIAL PRIMARY KEY,
                    stock TEXT NOT NULL,
                    stock_instrument_key TEXT NOT NULL,
                    currmth_future_symbol TEXT NOT NULL,
                    currmth_future_instrument_key TEXT NOT NULL,
                    buy_cost NUMERIC(16,4) NOT NULL,
                    buy_exit_cost NUMERIC(16,4),
                    current_future_state TEXT NOT NULL DEFAULT 'BUY',
                    nextmth_future_symbol TEXT NOT NULL,
                    nextmth_future_instrement_key TEXT NOT NULL,
                    sell_cost NUMERIC(16,4) NOT NULL,
                    sell_exit_cost NUMERIC(16,4),
                    nextmth_future_state TEXT NOT NULL DEFAULT 'SELL',
                    quantity INTEGER NOT NULL,
                    trade_status TEXT NOT NULL DEFAULT 'OPEN',
                    trade_entry_value NUMERIC(18,4) NOT NULL,
                    trade_entry_time TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP,
                    trade_exit_time TIMESTAMP,
                    trade_exit_value NUMERIC(18,4)
                )
                """
            )
        )
        conn.execute(
            text(
                """
                CREATE INDEX IF NOT EXISTS idx_arbitrage_order_stock_trade_status
                ON arbitrage_order (stock_instrument_key, trade_status)
                """
            )
        )
        conn.execute(
            text(
                """
                CREATE UNIQUE INDEX IF NOT EXISTS uq_arbitrage_order_open_stock
                ON arbitrage_order (stock_instrument_key)
                WHERE trade_status = 'OPEN'
                """
            )
        )


def _load_quantity_by_instrument_key() -> Dict[str, int]:
    instruments_file: Path = get_instruments_file_path()
    if not instruments_file.exists():
        raise FileNotFoundError(f"Instruments file not found: {instruments_file}")
    with instruments_file.open("r", encoding="utf-8") as f:
        instruments = json.load(f)
    quantity_map: Dict[str, int] = {}
    for inst in instruments if isinstance(instruments, list) else []:
        if not isinstance(inst, dict):
            continue
        key = inst.get("instrument_key")
        lot = inst.get("lot_size")
        if key and lot is not None:
            try:
                quantity_map[key] = int(float(lot))
            except (TypeError, ValueError):
                continue
    return quantity_map


@router.post("/daily-setup/run")
async def run_daily_setup_now():
    """
    On-demand run of arbitrage_dailySetup job.
    """
    try:
        result = run_arbitrage_daily_setup_now()
        return result
    except Exception as exc:
        raise HTTPException(status_code=500, detail=f"Failed to run arbitrage_dailySetup: {exc}")


@router.get("/daily-setup/status")
async def get_daily_setup_status():
    """
    Get scheduler status for arbitrage_dailySetup job.
    """
    try:
        status = arbitrage_daily_setup_scheduler.get_status()
        status["job_name"] = "arbitrage_dailySetup"
        status["schedule"] = "Every 15 minutes, 09:15-15:45 Asia/Kolkata (Mon-Fri)"
        return status
    except Exception as exc:
        raise HTTPException(status_code=500, detail=f"Failed to get scheduler status: {exc}")


@router.get("/selection")
async def get_arbitrage_selection():
    """
    Fetch arbitrage selection rows with filter:
    - currmth_future_ltp < stock_ltp
    - nextmth_future_ltp <= currmth_future_ltp
    - nextmth_future_ltp within 3 points below currmth_future_ltp
    """
    try:
        _ensure_arbitrage_order_table()
        with engine.begin() as conn:
            rows = conn.execute(
                text(
                    """
                    SELECT
                        stock,
                        stock_instrument_key,
                        stock_ltp,
                        currmth_future_symbol,
                        currmth_future_instrument_key,
                        currmth_future_ltp,
                        nextmth_future_symbol,
                        nextmth_future_instrement_key,
                        nextmth_future_ltp,
                        EXISTS (
                            SELECT 1
                            FROM arbitrage_order ao
                            WHERE ao.stock_instrument_key = arbitrage_master.stock_instrument_key
                              AND ao.trade_status = 'OPEN'
                        ) AS has_open_order
                    FROM arbitrage_master
                    WHERE
                        stock_ltp IS NOT NULL
                        AND currmth_future_ltp IS NOT NULL
                        AND nextmth_future_ltp IS NOT NULL
                        AND currmth_future_ltp < stock_ltp
                        AND nextmth_future_ltp <= currmth_future_ltp
                        AND nextmth_future_ltp >= (currmth_future_ltp - 3)
                    ORDER BY stock ASC
                    """
                )
            ).mappings().all()

        return {
            "success": True,
            "count": len(rows),
            "rows": [dict(row) for row in rows],
        }
    except Exception as exc:
        raise HTTPException(status_code=500, detail=f"Failed to fetch arbitrage selection: {exc}")


def _parse_candle_dt(ts: str) -> datetime:
    cleaned = (ts or "").replace("Z", "+00:00")
    return datetime.fromisoformat(cleaned)


def _candle_date_ist(candle: dict) -> str | None:
    """Extract YYYY-MM-DD (IST) from candle timestamp. Handles ISO string, epoch ms, or plain date."""
    ts = candle.get("timestamp")
    if ts is None:
        return None
    if isinstance(ts, str):
        if len(ts) >= 10 and ts[4] == "-" and ts[7] == "-":
            return ts[:10]
        try:
            dt = _parse_candle_dt(ts)
            return dt.astimezone(ZoneInfo("Asia/Kolkata")).strftime("%Y-%m-%d")
        except Exception:
            return ts[:10] if len(ts) >= 10 else None
    if isinstance(ts, (int, float)):
        sec = ts / 1000.0 if ts > 1e12 else ts
        dt = datetime.fromtimestamp(sec, tz=ZoneInfo("Asia/Kolkata"))
        return dt.strftime("%Y-%m-%d")
    return None


def _pick_candle_for_date(candles: list[dict], target_date_str: str) -> dict | None:
    """Return the candle matching target_date_str (YYYY-MM-DD) in IST, or None."""
    if not candles or not target_date_str:
        return None
    for c in candles:
        d = _candle_date_ist(c)
        if d == target_date_str:
            return c
    return None


def _pick_previous_trading_day_candle(candles: list[dict], before_date_str: str) -> dict | None:
    """Return the latest candle with date strictly before before_date_str (IST)."""
    if not candles or not before_date_str:
        return None
    candidates = [(c, _candle_date_ist(c)) for c in candles]
    valid = [(c, d) for c, d in candidates if d]
    ordered = sorted(valid, key=lambda x: x[1] or "", reverse=True)
    for candle, d in ordered:
        if d and d < before_date_str:
            return candle
    return None  # Do not fallback to wrong candle when no date < before_date_str


def _aggregate_intraday_to_daily(intraday_candles: list[dict]) -> list[dict]:
    """
    Aggregate intraday candles (15min, 1h, etc.) into daily OHLC by date (IST).
    Returns list of {date, open, high, low, close} sorted by date asc.
    """
    from collections import defaultdict
    by_date: dict[str, list[dict]] = defaultdict(list)
    for c in intraday_candles:
        d = _candle_date_ist(c)
        if d:
            by_date[d].append(c)
    result = []
    for date_str in sorted(by_date.keys()):
        candles = by_date[date_str]
        candles.sort(key=lambda x: (x.get("timestamp") or ""))
        if not candles:
            continue
        high = max(float(c.get("high", 0) or 0) for c in candles)
        low = min(float(c.get("low", 0) or 0) for c in candles)
        open_ = float(candles[0].get("open", 0) or 0)
        close = float(candles[-1].get("close", 0) or 0)
        if high > 0 and low > 0:
            result.append({"date": date_str, "high": high, "low": low, "open": open_, "close": close})
    return result


def _pivot_breakout_candle_mode(upstox: UpstoxService) -> tuple[str, bool]:
    """
    Determine which candle to use for R3/S3 based on current time (IST).
    Returns (target_date_str, use_same_day).
    - use_same_day=True: use OHLC of target_date for R3/S3 (same day as LTP/close).
    - use_same_day=False: use OHLC of previous trading day (target_date is today; we need prev day).
    """
    ist = ZoneInfo("Asia/Kolkata")
    now = datetime.now(ist)
    today_str = now.strftime("%Y-%m-%d")
    hour, minute = now.hour, now.minute

    is_trading_today = upstox.is_trading_day(now)
    after_close = (hour > 15) or (hour == 15 and minute >= 45)
    during_market = (hour > 9 or (hour == 9 and minute >= 15)) and (
        hour < 15 or (hour == 15 and minute < 45)
    )

    # After 15:45 on a trading day, or on a non-trading day: use same day (ref date) for both LTP and R3/S3.
    if (is_trading_today and after_close) or not is_trading_today:
        ref = upstox.get_last_trading_date(now)
        return ref.strftime("%Y-%m-%d"), True

    # 9:15 - 15:45 on a trading day: LTP = today, R3/S3 = previous trading day.
    if is_trading_today and during_market:
        return today_str, False

    # Before 9:15 on a trading day: show previous day's close and R3/S3 (same as post-close of prev day).
    prev_ref = upstox.get_last_trading_date(now - timedelta(days=1))
    return prev_ref.strftime("%Y-%m-%d"), True


def _get_prev_day_ohlc(
    upstox: UpstoxService,
    instrument_key: str,
    target_date_str: str,
    ohlc_interval: str,
    use_same_day: bool = False,
) -> tuple[float, float, float, str] | None:
    """
    Get OHLC for the candle used for R3/S3. Returns (high, low, close, candle_date) or None.
    NOTE: Pivots are ALWAYS based on daily candles from the previous trading day,
    matching TradingView "Traditional" pivots on a 1D basis.
    - use_same_day=True: use daily candle for target_date_str.
    - use_same_day=False: use daily candle strictly before target_date_str (previous trading day).
    ohlc_interval is kept for signature compatibility but ignored for pivot OHLC selection.
    """
    candles = upstox.get_historical_candles_by_instrument_key(
        instrument_key, interval="days/1", days_back=15
    ) or []
    if use_same_day:
        prev = _pick_candle_for_date(candles, target_date_str)
    else:
        prev = _pick_previous_trading_day_candle(candles, target_date_str)
    if not prev:
        return None
    high = float(prev.get("high", 0) or 0)
    low = float(prev.get("low", 0) or 0)
    close = float(prev.get("close", 0) or 0)
    candle_date = _candle_date_ist(prev) or (prev.get("timestamp") or "")[:10]
    return (high, low, close, candle_date)


def _dedupe_pivot_by_stock(items: list[dict], key: str = "stock") -> list[dict]:
    """Keep first occurrence of each stock to avoid duplicate rows."""
    seen: set[str] = set()
    out: list[dict] = []
    for r in items:
        s = (r.get(key) or "").strip()
        if s and s not in seen:
            seen.add(s)
            out.append(r)
    return out


def _process_pivot_batch(
    rows: list,
    upstox: UpstoxService,
    target_date_str: str,
    use_same_day: bool,
    ohlc_interval: str = "daily",
    threshold_pct: float = 5.0,
    vwap_filter_pct: float = 0.0,
) -> tuple[list[dict], list[dict]]:
    """
    Process a batch of rows and return (bullish, bearish) lists.
    R3/S3 from previous trading day OHLC. ohlc_interval: "daily" | "hourly" | "15min".
    threshold_pct: percentage distance band for R3/S3 (e.g. 5.0 for 5%).
    vwap_filter_pct: if > 0, only include rows where LTP is within ±vwap_filter_pct% of VWAP,
    using the same candle duration as selected by ohlc_interval ("daily" -> days/1, "hourly" -> hours/1, "15min" -> minutes/15).
    """
    # Sanitize threshold to a reasonable band (0.1% .. 10%).
    band_pct = max(0.1, min(threshold_pct or 5.0, 10.0))
    band = band_pct / 100.0
    vwap_band = max(0.0, min(vwap_filter_pct or 0.0, 20.0)) / 100.0  # cap at 20%
    bullish: list[dict] = []
    bearish: list[dict] = []
    for row in rows:
        # Always prefer live future LTP from Upstox; fall back to DB value on error.
        live_ltp = None
        try:
            quote = upstox.get_market_quote_by_key(row["currmth_future_instrument_key"])
            if quote:
                live_price = float(quote.get("last_price", 0) or 0)
                if live_price > 0:
                    live_ltp = live_price
        except Exception:
            live_ltp = None
        ltp = float(live_ltp if live_ltp is not None else row["currmth_future_ltp"])
        ohlc = _get_prev_day_ohlc(
            upstox, row["currmth_future_instrument_key"], target_date_str, ohlc_interval, use_same_day
        )
        if not ohlc:
            continue
        high, low, close, candle_date = ohlc
        if high <= 0 or low <= 0 or close <= 0:
            continue
        pivot = (high + low + close) / 3.0
        r3 = high + 2.0 * (pivot - low)
        s3 = low - 2.0 * (high - pivot)
        if r3 <= 0 or s3 <= 0:
            continue
        payload = {
            "stock": row["stock"],
            "currmth_future_symbol": row["currmth_future_symbol"],
            "currmth_future_ltp": ltp,
            "pivot_candle_date": candle_date,
            "previous_day_high": round(high, 4),
            "previous_day_low": round(low, 4),
            "previous_day_close": round(close, 4),
            "r3_pivot": round(r3, 4),
            "s3_pivot": round(s3, 4),
        }
        in_bullish = (ltp <= r3) and (ltp >= (r3 * (1.0 - band)))
        in_bearish = (ltp >= s3) and (ltp <= (s3 * (1.0 + band)))
        if in_bullish and in_bearish:
            # LTP in overlap zone: assign to the level it's closer to (by % distance)
            dist_r3_pct = (r3 - ltp) / r3 if r3 > 0 else 1.0
            dist_s3_pct = (ltp - s3) / s3 if s3 > 0 else 1.0
            if dist_r3_pct <= dist_s3_pct:
                in_bearish = False
            else:
                in_bullish = False
        if vwap_band > 0 and (in_bullish or in_bearish):
            # Align VWAP candle duration with selected OHLC interval
            if ohlc_interval == "hourly":
                vwap_interval = "hours/1"
            elif ohlc_interval == "15min":
                vwap_interval = "minutes/15"
            else:
                vwap_interval = "days/1"
            vwap_val = upstox.get_candle_vwap_by_instrument_key(
                row["currmth_future_instrument_key"], interval=vwap_interval, days_back=2
            )
            if vwap_val is None or vwap_val <= 0:
                continue
            low_bound = vwap_val * (1.0 - vwap_band)
            high_bound = vwap_val * (1.0 + vwap_band)
            if not (low_bound <= ltp <= high_bound):
                continue
            payload["vwap_price"] = round(vwap_val, 4)
        if in_bullish:
            diff_r3 = r3 - ltp
            pct_r3 = (diff_r3 / r3) * 100.0
            if pct_r3 <= band_pct:  # only include if % diff within threshold
                bullish.append({**payload, "difference_from_r3": round(diff_r3, 4), "difference_from_r3_pct": round(pct_r3, 4)})
        if in_bearish:
            diff_s3 = ltp - s3
            pct_s3 = (diff_s3 / s3) * 100.0
            if pct_s3 <= band_pct:  # only include if % diff within threshold
                bearish.append({**payload, "difference_from_s3": round(diff_s3, 4), "difference_from_s3_pct": round(pct_s3, 4)})
    return bullish, bearish


@router.get("/pivot-breakout")
async def get_pivot_breakout(
    ohlc_interval: str = Query("daily", description="OHLC source: 'daily', 'hourly', or '15min'"),
    threshold_pct: float = Query(
        5.0,
        ge=0.1,
        le=10.0,
        description="Percentage band for closeness to R3/S3 (e.g. 5.0 for 5%).",
    ),
    vwap_filter_pct: float = Query(
        0.0,
        ge=0.0,
        le=20.0,
        description="If > 0, only show rows where LTP is within ±this % of 1h candle VWAP (e.g. 5 = within 5%).",
    ),
):
    """
    Return bullish and bearish pivot breakout candidates.
    R3/S3 from previous trading day OHLC.
    - ohlc_interval=daily: use daily candles (default)
    - ohlc_interval=hourly: aggregate 1-hour candles into daily OHLC
    - ohlc_interval=15min: aggregate 15-minute candles into daily OHLC
    - threshold_pct: band in % for distance from R3/S3 (default 5.0).
    - vwap_filter_pct: if 5, only show candidates within ±5% of 1h candle VWAP.
    """
    try:
        with engine.begin() as conn:
            rows = conn.execute(
                text(
                    """
                    SELECT
                        stock,
                        currmth_future_symbol,
                        currmth_future_instrument_key,
                        currmth_future_ltp
                    FROM arbitrage_master
                    WHERE currmth_future_symbol IS NOT NULL
                      AND currmth_future_instrument_key IS NOT NULL
                      AND currmth_future_ltp IS NOT NULL
                    ORDER BY stock ASC
                    """
                )
            ).mappings().all()

        upstox = UpstoxService(settings.UPSTOX_API_KEY, settings.UPSTOX_API_SECRET)
        target_date_str, use_same_day = _pivot_breakout_candle_mode(upstox)
        interval = ohlc_interval if ohlc_interval in ("daily", "hourly", "15min") else "daily"
        bullish, bearish = _process_pivot_batch(
            rows,
            upstox,
            target_date_str,
            use_same_day,
            ohlc_interval=interval,
            threshold_pct=threshold_pct,
            vwap_filter_pct=vwap_filter_pct,
        )
        bullish = _dedupe_pivot_by_stock(bullish)
        bearish = _dedupe_pivot_by_stock(bearish)

        # Nearest candidates first.
        bullish.sort(key=lambda x: (x.get("difference_from_r3", 10**9), x.get("stock", "")))
        bearish.sort(key=lambda x: (x.get("difference_from_s3", 10**9), x.get("stock", "")))
        pivot_date = (bullish[0]["pivot_candle_date"] if bullish else bearish[0]["pivot_candle_date"]) if (bullish or bearish) else target_date_str
        return {
            "success": True,
            "ltp_date": target_date_str,
            "pivot_date": pivot_date,
            "ohlc_interval": interval,
            "threshold_pct": threshold_pct,
            "bullish_count": len(bullish),
            "bearish_count": len(bearish),
            "bullish": bullish,
            "bearish": bearish,
            "vwap_filter_pct": vwap_filter_pct,
            "note": "LTP and R3/S3 use current-month FUTURE contract. Use ?ohlc_interval=hourly or 15min for intraday-aggregated OHLC. Use ?threshold_pct=1|2|3|5 to control closeness band. Use ?vwap_filter_pct=5 to filter by 1h VWAP.",
        }
    except Exception as exc:
        raise HTTPException(status_code=500, detail=f"Failed to fetch pivot breakout: {exc}")


@router.get("/pivot-breakout/debug/{symbol}")
async def get_pivot_breakout_debug(
    symbol: str,
    ohlc_interval: str = Query("daily", description="OHLC source: 'daily', 'hourly', or '15min'"),
    threshold_pct: float = Query(
        5.0,
        ge=0.1,
        le=10.0,
        description="Percentage band for closeness to R3/S3 (e.g. 5.0 for 5%).",
    ),
):
    """
    Debug endpoint: trace pivot-breakout logic for a given symbol (e.g. NHPC).
    Returns LTP, candle used, computed R3/S3, and why it passed or failed the filter.
    Note: R3/S3 and OHLC are from the FUTURE contract, not Spot/Equity.
    """
    try:
        with engine.begin() as conn:
            row = conn.execute(
                text(
                    """
                    SELECT stock, currmth_future_symbol, currmth_future_instrument_key, currmth_future_ltp
                    FROM arbitrage_master
                    WHERE UPPER(stock) = UPPER(:symbol)
                      AND currmth_future_symbol IS NOT NULL
                      AND currmth_future_instrument_key IS NOT NULL
                      AND currmth_future_ltp IS NOT NULL
                    """
                ),
                {"symbol": symbol},
            ).mappings().first()
        if not row:
            return {"success": False, "error": f"Symbol {symbol} not found in arbitrage_master"}

        upstox = UpstoxService(settings.UPSTOX_API_KEY, settings.UPSTOX_API_SECRET)
        target_date_str, use_same_day = _pivot_breakout_candle_mode(upstox)
        live_ltp = None
        try:
            quote = upstox.get_market_quote_by_key(row["currmth_future_instrument_key"])
            if quote:
                live_price = float(quote.get("last_price", 0) or 0)
                if live_price > 0:
                    live_ltp = live_price
        except Exception:
            live_ltp = None
        ltp = float(live_ltp if live_ltp is not None else row["currmth_future_ltp"])
        interval = ohlc_interval if ohlc_interval in ("daily", "hourly", "15min") else "daily"
        ohlc = _get_prev_day_ohlc(
            upstox, row["currmth_future_instrument_key"], target_date_str, interval, use_same_day
        )
        if not ohlc:
            candles = upstox.get_historical_candles_by_instrument_key(
                row["currmth_future_instrument_key"],
                interval="minutes/15" if interval == "15min" else "hours/1" if interval == "hourly" else "days/1",
                days_back=5 if interval in ("hourly", "15min") else 15,
            ) or []
            all_dates = (
                [d["date"] for d in _aggregate_intraday_to_daily(candles)]
                if interval in ("hourly", "15min")
                else [_candle_date_ist(c) for c in candles if _candle_date_ist(c)]
            )
            return {
                "success": True,
                "symbol": row["stock"],
                "instrument_key": row["currmth_future_instrument_key"],
                "target_date_str": target_date_str,
                "use_same_day": use_same_day,
                "ltp": ltp,
                "candle_found": False,
                "available_candle_dates": sorted(set(all_dates)),
                "ohlc_source": f"{interval} (previous trading day)",
                "note": f"R3/S3 from previous day OHLC ({interval} candles).",
            }
        high, low, close, candle_date = ohlc
        pivot = (high + low + close) / 3.0
        r3 = high + 2.0 * (pivot - low)
        s3 = low - 2.0 * (high - pivot)

        band_pct = max(0.1, min(threshold_pct or 5.0, 10.0))
        band = band_pct / 100.0
        bullish_ok = (ltp <= r3) and (ltp >= (r3 * (1.0 - band)))
        bearish_ok = (ltp >= s3) and (ltp <= (s3 * (1.0 + band)))
        r3_min = r3 * (1.0 - band)
        s3_max = s3 * (1.0 + band)

        out = {
            "success": True,
            "symbol": row["stock"],
            "instrument_key": row["currmth_future_instrument_key"],
            "target_date_str": target_date_str,
            "use_same_day": use_same_day,
            "ltp": ltp,
            "candle_date": candle_date,
            "candle_ohlc": {"high": high, "low": low, "close": close},
            "r3": round(r3, 4),
            "s3": round(s3, 4),
            "bullish_range": f"LTP in [{r3_min:.2f}, {r3:.2f}]",
            "bearish_range": f"LTP in [{s3:.2f}, {s3_max:.2f}]",
            "bullish_pass": bullish_ok,
            "bearish_pass": bearish_ok,
            "ohlc_source": f"{interval} (previous trading day)",
            "ohlc_interval": interval,
            "threshold_pct": band_pct,
            "note": f"R3/S3 from previous day OHLC ({interval} candles). Use ?ohlc_interval=hourly or 15min for intraday-aggregated, and ?threshold_pct=1|2|3|5 for band.",
        }
        if interval == "daily":
            candles = upstox.get_historical_candles_by_instrument_key(
                row["currmth_future_instrument_key"], interval="days/1", days_back=15
            ) or []
            out["available_candle_dates"] = sorted(
                set(_candle_date_ist(c) for c in candles if _candle_date_ist(c))
            )
        return out
    except Exception as exc:
        raise HTTPException(status_code=500, detail=str(exc))


BATCH_SIZE = 10


@router.get("/pivot-breakout-stream")
async def get_pivot_breakout_stream(
    ohlc_interval: str = Query("daily", description="OHLC source: 'daily', 'hourly', or '15min'"),
    threshold_pct: float = Query(
        5.0,
        ge=0.1,
        le=10.0,
        description="Percentage band for closeness to R3/S3 (e.g. 5.0 for 5%).",
    ),
    vwap_filter_pct: float = Query(
        0.0,
        ge=0.0,
        le=20.0,
        description="If > 0, only show rows where LTP is within ±this % of 1h candle VWAP (e.g. 5 = within 5%).",
    ),
    segment: str = Query(
        "both",
        description="Return only 'bullish', only 'bearish', or 'both' (default). Use for tab lazy-load.",
    ),
):
    """
    Streaming pivot breakout: process in batches of 10, yield NDJSON chunks.
    Use ?ohlc_interval=hourly or 15min for intraday-aggregated OHLC.
    Use ?threshold_pct=1|2|3|5 for band of closeness to R3/S3.
    Use ?vwap_filter_pct=5 to filter by 1h VWAP.
    Use ?segment=bullish or ?segment=bearish to load only that segment (e.g. for tab focus).
    """
    seg = segment if segment in ("bullish", "bearish", "both") else "both"
    async def generate():
        try:
            with engine.begin() as conn:
                rows = conn.execute(
                    text(
                        """
                        SELECT stock, currmth_future_symbol, currmth_future_instrument_key, currmth_future_ltp
                        FROM arbitrage_master
                        WHERE currmth_future_symbol IS NOT NULL
                          AND currmth_future_instrument_key IS NOT NULL
                          AND currmth_future_ltp IS NOT NULL
                        ORDER BY stock ASC
                        """
                    )
                ).mappings().all()

            upstox = UpstoxService(settings.UPSTOX_API_KEY, settings.UPSTOX_API_SECRET)
            target_date_str, use_same_day = _pivot_breakout_candle_mode(upstox)
            interval = ohlc_interval if ohlc_interval in ("daily", "hourly", "15min") else "daily"
            band_pct = max(0.1, min(threshold_pct or 5.0, 10.0))
            all_bullish: list[dict] = []
            all_bearish: list[dict] = []
            seen_bullish: set[str] = set()
            seen_bearish: set[str] = set()

            for i in range(0, len(rows), BATCH_SIZE):
                batch = rows[i : i + BATCH_SIZE]
                b, be = _process_pivot_batch(
                    batch,
                    upstox,
                    target_date_str,
                    use_same_day,
                    ohlc_interval=interval,
                    threshold_pct=band_pct,
                    vwap_filter_pct=vwap_filter_pct,
                )
                new_b = [r for r in b if (r.get("stock") or "").strip() not in seen_bullish]
                for r in new_b:
                    seen_bullish.add((r.get("stock") or "").strip())
                new_be = [r for r in be if (r.get("stock") or "").strip() not in seen_bearish]
                for r in new_be:
                    seen_bearish.add((r.get("stock") or "").strip())
                all_bullish.extend(new_b)
                all_bearish.extend(new_be)
                chunk = {"batch": i // BATCH_SIZE, "done": False}
                if seg in ("bullish", "both") and new_b:
                    chunk["bullish"] = new_b
                if seg in ("bearish", "both") and new_be:
                    chunk["bearish"] = new_be
                yield json.dumps(chunk) + "\n"

            all_bullish.sort(key=lambda x: (x.get("difference_from_r3", 10**9), x.get("stock", "")))
            all_bearish.sort(key=lambda x: (x.get("difference_from_s3", 10**9), x.get("stock", "")))
            pivot_date = (all_bullish[0]["pivot_candle_date"] if all_bullish else all_bearish[0]["pivot_candle_date"]) if (all_bullish or all_bearish) else target_date_str
            final = {
                "done": True,
                "ltp_date": target_date_str,
                "pivot_date": pivot_date,
                "ohlc_interval": interval,
                "threshold_pct": band_pct,
                "vwap_filter_pct": vwap_filter_pct,
                "bullish_count": len(all_bullish),
                "bearish_count": len(all_bearish),
                "segment": seg,
                "note": "LTP and R3/S3 use current-month FUTURE contract. Use ?segment=bullish|bearish for tab lazy-load.",
            }
            yield json.dumps(final) + "\n"
        except Exception as exc:
            yield json.dumps({"done": True, "error": str(exc)}) + "\n"

    return StreamingResponse(
        generate(),
        media_type="application/x-ndjson",
        headers={"Cache-Control": "no-cache", "X-Accel-Buffering": "no"},
    )


@router.post("/order")
async def place_arbitrage_order(payload: dict):
    """
    Insert arbitrage order entry in arbitrage_order for a given stock_instrument_key.
    One OPEN order per stock_instrument_key is allowed.
    """
    stock_instrument_key = (payload or {}).get("stock_instrument_key")
    if not stock_instrument_key:
        raise HTTPException(status_code=400, detail="stock_instrument_key is required")

    try:
        _ensure_arbitrage_order_table()
        quantity_by_key = _load_quantity_by_instrument_key()
        with engine.begin() as conn:
            open_exists = conn.execute(
                text(
                    """
                    SELECT 1
                    FROM arbitrage_order
                    WHERE stock_instrument_key = :stock_instrument_key
                      AND trade_status = 'OPEN'
                    LIMIT 1
                    """
                ),
                {"stock_instrument_key": stock_instrument_key},
            ).fetchone()
            if open_exists:
                raise HTTPException(status_code=409, detail="Open order already exists for this stock")

            master_row = conn.execute(
                text(
                    """
                    SELECT
                        stock,
                        stock_instrument_key,
                        currmth_future_symbol,
                        currmth_future_instrument_key,
                        currmth_future_ltp,
                        nextmth_future_symbol,
                        nextmth_future_instrement_key,
                        nextmth_future_ltp
                    FROM arbitrage_master
                    WHERE stock_instrument_key = :stock_instrument_key
                    LIMIT 1
                    """
                ),
                {"stock_instrument_key": stock_instrument_key},
            ).mappings().first()
            if not master_row:
                raise HTTPException(status_code=404, detail="No arbitrage master row found for stock key")

            required_fields = [
                "stock",
                "stock_instrument_key",
                "currmth_future_symbol",
                "currmth_future_instrument_key",
                "currmth_future_ltp",
                "nextmth_future_symbol",
                "nextmth_future_instrement_key",
                "nextmth_future_ltp",
            ]
            missing = [f for f in required_fields if master_row.get(f) in (None, "")]
            if missing:
                raise HTTPException(status_code=400, detail=f"Missing required arbitrage data: {', '.join(missing)}")

            quantity = quantity_by_key.get(master_row["currmth_future_instrument_key"])
            if not quantity or quantity <= 0:
                raise HTTPException(
                    status_code=400,
                    detail="Unable to determine quantity (lot_size) from instruments JSON for current month future key",
                )

            buy_cost = float(master_row["currmth_future_ltp"])
            sell_cost = float(master_row["nextmth_future_ltp"])
            trade_entry_value = (sell_cost - buy_cost) * quantity

            conn.execute(
                text(
                    """
                    INSERT INTO arbitrage_order (
                        stock,
                        stock_instrument_key,
                        currmth_future_symbol,
                        currmth_future_instrument_key,
                        buy_cost,
                        buy_exit_cost,
                        current_future_state,
                        nextmth_future_symbol,
                        nextmth_future_instrement_key,
                        sell_cost,
                        sell_exit_cost,
                        nextmth_future_state,
                        quantity,
                        trade_status,
                        trade_entry_value,
                        trade_entry_time,
                        trade_exit_time,
                        trade_exit_value
                    ) VALUES (
                        :stock,
                        :stock_instrument_key,
                        :currmth_future_symbol,
                        :currmth_future_instrument_key,
                        :buy_cost,
                        NULL,
                        'BUY',
                        :nextmth_future_symbol,
                        :nextmth_future_instrement_key,
                        :sell_cost,
                        NULL,
                        'SELL',
                        :quantity,
                        'OPEN',
                        :trade_entry_value,
                        CURRENT_TIMESTAMP,
                        NULL,
                        NULL
                    )
                    """
                ),
                {
                    "stock": master_row["stock"],
                    "stock_instrument_key": master_row["stock_instrument_key"],
                    "currmth_future_symbol": master_row["currmth_future_symbol"],
                    "currmth_future_instrument_key": master_row["currmth_future_instrument_key"],
                    "buy_cost": buy_cost,
                    "nextmth_future_symbol": master_row["nextmth_future_symbol"],
                    "nextmth_future_instrement_key": master_row["nextmth_future_instrement_key"],
                    "sell_cost": sell_cost,
                    "quantity": quantity,
                    "trade_entry_value": trade_entry_value,
                },
            )

        return {
            "success": True,
            "message": f"Order placed for {master_row['stock']}",
            "stock_instrument_key": stock_instrument_key,
            "quantity": quantity,
            "trade_entry_value": round(trade_entry_value, 4),
        }
    except HTTPException:
        raise
    except Exception as exc:
        raise HTTPException(status_code=500, detail=f"Failed to place arbitrage order: {exc}")


@router.post("/order/exit")
async def exit_arbitrage_order(payload: dict):
    """
    Close an OPEN arbitrage order by id and stamp exit fields.
    """
    order_id = (payload or {}).get("order_id")
    if not order_id:
        raise HTTPException(status_code=400, detail="order_id is required")

    try:
        _ensure_arbitrage_order_table()
        with engine.begin() as conn:
            order_row = conn.execute(
                text(
                    """
                    SELECT
                        id,
                        stock,
                        stock_instrument_key,
                        quantity,
                        buy_cost,
                        sell_cost,
                        trade_status
                    FROM arbitrage_order
                    WHERE id = :order_id
                    LIMIT 1
                    """
                ),
                {"order_id": order_id},
            ).mappings().first()
            if not order_row:
                raise HTTPException(status_code=404, detail="Order not found")
            if (order_row.get("trade_status") or "").upper() != "OPEN":
                raise HTTPException(status_code=409, detail="Only OPEN orders can be exited")

            master_row = conn.execute(
                text(
                    """
                    SELECT
                        currmth_future_ltp,
                        nextmth_future_ltp
                    FROM arbitrage_master
                    WHERE stock_instrument_key = :stock_instrument_key
                    LIMIT 1
                    """
                ),
                {"stock_instrument_key": order_row["stock_instrument_key"]},
            ).mappings().first()

            buy_exit_cost = (
                float(master_row["currmth_future_ltp"])
                if master_row and master_row.get("currmth_future_ltp") is not None
                else float(order_row["buy_cost"])
            )
            sell_exit_cost = (
                float(master_row["nextmth_future_ltp"])
                if master_row and master_row.get("nextmth_future_ltp") is not None
                else float(order_row["sell_cost"])
            )
            quantity = int(order_row["quantity"] or 0)
            trade_exit_value = (sell_exit_cost - buy_exit_cost) * quantity

            conn.execute(
                text(
                    """
                    UPDATE arbitrage_order
                    SET
                        buy_exit_cost = :buy_exit_cost,
                        sell_exit_cost = :sell_exit_cost,
                        trade_exit_value = :trade_exit_value,
                        trade_exit_time = CURRENT_TIMESTAMP,
                        trade_status = 'CLOSED'
                    WHERE id = :order_id
                    """
                ),
                {
                    "order_id": order_id,
                    "buy_exit_cost": buy_exit_cost,
                    "sell_exit_cost": sell_exit_cost,
                    "trade_exit_value": trade_exit_value,
                },
            )

        return {
            "success": True,
            "message": f"Order exited for {order_row['stock']}",
            "order_id": int(order_id),
            "trade_exit_value": round(trade_exit_value, 4),
        }
    except HTTPException:
        raise
    except Exception as exc:
        raise HTTPException(status_code=500, detail=f"Failed to exit arbitrage order: {exc}")


@router.get("/orders")
async def get_arbitrage_orders(trade_status: str):
    """
    Fetch arbitrage orders by trade_status (OPEN/CLOSED), ordered by trade_entry_time desc.
    """
    status = (trade_status or "").strip().upper()
    if status not in {"OPEN", "CLOSED"}:
        raise HTTPException(status_code=400, detail="trade_status must be OPEN or CLOSED")

    try:
        _ensure_arbitrage_order_table()
        with engine.begin() as conn:
            rows = conn.execute(
                text(
                    """
                    SELECT
                        id,
                        stock,
                        stock_instrument_key,
                        currmth_future_symbol,
                        currmth_future_instrument_key,
                        buy_cost,
                        buy_exit_cost,
                        current_future_state,
                        nextmth_future_symbol,
                        nextmth_future_instrement_key,
                        sell_cost,
                        sell_exit_cost,
                        nextmth_future_state,
                        quantity,
                        trade_status,
                        trade_entry_value,
                        trade_entry_time,
                        trade_exit_time,
                        trade_exit_value
                    FROM arbitrage_order
                    WHERE trade_status = :trade_status
                    ORDER BY trade_entry_time DESC
                    """
                ),
                {"trade_status": status},
            ).mappings().all()

        return {
            "success": True,
            "trade_status": status,
            "count": len(rows),
            "rows": [dict(row) for row in rows],
        }
    except HTTPException:
        raise
    except Exception as exc:
        raise HTTPException(status_code=500, detail=f"Failed to fetch arbitrage orders: {exc}")

