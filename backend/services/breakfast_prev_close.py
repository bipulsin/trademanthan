"""Breakfast prev-session close prefill job (benchmarks + arbitrage_master FUT)."""
from __future__ import annotations

import logging
import time
from datetime import date, datetime
from typing import Any, Dict, List, Optional, Tuple

from sqlalchemy import text

from backend.config import settings
from backend.database import SessionLocal, engine
from backend.services import market_holiday as mh
from backend.services.sector_movers import (
    _index_key_to_sector_label,
    normalize_sector_instrument_key,
)
from backend.services.upstox_service import UpstoxService

logger = logging.getLogger(__name__)
_THROTTLE_SEC = 0.12


WICK_NONE = "NONE"
WICK_LONG_UP = "Long_Up_Wick"
WICK_LONG_DOWN = "Long_Down_Wick"
_WICK_TIE_REL = 0.05
_WICK_VS_BODY = 0.30


def classify_daily_wick(
    open_px: float,
    high_px: float,
    low_px: float,
    close_px: float,
) -> str:
    """Classify a completed daily bar. Zero body → NONE (no epsilon)."""
    try:
        o = float(open_px)
        h = float(high_px)
        lo = float(low_px)
        c = float(close_px)
    except (TypeError, ValueError):
        return WICK_NONE
    body = abs(c - o)
    upper = h - max(o, c)
    lower = min(o, c) - lo
    if upper < 0:
        upper = 0.0
    if lower < 0:
        lower = 0.0
    wick_max = max(upper, lower)
    if wick_max <= 0:
        return WICK_NONE
    if abs(upper - lower) / wick_max <= _WICK_TIE_REL:
        return WICK_NONE
    if body <= 0:
        return WICK_NONE
    if upper > lower and upper >= _WICK_VS_BODY * body:
        return WICK_LONG_UP
    if lower > upper and lower >= _WICK_VS_BODY * body:
        return WICK_LONG_DOWN
    return WICK_NONE


def required_wick_for_live_direction(direction: str) -> Optional[str]:
    d = str(direction or "").strip().upper()
    if d == "SHORT":
        return WICK_LONG_UP
    if d == "LONG":
        return WICK_LONG_DOWN
    return None


def _rerank_live_stock_rows(kept: List[Dict[str, Any]]) -> List[Dict[str, Any]]:
    for i, st in enumerate(kept, start=1):
        st["stock_rank"] = i
        st["rank_in_sector"] = i
        labels = ["Pick 1", "Pick 2", "Watch 3rd"]
        st["rank_label"] = labels[i - 1] if i <= len(labels) else f"#{i}"
    return kept


def filter_live_stocks_by_wick(
    stocks: List[Dict[str, Any]],
    *,
    direction: str,
    wick_by_symbol: Optional[Dict[str, str]] = None,
) -> List[Dict[str, Any]]:
    """Keep only wick-confirmed stock rows; re-rank. Mismatch / unknown → drop."""
    required = required_wick_for_live_direction(direction)
    wicks = wick_by_symbol or {}
    kept: List[Dict[str, Any]] = []
    if not required:
        return kept
    for st in stocks or []:
        row = dict(st)
        sym = str(row.get("symbol") or "").strip().upper()
        wick = str(row.get("wick") or wicks.get(sym) or WICK_NONE).strip()
        row["wick"] = wick
        if wick != required:
            continue
        kept.append(row)
    return _rerank_live_stock_rows(kept)


def first_5m_color_matches_direction(open_px: Any, close_px: Any, direction: str) -> bool:
    """LONG needs green (close > open); SHORT needs red (close < open). Doji fails both."""
    try:
        o = float(open_px)
        c = float(close_px)
    except (TypeError, ValueError):
        return False
    d = str(direction or "").strip().upper()
    if d == "LONG":
        return c > o
    if d == "SHORT":
        return c < o
    return False


def filter_live_stocks_by_first_5m_color(
    stocks: List[Dict[str, Any]],
    *,
    direction: str,
) -> List[Dict[str, Any]]:
    """Keep stocks whose 9:15–9:20 5m bar color matches direction. Missing OHLC → drop."""
    d = str(direction or "").strip().upper()
    kept: List[Dict[str, Any]] = []
    if d not in ("LONG", "SHORT"):
        return kept
    for st in stocks or []:
        row = dict(st)
        if not first_5m_color_matches_direction(row.get("signal_open"), row.get("signal_close"), d):
            continue
        kept.append(row)
    return _rerank_live_stock_rows(kept)


def filter_live_stocks_by_wick_and_color(
    stocks: List[Dict[str, Any]],
    *,
    direction: str,
    wick_by_symbol: Optional[Dict[str, str]] = None,
) -> List[Dict[str, Any]]:
    """Wick AND first-5m color. Empty if none pass."""
    return filter_live_stocks_by_first_5m_color(
        filter_live_stocks_by_wick(stocks, direction=direction, wick_by_symbol=wick_by_symbol),
        direction=direction,
    )


def filter_sector_members_by_first_5m_color(
    stocks_by_sector: Dict[str, List[Dict[str, str]]],
    bars_by_symbol: Dict[str, Dict[str, Any]],
    *,
    long_side: bool,
) -> Dict[str, List[Dict[str, str]]]:
    direction = "LONG" if long_side else "SHORT"
    bars = bars_by_symbol or {}
    out: Dict[str, List[Dict[str, str]]] = {}
    for skey, members in (stocks_by_sector or {}).items():
        kept = []
        for m in members or []:
            sym = str(m.get("stock") or "").strip().upper()
            bar = bars.get(sym) or {}
            if first_5m_color_matches_direction(bar.get("open"), bar.get("close"), direction):
                kept.append(m)
        out[skey] = kept
    return out


def filter_sector_members_by_wick(
    stocks_by_sector: Dict[str, List[Dict[str, str]]],
    wick_by_symbol: Dict[str, str],
    *,
    long_side: bool,
) -> Dict[str, List[Dict[str, str]]]:
    required = WICK_LONG_DOWN if long_side else WICK_LONG_UP
    out: Dict[str, List[Dict[str, str]]] = {}
    for skey, members in (stocks_by_sector or {}).items():
        kept = []
        for m in members or []:
            sym = str(m.get("stock") or "").strip().upper()
            if str(wick_by_symbol.get(sym) or WICK_NONE) == required:
                kept.append(m)
        out[skey] = kept
    return out


def parse_daily_ohlc(candles: List[dict]) -> List[Tuple[date, float, float, float, float]]:
    out: List[Tuple[date, float, float, float, float]] = []
    for c in candles or []:
        ts = str(c.get("timestamp") or "")
        try:
            cl = float(c.get("close") or 0)
            o = float(c.get("open") or 0)
            h = float(c.get("high") or 0)
            lo = float(c.get("low") or 0)
        except (TypeError, ValueError):
            continue
        if len(ts) < 10 or cl <= 0:
            continue
        try:
            d = datetime.strptime(ts[:10], "%Y-%m-%d").date()
        except ValueError:
            continue
        out.append((d, o, h, lo, cl))
    out.sort(key=lambda x: x[0])
    return out


def parse_daily_bars(candles: List[dict]) -> List[Tuple[date, float]]:
    return [(d, cl) for d, _o, _h, _l, cl in parse_daily_ohlc(candles)]


def daily_settled_for_ist(now_ist: datetime) -> bool:
    """True when today's NSE session daily bar should be treated as complete."""
    now_ist = mh._normalize_ist(now_ist)
    if mh.should_skip_scheduled_market_jobs_ist(now_ist):
        return True
    cutoff = now_ist.replace(hour=15, minute=35, second=0, microsecond=0)
    return now_ist >= cutoff


def latest_settled_daily_ohlc(
    candles: List[dict],
    *,
    now_ist: datetime,
) -> Optional[Tuple[date, float, float, float, float]]:
    """Most recent completed session daily OHLC relative to now (IST)."""
    bars = parse_daily_ohlc(candles)
    if not bars:
        return None
    today = mh._normalize_ist(now_ist).date()
    if daily_settled_for_ist(now_ist) and bars[-1][0] == today:
        return bars[-1]
    for row in reversed(bars):
        if row[0] < today:
            return row
    return None


def latest_settled_daily_close(
    candles: List[dict],
    *,
    now_ist: datetime,
) -> Tuple[Optional[date], Optional[float]]:
    """Most recent completed session daily close relative to now (IST)."""
    row = latest_settled_daily_ohlc(candles, now_ist=now_ist)
    if not row:
        return None, None
    return row[0], row[4]


def _source_tag(trigger: str) -> str:
    return f"upstox_days/1@{trigger}"


def _upsert_benchmark_prev_close(
    instrument_key: str,
    close_date: date,
    close_px: float,
    source: str,
) -> bool:
    with engine.begin() as conn:
        n = conn.execute(
            text(
                """
                UPDATE nifty_benchmark_reference
                SET prev_session_close = :px,
                    prev_session_close_for_date = CAST(:for_date AS date),
                    prev_session_close_source = :src,
                    updated_at = NOW()
                WHERE instrument_key = :ik
                  AND (
                      prev_session_close_for_date IS NULL
                      OR prev_session_close_for_date <= CAST(:for_date AS date)
                  )
                """
            ),
            {"ik": instrument_key, "px": close_px, "for_date": close_date.isoformat(), "src": source},
        ).rowcount
    return int(n or 0) > 0


def ensure_arbitrage_master_wick_column() -> None:
    with engine.begin() as conn:
        conn.execute(text("ALTER TABLE arbitrage_master ADD COLUMN IF NOT EXISTS wick TEXT"))


def _upsert_stock_prev_close(
    stock: str,
    close_date: date,
    close_px: float,
    source: str,
    wick: str = WICK_NONE,
) -> bool:
    with engine.begin() as conn:
        n = conn.execute(
            text(
                """
                UPDATE arbitrage_master
                SET prev_session_close = :px,
                    prev_session_close_for_date = CAST(:for_date AS date),
                    prev_session_close_source = :src,
                    wick = :wick
                WHERE UPPER(TRIM(stock)) = UPPER(TRIM(:stock))
                  AND (
                      prev_session_close_for_date IS NULL
                      OR prev_session_close_for_date <= CAST(:for_date AS date)
                  )
                """
            ),
            {
                "stock": stock,
                "px": close_px,
                "for_date": close_date.isoformat(),
                "src": source,
                "wick": wick or WICK_NONE,
            },
        ).rowcount
    return int(n or 0) > 0


def load_stored_prev_closes() -> Tuple[Dict[str, float], Dict[str, float]]:
    """instrument_key → prev close (Nifty + sectors) and stock symbol → prev close."""
    bench, stocks, _wicks = load_stored_prev_closes_and_wicks()
    return bench, stocks


def load_stored_wicks() -> Dict[str, str]:
    _b, _s, wicks = load_stored_prev_closes_and_wicks()
    return wicks


def _filled_wick_row(row: Any) -> Optional[Dict[str, str]]:
    if not hasattr(row, "get"):
        return None
    wick = str(row.get("wick") or "").strip()
    if wick not in (WICK_LONG_UP, WICK_LONG_DOWN):
        return None
    future = str(row.get("future_symbol") or row.get("currmth_future_symbol") or "").strip()
    stock = str(row.get("stock") or "").strip()
    symbol = future or stock
    if not symbol:
        return None
    return {"future_symbol": symbol, "wick": wick}


def partition_filled_wicks(rows: List[Any]) -> Dict[str, List[Dict[str, str]]]:
    """Split filled wick rows; exclude NONE. Sort A–Z by future symbol."""
    down: List[Dict[str, str]] = []
    up: List[Dict[str, str]] = []
    for raw in rows or []:
        item = _filled_wick_row(raw)
        if not item:
            continue
        if item["wick"] == WICK_LONG_DOWN:
            down.append(item)
        else:
            up.append(item)
    key = lambda r: r["future_symbol"].upper()
    down.sort(key=key)
    up.sort(key=key)
    return {"long_down_wick": down, "long_up_wick": up}


def load_filled_wicks() -> Dict[str, List[Dict[str, str]]]:
    """Read-only: current-month FUT symbols with Long_*_Wick on arbitrage_master."""
    rows: List[Any] = []
    db = SessionLocal()
    try:
        rows = list(
            db.execute(
                text(
                    """
                    SELECT TRIM(currmth_future_symbol) AS future_symbol,
                           UPPER(TRIM(stock)) AS stock,
                           TRIM(wick) AS wick
                    FROM arbitrage_master
                    WHERE wick IN (:down, :up)
                    """
                ),
                {"down": WICK_LONG_DOWN, "up": WICK_LONG_UP},
            ).mappings()
        )
    except Exception as e:
        logger.warning("load_filled_wicks failed: %s", e)
    finally:
        db.close()
    return partition_filled_wicks(rows)


def sector_label_for_wicks(raw_sector_index: Any) -> str:
    """Display label from arbitrage_master.sector_index (Upstox key or alias)."""
    raw = str(raw_sector_index or "").strip()
    if not raw:
        return "Unmapped"
    key = normalize_sector_instrument_key(raw) or raw
    labels = _index_key_to_sector_label()
    label = labels.get(key) or labels.get(raw)
    if label:
        return label
    if "|" in key:
        tail = key.split("|", 1)[1].strip()
        if tail:
            return tail
    return key


def _fmt_prev_session_close(px: Any) -> str:
    try:
        f = float(px)
    except (TypeError, ValueError):
        return ""
    if f <= 0:
        return ""
    return f"{f:.2f}"


def _norm_wick_or_none(wick: Any) -> str:
    s = str(wick or "").strip()
    if s in (WICK_LONG_UP, WICK_LONG_DOWN, WICK_NONE):
        return s
    return WICK_NONE


def group_wicks_by_sector(rows: List[Any]) -> List[Dict[str, Any]]:
    """Group FUT rows by sector display label. Null/unknown wick → NONE."""
    buckets: Dict[str, Dict[str, Any]] = {}
    for raw in rows or []:
        if not hasattr(raw, "get"):
            continue
        stock = str(raw.get("stock") or "").strip().upper()
        if not stock:
            continue
        sector = sector_label_for_wicks(raw.get("sector_index"))
        wick = _norm_wick_or_none(raw.get("wick"))
        item = {
            "stock": stock,
            "prev_session_close": _fmt_prev_session_close(raw.get("prev_session_close")),
        }
        b = buckets.setdefault(
            sector,
            {
                "sector": sector,
                "long_up_wick": [],
                "long_down_wick": [],
                "none": [],
            },
        )
        if wick == WICK_LONG_UP:
            b["long_up_wick"].append(item)
        elif wick == WICK_LONG_DOWN:
            b["long_down_wick"].append(item)
        else:
            b["none"].append(stock)
    out: List[Dict[str, Any]] = []
    for name in sorted(buckets.keys(), key=str.upper):
        b = buckets[name]
        b["long_up_wick"].sort(key=lambda r: r["stock"])
        b["long_down_wick"].sort(key=lambda r: r["stock"])
        b["none"].sort()
        out.append(b)
    return out


def load_wicks_grouped_by_sector() -> List[Dict[str, Any]]:
    """All current-month FUT rows on arbitrage_master, grouped by sector_index."""
    rows: List[Any] = []
    db = SessionLocal()
    try:
        rows = list(
            db.execute(
                text(
                    """
                    SELECT UPPER(TRIM(stock)) AS stock,
                           TRIM(sector_index) AS sector_index,
                           TRIM(wick) AS wick,
                           prev_session_close
                    FROM arbitrage_master
                    WHERE stock IS NOT NULL
                      AND TRIM(stock) <> ''
                      AND currmth_future_symbol IS NOT NULL
                      AND TRIM(currmth_future_symbol) <> ''
                    """
                )
            ).mappings()
        )
    except Exception as e:
        logger.warning("load_wicks_grouped_by_sector failed: %s", e)
    finally:
        db.close()
    return group_wicks_by_sector(rows)


def load_stored_prev_closes_and_wicks() -> Tuple[Dict[str, float], Dict[str, float], Dict[str, str]]:
    """instrument_key → prev close, stock → prev close, stock → wick."""
    bench: Dict[str, float] = {}
    stocks: Dict[str, float] = {}
    wicks: Dict[str, str] = {}
    db = SessionLocal()
    try:
        for row in db.execute(
            text(
                """
                SELECT instrument_key, prev_session_close
                FROM nifty_benchmark_reference
                WHERE prev_session_close IS NOT NULL AND prev_session_close > 0
                """
            )
        ).mappings():
            ik = str(row.get("instrument_key") or "").strip()
            px = float(row.get("prev_session_close") or 0)
            if ik and px > 0:
                bench[ik] = px
        for row in db.execute(
            text(
                """
                SELECT UPPER(TRIM(stock)) AS stock, prev_session_close, wick
                FROM arbitrage_master
                WHERE stock IS NOT NULL
                """
            )
        ).mappings():
            sym = str(row.get("stock") or "").strip().upper()
            if not sym:
                continue
            px = float(row.get("prev_session_close") or 0)
            if px > 0:
                stocks[sym] = px
            wick = str(row.get("wick") or "").strip()
            wicks[sym] = wick if wick in (WICK_LONG_UP, WICK_LONG_DOWN, WICK_NONE) else WICK_NONE
    except Exception as e:
        logger.warning("load_stored_prev_closes failed: %s", e)
    finally:
        db.close()
    return bench, stocks, wicks


def run_breakfast_prev_close_job(*, trigger: str = "manual") -> Dict[str, Any]:
    """Idempotent UPSERT of prev_session_close + wick on benchmarks + arbitrage_master FUT rows."""
    now = mh._normalize_ist(None)
    if mh.should_skip_scheduled_market_jobs_ist(now) and trigger.startswith("scheduled"):
        return {"ok": True, "skipped": "holiday_or_weekend", "trigger": trigger}

    try:
        ensure_arbitrage_master_wick_column()
    except Exception as e:
        logger.warning("ensure wick column: %s", e)

    ux = UpstoxService(settings.UPSTOX_API_KEY, settings.UPSTOX_API_SECRET)
    ux.reload_token_from_storage()
    source = _source_tag(trigger)

    db = SessionLocal()
    try:
        benchmarks = db.execute(
            text("SELECT instrument_key FROM nifty_benchmark_reference ORDER BY instrument_key")
        ).scalars().all()
        stocks = db.execute(
            text(
                """
                SELECT TRIM(stock) AS stock, TRIM(currmth_future_instrument_key) AS fut_key
                FROM arbitrage_master
                WHERE stock IS NOT NULL
                  AND currmth_future_instrument_key IS NOT NULL
                  AND TRIM(currmth_future_instrument_key) <> ''
                ORDER BY stock
                """
            )
        ).mappings().all()
    finally:
        db.close()

    bench_updated = bench_skipped = 0
    for ik in benchmarks:
        key = str(ik or "").strip()
        if not key:
            continue
        try:
            candles = ux.get_historical_candles_by_instrument_key(
                key, interval="days/1", days_back=12, range_end_date=now.date()
            ) or []
            d, px = latest_settled_daily_close(candles, now_ist=now)
            if d is None or px is None:
                bench_skipped += 1
                continue
            if _upsert_benchmark_prev_close(key, d, float(px), source):
                bench_updated += 1
            else:
                bench_skipped += 1
        except Exception as e:
            bench_skipped += 1
            logger.warning("breakfast prev_close benchmark %s: %s", key, e)
        time.sleep(_THROTTLE_SEC)

    stock_updated = stock_skipped = 0
    for row in stocks:
        sym = str(row.get("stock") or "").strip().upper()
        fut_key = str(row.get("fut_key") or "").strip()
        if not sym or not fut_key:
            stock_skipped += 1
            continue
        try:
            candles = ux.get_historical_candles_by_instrument_key(
                fut_key, interval="days/1", days_back=12, range_end_date=now.date()
            ) or []
            ohlc = latest_settled_daily_ohlc(candles, now_ist=now)
            if ohlc is None:
                stock_skipped += 1
                continue
            d, o, h, lo, px = ohlc
            wick = classify_daily_wick(o, h, lo, px)
            if _upsert_stock_prev_close(sym, d, float(px), source, wick=wick):
                stock_updated += 1
            else:
                stock_skipped += 1
        except Exception as e:
            stock_skipped += 1
            logger.debug("breakfast prev_close stock %s: %s", sym, e)
        time.sleep(_THROTTLE_SEC)

    out = {
        "ok": True,
        "trigger": trigger,
        "source": source,
        "benchmarks": {"updated": bench_updated, "skipped": bench_skipped, "total": len(benchmarks)},
        "stocks": {"updated": stock_updated, "skipped": stock_skipped, "total": len(stocks)},
    }
    logger.info("breakfast_prev_close_job: %s", out)
    return out
