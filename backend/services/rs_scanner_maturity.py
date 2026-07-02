"""Move Maturity / Fatigue layer for the Relative Strength Scanner.

Pure classification helpers (unit-tested) plus DB persistence for daily history rows.
"""
from __future__ import annotations

import logging
from datetime import datetime, timedelta
from typing import Any, Dict, List, Optional, Tuple

import pytz
from sqlalchemy import text

from backend.database import SessionLocal
from backend.services.kavach_engine import RANKING_BEARISH, RANKING_BULLISH
from backend.services.smart_futures_picker.indicators import wilder_atr_14

logger = logging.getLogger(__name__)

IST = pytz.timezone("Asia/Kolkata")

MATURITY_FRESH = "FRESH"
MATURITY_CONTINUING = "CONTINUING"
MATURITY_EXTENDED = "EXTENDED"
MATURITY_STRETCHED = "STRETCHED"

MATURITY_CLIMACTIC = "CLIMACTIC"

MATURITY_SORT_ORDER = {
    MATURITY_FRESH: 0,
    MATURITY_CONTINUING: 1,
    MATURITY_EXTENDED: 2,
    MATURITY_STRETCHED: 3,
    MATURITY_CLIMACTIC: 4,
}

_DIRECTION_BULLISH = "bullish"
_DIRECTION_BEARISH = "bearish"


def today_ist() -> str:
    return datetime.now(IST).strftime("%Y-%m-%d")


def _f(v: Any, default: float = 0.0) -> float:
    try:
        return float(v)
    except (TypeError, ValueError):
        return default


def _parse_ist_date(ts: Any) -> Optional[str]:
    if not ts:
        return None
    try:
        dt = datetime.fromisoformat(str(ts).replace("Z", "+00:00"))
    except Exception:
        try:
            dt = datetime.strptime(str(ts)[:19], "%Y-%m-%dT%H:%M:%S")
        except Exception:
            return None
    dt = dt.astimezone(IST) if dt.tzinfo else IST.localize(dt)
    return dt.strftime("%Y-%m-%d")


def direction_from_ranking(ranking_type: Optional[str]) -> str:
    return _DIRECTION_BEARISH if (ranking_type or "").upper() == RANKING_BEARISH else _DIRECTION_BULLISH


def is_climactic(
    consecutive_days: int,
    range_vs_atr_ratio: float,
    rs_pct: float,
    peer_rs_pcts: List[float],
) -> bool:
    """Day-1 exceptional prior move — mean-reversion risk."""
    if consecutive_days > 1:
        return False
    if range_vs_atr_ratio >= 2.0:
        return True
    peers = [abs(x) for x in peer_rs_pcts if x is not None]
    if len(peers) >= 2:
        peers_sorted = sorted(peers)
        median = peers_sorted[len(peers_sorted) // 2]
        if median > 0 and abs(rs_pct) >= 2.0 * median:
            return True
    return False


def classify_maturity_tag(consecutive_days: int, range_vs_atr_ratio: float) -> str:
    """Assign maturity tag; STRETCHED overrides at 4+ consecutive days on the list."""
    if consecutive_days <= 1:
        tag = MATURITY_FRESH
    elif range_vs_atr_ratio >= 1.5:
        tag = MATURITY_EXTENDED
    else:
        tag = MATURITY_CONTINUING
    if consecutive_days >= 4:
        tag = MATURITY_STRETCHED
    return tag


def compute_consecutive_days(
    yesterday_same_direction: bool, yesterday_consecutive: int
) -> int:
    if yesterday_same_direction:
        return max(1, yesterday_consecutive) + 1
    return 1


def _sorted_daily_candles(candles: List[Dict]) -> List[Dict]:
    dated: List[Tuple[str, Dict]] = []
    for c in candles:
        d = _parse_ist_date(c.get("timestamp"))
        if d:
            dated.append((d, c))
    dated.sort(key=lambda x: x[0])
    return [c for _, c in dated]


def compute_yesterday_range_metrics(
    daily_candles: List[Dict],
    *,
    as_of_date: Optional[str] = None,
) -> Tuple[float, float, float]:
    """Return (daily_range_pct, atr14_pct, range_vs_atr_ratio) for yesterday's session.

  Uses the last *completed* daily bar before ``as_of_date`` (defaults to today IST).
  When insufficient history, returns (0.0, 0.0, 0.0).
    """
    candles = _sorted_daily_candles(daily_candles)
    if len(candles) < 2:
        return 0.0, 0.0, 0.0

    ref = as_of_date or today_ist()
    dates = [_parse_ist_date(c.get("timestamp")) for c in candles]
    # Drop today's in-progress bar when present.
    if dates and dates[-1] == ref:
        candles = candles[:-1]
        dates = dates[:-1]
    if len(candles) < 2:
        return 0.0, 0.0, 0.0

    y_candle = candles[-1]
    prev_candle = candles[-2]
    y_high = _f(y_candle.get("high"))
    y_low = _f(y_candle.get("low"))
    y_close = _f(y_candle.get("close"))
    prev_close = _f(prev_candle.get("close"))
    if prev_close <= 0 or y_close <= 0:
        return 0.0, 0.0, 0.0

    daily_range_pct = (y_high - y_low) / prev_close * 100.0

    highs = [_f(c.get("high")) for c in candles]
    lows = [_f(c.get("low")) for c in candles]
    closes = [_f(c.get("close")) for c in candles]
    atr14 = wilder_atr_14(highs, lows, closes)
    if atr14 is None or atr14 <= 0:
        return daily_range_pct, 0.0, 0.0

    atr14_pct = atr14 / y_close * 100.0
    if atr14_pct <= 0:
        return daily_range_pct, 0.0, 0.0

    range_vs_atr_ratio = daily_range_pct / atr14_pct
    return daily_range_pct, atr14_pct, range_vs_atr_ratio


def build_maturity_record(
    *,
    symbol: str,
    direction: str,
    rs_pct: float,
    yesterday_row: Optional[Dict[str, Any]],
    daily_range_pct: float,
    atr14_pct: float,
    range_vs_atr_ratio: float,
    session_date: Optional[str] = None,
) -> Dict[str, Any]:
    """Build one rs_scanner_history row dict for persistence."""
    y_dir = (yesterday_row or {}).get("direction")
    y_consec = int((yesterday_row or {}).get("consecutive_days_on_list") or 0)
    same_dir = bool(yesterday_row) and y_dir == direction
    consecutive = compute_consecutive_days(same_dir, y_consec)
    maturity_tag = classify_maturity_tag(consecutive, range_vs_atr_ratio)
    return {
        "date": session_date or today_ist(),
        "symbol": symbol,
        "direction": direction,
        "rs_pct": round(rs_pct, 2),
        "daily_range_pct": round(daily_range_pct, 4),
        "atr14_pct": round(atr14_pct, 4),
        "range_vs_atr_ratio": round(range_vs_atr_ratio, 2),
        "consecutive_days_on_list": consecutive,
        "maturity_tag": maturity_tag,
    }


def _yesterday_date(session_date: str) -> str:
    d = datetime.strptime(session_date, "%Y-%m-%d").date()
    return (d - timedelta(days=1)).isoformat()


_YESTERDAY_SQL = text(
    """
    SELECT symbol, direction, consecutive_days_on_list, maturity_tag
    FROM rs_scanner_history
    WHERE date = :yd AND symbol = :sym
    LIMIT 1
    """
)

_UPSERT_HISTORY_SQL = text(
    """
    INSERT INTO rs_scanner_history (
        date, symbol, direction, rs_pct, daily_range_pct, atr14_pct,
        range_vs_atr_ratio, consecutive_days_on_list, maturity_tag
    ) VALUES (
        :date, :symbol, :direction, :rs_pct, :daily_range_pct, :atr14_pct,
        :range_vs_atr_ratio, :consecutive_days_on_list, :maturity_tag
    )
    ON CONFLICT (date, symbol) DO UPDATE SET
        direction = EXCLUDED.direction,
        rs_pct = EXCLUDED.rs_pct,
        daily_range_pct = EXCLUDED.daily_range_pct,
        atr14_pct = EXCLUDED.atr14_pct,
        range_vs_atr_ratio = EXCLUDED.range_vs_atr_ratio,
        consecutive_days_on_list = EXCLUDED.consecutive_days_on_list,
        maturity_tag = EXCLUDED.maturity_tag
    """
)

_TODAY_HISTORY_SQL = text(
    """
    SELECT symbol, direction, rs_pct, daily_range_pct, atr14_pct,
           range_vs_atr_ratio, consecutive_days_on_list, maturity_tag
    FROM rs_scanner_history
    WHERE date = :d
    """
)


def enrich_ranked_with_maturity(
    ranked: List[Dict[str, Any]],
    upstox: Any,
    *,
    session_date: Optional[str] = None,
) -> List[Dict[str, Any]]:
    """Compute maturity fields for Top-5 rows and persist to rs_scanner_history."""
    sd = session_date or today_ist()
    yd = _yesterday_date(sd)
    db = SessionLocal()
    enriched: List[Dict[str, Any]] = []
    daily_cache: Dict[str, List[Dict]] = {}

    try:
        by_dir: Dict[str, List[Dict[str, Any]]] = {_DIRECTION_BULLISH: [], _DIRECTION_BEARISH: []}
        for row in ranked:
            by_dir[direction_from_ranking(row.get("ranking_type"))].append(row)

        for row in ranked:
            symbol = row.get("symbol") or ""
            direction = direction_from_ranking(row.get("ranking_type"))
            instrument_key = row.get("instrument_key") or ""

            yesterday_row = None
            if symbol:
                prev = db.execute(_YESTERDAY_SQL, {"yd": yd, "sym": symbol}).fetchone()
                if prev:
                    yesterday_row = {
                        "direction": prev.direction,
                        "consecutive_days_on_list": prev.consecutive_days_on_list,
                        "maturity_tag": prev.maturity_tag,
                    }

            daily_range_pct = atr14_pct = range_vs_atr_ratio = 0.0
            if instrument_key:
                if instrument_key not in daily_cache:
                    try:
                        daily_cache[instrument_key] = upstox.get_historical_candles_by_instrument_key(
                            instrument_key, interval="days/1", days_back=30
                        ) or []
                    except Exception as exc:
                        logger.debug("daily candles for %s failed: %s", symbol, exc)
                        daily_cache[instrument_key] = []
                daily_range_pct, atr14_pct, range_vs_atr_ratio = compute_yesterday_range_metrics(
                    daily_cache[instrument_key], as_of_date=sd
                )

            record = build_maturity_record(
                symbol=symbol,
                direction=direction,
                rs_pct=_f(row.get("relative_strength")),
                yesterday_row=yesterday_row,
                daily_range_pct=daily_range_pct,
                atr14_pct=atr14_pct,
                range_vs_atr_ratio=range_vs_atr_ratio,
                session_date=sd,
            )
            peers = [
                _f(r.get("relative_strength"))
                for r in by_dir.get(direction, [])
                if (r.get("symbol") or "") != symbol
            ]
            if is_climactic(
                record["consecutive_days_on_list"],
                range_vs_atr_ratio,
                _f(row.get("relative_strength")),
                peers,
            ):
                record["maturity_tag"] = MATURITY_CLIMACTIC
            db.execute(_UPSERT_HISTORY_SQL, record)

            out = dict(row)
            out.update(
                {
                    "maturity_tag": record["maturity_tag"],
                    "consecutive_days_on_list": record["consecutive_days_on_list"],
                    "range_vs_atr_ratio": record["range_vs_atr_ratio"],
                }
            )
            enriched.append(out)

        db.commit()
    except Exception:
        db.rollback()
        raise
    finally:
        db.close()

    return enriched


def load_today_maturity_map(session_date: Optional[str] = None) -> Dict[str, Dict[str, Any]]:
    """Return {symbol: maturity fields} for today's history rows."""
    sd = session_date or today_ist()
    db = SessionLocal()
    try:
        rows = db.execute(_TODAY_HISTORY_SQL, {"d": sd}).fetchall()
    finally:
        db.close()
    out: Dict[str, Dict[str, Any]] = {}
    for r in rows:
        out[r.symbol] = {
            "maturity_tag": r.maturity_tag,
            "consecutive_days_on_list": int(r.consecutive_days_on_list or 1),
            "range_vs_atr_ratio": round(_f(r.range_vs_atr_ratio), 2),
        }
    return out


def default_maturity_fields() -> Dict[str, Any]:
    return {
        "maturity_tag": MATURITY_FRESH,
        "consecutive_days_on_list": 1,
        "range_vs_atr_ratio": 0.0,
    }
