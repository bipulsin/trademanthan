"""Shadow-only: VWAP close-confirmation for READY episodes (research).

Flag (sticky within a READY episode):
  LONG:  True once a *closed* 10m candle closes above session VWAP
  SHORT: True once a *closed* 10m candle closes below session VWAP

Never gates READY / Take Trade / Confidence / Trade Score. No UI required.
"""
from __future__ import annotations

import logging
from datetime import datetime, time, timedelta
from typing import Any, Dict, List, Optional, Sequence

import pytz
from sqlalchemy import text

from backend.database import engine

logger = logging.getLogger(__name__)
IST = pytz.timezone("Asia/Kolkata")
TABLE = "kavach_vwap_close_confirm_shadow"
_ENSURED = False

# Session end for fizzle bar-count when still open near EOD.
_SESSION_END = time(15, 30)


_CREATE = f"""
CREATE TABLE IF NOT EXISTS {TABLE} (
    id BIGSERIAL PRIMARY KEY,
    session_date DATE NOT NULL,
    symbol TEXT NOT NULL,
    direction TEXT NOT NULL,
    ts_ready_first_flagged TIMESTAMPTZ NOT NULL,
    price_at_ready DOUBLE PRECISION,
    vwap_at_ready DOUBLE PRECISION,
    vwap_close_confirmed BOOLEAN NOT NULL DEFAULT FALSE,
    ts_vwap_close_confirmed TIMESTAMPTZ,
    price_at_vwap_confirm DOUBLE PRECISION,
    candles_to_confirm INTEGER,
    bars_since_ready_at_eod_or_expiry INTEGER,
    episode_ended_at TIMESTAMPTZ,
    episode_end_reason TEXT,
    source TEXT DEFAULT 'live',
    updated_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    UNIQUE (session_date, symbol, ts_ready_first_flagged)
)
"""

_UPSERT = text(
    f"""
    INSERT INTO {TABLE} (
        session_date, symbol, direction, ts_ready_first_flagged,
        price_at_ready, vwap_at_ready,
        vwap_close_confirmed, ts_vwap_close_confirmed, price_at_vwap_confirm,
        candles_to_confirm, bars_since_ready_at_eod_or_expiry,
        episode_ended_at, episode_end_reason, source, updated_at
    ) VALUES (
        CAST(:session_date AS date), :symbol, :direction, :ts_ready_first_flagged,
        :price_at_ready, :vwap_at_ready,
        :vwap_close_confirmed, :ts_vwap_close_confirmed, :price_at_vwap_confirm,
        :candles_to_confirm, :bars_since_ready_at_eod_or_expiry,
        :episode_ended_at, :episode_end_reason, :source, NOW()
    )
    ON CONFLICT (session_date, symbol, ts_ready_first_flagged) DO UPDATE SET
        direction = EXCLUDED.direction,
        price_at_ready = COALESCE({TABLE}.price_at_ready, EXCLUDED.price_at_ready),
        vwap_at_ready = COALESCE({TABLE}.vwap_at_ready, EXCLUDED.vwap_at_ready),
        vwap_close_confirmed = {TABLE}.vwap_close_confirmed OR EXCLUDED.vwap_close_confirmed,
        ts_vwap_close_confirmed = COALESCE(
            {TABLE}.ts_vwap_close_confirmed, EXCLUDED.ts_vwap_close_confirmed
        ),
        price_at_vwap_confirm = COALESCE(
            {TABLE}.price_at_vwap_confirm, EXCLUDED.price_at_vwap_confirm
        ),
        candles_to_confirm = COALESCE(
            {TABLE}.candles_to_confirm, EXCLUDED.candles_to_confirm
        ),
        bars_since_ready_at_eod_or_expiry = COALESCE(
            EXCLUDED.bars_since_ready_at_eod_or_expiry,
            {TABLE}.bars_since_ready_at_eod_or_expiry
        ),
        episode_ended_at = COALESCE(
            {TABLE}.episode_ended_at, EXCLUDED.episode_ended_at
        ),
        episode_end_reason = COALESCE(
            {TABLE}.episode_end_reason, EXCLUDED.episode_end_reason
        ),
        source = EXCLUDED.source,
        updated_at = NOW()
    """
)

_LOAD_OPEN = text(
    f"""
    SELECT id, session_date, symbol, direction, ts_ready_first_flagged,
           price_at_ready, vwap_at_ready, vwap_close_confirmed,
           ts_vwap_close_confirmed, price_at_vwap_confirm, candles_to_confirm,
           bars_since_ready_at_eod_or_expiry, episode_ended_at, episode_end_reason
    FROM {TABLE}
    WHERE session_date = CAST(:d AS date)
      AND episode_ended_at IS NULL
    """
)


def ensure_vwap_close_confirm_table() -> None:
    global _ENSURED
    if _ENSURED:
        return
    with engine.begin() as conn:
        conn.execute(text(_CREATE))
        conn.execute(
            text(
                f"CREATE INDEX IF NOT EXISTS idx_{TABLE}_session "
                f"ON {TABLE} (session_date DESC, symbol)"
            )
        )
        conn.execute(
            text(
                f"CREATE INDEX IF NOT EXISTS idx_{TABLE}_open "
                f"ON {TABLE} (session_date, symbol) "
                f"WHERE episode_ended_at IS NULL"
            )
        )
    _ENSURED = True


def is_ready_like(state: Optional[str]) -> bool:
    s = (state or "").upper()
    return s in ("READY", "READY(RECHECK)") or s.startswith("READY")


def compute_vwap_close_confirmed(
    *,
    direction: str,
    close: Optional[float],
    vwap: Optional[float],
) -> bool:
    """Closed-bar confirmation (not wick/touch)."""
    d = (direction or "").upper()
    try:
        c, v = float(close), float(vwap)
    except (TypeError, ValueError):
        return False
    if d == "LONG":
        return c > v
    if d == "SHORT":
        return c < v
    return False


def _as_ist(dt: Any) -> Optional[datetime]:
    if dt is None:
        return None
    if isinstance(dt, str):
        dt = datetime.fromisoformat(dt.replace("Z", "+00:00"))
    if not isinstance(dt, datetime):
        return None
    if dt.tzinfo is None:
        return IST.localize(dt)
    return dt.astimezone(IST)


def _f(x: Any) -> Optional[float]:
    try:
        if x is None:
            return None
        return float(x)
    except (TypeError, ValueError):
        return None


def closed_10m_snapshot(
    candles: Sequence[Dict[str, Any]],
    *,
    direction: str,
    nifty_pct: float = 0.0,
    now: Optional[datetime] = None,
) -> Optional[Dict[str, Any]]:
    """Last *closed* 10m close + session VWAP (+ bar_end)."""
    if not candles:
        return None
    try:
        from backend.services.kavach_10m import metrics_from_10m_candles
        from backend.services.relative_strength_scanner import RANKING_BEARISH, RANKING_BULLISH

        ranking = RANKING_BEARISH if (direction or "").upper() == "SHORT" else RANKING_BULLISH
        m = metrics_from_10m_candles(
            list(candles),
            ranking_type=ranking,
            nifty_pct=float(nifty_pct or 0.0),
            now=now,
            include_forming=False,
        )
        if not m:
            return None
        return {
            "close": _f(m.get("price") if m.get("bar_close") is None else m.get("bar_close"))
            or _f(m.get("price")),
            "vwap": _f(m.get("vwap")),
            "bar_end": _as_ist(m.get("bar_evaluated_at")),
        }
    except Exception:
        logger.debug("closed_10m_snapshot failed", exc_info=True)
        return None


def count_closed_10m_since(
    candles: Sequence[Dict[str, Any]],
    *,
    since: datetime,
    until: Optional[datetime] = None,
) -> Optional[int]:
    """Count closed 10m bars with bar_end in (since, until] (until=now if None)."""
    try:
        from backend.services.kavach_10m import (
            _current_and_prev_day_close,
            _parse_ist,
            _sorted_candles,
            aggregate_10m_bars,
            last_closed_10m_pair_end_idx,
        )

        since_i = _as_ist(since)
        until_i = _as_ist(until) or datetime.now(IST)
        if since_i is None:
            return None
        c5 = _sorted_candles(list(candles))
        split = _current_and_prev_day_close(c5)
        if split is None:
            return None
        _, _, first_today = split
        pair_end = last_closed_10m_pair_end_idx(c5, now=until_i)
        if pair_end < first_today:
            return 0
        today = c5[first_today : pair_end + 1]
        bars = aggregate_10m_bars(today)
        n = 0
        for b in bars:
            end = b.get("bar_end")
            if end is None:
                ts = _parse_ist(b.get("timestamp"))
                end = ts + timedelta(minutes=5) if ts else None
            end = _as_ist(end)
            if end is None:
                continue
            if since_i < end <= until_i:
                n += 1
        return n
    except Exception:
        logger.debug("count_closed_10m_since failed", exc_info=True)
        return None


def _episode_ready_active(stock: Dict[str, Any]) -> bool:
    """READY / Take-Trade card still in an active episode."""
    if is_ready_like(stock.get("trade_state")):
        return True
    if is_ready_like(stock.get("_pre_stack_state")):
        return True
    # Dwell soft-hold: card still visible as READY episode for research.
    if stock.get("card_visible") and stock.get("ready_visible_since"):
        return True
    return False


def _direction_of(stock: Dict[str, Any]) -> str:
    d = (stock.get("direction") or stock.get("lock_direction") or "").upper()
    if d in ("LONG", "SHORT"):
        return d
    return "LONG"


def _price_vwap_at_ready(stock: Dict[str, Any], levels: Optional[Dict[str, Any]]) -> tuple:
    levels = levels or {}
    price = _f(stock.get("live_candle_price") or levels.get("price") or stock.get("ltp"))
    vwap = _f(stock.get("live_candle_vwap") or levels.get("vwap"))
    return price, vwap


def _load_open_episodes(db, session_date: str) -> Dict[str, Dict[str, Any]]:
    ensure_vwap_close_confirm_table()
    out: Dict[str, Dict[str, Any]] = {}
    try:
        for r in db.execute(_LOAD_OPEN, {"d": session_date}).mappings():
            out[(r["symbol"] or "").upper()] = dict(r)
    except Exception:
        logger.exception("load open vwap_close_confirm episodes failed")
    return out


def _persist_episode(db, row: Dict[str, Any]) -> None:
    try:
        db.execute(
            _UPSERT,
            {
                "session_date": row["session_date"],
                "symbol": row["symbol"],
                "direction": row["direction"],
                "ts_ready_first_flagged": row["ts_ready_first_flagged"],
                "price_at_ready": row.get("price_at_ready"),
                "vwap_at_ready": row.get("vwap_at_ready"),
                "vwap_close_confirmed": bool(row.get("vwap_close_confirmed")),
                "ts_vwap_close_confirmed": row.get("ts_vwap_close_confirmed"),
                "price_at_vwap_confirm": row.get("price_at_vwap_confirm"),
                "candles_to_confirm": row.get("candles_to_confirm"),
                "bars_since_ready_at_eod_or_expiry": row.get(
                    "bars_since_ready_at_eod_or_expiry"
                ),
                "episode_ended_at": row.get("episode_ended_at"),
                "episode_end_reason": row.get("episode_end_reason"),
                "source": row.get("source") or "live",
            },
        )
    except Exception:
        logger.exception("persist vwap_close_confirm failed %s", row.get("symbol"))


def update_vwap_close_confirm_shadow(
    db,
    *,
    session_date: str,
    stocks: List[Dict[str, Any]],
    candle_cache: Dict[str, List[Any]],
    levels_map: Optional[Dict[str, Dict[str, Any]]] = None,
    nifty_pct: float = 0.0,
    now: Optional[datetime] = None,
    source: str = "live",
) -> Dict[str, int]:
    """Upsert sticky VWAP-close-confirm state for READY episodes. Shadow only."""
    ensure_vwap_close_confirm_table()
    now = _as_ist(now) or datetime.now(IST)
    levels_map = levels_map or {}
    open_eps = _load_open_episodes(db, session_date)
    stats = {"started": 0, "confirmed": 0, "ended": 0, "touched": 0}

    seen: set = set()
    for stock in stocks:
        sym = (stock.get("symbol") or "").upper()
        if not sym:
            continue
        seen.add(sym)
        active = _episode_ready_active(stock)
        direction = _direction_of(stock)
        candles = candle_cache.get(sym) or []
        levels = levels_map.get(sym) or {}
        ep = open_eps.get(sym)

        if active:
            # New episode or direction flip → start fresh (end prior if needed).
            if ep is not None and (ep.get("direction") or "").upper() != direction:
                bars = count_closed_10m_since(
                    candles, since=ep["ts_ready_first_flagged"], until=now
                )
                if not ep.get("vwap_close_confirmed"):
                    ep["bars_since_ready_at_eod_or_expiry"] = bars
                ep["episode_ended_at"] = now
                ep["episode_end_reason"] = "direction_change"
                _persist_episode(db, ep)
                stats["ended"] += 1
                ep = None
                open_eps.pop(sym, None)

            if ep is None:
                price, vwap = _price_vwap_at_ready(stock, levels)
                # Prefer sticky ready_visible_since when dwell already started the clock.
                ts_ready = _as_ist(stock.get("ready_visible_since")) or now
                ep = {
                    "session_date": session_date,
                    "symbol": sym,
                    "direction": direction,
                    "ts_ready_first_flagged": ts_ready,
                    "price_at_ready": price,
                    "vwap_at_ready": vwap,
                    "vwap_close_confirmed": False,
                    "ts_vwap_close_confirmed": None,
                    "price_at_vwap_confirm": None,
                    "candles_to_confirm": None,
                    "bars_since_ready_at_eod_or_expiry": None,
                    "episode_ended_at": None,
                    "episode_end_reason": None,
                    "source": source,
                }
                open_eps[sym] = ep
                stats["started"] += 1

            # Sticky confirm from last *closed* 10m bar.
            if not ep.get("vwap_close_confirmed"):
                snap = closed_10m_snapshot(
                    candles, direction=direction, nifty_pct=nifty_pct, now=now
                )
                if snap and snap.get("close") is not None and snap.get("vwap") is not None:
                    bar_end = snap.get("bar_end")
                    ready_ts = _as_ist(ep["ts_ready_first_flagged"])
                    # Only count closes at/after the READY episode started.
                    if bar_end is not None and ready_ts is not None and bar_end > ready_ts:
                        if compute_vwap_close_confirmed(
                            direction=direction,
                            close=snap["close"],
                            vwap=snap["vwap"],
                        ):
                            ep["vwap_close_confirmed"] = True
                            ep["ts_vwap_close_confirmed"] = bar_end
                            ep["price_at_vwap_confirm"] = snap["close"]
                            ep["candles_to_confirm"] = count_closed_10m_since(
                                candles, since=ready_ts, until=bar_end
                            )
                            stats["confirmed"] += 1

            # Near EOD while still open+unconfirmed: stamp fizzle bar count (sticky).
            if (
                not ep.get("vwap_close_confirmed")
                and ep.get("bars_since_ready_at_eod_or_expiry") is None
                and now.time() >= _SESSION_END
            ):
                ep["bars_since_ready_at_eod_or_expiry"] = count_closed_10m_since(
                    candles, since=ep["ts_ready_first_flagged"], until=now
                )

            _persist_episode(db, ep)
            stats["touched"] += 1
            # Attach slim snapshot for consistency-log join (research only).
            stock["vwap_close_confirm_shadow"] = {
                "vwap_close_confirmed": bool(ep.get("vwap_close_confirmed")),
                "ts_ready_first_flagged": str(ep.get("ts_ready_first_flagged")),
                "ts_vwap_close_confirmed": (
                    str(ep["ts_vwap_close_confirmed"])
                    if ep.get("ts_vwap_close_confirmed")
                    else None
                ),
                "candles_to_confirm": ep.get("candles_to_confirm"),
                "price_at_ready": ep.get("price_at_ready"),
                "vwap_at_ready": ep.get("vwap_at_ready"),
                "price_at_vwap_confirm": ep.get("price_at_vwap_confirm"),
            }
            continue

        # Not active READY — end open episode if any.
        if ep is not None:
            if not ep.get("vwap_close_confirmed"):
                ep["bars_since_ready_at_eod_or_expiry"] = count_closed_10m_since(
                    candles, since=ep["ts_ready_first_flagged"], until=now
                )
            ep["episode_ended_at"] = now
            reason = "left_ready"
            st = (stock.get("trade_state") or "").upper()
            if "EXPIRED" in st:
                reason = "expired"
            elif now.time() >= _SESSION_END:
                reason = "eod"
            ep["episode_end_reason"] = reason
            _persist_episode(db, ep)
            open_eps.pop(sym, None)
            stats["ended"] += 1

    # Open episodes for symbols missing from this enrich pass → end as left_ready.
    for sym, ep in list(open_eps.items()):
        if sym in seen:
            continue
        candles = candle_cache.get(sym) or []
        if not ep.get("vwap_close_confirmed"):
            ep["bars_since_ready_at_eod_or_expiry"] = count_closed_10m_since(
                candles, since=ep["ts_ready_first_flagged"], until=now
            )
        ep["episode_ended_at"] = now
        ep["episode_end_reason"] = "left_ready"
        _persist_episode(db, ep)
        stats["ended"] += 1

    return stats
