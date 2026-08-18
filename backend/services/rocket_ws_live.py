"""Live Rocket/Crash state from the existing Upstox v3 websocket feed.

Builds 5/10/15m OHLCV + signed-volume candles from ticks already ingested by
``upstox_market_feed``. Does not open a second websocket.
"""
from __future__ import annotations

import logging
import threading
import time
from collections import deque
from datetime import datetime, timedelta
from typing import Any, Dict, List, Optional, Tuple

import pytz
from sqlalchemy import text

from backend.database import SessionLocal, engine
from backend.services.rocket_pre_ignition import compute_rocket_crash

logger = logging.getLogger(__name__)
IST = pytz.timezone("Asia/Kolkata")

TIMEFRAMES = (5, 10, 15)
DEFAULT_TF = 10
MAX_COMPLETED = 30
SESSION_OPEN = (9, 15)
SESSION_CLOSE = (15, 30)
STALE_SEC = 120.0
PERSIST_DEBOUNCE_SEC = 2.0
MAP_REFRESH_SEC = 300.0
SOURCE = "upstox_websocket_live"

_LOCK = threading.Lock()
# (symbol, tf) -> live book
_BOOKS: Dict[Tuple[str, int], Dict[str, Any]] = {}
# instrument_key -> symbol
_IK_TO_SYM: Dict[str, str] = {}
_MAP_LOADED_MONO = 0.0
_LAST_PERSIST: Dict[Tuple[str, int], float] = {}
_TICK_PREV: Dict[str, Dict[str, Any]] = {}
_SCHEMA_READY = False
_FEED_SEEN = False

_CREATE_STATE_SQL = """
CREATE TABLE IF NOT EXISTS rocket_live_state (
    symbol TEXT NOT NULL,
    timeframe INTEGER NOT NULL,
    candle_start TIMESTAMPTZ,
    candle_end TIMESTAMPTZ,
    rocket_score INTEGER NOT NULL DEFAULT 0,
    rocket_signals TEXT,
    rocket_label TEXT,
    crash_score INTEGER NOT NULL DEFAULT 0,
    crash_signals TEXT,
    crash_label TEXT,
    active_side TEXT,
    candle_delta DOUBLE PRECISION,
    session_cum_delta DOUBLE PRECISION,
    volume DOUBLE PRECISION,
    ema5 DOUBLE PRECISION,
    atr10 DOUBLE PRECISION,
    lookback_used INTEGER,
    session_bar_number INTEGER,
    candle_status TEXT,
    data_quality_flag TEXT,
    last_update TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    source TEXT NOT NULL DEFAULT 'upstox_websocket_live',
    PRIMARY KEY (symbol, timeframe)
);
CREATE INDEX IF NOT EXISTS ix_rocket_live_state_upd
    ON rocket_live_state (last_update DESC);
CREATE INDEX IF NOT EXISTS ix_rocket_live_state_rocket
    ON rocket_live_state (rocket_score DESC);
CREATE INDEX IF NOT EXISTS ix_rocket_live_state_crash
    ON rocket_live_state (crash_score DESC);
"""

_CREATE_EVENT_SQL = """
CREATE TABLE IF NOT EXISTS rocket_crash_event_log (
    event_id BIGSERIAL PRIMARY KEY,
    symbol TEXT NOT NULL,
    timeframe INTEGER NOT NULL,
    event_timestamp TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    candle_timestamp TIMESTAMPTZ,
    side TEXT NOT NULL,
    score INTEGER NOT NULL,
    s1_flag BOOLEAN NOT NULL DEFAULT FALSE,
    s2_flag BOOLEAN NOT NULL DEFAULT FALSE,
    s3_flag BOOLEAN NOT NULL DEFAULT FALSE,
    s4_flag BOOLEAN NOT NULL DEFAULT FALSE,
    close DOUBLE PRECISION,
    high DOUBLE PRECISION,
    low DOUBLE PRECISION,
    volume DOUBLE PRECISION,
    candle_delta DOUBLE PRECISION,
    cumulative_delta_session DOUBLE PRECISION,
    ema5 DOUBLE PRECISION,
    atr10 DOUBLE PRECISION,
    lookback_used INTEGER,
    session_bar_number INTEGER,
    candle_status TEXT,
    source TEXT NOT NULL DEFAULT 'upstox_websocket_live',
    data_quality_flag TEXT,
    notes TEXT
);
CREATE INDEX IF NOT EXISTS ix_rocket_crash_event_sym_ts
    ON rocket_crash_event_log (symbol, timeframe, event_timestamp DESC);
CREATE INDEX IF NOT EXISTS ix_rocket_crash_event_side_score
    ON rocket_crash_event_log (side, score, event_timestamp DESC);
"""


def ensure_rocket_live_tables() -> None:
    global _SCHEMA_READY
    if _SCHEMA_READY:
        return
    with engine.begin() as conn:
        for stmt in (_CREATE_STATE_SQL + _CREATE_EVENT_SQL).strip().split(";"):
            s = stmt.strip()
            if s:
                conn.execute(text(s))
    _SCHEMA_READY = True


def _normalize_ik(key: str) -> str:
    return (key or "").strip().replace(":", "|")


def _refresh_symbol_map(*, force: bool = False) -> None:
    global _MAP_LOADED_MONO, _IK_TO_SYM
    now = time.monotonic()
    if not force and _IK_TO_SYM and (now - _MAP_LOADED_MONO) < MAP_REFRESH_SEC:
        return
    db = SessionLocal()
    try:
        rows = db.execute(
            text(
                """
                SELECT UPPER(TRIM(stock)) AS symbol, currmth_future_instrument_key
                FROM arbitrage_master
                WHERE currmth_future_instrument_key IS NOT NULL
                  AND TRIM(currmth_future_instrument_key) <> ''
                """
            )
        ).fetchall()
        mapping: Dict[str, str] = {}
        for r in rows:
            sym = str(r[0] or "").strip().upper()
            ik = _normalize_ik(str(r[1] or ""))
            if sym and ik:
                mapping[ik] = sym
        if mapping:
            _IK_TO_SYM = mapping
            _MAP_LOADED_MONO = now
            logger.info("rocket_ws_live: mapped %s current-month futures", len(mapping))
    except Exception as exc:
        logger.warning("rocket_ws_live: symbol map load failed: %s", exc)
    finally:
        db.close()


def arbitrage_currmth_instrument_keys() -> List[str]:
    _refresh_symbol_map()
    return list(_IK_TO_SYM.keys())


def _session_open(now: datetime) -> datetime:
    return now.replace(hour=SESSION_OPEN[0], minute=SESSION_OPEN[1], second=0, microsecond=0)


def _in_session(now: datetime) -> bool:
    if now.weekday() >= 5:
        return False
    open_dt = _session_open(now)
    close_dt = now.replace(
        hour=SESSION_CLOSE[0], minute=SESSION_CLOSE[1], second=0, microsecond=0
    )
    return open_dt <= now <= close_dt


def bucket_bounds(now: datetime, tf_min: int) -> Tuple[datetime, datetime]:
    """NSE session-aligned bucket (09:15 origin)."""
    open_dt = _session_open(now)
    if now < open_dt:
        start = open_dt
    else:
        elapsed_min = int((now - open_dt).total_seconds() // 60)
        start = open_dt + timedelta(minutes=(elapsed_min // tf_min) * tf_min)
    return start, start + timedelta(minutes=tf_min)


def classify_signed_volume(
    ltp: float,
    qty: int,
    *,
    best_bid: Optional[float],
    best_ask: Optional[float],
    last_px: Optional[float],
    last_dir: int,
) -> Tuple[float, int]:
    """Aggressive buy/sell via bid/ask when possible; else tick-rule."""
    if qty <= 0 or ltp <= 0:
        return 0.0, last_dir
    q = float(qty)
    if best_ask and best_ask > 0 and ltp >= best_ask - 1e-9:
        return q, 1
    if best_bid and best_bid > 0 and ltp <= best_bid + 1e-9:
        return -q, -1
    if last_px is None:
        return 0.0, last_dir
    if ltp > last_px:
        return q, 1
    if ltp < last_px:
        return -q, -1
    if last_dir:
        return last_dir * q, last_dir
    return 0.0, last_dir


def _empty_forming(start: datetime, end: datetime, px: float) -> Dict[str, Any]:
    return {
        "open": px,
        "high": px,
        "low": px,
        "close": px,
        "volume": 0.0,
        "delta": 0.0,
        "start": start,
        "end": end,
        "forming": True,
    }


def _new_book(symbol: str, tf: int, session_date: str) -> Dict[str, Any]:
    return {
        "symbol": symbol,
        "timeframe": tf,
        "session_date": session_date,
        "forming": None,
        "completed": deque(maxlen=MAX_COMPLETED),
        "session_cum_delta": 0.0,
        "last_px": None,
        "last_dir": 0,
        "last_ltq": None,
        "quality": "ok",
        "last_rocket": 0,
        "last_crash": 0,
        "scored": empty_score_payload(),
        "updated_at": None,
    }


def empty_score_payload() -> Dict[str, Any]:
    return {
        "rocket_score": 0,
        "rocket_signals": [],
        "rocket_label": "",
        "crash_score": 0,
        "crash_signals": [],
        "crash_label": "",
        "active_side": "",
        "lookback_used": 0,
        "session_bar_number": 0,
        "ema5": None,
        "atr10": None,
        "candle_delta": 0.0,
        "cumulative_delta": 0.0,
    }


def mark_feed_reconnect() -> None:
    """Call when the Upstox websocket (re)connects so quality is not silent."""
    global _FEED_SEEN
    if not _FEED_SEEN:
        _FEED_SEEN = True
        logger.info("rocket_ws_live: websocket connected")
        return
    with _LOCK:
        for book in _BOOKS.values():
            book["quality"] = "reconnect_gap"
    logger.warning("rocket_ws_live: feed reconnect — marking data_quality_flag=reconnect_gap")


def on_upstox_tick(
    instrument_key: str,
    *,
    ltp: Optional[float],
    ltq: int = 0,
    best_bid: Optional[float] = None,
    best_ask: Optional[float] = None,
    now: Optional[datetime] = None,
) -> None:
    if ltp is None or float(ltp) <= 0:
        return
    now = now or datetime.now(IST)
    if now.tzinfo is None:
        now = IST.localize(now)
    else:
        now = now.astimezone(IST)
    if not _in_session(now):
        return
    _refresh_symbol_map()
    ik = _normalize_ik(instrument_key)
    symbol = _IK_TO_SYM.get(ik)
    if not symbol:
        return
    px = float(ltp)
    qty = int(ltq or 0)
    session_date = now.strftime("%Y-%m-%d")

    with _LOCK:
        prev_tick = _TICK_PREV.get(symbol) or {}
        last_px = prev_tick.get("last_px")
        last_dir = int(prev_tick.get("last_dir") or 0)
        last_ltq = prev_tick.get("last_ltq")
        duplicate = last_px is not None and abs(px - float(last_px)) < 1e-12 and qty == last_ltq
        signed, new_dir = (0.0, last_dir)
        if not duplicate:
            signed, new_dir = classify_signed_volume(
                px, qty, best_bid=best_bid, best_ask=best_ask, last_px=last_px, last_dir=last_dir
            )
        _TICK_PREV[symbol] = {"last_px": px, "last_dir": new_dir, "last_ltq": qty}

        persist_jobs: List[Tuple[str, int]] = []
        event_jobs: List[Dict[str, Any]] = []
        for tf in TIMEFRAMES:
            key = (symbol, tf)
            book = _BOOKS.get(key)
            if book is None or book.get("session_date") != session_date:
                book = _new_book(symbol, tf, session_date)
                _BOOKS[key] = book
            book["last_px"] = px
            book["last_dir"] = new_dir
            start, end = bucket_bounds(now, tf)
            forming = book.get("forming")
            rolled = False
            if forming is not None:
                f_start = forming.get("start")
                if f_start != start:
                    forming["forming"] = False
                    book["completed"].append(forming)
                    book["session_cum_delta"] = float(book.get("session_cum_delta") or 0.0) + float(
                        forming.get("delta") or 0.0
                    )
                    rolled = True
                    logger.debug(
                        "rocket_ws_live: candle rollover %s %sm start=%s",
                        symbol,
                        tf,
                        f_start,
                    )
                    forming = None
            if forming is None:
                forming = _empty_forming(start, end, px)
                book["forming"] = forming
            forming["high"] = max(float(forming["high"]), px)
            forming["low"] = min(float(forming["low"]), px)
            forming["close"] = px
            if not duplicate and qty > 0:
                forming["volume"] = float(forming.get("volume") or 0.0) + float(qty)
                forming["delta"] = float(forming.get("delta") or 0.0) + float(signed)
            book["updated_at"] = now

            bars = list(book["completed"]) + [dict(forming, forming=True)]
            session_n = len(book["completed"]) + 1
            scored = compute_rocket_crash(bars, session_bar_count=session_n)
            prev_r = int(book.get("last_rocket") or 0)
            prev_c = int(book.get("last_crash") or 0)
            r_now = int(scored.get("rocket_score") or 0)
            c_now = int(scored.get("crash_score") or 0)
            book["scored"] = scored
            book["last_rocket"] = r_now
            book["last_crash"] = c_now

            candle_status = "confirmed" if rolled else "forming"
            if r_now >= 3 and (rolled or prev_r < 3):
                event_jobs.append(_event_payload(book, "bullish_rocket", r_now, candle_status, now))
            if c_now >= 3 and (rolled or prev_c < 3):
                event_jobs.append(_event_payload(book, "bearish_crash", c_now, candle_status, now))
            persist_jobs.append(key)

    for key in persist_jobs:
        _maybe_persist_state(key)
    for ev in event_jobs:
        _append_event(ev)


def _event_payload(
    book: Dict[str, Any],
    side: str,
    score: int,
    candle_status: str,
    now: datetime,
) -> Dict[str, Any]:
    forming = book.get("forming") or {}
    scored = book.get("scored") or {}
    sigs = scored.get("rocket_signals") if side == "bullish_rocket" else scored.get("crash_signals")
    sigs = list(sigs or [])
    if side == "bullish_rocket":
        s1, s2, s3, s4 = (
            "seller_failure" in sigs,
            "cumdelta_lead" in sigs,
            "shallower_dips" in sigs,
            "volume_coil_wakeup" in sigs,
        )
    else:
        s1, s2, s3, s4 = (
            "buyer_failure" in sigs,
            "cumdelta_lead_down" in sigs,
            "falling_highs" in sigs,
            "volume_coil_wakeup" in sigs,
        )
    return {
        "symbol": book["symbol"],
        "timeframe": int(book["timeframe"]),
        "event_timestamp": now,
        "candle_timestamp": forming.get("start"),
        "side": side,
        "score": int(score),
        "s1_flag": s1,
        "s2_flag": s2,
        "s3_flag": s3,
        "s4_flag": s4,
        "close": forming.get("close"),
        "high": forming.get("high"),
        "low": forming.get("low"),
        "volume": forming.get("volume"),
        "candle_delta": forming.get("delta"),
        "cumulative_delta_session": float(book.get("session_cum_delta") or 0.0)
        + float(forming.get("delta") or 0.0),
        "ema5": scored.get("ema5"),
        "atr10": scored.get("atr10"),
        "lookback_used": scored.get("lookback_used"),
        "session_bar_number": scored.get("session_bar_number"),
        "candle_status": candle_status,
        "source": SOURCE,
        "data_quality_flag": book.get("quality") or "ok",
        "notes": None,
    }


def _maybe_persist_state(key: Tuple[str, int]) -> None:
    now_m = time.monotonic()
    last = float(_LAST_PERSIST.get(key) or 0.0)
    if now_m - last < PERSIST_DEBOUNCE_SEC:
        return
    _LAST_PERSIST[key] = now_m
    with _LOCK:
        book = _BOOKS.get(key)
        if not book:
            return
        snapshot = _state_row(book)
    try:
        ensure_rocket_live_tables()
        db = SessionLocal()
        try:
            db.execute(
                text(
                    """
                    INSERT INTO rocket_live_state (
                        symbol, timeframe, candle_start, candle_end,
                        rocket_score, rocket_signals, rocket_label,
                        crash_score, crash_signals, crash_label, active_side,
                        candle_delta, session_cum_delta, volume, ema5, atr10,
                        lookback_used, session_bar_number, candle_status,
                        data_quality_flag, last_update, source
                    ) VALUES (
                        :symbol, :timeframe, :candle_start, :candle_end,
                        :rocket_score, :rocket_signals, :rocket_label,
                        :crash_score, :crash_signals, :crash_label, :active_side,
                        :candle_delta, :session_cum_delta, :volume, :ema5, :atr10,
                        :lookback_used, :session_bar_number, :candle_status,
                        :data_quality_flag, NOW(), :source
                    )
                    ON CONFLICT (symbol, timeframe) DO UPDATE SET
                        candle_start = EXCLUDED.candle_start,
                        candle_end = EXCLUDED.candle_end,
                        rocket_score = EXCLUDED.rocket_score,
                        rocket_signals = EXCLUDED.rocket_signals,
                        rocket_label = EXCLUDED.rocket_label,
                        crash_score = EXCLUDED.crash_score,
                        crash_signals = EXCLUDED.crash_signals,
                        crash_label = EXCLUDED.crash_label,
                        active_side = EXCLUDED.active_side,
                        candle_delta = EXCLUDED.candle_delta,
                        session_cum_delta = EXCLUDED.session_cum_delta,
                        volume = EXCLUDED.volume,
                        ema5 = EXCLUDED.ema5,
                        atr10 = EXCLUDED.atr10,
                        lookback_used = EXCLUDED.lookback_used,
                        session_bar_number = EXCLUDED.session_bar_number,
                        candle_status = EXCLUDED.candle_status,
                        data_quality_flag = EXCLUDED.data_quality_flag,
                        last_update = NOW(),
                        source = EXCLUDED.source
                    """
                ),
                snapshot,
            )
            db.commit()
        except Exception as exc:
            db.rollback()
            logger.warning("rocket_ws_live: state persist failed %s %s: %s", key[0], key[1], exc)
        finally:
            db.close()
    except Exception as exc:
        logger.warning("rocket_ws_live: state persist outer failed: %s", exc)


def _state_row(book: Dict[str, Any]) -> Dict[str, Any]:
    forming = book.get("forming") or {}
    scored = book.get("scored") or empty_score_payload()
    return {
        "symbol": book["symbol"],
        "timeframe": int(book["timeframe"]),
        "candle_start": forming.get("start"),
        "candle_end": forming.get("end"),
        "rocket_score": int(scored.get("rocket_score") or 0),
        "rocket_signals": ",".join(scored.get("rocket_signals") or []),
        "rocket_label": scored.get("rocket_label") or "",
        "crash_score": int(scored.get("crash_score") or 0),
        "crash_signals": ",".join(scored.get("crash_signals") or []),
        "crash_label": scored.get("crash_label") or "",
        "active_side": scored.get("active_side") or "",
        "candle_delta": float(forming.get("delta") or 0.0),
        "session_cum_delta": float(book.get("session_cum_delta") or 0.0)
        + float(forming.get("delta") or 0.0),
        "volume": float(forming.get("volume") or 0.0),
        "ema5": scored.get("ema5"),
        "atr10": scored.get("atr10"),
        "lookback_used": scored.get("lookback_used") or 0,
        "session_bar_number": scored.get("session_bar_number") or 0,
        "candle_status": "forming",
        "data_quality_flag": book.get("quality") or "ok",
        "source": SOURCE,
    }


def _append_event(ev: Dict[str, Any]) -> None:
    try:
        ensure_rocket_live_tables()
        db = SessionLocal()
        try:
            db.execute(
                text(
                    """
                    INSERT INTO rocket_crash_event_log (
                        symbol, timeframe, event_timestamp, candle_timestamp, side, score,
                        s1_flag, s2_flag, s3_flag, s4_flag,
                        close, high, low, volume, candle_delta, cumulative_delta_session,
                        ema5, atr10, lookback_used, session_bar_number, candle_status,
                        source, data_quality_flag, notes
                    ) VALUES (
                        :symbol, :timeframe, :event_timestamp, :candle_timestamp, :side, :score,
                        :s1_flag, :s2_flag, :s3_flag, :s4_flag,
                        :close, :high, :low, :volume, :candle_delta, :cumulative_delta_session,
                        :ema5, :atr10, :lookback_used, :session_bar_number, :candle_status,
                        :source, :data_quality_flag, :notes
                    )
                    """
                ),
                ev,
            )
            db.commit()
            logger.info(
                "rocket_ws_live: threshold event %s %s %sm score=%s status=%s",
                ev.get("side"),
                ev.get("symbol"),
                ev.get("timeframe"),
                ev.get("score"),
                ev.get("candle_status"),
            )
        except Exception as exc:
            db.rollback()
            logger.warning("rocket_ws_live: event log write failed: %s", exc)
        finally:
            db.close()
    except Exception as exc:
        logger.warning("rocket_ws_live: event log outer failed: %s", exc)


def get_live_10m(symbol: str) -> Optional[Dict[str, Any]]:
    """In-memory 10m live state if fresh; else latest DB row."""
    sym = (symbol or "").strip().upper()
    if not sym:
        return None
    with _LOCK:
        book = _BOOKS.get((sym, DEFAULT_TF))
        if book and book.get("updated_at"):
            age = (datetime.now(IST) - book["updated_at"]).total_seconds()
            if age <= STALE_SEC:
                row = _state_row(book)
                row["age_sec"] = age
                return row
    try:
        ensure_rocket_live_tables()
        db = SessionLocal()
        try:
            r = db.execute(
                text(
                    """
                    SELECT * FROM rocket_live_state
                    WHERE symbol = :s AND timeframe = :tf
                    """
                ),
                {"s": sym, "tf": DEFAULT_TF},
            ).mappings().first()
            if not r:
                return None
            row = dict(r)
            upd = row.get("last_update")
            if upd is not None:
                if getattr(upd, "tzinfo", None) is None:
                    upd = IST.localize(upd)
                age = (datetime.now(IST) - upd.astimezone(IST)).total_seconds()
                if age > STALE_SEC:
                    return None
                row["age_sec"] = age
            return row
        finally:
            db.close()
    except Exception:
        return None


def overlay_live_rocket_crash(stock: Dict[str, Any]) -> bool:
    """Overwrite rocket/crash fields from live WS state when the book is usable."""
    live = get_live_10m(str(stock.get("symbol") or ""))
    if not live:
        return False
    lookback = int(live.get("lookback_used") or 0)
    r_score = int(live.get("rocket_score") or 0)
    c_score = int(live.get("crash_score") or 0)
    # Cold WS book (just reconnected / first ticks) must not wipe REST 20-bar scores.
    if lookback < 8 and r_score <= 0 and c_score <= 0:
        return False
    stock["rocket_score"] = r_score
    stock["rocket_signals"] = _split_sigs(live.get("rocket_signals"))
    stock["rocket_label"] = live.get("rocket_label") or (f"🚀 {r_score}/4" if r_score >= 1 else "")
    stock["crash_score"] = c_score
    stock["crash_signals"] = _split_sigs(live.get("crash_signals"))
    stock["crash_label"] = live.get("crash_label") or (f"💥 {c_score}/4" if c_score >= 1 else "")
    stock["rocket_active_side"] = live.get("active_side") or ""
    stock["rocket_source"] = SOURCE
    return True


def _split_sigs(v: Any) -> List[str]:
    if v is None or v == "":
        return []
    if isinstance(v, list):
        return [str(x) for x in v if x]
    return [p.strip() for p in str(v).split(",") if p.strip()]


def load_live_10m_map() -> Dict[str, Dict[str, Any]]:
    """All fresh 10m live rows keyed by symbol (memory, then DB fill)."""
    out: Dict[str, Dict[str, Any]] = {}
    now = datetime.now(IST)
    with _LOCK:
        for (sym, tf), book in _BOOKS.items():
            if tf != DEFAULT_TF or not book.get("updated_at"):
                continue
            age = (now - book["updated_at"]).total_seconds()
            if age <= STALE_SEC:
                row = _state_row(book)
                row["age_sec"] = age
                out[sym] = row
    try:
        ensure_rocket_live_tables()
        db = SessionLocal()
        try:
            rows = db.execute(
                text(
                    """
                    SELECT * FROM rocket_live_state
                    WHERE timeframe = :tf
                      AND last_update > NOW() - INTERVAL '2 minutes'
                    """
                ),
                {"tf": DEFAULT_TF},
            ).mappings().all()
            for r in rows:
                sym = str(r.get("symbol") or "").upper()
                if sym and sym not in out:
                    out[sym] = dict(r)
        finally:
            db.close()
    except Exception:
        pass
    return out


def ensure_rocket_feed_running() -> None:
    """Start Upstox WS on current-month futures from arbitrage_master (OI pause independent)."""
    from backend.config import settings
    from backend.services.upstox_market_feed import ensure_market_feed_running

    if not getattr(settings, "UPSTOX_MARKET_FEED_ENABLED", True):
        return
    keys = arbitrage_currmth_instrument_keys()
    if not keys:
        logger.warning("rocket_ws_live: no arbitrage_master current-month keys")
        return
    ensure_market_feed_running(keys)
    logger.info("rocket_ws_live: ensured feed for %s current-month futures", len(keys))
