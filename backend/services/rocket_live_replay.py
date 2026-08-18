"""Replay live compute_rocket_crash() on historical REST 10m OHLCV.

Uses the exact scorer from rocket_pre_ignition. Bars have no delta field, so
_bar_delta falls back to close-vs-open volume (same as the REST path).
Lookback/phases match Ready Now WS: session_bar_count on a per-session prefix.

Does not write to rocket_live_state / rocket_crash_event_log / layer10f tables.
Does not modify rocket_pre_ignition.py or rocket_ws_live.py.
"""
from __future__ import annotations

import logging
import time
import uuid
from datetime import date, datetime, timedelta
from typing import Any, Dict, List, Optional, Sequence, Tuple
from uuid import uuid4

import pytz
from sqlalchemy import text

from backend.config import settings
from backend.database import SessionLocal
from backend.services.arbitrage_universe import load_arbitrage_curr_mth_universe
from backend.services.rocket_layer10f import (
    _adx_series,
    adx_bucket as _adx_bucket,
    in_session,
    parse_ist,
    session_phase,
)
from backend.services.rocket_layer10f_backtest import (
    DEFAULT_FROM,
    DEFAULT_TO,
    FETCH_SLEEP_SEC,
    WARMUP_CALENDAR_DAYS,
    fetch_10m_ohlcv,
)
from backend.services.rocket_pre_ignition import compute_rocket_crash
from backend.services.upstox_service import UpstoxService

logger = logging.getLogger(__name__)

IST = pytz.timezone("Asia/Kolkata")
SCORE_MIN = 2

_EVENT_COLS = (
    "event_id",
    "symbol",
    "session_date",
    "bar_timestamp",
    "sess_bar_number",
    "rocket_score",
    "crash_score",
    "side",
    "close_at_signal",
    "volume_at_signal",
    "delta_at_signal",
    "fwd_ret_1bar",
    "fwd_ret_3bar",
    "fwd_ret_5bar",
    "fwd_mfe_5bar",
    "fwd_mae_5bar",
    "fwd_direction_correct_1bar",
    "fwd_direction_correct_3bar",
    "adx_at_signal",
    "session_phase",
)


def ensure_live_replay_tables(conn=None) -> None:
    sql_events = """
        CREATE TABLE IF NOT EXISTS rocket_live_replay_events (
            event_id UUID PRIMARY KEY,
            symbol TEXT NOT NULL,
            session_date DATE NOT NULL,
            bar_timestamp TIMESTAMPTZ NOT NULL,
            sess_bar_number INTEGER NOT NULL,
            rocket_score INTEGER NOT NULL,
            crash_score INTEGER NOT NULL,
            side TEXT NOT NULL,
            close_at_signal DOUBLE PRECISION,
            volume_at_signal DOUBLE PRECISION,
            delta_at_signal DOUBLE PRECISION,
            fwd_ret_1bar DOUBLE PRECISION,
            fwd_ret_3bar DOUBLE PRECISION,
            fwd_ret_5bar DOUBLE PRECISION,
            fwd_mfe_5bar DOUBLE PRECISION,
            fwd_mae_5bar DOUBLE PRECISION,
            fwd_direction_correct_1bar BOOLEAN,
            fwd_direction_correct_3bar BOOLEAN,
            adx_at_signal DOUBLE PRECISION,
            session_phase TEXT NOT NULL
        )
    """
    sql_ix = (
        "CREATE INDEX IF NOT EXISTS ix_rocket_live_replay_sym_dt "
        "ON rocket_live_replay_events (symbol, session_date, bar_timestamp)"
    )
    sql_sum = """
        CREATE TABLE IF NOT EXISTS rocket_live_replay_summary (
            score_bucket INTEGER NOT NULL,
            side TEXT NOT NULL,
            session_phase TEXT NOT NULL,
            adx_bucket TEXT NOT NULL,
            signal_count INTEGER NOT NULL,
            win_rate_1bar DOUBLE PRECISION,
            win_rate_3bar DOUBLE PRECISION,
            win_rate_5bar DOUBLE PRECISION,
            avg_fwd_ret_1bar DOUBLE PRECISION,
            avg_fwd_ret_3bar DOUBLE PRECISION,
            avg_fwd_ret_5bar DOUBLE PRECISION,
            avg_mfe_5bar DOUBLE PRECISION,
            avg_mae_5bar DOUBLE PRECISION,
            pct_direction_correct_1bar DOUBLE PRECISION,
            pct_direction_correct_3bar DOUBLE PRECISION,
            PRIMARY KEY (score_bucket, side, session_phase, adx_bucket)
        )
    """
    own = conn is None
    db = SessionLocal() if own else None
    try:
        c = conn if conn is not None else db.connection()
        c.execute(text(sql_events))
        c.execute(text(sql_ix))
        c.execute(text(sql_sum))
        if own:
            db.commit()
    finally:
        if own and db is not None:
            db.close()


def _ohlcv(c: Dict[str, Any]) -> Dict[str, Any]:
    return {
        "timestamp": c.get("timestamp"),
        "open": float(c.get("open") or 0),
        "high": float(c.get("high") or 0),
        "low": float(c.get("low") or 0),
        "close": float(c.get("close") or 0),
        "volume": float(c.get("volume") or 0),
    }


def _fwd_ret(closes: Sequence[float], i: int, n: int, last: int) -> Optional[float]:
    j = i + n
    if j > last:
        return None
    c0 = float(closes[i])
    if c0 == 0:
        return None
    return (float(closes[j]) - c0) / c0


def _mfe_mae(
    highs: Sequence[float],
    lows: Sequence[float],
    close_i: float,
    i: int,
    last: int,
    side: str,
    horizon: int = 5,
) -> Tuple[Optional[float], Optional[float]]:
    end = min(i + horizon, last)
    if end <= i:
        return None, None
    hi = max(float(highs[j]) for j in range(i + 1, end + 1))
    lo = min(float(lows[j]) for j in range(i + 1, end + 1))
    if side == "short":
        return close_i - lo, close_i - hi
    return hi - close_i, lo - close_i


def replay_symbol_sessions(
    candles: Sequence[Dict[str, Any]],
    symbol: str,
    date_from: date,
    date_to: date,
) -> List[Dict[str, Any]]:
    """Score each in-session bar with compute_rocket_crash(); keep score >= 2."""
    parsed: List[Dict[str, Any]] = []
    for c in candles:
        dt = parse_ist(c.get("timestamp"))
        if dt is None or not in_session(dt):
            continue
        bar = _ohlcv(c)
        bar["dt"] = dt
        parsed.append(bar)
    parsed.sort(key=lambda b: b["dt"])
    if not parsed:
        return []

    highs = [b["high"] for b in parsed]
    lows = [b["low"] for b in parsed]
    closes = [b["close"] for b in parsed]
    adx_s = _adx_series(highs, lows, closes)
    last_of_date: Dict[date, int] = {}
    for i, b in enumerate(parsed):
        last_of_date[b["dt"].date()] = i

    events: List[Dict[str, Any]] = []
    session_prefix: List[Dict[str, Any]] = []
    prev_d: Optional[date] = None
    sess_bar = 0

    for i, b in enumerate(parsed):
        d = b["dt"].date()
        if prev_d != d:
            session_prefix = []
            sess_bar = 0
            prev_d = d
        sess_bar += 1
        # No delta key — compute_rocket_crash uses close-vs-open volume fallback.
        session_prefix.append(
            {
                "open": b["open"],
                "high": b["high"],
                "low": b["low"],
                "close": b["close"],
                "volume": b["volume"],
            }
        )
        if d < date_from or d > date_to:
            continue
        scored = compute_rocket_crash(session_prefix, session_bar_count=sess_bar)
        r = int(scored.get("rocket_score") or 0)
        cscore = int(scored.get("crash_score") or 0)
        if r < SCORE_MIN and cscore < SCORE_MIN:
            continue
        if r >= cscore:
            side = "long"
            bucket_score = r
        else:
            side = "short"
            bucket_score = cscore
        last = last_of_date[d]
        ret1 = _fwd_ret(closes, i, 1, last)
        ret3 = _fwd_ret(closes, i, 3, last)
        ret5 = _fwd_ret(closes, i, 5, last)
        mfe, mae = _mfe_mae(highs, lows, b["close"], i, last, side, 5)
        d1 = None if ret1 is None else ((ret1 > 0) if side == "long" else (ret1 < 0))
        d3 = None if ret3 is None else ((ret3 > 0) if side == "long" else (ret3 < 0))
        events.append(
            {
                "event_id": str(uuid4()),
                "symbol": symbol,
                "session_date": d,
                "bar_timestamp": b["dt"],
                "sess_bar_number": sess_bar,
                "rocket_score": r,
                "crash_score": cscore,
                "side": side,
                "score_bucket": bucket_score,
                "close_at_signal": b["close"],
                "volume_at_signal": b["volume"],
                "delta_at_signal": scored.get("candle_delta"),
                "fwd_ret_1bar": ret1,
                "fwd_ret_3bar": ret3,
                "fwd_ret_5bar": ret5,
                "fwd_mfe_5bar": mfe,
                "fwd_mae_5bar": mae,
                "fwd_direction_correct_1bar": d1,
                "fwd_direction_correct_3bar": d3,
                "adx_at_signal": adx_s[i],
                "session_phase": session_phase(sess_bar),
            }
        )
    return events


def _signed(raw: Optional[float], side: str) -> Optional[float]:
    if raw is None:
        return None
    return -raw if side == "short" else raw


def build_summary_rows(events: Sequence[Dict[str, Any]]) -> List[Dict[str, Any]]:
    groups: Dict[Tuple, Dict[str, Any]] = {}

    def g(key: Tuple) -> Dict[str, Any]:
        row = groups.get(key)
        if row is None:
            row = {
                "n": 0,
                "w1": [],
                "w3": [],
                "w5": [],
                "r1": [],
                "r3": [],
                "r5": [],
                "mfe": [],
                "mae": [],
                "d1": [],
                "d3": [],
            }
            groups[key] = row
        return row

    for ev in events:
        adx_b = _adx_bucket(ev.get("adx_at_signal"))
        if adx_b is None:
            continue
        score = int(ev.get("score_bucket") or 0)
        if score not in (2, 3, 4):
            continue
        side = str(ev.get("side") or "")
        phase = str(ev.get("session_phase") or "")
        row = g((score, side, phase, adx_b))
        row["n"] += 1
        side_s = side
        for ret, wlist, rlist in (
            (ev.get("fwd_ret_1bar"), row["w1"], row["r1"]),
            (ev.get("fwd_ret_3bar"), row["w3"], row["r3"]),
            (ev.get("fwd_ret_5bar"), row["w5"], row["r5"]),
        ):
            if ret is None:
                continue
            signed = _signed(ret, side_s)
            rlist.append(signed)
            wlist.append(1.0 if signed > 0 else 0.0)
        if ev.get("fwd_mfe_5bar") is not None:
            row["mfe"].append(float(ev["fwd_mfe_5bar"]))
        if ev.get("fwd_mae_5bar") is not None:
            row["mae"].append(float(ev["fwd_mae_5bar"]))
        if ev.get("fwd_direction_correct_1bar") is not None:
            row["d1"].append(1.0 if ev["fwd_direction_correct_1bar"] else 0.0)
        if ev.get("fwd_direction_correct_3bar") is not None:
            row["d3"].append(1.0 if ev["fwd_direction_correct_3bar"] else 0.0)

    def avg(xs: List[float]) -> Optional[float]:
        return (sum(xs) / len(xs)) if xs else None

    out: List[Dict[str, Any]] = []
    for (score, side, phase, adx_b), row in sorted(groups.items()):
        out.append(
            {
                "score_bucket": score,
                "side": side,
                "session_phase": phase,
                "adx_bucket": adx_b,
                "signal_count": row["n"],
                "win_rate_1bar": avg(row["w1"]),
                "win_rate_3bar": avg(row["w3"]),
                "win_rate_5bar": avg(row["w5"]),
                "avg_fwd_ret_1bar": avg(row["r1"]),
                "avg_fwd_ret_3bar": avg(row["r3"]),
                "avg_fwd_ret_5bar": avg(row["r5"]),
                "avg_mfe_5bar": avg(row["mfe"]),
                "avg_mae_5bar": avg(row["mae"]),
                "pct_direction_correct_1bar": avg(row["d1"]),
                "pct_direction_correct_3bar": avg(row["d3"]),
            }
        )
    return out


def _event_params(ev: Dict[str, Any]) -> Dict[str, Any]:
    p = {k: ev.get(k) for k in _EVENT_COLS}
    eid = p.get("event_id")
    if isinstance(eid, str):
        p["event_id"] = uuid.UUID(eid)
    ts = p.get("bar_timestamp")
    if isinstance(ts, datetime) and ts.tzinfo is None:
        p["bar_timestamp"] = IST.localize(ts)
    return p


def persist_events_and_summary(events: Sequence[Dict[str, Any]]) -> int:
    ensure_live_replay_tables()
    db = SessionLocal()
    try:
        db.execute(text("DELETE FROM rocket_live_replay_events"))
        db.execute(text("DELETE FROM rocket_live_replay_summary"))
        if events:
            placeholders = ", ".join(f":{c}" for c in _EVENT_COLS)
            cols = ", ".join(_EVENT_COLS)
            sql = text(f"INSERT INTO rocket_live_replay_events ({cols}) VALUES ({placeholders})")
            payload = [_event_params(ev) for ev in events]
            for i in range(0, len(payload), 200):
                db.execute(sql, payload[i : i + 200])
        summary = build_summary_rows(events)
        if summary:
            db.execute(
                text(
                    "INSERT INTO rocket_live_replay_summary ("
                    "score_bucket, side, session_phase, adx_bucket, signal_count, "
                    "win_rate_1bar, win_rate_3bar, win_rate_5bar, "
                    "avg_fwd_ret_1bar, avg_fwd_ret_3bar, avg_fwd_ret_5bar, "
                    "avg_mfe_5bar, avg_mae_5bar, "
                    "pct_direction_correct_1bar, pct_direction_correct_3bar"
                    ") VALUES ("
                    ":score_bucket, :side, :session_phase, :adx_bucket, :signal_count, "
                    ":win_rate_1bar, :win_rate_3bar, :win_rate_5bar, "
                    ":avg_fwd_ret_1bar, :avg_fwd_ret_3bar, :avg_fwd_ret_5bar, "
                    ":avg_mfe_5bar, :avg_mae_5bar, "
                    ":pct_direction_correct_1bar, :pct_direction_correct_3bar)"
                ),
                summary,
            )
        db.commit()
        return len(events)
    except Exception:
        db.rollback()
        raise
    finally:
        db.close()


def fetch_summary(
    *,
    score: Optional[int] = None,
    side: Optional[str] = None,
    session_phase: Optional[str] = None,
    adx_bucket: Optional[str] = None,
) -> List[Dict[str, Any]]:
    ensure_live_replay_tables()
    clauses = []
    params: Dict[str, Any] = {}
    if score is not None:
        clauses.append("score_bucket = :score")
        params["score"] = int(score)
    if side:
        clauses.append("side = :side")
        params["side"] = side.lower()
    if session_phase:
        clauses.append("session_phase = :session_phase")
        params["session_phase"] = session_phase.lower()
    if adx_bucket:
        clauses.append("adx_bucket = :adx_bucket")
        params["adx_bucket"] = adx_bucket
    where = (" WHERE " + " AND ".join(clauses)) if clauses else ""
    sql = text(
        "SELECT score_bucket, side, session_phase, adx_bucket, signal_count, "
        "win_rate_1bar, win_rate_3bar, win_rate_5bar, "
        "avg_fwd_ret_1bar, avg_fwd_ret_3bar, avg_fwd_ret_5bar, "
        "avg_mfe_5bar, avg_mae_5bar, "
        "pct_direction_correct_1bar, pct_direction_correct_3bar "
        f"FROM rocket_live_replay_summary{where} "
        "ORDER BY score_bucket, side, session_phase, adx_bucket"
    )
    db = SessionLocal()
    try:
        return [dict(r) for r in db.execute(sql, params).mappings().all()]
    finally:
        db.close()


def run_replay(
    *,
    date_from: date = DEFAULT_FROM,
    date_to: date = DEFAULT_TO,
    symbol_limit: Optional[int] = None,
    symbols: Optional[Sequence[str]] = None,
) -> Dict[str, Any]:
    started = datetime.now(IST)
    logger.info(
        "rocket_live_replay started=%s range=%s..%s",
        started.isoformat(),
        date_from,
        date_to,
    )
    universe = load_arbitrage_curr_mth_universe()
    if symbols:
        want = {s.upper().strip() for s in symbols}
        universe = [u for u in universe if str(u.get("stock") or "").upper() in want]
    if symbol_limit is not None:
        universe = universe[: max(0, int(symbol_limit))]

    fetch_start = date_from - timedelta(days=WARMUP_CALENDAR_DAYS)
    ux = UpstoxService(settings.UPSTOX_API_KEY, settings.UPSTOX_API_SECRET)
    all_events: List[Dict[str, Any]] = []
    processed = 0
    errors = 0

    for i, u in enumerate(universe):
        stock = str(u.get("stock") or "").upper().strip()
        ikey = str(u.get("instrument_key") or "").strip()
        if not stock or not ikey:
            continue
        try:
            candles = fetch_10m_ohlcv(ux, ikey, fetch_start, date_to)
            processed += 1
            if len(candles) < 5:
                continue
            all_events.extend(replay_symbol_sessions(candles, stock, date_from, date_to))
        except Exception:
            logger.exception("rocket_live_replay failed for %s", stock)
            errors += 1
        if (i + 1) % 10 == 0:
            logger.info(
                "rocket_live_replay %s/%s symbols processed=%s events=%s",
                i + 1,
                len(universe),
                processed,
                len(all_events),
            )
        time.sleep(FETCH_SLEEP_SEC)

    n = persist_events_and_summary(all_events)
    finished = datetime.now(IST)
    elapsed = (finished - started).total_seconds()
    result = {
        "started": started.isoformat(),
        "completed": finished.isoformat(),
        "elapsed_sec": round(elapsed, 1),
        "date_from": date_from.isoformat(),
        "date_to": date_to.isoformat(),
        "universe": len(universe),
        "symbols_processed": processed,
        "errors": errors,
        "events": n,
        "summary_rows": len(build_summary_rows(all_events)),
        "scorer": "backend.services.rocket_pre_ignition.compute_rocket_crash",
        "delta": "rest_fallback_close_vs_open",
    }
    logger.info("rocket_live_replay completed %s", result)
    return result
