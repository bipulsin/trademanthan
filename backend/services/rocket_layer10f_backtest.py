"""Historical Layer 10f backtest: fetch 10m futures OHLCV, score, persist.

Does not write to rocket_live_state / rocket_crash_event_log / live scorers.
"""
from __future__ import annotations

import logging
import time
import uuid
from datetime import date, datetime, time as dt_time, timedelta
from typing import Any, Dict, List, Optional, Sequence, Tuple

import pytz
from sqlalchemy import text

from backend.config import settings
from backend.database import SessionLocal
from backend.services.arbitrage_universe import load_arbitrage_curr_mth_universe
from backend.services.rocket_layer10f import (
    attach_forward_outcomes,
    events_from_scored,
    in_session,
    parse_ist,
    score_bars,
    adx_bucket as _adx_bucket,
)
from backend.services.upstox_service import UpstoxService

logger = logging.getLogger(__name__)

IST = pytz.timezone("Asia/Kolkata")
DEFAULT_FROM = date(2026, 7, 17)
DEFAULT_TO = date(2026, 8, 18)
WARMUP_CALENDAR_DAYS = 14
CHUNK_DAYS = 30
FETCH_SLEEP_SEC = 0.08

_EVENT_COLS = (
    "event_id",
    "symbol",
    "session_date",
    "candle_time",
    "sess_bar_number",
    "score_long",
    "score_short",
    "dominant_side",
    "s1",
    "s2",
    "s3",
    "s4",
    "bs1",
    "bs2",
    "bs3",
    "bs4",
    "close",
    "open",
    "high",
    "low",
    "volume",
    "cum_delta_session",
    "delta_slope",
    "atr5",
    "atr14",
    "atr10",
    "ema5",
    "squeeze",
    "prior_quiet",
    "over_extended",
    "under_extended",
    "fwd_ret_1bar",
    "fwd_ret_3bar",
    "fwd_ret_5bar",
    "fwd_ret_10bar",
    "fwd_mfe_5bar",
    "fwd_mae_5bar",
    "fwd_direction_correct_1bar",
    "fwd_direction_correct_3bar",
    "adx_at_signal",
    "session_phase",
)


def ensure_backtest_tables(conn=None) -> None:
    sql_events = """
        CREATE TABLE IF NOT EXISTS rocket_backtest_events (
            event_id UUID PRIMARY KEY,
            symbol TEXT NOT NULL,
            session_date DATE NOT NULL,
            candle_time TIMESTAMPTZ NOT NULL,
            sess_bar_number INTEGER NOT NULL,
            score_long INTEGER NOT NULL,
            score_short INTEGER NOT NULL,
            dominant_side TEXT NOT NULL,
            s1 BOOLEAN NOT NULL,
            s2 BOOLEAN NOT NULL,
            s3 BOOLEAN NOT NULL,
            s4 BOOLEAN NOT NULL,
            bs1 BOOLEAN NOT NULL,
            bs2 BOOLEAN NOT NULL,
            bs3 BOOLEAN NOT NULL,
            bs4 BOOLEAN NOT NULL,
            close DOUBLE PRECISION,
            open DOUBLE PRECISION,
            high DOUBLE PRECISION,
            low DOUBLE PRECISION,
            volume DOUBLE PRECISION,
            cum_delta_session DOUBLE PRECISION,
            delta_slope DOUBLE PRECISION,
            atr5 DOUBLE PRECISION,
            atr14 DOUBLE PRECISION,
            atr10 DOUBLE PRECISION,
            ema5 DOUBLE PRECISION,
            squeeze BOOLEAN NOT NULL,
            prior_quiet BOOLEAN NOT NULL,
            over_extended BOOLEAN NOT NULL,
            under_extended BOOLEAN NOT NULL,
            fwd_ret_1bar DOUBLE PRECISION,
            fwd_ret_3bar DOUBLE PRECISION,
            fwd_ret_5bar DOUBLE PRECISION,
            fwd_ret_10bar DOUBLE PRECISION,
            fwd_mfe_5bar DOUBLE PRECISION,
            fwd_mae_5bar DOUBLE PRECISION,
            fwd_direction_correct_1bar BOOLEAN,
            fwd_direction_correct_3bar BOOLEAN,
            adx_at_signal DOUBLE PRECISION,
            session_phase TEXT NOT NULL
        )
    """
    sql_ix = (
        "CREATE INDEX IF NOT EXISTS ix_rocket_bt_events_sym_dt "
        "ON rocket_backtest_events (symbol, session_date, candle_time)"
    )
    sql_sum = """
        CREATE TABLE IF NOT EXISTS rocket_backtest_summary (
            score_bucket INTEGER NOT NULL,
            side TEXT NOT NULL,
            session_phase TEXT NOT NULL,
            adx_bucket TEXT NOT NULL,
            squeeze BOOLEAN NOT NULL,
            signal_count INTEGER NOT NULL,
            win_rate_1bar DOUBLE PRECISION,
            win_rate_3bar DOUBLE PRECISION,
            win_rate_5bar DOUBLE PRECISION,
            avg_fwd_ret_3bar DOUBLE PRECISION,
            avg_mfe_5bar DOUBLE PRECISION,
            avg_mae_5bar DOUBLE PRECISION,
            avg_score_when_correct DOUBLE PRECISION,
            avg_score_when_wrong DOUBLE PRECISION,
            PRIMARY KEY (score_bucket, side, session_phase, adx_bucket, squeeze)
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


def _ux() -> UpstoxService:
    return UpstoxService(settings.UPSTOX_API_KEY, settings.UPSTOX_API_SECRET)


def _merge_candles(chunks: Sequence[Sequence[Dict[str, Any]]]) -> List[Dict[str, Any]]:
    by_ts: Dict[str, Dict[str, Any]] = {}
    for chunk in chunks:
        for c in chunk or []:
            ts = str(c.get("timestamp") or c.get("candle_start") or "")
            if not ts:
                continue
            by_ts[ts] = c
    out = list(by_ts.values())
    out.sort(key=lambda c: str(c.get("timestamp") or c.get("candle_start") or ""))
    return out


def _agg5_to_records(candles_5m: Sequence[Dict[str, Any]]) -> List[Dict[str, Any]]:
    """09:15-aligned 5m → 10m (OHLC first/max/min/last, volume sum)."""
    buckets: Dict[datetime, List[Dict[str, Any]]] = {}
    for c in candles_5m or []:
        dt = parse_ist(c.get("timestamp"))
        if dt is None or not in_session(dt):
            continue
        open_dt = dt.replace(hour=9, minute=15, second=0, microsecond=0)
        minutes = int((dt - open_dt).total_seconds() // 60)
        if minutes < 0:
            continue
        start = open_dt + timedelta(minutes=(minutes // 10) * 10)
        buckets.setdefault(start, []).append(c)
    out: List[Dict[str, Any]] = []
    for start in sorted(buckets):
        rows = sorted(buckets[start], key=lambda r: parse_ist(r.get("timestamp")) or start)
        is_last = start.time() == dt_time(15, 25)
        if len(rows) < 2 and not is_last:
            continue
        try:
            o = float(rows[0]["open"])
            h = max(float(r["high"]) for r in rows)
            lo = min(float(r["low"]) for r in rows)
            cl = float(rows[-1]["close"])
            vol = sum(float(r.get("volume") or 0) for r in rows)
        except (TypeError, ValueError, KeyError):
            continue
        out.append(
            {
                "timestamp": start.isoformat(),
                "open": o,
                "high": h,
                "low": lo,
                "close": cl,
                "volume": vol,
            }
        )
    return out


def fetch_10m_ohlcv(
    ux: UpstoxService,
    instrument_key: str,
    start: date,
    end: date,
) -> List[Dict[str, Any]]:
    """Native minutes/10, chunked to Upstox span limits. 5m→10m fallback."""
    chunks: List[List[Dict[str, Any]]] = []
    chunk_end = end
    while chunk_end >= start:
        chunk_start = max(start, chunk_end - timedelta(days=CHUNK_DAYS))
        span = (chunk_end - chunk_start).days
        raw = ux.get_historical_candles_by_instrument_key(
            instrument_key,
            interval="minutes/10",
            days_back=max(span, 0),
            range_end_date=chunk_end,
        )
        if raw:
            chunks.append(raw)
        else:
            raw5 = ux.get_historical_candles_by_instrument_key(
                instrument_key,
                interval="minutes/5",
                days_back=max(span, 0),
                range_end_date=chunk_end,
            )
            if raw5:
                chunks.append(_agg5_to_records(raw5))
        if chunk_start <= start:
            break
        chunk_end = chunk_start - timedelta(days=1)
        time.sleep(FETCH_SLEEP_SEC)
    merged = _merge_candles(chunks)
    kept: List[Dict[str, Any]] = []
    for c in merged:
        dt = parse_ist(c.get("timestamp"))
        if dt is None:
            continue
        if dt.date() < start or dt.date() > end:
            continue
        kept.append(c)
    return kept


def _signed_ret(raw: Optional[float], side: str) -> Optional[float]:
    if raw is None:
        return None
    return -raw if side == "short" else raw


def _win(raw: Optional[float], side: str) -> Optional[bool]:
    if raw is None:
        return None
    return (raw < 0) if side == "short" else (raw > 0)


def _mfe_mae_for_side(ev: Dict[str, Any], side: str) -> Tuple[Optional[float], Optional[float]]:
    mfe, mae = ev.get("fwd_mfe_5bar"), ev.get("fwd_mae_5bar")
    if ev.get("dominant_side") == "both" and side == "short":
        return mae, mfe
    return mfe, mae


def build_summary_rows(events: Sequence[Dict[str, Any]]) -> List[Dict[str, Any]]:
    groups: Dict[Tuple, Dict[str, Any]] = {}

    def bucket(key: Tuple) -> Dict[str, Any]:
        g = groups.get(key)
        if g is None:
            g = {
                "n": 0,
                "w1": [],
                "w3": [],
                "w5": [],
                "r3": [],
                "mfe": [],
                "mae": [],
                "sc_ok": [],
                "sc_bad": [],
            }
            groups[key] = g
        return g

    for ev in events:
        adx_b = _adx_bucket(ev.get("adx_at_signal"))
        if adx_b is None:
            continue
        squeeze = bool(ev.get("squeeze"))
        phase = str(ev.get("session_phase") or "")
        for side, score in (("long", int(ev.get("score_long") or 0)), ("short", int(ev.get("score_short") or 0))):
            if score not in (2, 3, 4):
                continue
            g = bucket((score, side, phase, adx_b, squeeze))
            g["n"] += 1
            w1 = _win(ev.get("fwd_ret_1bar"), side)
            w3 = _win(ev.get("fwd_ret_3bar"), side)
            w5 = _win(ev.get("fwd_ret_5bar"), side)
            if w1 is not None:
                g["w1"].append(1.0 if w1 else 0.0)
            if w3 is not None:
                g["w3"].append(1.0 if w3 else 0.0)
            if w5 is not None:
                g["w5"].append(1.0 if w5 else 0.0)
            r3 = _signed_ret(ev.get("fwd_ret_3bar"), side)
            if r3 is not None:
                g["r3"].append(r3)
            mfe, mae = _mfe_mae_for_side(ev, side)
            if mfe is not None:
                g["mfe"].append(float(mfe))
            if mae is not None:
                g["mae"].append(float(mae))
            correct = w3 if w3 is not None else w1
            if correct is True:
                g["sc_ok"].append(float(score))
            elif correct is False:
                g["sc_bad"].append(float(score))

    def avg(xs: List[float]) -> Optional[float]:
        return (sum(xs) / len(xs)) if xs else None

    rows: List[Dict[str, Any]] = []
    for (score, side, phase, adx_b, squeeze), g in sorted(groups.items()):
        rows.append(
            {
                "score_bucket": score,
                "side": side,
                "session_phase": phase,
                "adx_bucket": adx_b,
                "squeeze": squeeze,
                "signal_count": g["n"],
                "win_rate_1bar": avg(g["w1"]),
                "win_rate_3bar": avg(g["w3"]),
                "win_rate_5bar": avg(g["w5"]),
                "avg_fwd_ret_3bar": avg(g["r3"]),
                "avg_mfe_5bar": avg(g["mfe"]),
                "avg_mae_5bar": avg(g["mae"]),
                "avg_score_when_correct": avg(g["sc_ok"]),
                "avg_score_when_wrong": avg(g["sc_bad"]),
            }
        )
    return rows


def _event_params(ev: Dict[str, Any]) -> Dict[str, Any]:
    p = {k: ev.get(k) for k in _EVENT_COLS}
    eid = p.get("event_id")
    if isinstance(eid, str):
        p["event_id"] = uuid.UUID(eid)
    ct = p.get("candle_time")
    if isinstance(ct, datetime) and ct.tzinfo is None:
        p["candle_time"] = IST.localize(ct)
    return p


def persist_events_and_summary(events: Sequence[Dict[str, Any]], *, replace: bool = True) -> int:
    ensure_backtest_tables()
    db = SessionLocal()
    try:
        if replace:
            db.execute(text("DELETE FROM rocket_backtest_events"))
            db.execute(text("DELETE FROM rocket_backtest_summary"))
        if events:
            placeholders = ", ".join(f":{c}" for c in _EVENT_COLS)
            cols = ", ".join(_EVENT_COLS)
            sql = text(f"INSERT INTO rocket_backtest_events ({cols}) VALUES ({placeholders})")
            payload = [_event_params(ev) for ev in events]
            for i in range(0, len(payload), 200):
                db.execute(sql, payload[i : i + 200])
        summary = build_summary_rows(events)
        if summary:
            scol = (
                "score_bucket, side, session_phase, adx_bucket, squeeze, signal_count, "
                "win_rate_1bar, win_rate_3bar, win_rate_5bar, avg_fwd_ret_3bar, "
                "avg_mfe_5bar, avg_mae_5bar, avg_score_when_correct, avg_score_when_wrong"
            )
            db.execute(
                text(
                    f"INSERT INTO rocket_backtest_summary ({scol}) VALUES ("
                    ":score_bucket, :side, :session_phase, :adx_bucket, :squeeze, :signal_count, "
                    ":win_rate_1bar, :win_rate_3bar, :win_rate_5bar, :avg_fwd_ret_3bar, "
                    ":avg_mfe_5bar, :avg_mae_5bar, :avg_score_when_correct, :avg_score_when_wrong)"
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
    ensure_backtest_tables()
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
        "SELECT score_bucket, side, session_phase, adx_bucket, squeeze, signal_count, "
        "win_rate_1bar, win_rate_3bar, win_rate_5bar, avg_fwd_ret_3bar, "
        "avg_mfe_5bar, avg_mae_5bar, avg_score_when_correct, avg_score_when_wrong "
        f"FROM rocket_backtest_summary{where} "
        "ORDER BY score_bucket, side, session_phase, adx_bucket, squeeze"
    )
    db = SessionLocal()
    try:
        rows = db.execute(sql, params).mappings().all()
        return [dict(r) for r in rows]
    finally:
        db.close()


def run_backtest(
    *,
    date_from: date = DEFAULT_FROM,
    date_to: date = DEFAULT_TO,
    symbol_limit: Optional[int] = None,
    symbols: Optional[Sequence[str]] = None,
) -> Dict[str, Any]:
    universe = load_arbitrage_curr_mth_universe()
    if symbols:
        want = {s.upper().strip() for s in symbols}
        universe = [u for u in universe if str(u.get("stock") or "").upper() in want]
    if symbol_limit is not None:
        universe = universe[: max(0, int(symbol_limit))]

    fetch_start = date_from - timedelta(days=WARMUP_CALENDAR_DAYS)
    ux = _ux()
    all_events: List[Dict[str, Any]] = []
    fetched = 0
    skipped = 0
    errors = 0

    for i, u in enumerate(universe):
        stock = str(u.get("stock") or "").upper().strip()
        ikey = str(u.get("instrument_key") or "").strip()
        if not stock or not ikey:
            skipped += 1
            continue
        try:
            candles = fetch_10m_ohlcv(ux, ikey, fetch_start, date_to)
            fetched += 1
            if len(candles) < 20:
                skipped += 1
                continue
            scored = score_bars(candles)
            attach_forward_outcomes(scored)
            evs = events_from_scored(scored, stock)
            evs = [
                e
                for e in evs
                if date_from <= e["session_date"] <= date_to
            ]
            all_events.extend(evs)
        except Exception:
            logger.exception("layer10f backtest failed for %s", stock)
            errors += 1
        if (i + 1) % 10 == 0:
            logger.info(
                "layer10f backtest %s/%s symbols, events=%s",
                i + 1,
                len(universe),
                len(all_events),
            )
        time.sleep(FETCH_SLEEP_SEC)

    n = persist_events_and_summary(all_events, replace=True)
    return {
        "date_from": date_from.isoformat(),
        "date_to": date_to.isoformat(),
        "universe": len(universe),
        "fetched": fetched,
        "skipped": skipped,
        "errors": errors,
        "events": n,
        "summary_rows": len(build_summary_rows(all_events)),
    }
