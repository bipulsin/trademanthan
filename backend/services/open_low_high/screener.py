"""Live Open Low High screener — universe scan, Phase A/B, freeze + sector tags.

Schedule (IST) — see ``scheduler.py`` / ``phases.py``:
  16:05        EOD ``arbitrage_master.equity_prev_close`` (PDC) fill
  09:00        PDC null cross-check
  09:05        Pre-warm WS Equity / Index / Future (only pre-warm)
  09:15        WS warm check + EQ/INDEX lock through 09:25
  09:20:10–09:23  Phase A on completed 09:15 5m (WS then REST) — grey cards live
  09:24        Phase A refill for null 09:15 OHLC only
  09:25:03–09:29  Phase B on Phase A qualifiers only (WS→REST→wait30→REST)

Data split (equity vs stock future):
  - **Algo filters** (Phase A/B, PDH/PDL/prev close, VWAP, doji, wick/body,
    extremes): evaluated on the **underlying NSE equity** 5m + daily bars.
  - **Trade display** (Entry, SL, T1/T2, chart levels): expressed on the
    **front-month stock future**. SL = future 09:15 open; Entry is **frozen**
    at Phase B (prefer ~09:22 future 1m close; else 09:20 future close
    ± small directional slippage) and is never overwritten by live LTP;
    T1/T2 = traditional pivots from **future** prior-day H/L/C.

Phase A passers persist immediately (grey). Phase B passers are
``active=True`` (golden); Wait List 35–90% wick; wick ≤0.35 Active.
"""
from __future__ import annotations

import logging
import threading
import time as time_mod
from concurrent.futures import ThreadPoolExecutor, as_completed
from datetime import date, datetime, time, timedelta
from typing import Any, Dict, List, Optional, Tuple

import pytz

from backend.services.open_high_low.bars import Bar, bar_at, first_vwap
from backend.services.open_high_low.config import NIFTY_INDEX_KEY
from backend.services.open_high_low.data import (
    front_month_expiry,
    load_universe,
    previous_trading_day,
    resolve_contract,
)
from backend.services.open_low_high.persistence import (
    ensure_tables,
    get_day_meta,
    is_day_frozen,
    list_screen_for_date,
    list_trade_log,
    sort_screen_rows,
    upsert_day_meta,
    upsert_screen_rows,
)
from backend.services.open_low_high.signals import (
    evaluate_nifty_cue,
    evaluate_phase_a,
    evaluate_phase_b,
    freeze_tags_immutable,
    next_pivot_targets,
    open_extreme_badge,
)

logger = logging.getLogger(__name__)
IST = pytz.timezone("Asia/Kolkata")

FIRST_CANDLE = time(9, 15)
SECOND_CANDLE = time(9, 20)
ENTRY_QUOTE_MINUTE = time(9, 22)
# Small directional slip when freezing from 09:20 future close (fallback).
ENTRY_SLIPPAGE_FRAC = 0.0002  # 2 bps
# Phase B window (UI ``window_status`` + force auto-run).
SCREEN_AFTER = time(9, 25, 3)
FREEZE_BY = time(9, 29, 0)
HARD_DEADLINE = time(9, 29, 0)
from backend.services.open_low_high.config import (
    FREEZE_COVERAGE_MIN,
    MISSING_DATA_RATE_THRESHOLD,
    WS_FIRST_TICK_TRUST_SEC,
    WS_TRY_COUNT,
    WS_TRY_MIN_1M_BARS,
    WS_TRY_WAIT_SEC,
)

_run_lock = threading.Lock()
_last_run: Dict[str, Any] = {}
_prewarm_lock = threading.Lock()
_last_prewarm: Dict[str, Any] = {}


def _now() -> datetime:
    return datetime.now(IST)


def _today() -> date:
    return _now().date()


def _token_tail(token: Optional[str]) -> str:
    t = str(token or "")
    return t[-4:] if len(t) >= 4 else ("****" if t else "none")


def _upstox():
    from backend.config import settings
    from backend.services.upstox_service import UpstoxService

    ux = UpstoxService(settings.UPSTOX_API_KEY, settings.UPSTOX_API_SECRET)
    try:
        ux.reload_token_from_storage()
    except Exception:
        pass
    logger.info(
        "OLH upstox token loaded last4=%s has_token=%s",
        _token_tail(getattr(ux, "access_token", None)),
        bool(getattr(ux, "access_token", None)),
    )
    return ux


def window_status(now: Optional[datetime] = None) -> str:
    """waiting | open | past — Phase B window 09:25:03–09:29 IST.

    Phase A cards may already be on the board while this is still ``waiting``.
    """
    t = now or _now()
    if t.tzinfo is None:
        t = IST.localize(t)
    else:
        t = t.astimezone(IST)
    tt = t.time()
    if tt < SCREEN_AFTER:
        return "waiting"
    if tt <= FREEZE_BY:
        return "open"
    return "past"


def _past_hard_deadline(now: Optional[datetime] = None) -> bool:
    t = now or _now()
    if t.tzinfo is None:
        t = IST.localize(t)
    else:
        t = t.astimezone(IST)
    return t.time() >= HARD_DEADLINE


def _miss_bump(stats: Optional[Dict[str, int]], key: str) -> None:
    if stats is None:
        return
    stats[key] = int(stats.get(key) or 0) + 1


def _nifty_data_ready(cue: Dict[str, Any], *, require_0920: bool = True) -> bool:
    """True when Nifty REST/prev-day inputs needed for the header cue are present."""
    if cue.get("nifty_prev_close") is None:
        return False
    if cue.get("nifty_0915_open") is None and cue.get("nifty_0915_close") is None:
        return False
    if require_0920 and cue.get("nifty_0920_close") is None:
        return False
    return True


def data_quality_incomplete(
    *,
    phase_a_count: int,
    scanned: int,
    miss_stats: Dict[str, int],
    nifty_ready: bool,
    missing_rate_threshold: float = MISSING_DATA_RATE_THRESHOLD,
) -> Tuple[bool, Dict[str, Any]]:
    """Decide whether an empty Phase A freeze would be data-starved (not a real flat day).

    Returns ``(incomplete, details)``. Incomplete only when Phase A is empty *and*
    Nifty is null or a large share of names lacked equity 09:15 / prev-day bars.
    """
    scanned_n = max(int(scanned or 0), 0)
    no_bars = int(miss_stats.get("no_bars") or 0)
    no_0915 = int(miss_stats.get("no_0915") or 0)
    no_prev = int(miss_stats.get("no_prev") or 0)
    no_equity_key = int(miss_stats.get("no_equity_key") or 0)
    data_missing = no_bars + no_0915 + no_prev + no_equity_key
    missing_rate = (float(data_missing) / float(scanned_n)) if scanned_n else 1.0
    details = {
        "phase_a": int(phase_a_count),
        "scanned": scanned_n,
        "data_missing": data_missing,
        "missing_rate": round(missing_rate, 4),
        "missing_rate_threshold": float(missing_rate_threshold),
        "nifty_ready": bool(nifty_ready),
        "no_bars": no_bars,
        "no_0915": no_0915,
        "no_prev": no_prev,
        "no_equity_key": no_equity_key,
    }
    if int(phase_a_count) > 0:
        return False, details
    incomplete = (not nifty_ready) or (missing_rate >= float(missing_rate_threshold))
    details["incomplete"] = incomplete
    return incomplete, details


def freeze_gate(
    *,
    coverage: float,
    nifty_present: bool,
    coverage_min: float = FREEZE_COVERAGE_MIN,
) -> Tuple[bool, str]:
    """Return ``(may_freeze, reason)``. Freeze only if coverage ≥ min AND Nifty present."""
    cov = float(coverage or 0.0)
    if not nifty_present:
        return False, "nifty_missing"
    if cov < float(coverage_min):
        return False, f"coverage_below_{coverage_min:.0%}"
    return True, "ok"


def equity_coverage_from_miss(
    *,
    scanned: int,
    miss_stats: Dict[str, int],
) -> float:
    """Share of universe that had usable equity 09:15 bars (approx from miss counters)."""
    scanned_n = max(int(scanned or 0), 0)
    if scanned_n <= 0:
        return 0.0
    no_bars = int(miss_stats.get("no_bars") or 0)
    no_0915 = int(miss_stats.get("no_0915") or 0)
    no_equity_key = int(miss_stats.get("no_equity_key") or 0)
    # Conservative: each miss counter counts distinct symbols in Phase A path.
    missing = min(scanned_n, no_bars + no_0915 + no_equity_key)
    return max(0.0, 1.0 - (float(missing) / float(scanned_n)))


def _bars_from_rows(rows: List[Dict[str, Any]], contract: str) -> List[Bar]:
    from backend.services.open_high_low.bars import parse_ts

    out: List[Bar] = []
    for row in rows or []:
        ts = parse_ts(row.get("timestamp") or row.get("candle_time"))
        if ts is None:
            continue
        try:
            out.append(
                Bar(
                    ts=ts,
                    open=float(row["open"]),
                    high=float(row["high"]),
                    low=float(row["low"]),
                    close=float(row["close"]),
                    volume=float(row.get("volume") or 0),
                    contract=contract,
                )
            )
        except (KeyError, TypeError, ValueError):
            continue
    out.sort(key=lambda b: b.ts)
    return out


def _fetch_ws_1m_rows(instrument_key: str, session: date) -> List[Dict[str, Any]]:
    """Raw WS 1m rows for today's session (empty on miss)."""
    from sqlalchemy import text

    from backend.database import SessionLocal

    db = SessionLocal()
    try:
        start = IST.localize(datetime(session.year, session.month, session.day, 9, 0, 0))
        end = start + timedelta(hours=7)
        rows = db.execute(
            text(
                """
                SELECT candle_time AS timestamp, open, high, low, close, volume
                FROM upstox_ws_intraday_1m
                WHERE instrument_key = :ik
                  AND candle_time >= :start
                  AND candle_time < :end
                ORDER BY candle_time ASC
                """
            ),
            {"ik": instrument_key, "start": start, "end": end},
        ).mappings().all()
        return [dict(r) for r in rows]
    except Exception:
        logger.debug("olh ws 1m fetch miss %s", instrument_key, exc_info=True)
        return []
    finally:
        db.close()


def _parse_bar_ts(raw: Any) -> Optional[datetime]:
    ts = raw
    if isinstance(ts, str):
        from backend.services.open_high_low.bars import parse_ts

        ts = parse_ts(ts)
    if ts is None:
        return None
    if isinstance(ts, datetime):
        if ts.tzinfo is None:
            return IST.localize(ts)
        return ts.astimezone(IST)
    return None


def _fetch_1m_close_at(
    instrument_key: str, session: date, at: time
) -> Optional[float]:
    """Close of the 1m bar whose IST minute matches ``at`` (e.g. 09:22)."""
    rows = _fetch_ws_1m_rows(instrument_key, session)
    target_h, target_m = at.hour, at.minute
    for r in rows:
        ts = _parse_bar_ts(r.get("timestamp"))
        if ts is None:
            continue
        if ts.hour == target_h and ts.minute == target_m:
            try:
                px = float(r["close"])
            except (TypeError, ValueError, KeyError):
                return None
            return px if px > 0 else None
    return None


def _freeze_fut_entry(
    *,
    side: str,
    fut_ik: str,
    session: date,
    fut_second_close: Optional[float],
    fut_first_close: Optional[float],
) -> Tuple[Optional[float], Optional[str]]:
    """Frozen future Entry: prefer ~09:22 1m close; else 09:20 close ± slip.

    Returns ``(price, method)`` where method is one of
    ``09:22_1m``, ``09:20_close+slip``, ``09:20_close-slip``, ``09:20_close``,
    ``09:15_close``, or None.
    """
    px_922 = _fetch_1m_close_at(fut_ik, session, ENTRY_QUOTE_MINUTE)
    if px_922 is not None:
        return px_922, "09:22_1m"

    base = fut_second_close
    method_base = "09:20_close"
    if base is None and fut_first_close is not None:
        base = fut_first_close
        method_base = "09:15_close"
    if base is None:
        return None, None

    side_u = str(side or "").upper()
    slip = float(ENTRY_SLIPPAGE_FRAC)
    if method_base == "09:20_close" and slip > 0:
        if side_u == "BULLISH":
            return float(base) * (1.0 + slip), "09:20_close+slip"
        if side_u == "BEARISH":
            return float(base) * (1.0 - slip), "09:20_close-slip"
    return float(base), method_base


def _ws_bucket_trusted(
    bucket_start: datetime,
    first_1m_ts: datetime,
    *,
    trust_sec: int = WS_FIRST_TICK_TRUST_SEC,
) -> bool:
    """TRUSTED only if first 1m bar of the 5m bucket is within ``trust_sec`` of bucket open."""
    delta = (first_1m_ts - bucket_start).total_seconds()
    return 0.0 <= float(delta) <= float(trust_sec)


def _fetch_ws_5m_bars(
    instrument_key: str,
    session: date,
    *,
    require_trusted: bool = False,
) -> Tuple[List[Bar], str]:
    """Aggregate Upstox WS 1m → exchange-aligned 5m (09:15–09:19, 09:20–09:24, …).

    Returns ``(bars, trust)`` where trust is ``TRUSTED`` / ``UNTRUSTED`` / ``none``.
    UNTRUSTED bars are still returned unless ``require_trusted=True``.
    """
    rows = _fetch_ws_1m_rows(instrument_key, session)
    if not rows:
        return [], "none"
    # Aggregate 1m → 5m aligned to :00/:05/:10… IST; track first 1m ts per bucket.
    buckets: Dict[datetime, Dict[str, Any]] = {}
    for r in rows:
        ts = _parse_bar_ts(r.get("timestamp"))
        if ts is None:
            continue
        minute = (ts.minute // 5) * 5
        bucket = ts.replace(minute=minute, second=0, microsecond=0)
        o, h, l, c = float(r["open"]), float(r["high"]), float(r["low"]), float(r["close"])
        vol = float(r.get("volume") or 0)
        cur = buckets.get(bucket)
        if cur is None:
            buckets[bucket] = {
                "open": o,
                "high": h,
                "low": l,
                "close": c,
                "volume": vol,
                "first_1m": ts,
            }
        else:
            cur["high"] = max(cur["high"], h)
            cur["low"] = min(cur["low"], l)
            cur["close"] = c
            cur["volume"] += vol
            if ts < cur["first_1m"]:
                cur["first_1m"] = ts
    out: List[Bar] = []
    untrusted = False
    for bucket, ohlc in sorted(buckets.items()):
        trusted = _ws_bucket_trusted(bucket, ohlc["first_1m"])
        if not trusted:
            untrusted = True
            if require_trusted:
                continue
        out.append(
            Bar(
                ts=bucket,
                open=ohlc["open"],
                high=ohlc["high"],
                low=ohlc["low"],
                close=ohlc["close"],
                volume=ohlc["volume"],
                contract=instrument_key,
            )
        )
    if not out:
        return [], "none"
    return out, ("UNTRUSTED" if untrusted else "TRUSTED")


def _hist_range_end(session: date) -> date:
    """Upstox ``range_end_date`` often omits that calendar day's candles.

    Equity usually appears with ``session+1``; index (NIFTY) can need
    ``session+2``. Use the later of those floors so overnight force screens
    still see the session's 5m bars.
    """
    return max(session + timedelta(days=2), _today() + timedelta(days=1))


def _fetch_rest_5m(upstox: Any, instrument_key: str, session: date) -> List[Bar]:
    """Session 5m bars from Upstox REST.

    - **Today (live):** V3 **intraday** only
      (``/v3/historical-candle/intraday/{key}/minutes/5``). Never historical.
    - **Previous days:** V3/V2 **historical**
      (``get_historical_candles_by_instrument_key``); intraday fallback only if
      historical returns empty.
    """
    raw = None
    if session >= _today():
        try:
            raw = upstox._fetch_intraday_candles_v3(instrument_key, "minutes/5")
            logger.info(
                "olh rest 5m endpoint=intraday instrument=%s session=%s bars=%s",
                instrument_key,
                session.isoformat(),
                len(raw or []),
            )
        except Exception as exc:
            logger.info(
                "olh rest intraday 5m %s session=%s: %s",
                instrument_key,
                session.isoformat(),
                exc,
            )
            raw = None
        return _bars_from_rows(list(raw or []), instrument_key)

    # Previous calendar sessions: historical is authoritative.
    try:
        raw = upstox.get_historical_candles_by_instrument_key(
            instrument_key,
            interval="minutes/5",
            days_back=5,
            range_end_date=_hist_range_end(session),
        )
        logger.info(
            "olh rest 5m endpoint=historical instrument=%s session=%s bars=%s",
            instrument_key,
            session.isoformat(),
            len(raw or []),
        )
    except Exception as exc:
        logger.info("olh rest 5m %s: %s", instrument_key, exc)
        raw = None
    if not raw:
        try:
            raw = upstox._fetch_intraday_candles_v3(instrument_key, "minutes/5")
            logger.info(
                "olh rest 5m endpoint=intraday_fallback instrument=%s session=%s bars=%s",
                instrument_key,
                session.isoformat(),
                len(raw or []),
            )
        except Exception:
            raw = None
    return _bars_from_rows(list(raw or []), instrument_key)


def _fetch_daily_prev(
    upstox: Any, instrument_key: str, session: date
) -> Tuple[Optional[float], Optional[float], Optional[float]]:
    """Return (prev_high, prev_low, prev_close) for previous trading day on this contract."""
    prev = previous_trading_day(session)
    try:
        raw = upstox.get_historical_candles_by_instrument_key(
            instrument_key,
            interval="days/1",
            days_back=10,
            range_end_date=_hist_range_end(session),
        )
    except Exception as exc:
        logger.info("olh daily %s: %s", instrument_key, exc)
        return None, None, None
    bars = _bars_from_rows(list(raw or []), instrument_key)
    for b in bars:
        if b.session_date == prev:
            return b.high, b.low, b.close
    # Fallback: last bar strictly before session
    earlier = [b for b in bars if b.session_date < session]
    if earlier:
        b = earlier[-1]
        return b.high, b.low, b.close
    return None, None, None


def _merge_bars(primary: List[Bar], fallback: List[Bar]) -> List[Bar]:
    """Merge by timestamp; ``primary`` wins on conflicts."""
    by_ts: Dict[datetime, Bar] = {b.ts: b for b in fallback}
    for b in primary:
        by_ts[b.ts] = b
    return [by_ts[k] for k in sorted(by_ts)]


def _resolve_equity_key(upstox: Any, row: Dict[str, str], symbol: str) -> str:
    """NSE cash instrument key for algo bars (``NSE_EQ|ISIN``).

    Prefer ``arbitrage_master.stock_instrument_key`` (universe ``underlying_key``);
    fall back to Upstox/ISIN mapping via ``get_instrument_key``.
    """
    raw = str(row.get("underlying_key") or "").strip().replace(":", "|")
    if raw.upper().startswith("NSE_EQ|"):
        return raw
    try:
        key = upstox.get_instrument_key(symbol)
        if key:
            return str(key).replace(":", "|")
    except Exception:
        logger.debug("olh equity key resolve failed %s", symbol, exc_info=True)
    # Accept a bare ISIN token if master only stored the ISIN portion.
    if raw.upper().startswith("INE") and "|" not in raw:
        return f"NSE_EQ|{raw}"
    return raw if raw.upper().startswith("NSE_EQ") else ""


def _session_5m_bars(upstox: Any, instrument_key: str, session: date) -> List[Bar]:
    """Merge WS + REST 5m; REST wins on timestamp conflicts (legacy path)."""
    if not instrument_key:
        return []
    ws_bars, _trust = _fetch_ws_5m_bars(instrument_key, session)
    rest_bars = _fetch_rest_5m(upstox, instrument_key, session)
    return _merge_bars(rest_bars, ws_bars)


def _record_attempt(
    session: date,
    *,
    symbol: str,
    instrument_key: str,
    source: str,
    status: str,
    candle_count: int = 0,
    error: str = "",
    detail: str = "",
) -> None:
    try:
        from backend.services.open_low_high.persistence import record_fetch_attempt

        record_fetch_attempt(
            trade_date=session,
            symbol=symbol,
            instrument_key=instrument_key,
            source=source,
            status=status,
            candle_count=candle_count,
            error=error,
            detail=detail,
        )
    except Exception:
        logger.debug("olh record_fetch_attempt failed", exc_info=True)


def _ws_try_once(
    instrument_key: str,
    session: date,
    *,
    symbol: str,
    action: str,
    try_n: int,
) -> Tuple[List[Bar], str]:
    """One WS try: optional short wait for ≥N 1m bars, then aggregate; TRUSTED only if early ticks."""
    from backend.services.open_low_high.ohl_log import ohl_log

    wait = float(WS_TRY_WAIT_SEC)
    need = max(1, int(WS_TRY_MIN_1M_BARS))
    if wait > 0:
        rows = _fetch_ws_1m_rows(instrument_key, session)
        if len(rows) < need:
            time_mod.sleep(min(wait, 5.0))
    bars, trust = _fetch_ws_5m_bars(instrument_key, session, require_trusted=False)
    src_label = f"ws_try{try_n}"
    if not bars:
        ohl_log("failed", action=action, symbol=symbol, source=src_label, detail="no_bars")
        _record_attempt(
            session,
            symbol=symbol,
            instrument_key=instrument_key,
            source=src_label,
            status="empty",
            detail="no_bars",
        )
        return [], "none"
    if trust != "TRUSTED":
        ohl_log(
            "failed",
            action=action,
            symbol=symbol,
            source=src_label,
            detail=f"UNTRUSTED bars={len(bars)} → REST",
        )
        _record_attempt(
            session,
            symbol=symbol,
            instrument_key=instrument_key,
            source=src_label,
            status="untrusted",
            candle_count=len(bars),
            detail=trust,
        )
        return [], "UNTRUSTED"
    ohl_log(
        "success",
        action=action,
        symbol=symbol,
        source=src_label,
        detail=f"TRUSTED bars={len(bars)}",
    )
    _record_attempt(
        session,
        symbol=symbol,
        instrument_key=instrument_key,
        source=src_label,
        status="ok",
        candle_count=len(bars),
        detail=trust,
    )
    return bars, "ws"


def _session_5m_bars_try(
    upstox: Any,
    instrument_key: str,
    session: date,
    *,
    symbol: str = "",
    action: str = "bars_try",
    prefer: str = "ws",
) -> Tuple[List[Bar], str]:
    """Per symbol: WS try1 → WS try2 → REST. Records source + fetch_attempt rows."""
    from backend.services.open_low_high.ohl_log import ohl_log

    if not instrument_key:
        ohl_log("skipped", action=action, symbol=symbol, detail="no_instrument_key")
        return [], "none"

    collected: List[Bar] = []
    used = "none"

    if prefer == "ws":
        tries = max(1, int(WS_TRY_COUNT))
        for i in range(1, tries + 1):
            bars, src = _ws_try_once(
                instrument_key, session, symbol=symbol, action=action, try_n=i
            )
            if bars:
                collected = bars
                used = src
                break
        rest = _fetch_rest_5m(upstox, instrument_key, session)
        if rest:
            _record_attempt(
                session,
                symbol=symbol,
                instrument_key=instrument_key,
                source="rest",
                status="ok",
                candle_count=len(rest),
            )
            ohl_log(
                "success",
                action=action,
                symbol=symbol,
                source="rest",
                detail=f"bars={len(rest)}",
            )
            if not collected:
                collected = rest
                used = "rest"
            else:
                collected = _merge_bars(collected, rest)
        else:
            _record_attempt(
                session,
                symbol=symbol,
                instrument_key=instrument_key,
                source="rest",
                status="empty",
                detail="no_bars",
            )
            ohl_log("failed", action=action, symbol=symbol, source="rest", detail="no_bars")
        return collected, used

    # prefer rest
    rest = _fetch_rest_5m(upstox, instrument_key, session)
    if rest:
        _record_attempt(
            session,
            symbol=symbol,
            instrument_key=instrument_key,
            source="rest",
            status="ok",
            candle_count=len(rest),
        )
        return rest, "rest"
    bars, src = _ws_try_once(instrument_key, session, symbol=symbol, action=action, try_n=1)
    return bars, src


def _screen_one(
    upstox: Any,
    row: Dict[str, str],
    session: date,
    expiry: date,
    miss_stats: Optional[Dict[str, int]] = None,
    *,
    phase_a_only: bool = False,
) -> Optional[Dict[str, Any]]:
    """Return a Phase A candidate. ``active`` is True only when Phase B also passes.

    Screening uses **equity** OHLC; Entry/SL/T1/T2 use the **stock future**.
    ``miss_stats`` accumulates data gaps (not Phase A filter rejects).
    When ``phase_a_only``, skip Phase B / Entry freeze (grey card for early UI).
    """
    from backend.services.open_low_high.ohl_log import ohl_log
    from backend.services.open_low_high.pdc import get_equity_prev_close

    symbol = str(row.get("symbol") or "").upper()
    if not symbol:
        return None

    equity_ik = _resolve_equity_key(upstox, row, symbol)
    if not equity_ik:
        logger.info("olh skip %s: no equity instrument key", symbol)
        ohl_log("skipped", action="phase_a_screen", symbol=symbol, detail="no_equity_key")
        _miss_bump(miss_stats, "no_equity_key")
        return None

    resolved = resolve_contract(
        upstox,
        symbol,
        expiry,
        equity_ik,
        universe_row=row,
    )
    fut_ik = str(resolved.get("instrument_key") or "").strip()
    if not fut_ik:
        ohl_log("skipped", action="phase_a_screen", symbol=symbol, detail="no_future_key")
        return None

    # --- Algo path: underlying equity (WS first, then REST) ---
    eq_bars, bar_src = _session_5m_bars_try(
        upstox,
        equity_ik,
        session,
        symbol=symbol,
        action="phase_a_0915",
        prefer="ws",
    )
    if not eq_bars:
        _miss_bump(miss_stats, "no_bars")
        ohl_log("failed", action="phase_a_0915", symbol=symbol, detail="no_bars")
        return None

    first = bar_at(eq_bars, session, FIRST_CANDLE)
    second = bar_at(eq_bars, session, SECOND_CANDLE)
    if first is None:
        _miss_bump(miss_stats, "no_0915")
        ohl_log("failed", action="phase_a_0915", symbol=symbol, source=bar_src, detail="no_0915_bar")
        return None
    ohl_log(
        "success",
        action="phase_a_0915",
        symbol=symbol,
        source=bar_src,
        detail=f"o={first.open} h={first.high} l={first.low} c={first.close}",
    )

    vwap = first_vwap(eq_bars, session, FIRST_CANDLE)
    if vwap is None:
        vwap = first.typical

    prev_high, prev_low, prev_close = _fetch_daily_prev(upstox, equity_ik, session)
    # Prefer arbitrage_master.equity_prev_close when present.
    master_pdc = get_equity_prev_close(symbol)
    if master_pdc is not None:
        prev_close = master_pdc
    if prev_high is None or prev_low is None:
        _miss_bump(miss_stats, "no_prev")
        ohl_log("failed", action="phase_a_prev", symbol=symbol, detail="no_pdh_pdl")
        return None

    phase_a = evaluate_phase_a(
        first_open=first.open,
        first_high=first.high,
        first_low=first.low,
        first_close=first.close,
        vwap_0915=vwap,
        prev_high=prev_high,
        prev_low=prev_low,
        prev_close=prev_close,
    )
    if phase_a is None:
        ohl_log("skipped", action="phase_a_filter", symbol=symbol, detail="not_qualified")
        return None
    ohl_log("success", action="phase_a_filter", symbol=symbol, detail=f"side={phase_a.side}")

    olh_badge = open_extreme_badge(
        phase_a.side,
        first_open=phase_a.first_open,
        first_high=phase_a.first_high,
        first_low=phase_a.first_low,
    )

    second_close: Optional[float] = None
    phase_b_ok = False
    wait_list = False
    phase_b_fail_reason: Optional[str] = "Phase B pending" if phase_a_only else "Phase B failed"
    wick_body_ratio = None

    if not phase_a_only:
        second_close = float(second.close) if second is not None else None
        phase_b = evaluate_phase_b(
            phase_a.side,
            first_close=phase_a.first_close,
            second_open=float(second.open) if second is not None else None,
            second_high=float(second.high) if second is not None else None,
            second_low=float(second.low) if second is not None else None,
            second_close=second_close,
        )
        phase_b_ok = bool(phase_b.ok)
        wait_list = bool(getattr(phase_b, "wait_list", False)) and not phase_b_ok
        phase_b_fail_reason = None if phase_b_ok else (phase_b.fail_reason or "Phase B failed")
        wick_body_ratio = getattr(phase_b, "wick_body_ratio", None)

    # --- Display path: front-month stock future ---
    fut_bars = _session_5m_bars(upstox, fut_ik, session)
    fut_first = bar_at(fut_bars, session, FIRST_CANDLE) if fut_bars else None
    fut_second = bar_at(fut_bars, session, SECOND_CANDLE) if fut_bars else None

    if fut_first is not None:
        sl = float(fut_first.open)
    else:
        logger.info("olh %s: no future 09:15 bar for SL; skip display levels", symbol)
        sl = None

    fut_entry_snap: Optional[float] = None
    entry_method: Optional[str] = None
    t1: Optional[float] = None
    t2: Optional[float] = None

    if not phase_a_only:
        fut_second_close: Optional[float] = (
            float(fut_second.close) if fut_second is not None else None
        )
        fut_first_close: Optional[float] = (
            float(fut_first.close) if fut_first is not None else None
        )
        fut_entry_snap, entry_method = _freeze_fut_entry(
            side=phase_a.side,
            fut_ik=fut_ik,
            session=session,
            fut_second_close=fut_second_close,
            fut_first_close=fut_first_close,
        )
        fut_prev_h, fut_prev_l, fut_prev_c = _fetch_daily_prev(upstox, fut_ik, session)
        if (
            fut_entry_snap is not None
            and fut_prev_h is not None
            and fut_prev_l is not None
            and fut_prev_c is not None
        ):
            try:
                t1, t2 = next_pivot_targets(
                    phase_a.side,
                    prev_high=float(fut_prev_h),
                    prev_low=float(fut_prev_l),
                    prev_close=float(fut_prev_c),
                    reference=float(fut_entry_snap),
                )
            except Exception:
                logger.debug("olh pivot targets failed %s", symbol, exc_info=True)
    else:
        # Phase A early card: SL from future 09:15 open when available; Entry later.
        if fut_first is not None:
            sl = float(fut_first.open)

    contract_sym = (
        str(row.get("currmth_symbol") or "").strip()
        or f"{symbol} FUT {expiry.strftime('%d %b %y').upper()}"
    )
    return {
        "symbol": symbol,
        "side": phase_a.side,
        "contract": contract_sym,
        "expiry": expiry,
        "instrument_key": fut_ik,
        "equity_instrument_key": equity_ik,
        "prev_close": prev_close,
        "prev_high": prev_high,
        "prev_low": prev_low,
        "first_open": phase_a.first_open,
        "first_high": phase_a.first_high,
        "first_low": phase_a.first_low,
        "first_close": phase_a.first_close,
        "second_close": second_close,
        "vwap_0915": phase_a.vwap_0915,
        "active": phase_b_ok,
        "phase_b_ok": phase_b_ok,
        "wait_list": wait_list,
        "phase_b_fail_reason": phase_b_fail_reason,
        "wick_body_ratio": wick_body_ratio,
        "olh_badge": olh_badge,
        "sl": sl,
        "entry": fut_entry_snap,
        "entry_method": entry_method,
        "t1": t1,
        "t2": t2,
        "data_source_logic": "equity",
        "data_source_levels": "future",
        "data_source": bar_src,
    }


def _evaluate_nifty_header_cue(upstox: Any, session: date) -> Dict[str, Any]:
    """NIFTY 50 gap + bias at ~09:20 for Bullish/Bearish subsection badges."""
    try:
        bars = _session_5m_bars(upstox, NIFTY_INDEX_KEY, session)
        first = bar_at(bars, session, FIRST_CANDLE) if bars else None
        second = bar_at(bars, session, SECOND_CANDLE) if bars else None
        _ph, _pl, prev_close = _fetch_daily_prev(upstox, NIFTY_INDEX_KEY, session)
        cue = evaluate_nifty_cue(
            prev_close=prev_close,
            first_open=float(first.open) if first is not None else None,
            first_close=float(first.close) if first is not None else None,
            second_close=float(second.close) if second is not None else None,
        )
        cue["nifty_instrument_key"] = NIFTY_INDEX_KEY
        return cue
    except Exception:
        logger.exception("olh nifty cue failed")
        return {
            "nifty_gap": "flat",
            "nifty_bias": "flat",
            "nifty_show_on": None,
            "nifty_instrument_key": NIFTY_INDEX_KEY,
        }


def _apply_sector_badges(cards: List[Dict[str, Any]]) -> List[Dict[str, Any]]:
    try:
        from backend.services.momentx.sector_badges import badges_for_symbols
    except Exception:
        logger.exception("olh sector badges import failed")
        return cards
    by = badges_for_symbols(c.get("symbol") for c in cards)
    out: List[Dict[str, Any]] = []
    for c in cards:
        card = dict(c)
        info = by.get(str(card.get("symbol") or "").upper()) or {}
        tags = freeze_tags_immutable(
            {
                "nifty_sector_label": info.get("nifty_sector_label"),
                "sector_badge": info.get("sector_badge"),
                "sector_star": bool(info.get("sector_star")),
                "sector_badge_display": info.get("sector_badge_display"),
            },
            frozen=True,
        )
        card.update(
            {
                "nifty_sector_label": tags.get("nifty_sector_label"),
                "sector_badge": tags.get("sector_badge"),
                "sector_star": bool(tags.get("sector_star")),
                "sector_badge_display": tags.get("sector_badge_display"),
                "frozen": True,
            }
        )
        out.append(card)
    return out


def prewarm_equity_data(
    *,
    session: Optional[date] = None,
    max_workers: int = 6,
) -> Dict[str, Any]:
    """Prefetch Nifty + universe equity 5m and prev-day bars before the screen window.

    Warms Upstox REST so ~09:25 Phase A/B is less likely to see null equity OHLC
    (WS only carries futures). Safe to call repeatedly; no freeze side effects.
    """
    if not _prewarm_lock.acquire(blocking=False):
        return {
            "ok": False,
            "status": "busy",
            "message": "Pre-warm already running.",
        }
    trade_date = session or _today()
    started = _now()
    try:
        upstox = _upstox()
        universe = load_universe()
        keys: List[str] = [NIFTY_INDEX_KEY]
        for urow in universe:
            sym = str(urow.get("symbol") or "").upper()
            if not sym:
                continue
            ik = _resolve_equity_key(upstox, urow, sym)
            if ik:
                keys.append(ik)
        # Preserve order, drop dupes.
        uniq = list(dict.fromkeys(k for k in keys if k))
        ok_5m = 0
        ok_daily = 0
        fail = 0

        def _one(ik: str) -> Tuple[str, bool, bool]:
            bars = _fetch_rest_5m(upstox, ik, trade_date)
            ph, pl, pc = _fetch_daily_prev(upstox, ik, trade_date)
            has_5m = bool(bars)
            has_daily = ph is not None and pl is not None and pc is not None
            return ik, has_5m, has_daily

        with ThreadPoolExecutor(max_workers=max(1, min(max_workers, 8))) as pool:
            futs = [pool.submit(_one, ik) for ik in uniq]
            for fut in as_completed(futs):
                try:
                    _ik, has_5m, has_daily = fut.result()
                except Exception:
                    fail += 1
                    continue
                if has_5m:
                    ok_5m += 1
                if has_daily:
                    ok_daily += 1
                time_mod.sleep(0.02)

        nifty_bars = _session_5m_bars(upstox, NIFTY_INDEX_KEY, trade_date)
        nifty_0915 = bar_at(nifty_bars, trade_date, FIRST_CANDLE) is not None
        nifty_0920 = bar_at(nifty_bars, trade_date, SECOND_CANDLE) is not None
        _ph, _pl, nifty_pc = _fetch_daily_prev(upstox, NIFTY_INDEX_KEY, trade_date)
        elapsed = (_now() - started).total_seconds()
        payload = {
            "ok": True,
            "status": "prewarmed",
            "trade_date": trade_date.isoformat(),
            "keys": len(uniq),
            "ok_5m": ok_5m,
            "ok_daily": ok_daily,
            "fail": fail,
            "nifty_0915": nifty_0915,
            "nifty_0920": nifty_0920,
            "nifty_prev_close": nifty_pc is not None,
            "elapsed_sec": round(elapsed, 1),
            "message": (
                f"Pre-warmed {ok_5m}/{len(uniq)} equity/Nifty 5m and "
                f"{ok_daily}/{len(uniq)} daily bars in {elapsed:.1f}s."
            ),
        }
        global _last_prewarm
        _last_prewarm = dict(payload)
        _last_prewarm["at"] = _now().isoformat()
        logger.info(
            "olh prewarm done keys=%s ok_5m=%s ok_daily=%s fail=%s "
            "nifty_0915=%s nifty_0920=%s nifty_pc=%s elapsed=%.1fs",
            len(uniq),
            ok_5m,
            ok_daily,
            fail,
            nifty_0915,
            nifty_0920,
            nifty_pc is not None,
            elapsed,
        )
        return payload
    except Exception as exc:
        logger.exception("olh prewarm failed")
        return {
            "ok": False,
            "status": "error",
            "trade_date": trade_date.isoformat(),
            "message": str(exc),
        }
    finally:
        _prewarm_lock.release()


def run_phase_a(*, force: bool = False, session: Optional[date] = None, **kwargs: Any) -> Dict[str, Any]:
    from backend.services.open_low_high.phases import run_phase_a as _run

    return _run(force=force, session=session, **kwargs)


def run_phase_a_refill(
    *, force: bool = False, session: Optional[date] = None, **kwargs: Any
) -> Dict[str, Any]:
    from backend.services.open_low_high.phases import run_phase_a_refill as _run

    return _run(force=force, session=session, **kwargs)


def run_phase_b(*, force: bool = False, session: Optional[date] = None, **kwargs: Any) -> Dict[str, Any]:
    from backend.services.open_low_high.phases import run_phase_b as _run

    return _run(force=force, session=session, **kwargs)


def run_screen(
    *,
    force: bool = False,
    session: Optional[date] = None,
    max_workers: int = 6,
) -> Dict[str, Any]:
    """Run Phase A then Phase B (force/manual path). Scheduler uses split jobs."""
    global _last_run
    ensure_tables()
    trade_date = session or _today()
    win = window_status()

    if is_day_frozen(trade_date) and not force:
        rows = list_screen_for_date(trade_date)
        meta = get_day_meta(trade_date) or {}
        nifty = (meta.get("meta") if isinstance(meta.get("meta"), dict) else None) or {}
        return {
            "ok": True,
            "status": "frozen",
            "trade_date": trade_date.isoformat(),
            "window": win,
            "frozen": True,
            "bullish": [r for r in rows if r.get("side") == "BULLISH"],
            "bearish": [r for r in rows if r.get("side") == "BEARISH"],
            "meta": meta,
            "nifty_gap": nifty.get("nifty_gap"),
            "nifty_bias": nifty.get("nifty_bias"),
            "nifty_show_on": nifty.get("nifty_show_on"),
            "message": "Already frozen for today — cards and sector tags immutable.",
        }

    from backend.services.open_low_high.phases import (
        phase_a_window_status,
        phase_b_window_status,
    )

    # Inside Phase A window (not yet Phase B): run A only so greys paint early.
    if not force and phase_a_window_status() == "open" and phase_b_window_status() == "waiting":
        result = run_phase_a(force=False, session=trade_date, max_workers=max_workers)
        _last_run = dict(result)
        _last_run["at"] = _now().isoformat()
        return result

    if not force and win == "waiting":
        rows = list_screen_for_date(trade_date)
        if rows:
            bullish = [r for r in rows if r.get("side") == "BULLISH"]
            bearish = [r for r in rows if r.get("side") == "BEARISH"]
            return {
                "ok": True,
                "status": "phase_a",
                "trade_date": trade_date.isoformat(),
                "window": win,
                "frozen": False,
                "bullish": bullish,
                "bearish": bearish,
                "phase_a": len(rows),
                "message": f"Phase A on board — {len(rows)} grey candidate(s).",
            }
        return {
            "ok": True,
            "status": "waiting",
            "trade_date": trade_date.isoformat(),
            "window": win,
            "frozen": False,
            "bullish": [],
            "bearish": [],
            "message": "Waiting for Phase A (09:20:10) / Phase B (09:25:03) IST.",
        }

    if not _run_lock.acquire(blocking=False):
        return {
            "ok": False,
            "status": "busy",
            "trade_date": trade_date.isoformat(),
            "message": "Screen already running.",
        }
    try:
        a = run_phase_a(force=True, session=trade_date, max_workers=max_workers)
        b = run_phase_b(force=True, session=trade_date, max_workers=max_workers)
        payload = dict(b)
        payload["phase_a_result"] = {
            "status": a.get("status"),
            "phase_a": a.get("phase_a"),
        }
        _last_run = dict(payload)
        _last_run["at"] = _now().isoformat()
        return payload
    finally:
        _run_lock.release()


def _enrich_ltp(rows: List[Dict[str, Any]]) -> List[Dict[str, Any]]:
    """Attach live FUT ``ltp`` only — never overwrite frozen ``entry``."""
    if not rows:
        return rows
    keys = []
    for r in rows:
        ik = str(r.get("instrument_key") or "").strip()
        if ik:
            keys.append(ik)
    if not keys:
        return rows
    ltp_map: Dict[str, float] = {}
    try:
        upstox = _upstox()
        uniq = list(dict.fromkeys(keys))
        batch = upstox.get_market_quotes_batch_by_keys(uniq) or {}
        for k, v in batch.items():
            try:
                px = float(v)
            except (TypeError, ValueError):
                continue
            if px > 0:
                ltp_map[str(k)] = px
    except Exception:
        logger.debug("olh ltp enrich failed", exc_info=True)
        return rows

    out: List[Dict[str, Any]] = []
    for r in rows:
        card = dict(r)
        ik = str(card.get("instrument_key") or "").strip()
        ltp = ltp_map.get(ik)
        if ltp is not None:
            card["ltp"] = ltp
        # Entry stays frozen from screen-time snapshot (09:22 / 09:20±slip).
        if card.get("entry") is None and not card.get("equity_instrument_key"):
            # Legacy future-only rows: first_*/second_* were FUT OHLC.
            if card.get("second_close") is not None:
                card["entry"] = card.get("second_close")
            elif card.get("first_close") is not None:
                card["entry"] = card.get("first_close")
        out.append(card)
    return out


def get_today_state(*, auto_run: bool = True) -> Dict[str, Any]:
    """API payload for the live page. Phase A greys return before Phase B freeze."""
    ensure_tables()
    trade_date = _today()
    win = window_status()
    meta = get_day_meta(trade_date)
    frozen = bool(meta and meta.get("frozen")) or is_day_frozen(trade_date)

    if auto_run and not frozen:
        from backend.services.open_low_high.phases import (
            phase_a_window_status,
            phase_b_window_status,
        )

        if phase_b_window_status() == "open":
            return run_phase_b(force=False, session=trade_date)
        if phase_a_window_status() == "open":
            return run_phase_a(force=False, session=trade_date)

    rows = _enrich_ltp(list_screen_for_date(trade_date))
    bullish = [r for r in rows if str(r.get("side") or "").upper() == "BULLISH"]
    bearish = [r for r in rows if str(r.get("side") or "").upper() == "BEARISH"]
    active_n = sum(1 for r in rows if r.get("active"))
    wait_n = sum(1 for r in rows if r.get("wait_list") and not r.get("active"))
    non_active_n = len(rows) - active_n - wait_n
    grey_n = wait_n + non_active_n

    nifty = (
        (meta.get("meta") if isinstance(meta, dict) and isinstance(meta.get("meta"), dict) else None)
        or {}
    )
    failure_banner = nifty.get("failure_banner")
    meta_status = str((meta or {}).get("status") or "")

    if frozen:
        status = "frozen"
        message = (
            f"Frozen for today — {active_n} active, {wait_n} wait, "
            f"{non_active_n} non-active. Cards and sector tags will not change."
        )
    elif meta_status in {"phase_b_partial", "phase_b_failed"} or failure_banner:
        status = meta_status or "phase_b_partial"
        message = str(failure_banner or (meta or {}).get("notes") or "Phase B incomplete.")
    elif meta_status == "phase_a" or (rows and not frozen and active_n == 0 and win == "waiting"):
        status = "phase_a"
        message = (
            f"Phase A on board — {len(rows)} grey candidate(s). "
            f"Phase B from {SCREEN_AFTER.strftime('%H:%M:%S')} IST."
        )
    elif meta_status == "incomplete":
        status = "incomplete"
        message = (
            "Screen deferred — equity/Nifty data incomplete. Retrying until ~09:29 IST."
        )
    elif win == "waiting" and not rows:
        status = "waiting"
        message = (
            "Waiting for Phase A (09:20:10 IST) on the completed 09:15 candle; "
            "Phase B follows at 09:25:03."
        )
    elif win == "past" and not rows:
        status = "empty"
        message = "No screen stored for today. Use Refresh (force) or wait for tomorrow's window."
    elif rows:
        status = meta_status or "live"
        message = "Showing stored screen rows."
    else:
        status = "waiting"
        message = "No qualifiers yet."

    if failure_banner:
        message = str(failure_banner)

    return {
        "ok": True,
        "status": status,
        "trade_date": trade_date.isoformat(),
        "window": win,
        "frozen": frozen,
        "bullish": bullish,
        "bearish": bearish,
        "active": active_n,
        "wait": wait_n,
        "non_active": non_active_n,
        "candidates": grey_n,
        "phase_a": len(rows),
        "meta": meta,
        "nifty_gap": nifty.get("nifty_gap"),
        "nifty_bias": nifty.get("nifty_bias"),
        "nifty_show_on": nifty.get("nifty_show_on"),
        "failure_banner": failure_banner,
        "server_time": _now().isoformat(),
        "message": message,
        "last_run": _last_run or None,
    }


def get_trade_log_payload(
    *,
    limit_days: int = 60,
    side: Optional[str] = None,
) -> Dict[str, Any]:
    rows = list_trade_log(limit_days=limit_days, side=side)
    by_date: Dict[str, List[Dict[str, Any]]] = {}
    for r in rows:
        # Chart modal needs these on every log row (same as Screen cards).
        d = str(r.get("trade_date") or "")[:10]
        r["trade_date"] = d
        r["symbol"] = str(r.get("symbol") or "").upper()
        r["instrument_key"] = str(r.get("instrument_key") or "").strip() or None
        r["equity_instrument_key"] = (
            str(r.get("equity_instrument_key") or "").strip() or None
        )
        by_date.setdefault(d, []).append(r)
    sections = [{"date": d, "rows": by_date[d]} for d in sorted(by_date.keys(), reverse=True)]
    return {
        "ok": True,
        "sections": sections,
        "total": len(rows),
        "limit_days": limit_days,
    }
