"""Live Open Low High screener — universe scan, Phase A/B, freeze + sector tags.

Schedule (IST):
  ~09:14–09:23  Pre-warm Upstox REST equity/Nifty 5m + prev-day bars.
  ~09:25:10     Phase A on completed 09:15 bar + Phase B on completed 09:20 bar
                (Upstox REST 5m preferred; WS 1m→5m fills gaps only).
  ~09:25–09:29  If Phase A is empty and equity REST is missing, defer freeze and
                retry every ~20s until data quality OK (hard deadline ~09:29:30).
                Never persist a frozen-empty day that blocks later screens.
  After freeze: card list + S1–S3/W1–W3 tags are immutable for the day.

Data split (equity vs stock future):
  - **Algo filters** (Phase A/B, PDH/PDL/prev close, VWAP, doji, wick/body,
    extremes): evaluated on the **underlying NSE equity** 5m + daily bars.
  - **Trade display** (Entry, SL, T1/T2, chart levels): expressed on the
    **front-month stock future**. SL = future 09:15 open; Entry is **frozen**
    at screen time (prefer ~09:22 future 1m close; else 09:20 future close
    ± small directional slippage) and is never overwritten by live LTP;
    T1/T2 = traditional pivots from **future** prior-day H/L/C. Stored
    ``first_*`` / ``prev_*`` / VWAP remain equity values for audit.

Phase A passers always persist (Screen + Trade Log). Phase B passers are
``active=True`` (golden border, sorted to top); Phase A-only are greyed
candidates.

Phase B uses 09:20 close vs 09:15 close, then a 09:20 wick/body filter
(≤0.35). The 09:20 bar is only final at 09:25 — screening earlier on a
forming bar false-negatives confirms that arrive later (seen 2026-11-18 POLYCAB).
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
# Window opens once the 09:20 bar has closed (09:25:00); small buffer for WS/REST.
SCREEN_AFTER = time(9, 25, 5)
# Keep window "open" through incomplete-data retries (scheduler fires ~every 20s).
FREEZE_BY = time(9, 29, 30)
HARD_DEADLINE = time(9, 29, 30)
# Fraction of universe missing equity 09:15 / prev-day that blocks empty freeze.
MISSING_DATA_RATE_THRESHOLD = 0.35

_run_lock = threading.Lock()
_last_run: Dict[str, Any] = {}
_prewarm_lock = threading.Lock()
_last_prewarm: Dict[str, Any] = {}


def _now() -> datetime:
    return datetime.now(IST)


def _today() -> date:
    return _now().date()


def _upstox():
    from backend.config import settings
    from backend.services.upstox_service import UpstoxService

    return UpstoxService(settings.UPSTOX_API_KEY, settings.UPSTOX_API_SECRET)


def window_status(now: Optional[datetime] = None) -> str:
    """waiting | open | past — relative to the 09:25:05–09:29:30 screen window."""
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


def _fetch_ws_5m_bars(instrument_key: str, session: date) -> List[Bar]:
    """Aggregate Upstox WS 1m candles into 5m bars for today's session (gap-fill only)."""
    rows = _fetch_ws_1m_rows(instrument_key, session)
    if not rows:
        return []
    # Aggregate 1m → 5m aligned to :00/:05/:10… IST
    buckets: Dict[datetime, Dict[str, float]] = {}
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
            buckets[bucket] = {"open": o, "high": h, "low": l, "close": c, "volume": vol}
        else:
            cur["high"] = max(cur["high"], h)
            cur["low"] = min(cur["low"], l)
            cur["close"] = c
            cur["volume"] += vol
    out: List[Bar] = []
    for bucket, ohlc in sorted(buckets.items()):
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
    return out


def _hist_range_end(session: date) -> date:
    """Upstox ``range_end_date`` often omits that calendar day's candles.

    Equity usually appears with ``session+1``; index (NIFTY) can need
    ``session+2``. Use the later of those floors so overnight force screens
    still see the session's 5m bars.
    """
    return max(session + timedelta(days=2), _today() + timedelta(days=1))


def _fetch_rest_5m(upstox: Any, instrument_key: str, session: date) -> List[Bar]:
    try:
        raw = upstox.get_historical_candles_by_instrument_key(
            instrument_key,
            interval="minutes/5",
            days_back=5,
            range_end_date=_hist_range_end(session),
        )
    except Exception as exc:
        logger.info("olh rest 5m %s: %s", instrument_key, exc)
        raw = None
    if not raw:
        # Live intraday endpoint as last resort
        try:
            raw = upstox._fetch_intraday_candles_v3(instrument_key, "minutes/5")
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
    """REST 5m preferred; WS 1m→5m gap-fill only (same merge as future path)."""
    if not instrument_key:
        return []
    ws_bars = _fetch_ws_5m_bars(instrument_key, session)
    rest_bars = _fetch_rest_5m(upstox, instrument_key, session)
    return _merge_bars(rest_bars, ws_bars)


def _screen_one(
    upstox: Any,
    row: Dict[str, str],
    session: date,
    expiry: date,
    miss_stats: Optional[Dict[str, int]] = None,
) -> Optional[Dict[str, Any]]:
    """Return a Phase A candidate. ``active`` is True only when Phase B also passes.

    Screening uses **equity** OHLC; Entry/SL/T1/T2 use the **stock future**.
    ``miss_stats`` accumulates REST/equity data gaps (not Phase A filter rejects).
    """
    symbol = str(row.get("symbol") or "").upper()
    if not symbol:
        return None

    equity_ik = _resolve_equity_key(upstox, row, symbol)
    if not equity_ik:
        logger.info("olh skip %s: no equity instrument key", symbol)
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
        return None

    # --- Algo path: underlying equity ---
    eq_bars = _session_5m_bars(upstox, equity_ik, session)
    if not eq_bars:
        _miss_bump(miss_stats, "no_bars")
        return None

    first = bar_at(eq_bars, session, FIRST_CANDLE)
    second = bar_at(eq_bars, session, SECOND_CANDLE)
    if first is None:
        _miss_bump(miss_stats, "no_0915")
        return None

    vwap = first_vwap(eq_bars, session, FIRST_CANDLE)
    if vwap is None:
        vwap = first.typical

    prev_high, prev_low, prev_close = _fetch_daily_prev(upstox, equity_ik, session)
    if prev_high is None or prev_low is None:
        _miss_bump(miss_stats, "no_prev")
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
        return None

    # Phase B on equity 09:20 close confirm + wick/body (window ~09:25:05).
    second_close: Optional[float] = float(second.close) if second is not None else None
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
    olh_badge = open_extreme_badge(
        phase_a.side,
        first_open=phase_a.first_open,
        first_high=phase_a.first_high,
        first_low=phase_a.first_low,
    )

    # --- Display path: front-month stock future ---
    fut_bars = _session_5m_bars(upstox, fut_ik, session)
    fut_first = bar_at(fut_bars, session, FIRST_CANDLE) if fut_bars else None
    fut_second = bar_at(fut_bars, session, SECOND_CANDLE) if fut_bars else None

    # SL = future 09:15 open (required for trade levels on the FUT chart).
    if fut_first is not None:
        sl = float(fut_first.open)
    else:
        # Rare: future 09:15 missing — refuse to invent equity SL on FUT scale.
        logger.info("olh %s: no future 09:15 bar for SL; skip display levels", symbol)
        sl = None

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

    # T1/T2: pivots from future prior-day H/L/C; reference = future entry snapshot.
    t1: Optional[float] = None
    t2: Optional[float] = None
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
        # Equity audit fields (algo inputs)
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
        # Future trade levels — Entry frozen at screen (never live-LTP overwritten)
        "sl": sl,
        "entry": fut_entry_snap,
        "entry_method": entry_method,
        "t1": t1,
        "t2": t2,
        "data_source_logic": "equity",
        "data_source_levels": "future",
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


def run_screen(
    *,
    force: bool = False,
    session: Optional[date] = None,
    max_workers: int = 6,
) -> Dict[str, Any]:
    """Run Phase A+B screen and freeze today's set.

    Outside the 09:25:05–09:29:30 window, refuses unless ``force=True`` (manual/test).
    If already frozen for the day, returns stored rows without re-scanning.

    Empty Phase A with high missing equity/Nifty REST rate does **not** freeze;
    returns ``status=incomplete`` so the scheduler can retry until the hard deadline.
    """
    ensure_tables()
    trade_date = session or _today()
    now = _now()
    win = window_status(now)

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

    if win == "waiting" and not force:
        return {
            "ok": True,
            "status": "waiting",
            "trade_date": trade_date.isoformat(),
            "window": win,
            "frozen": False,
            "bullish": [],
            "bearish": [],
            "message": "Waiting for 09:25 IST screen window (09:20 candle must complete).",
        }

    if not _run_lock.acquire(blocking=False):
        return {
            "ok": False,
            "status": "busy",
            "trade_date": trade_date.isoformat(),
            "message": "Screen already running.",
        }

    try:
        global _last_run
        upsert_day_meta(trade_date, status="screening", frozen=False)
        upstox = _upstox()
        universe = load_universe()
        expiry = front_month_expiry(trade_date)
        logger.info(
            "olh screen start date=%s expiry=%s symbols=%s force=%s",
            trade_date,
            expiry,
            len(universe),
            force,
        )

        results: List[Dict[str, Any]] = []
        errors = 0
        miss_stats: Dict[str, int] = {
            "no_equity_key": 0,
            "no_bars": 0,
            "no_0915": 0,
            "no_prev": 0,
        }
        miss_lock = threading.Lock()

        def _one(urow: Dict[str, str]) -> Optional[Dict[str, Any]]:
            local_miss: Dict[str, int] = {}
            try:
                got = _screen_one(upstox, urow, trade_date, expiry, local_miss)
            except Exception:
                logger.exception("olh screen symbol failed %s", urow.get("symbol"))
                return None
            if local_miss:
                with miss_lock:
                    for k, v in local_miss.items():
                        miss_stats[k] = int(miss_stats.get(k) or 0) + int(v)
            return got

        # Bounded parallelism to respect Upstox rate limits somewhat.
        with ThreadPoolExecutor(max_workers=max(1, min(max_workers, 8))) as pool:
            futs = {pool.submit(_one, u): u for u in universe}
            for fut in as_completed(futs):
                try:
                    got = fut.result()
                except Exception:
                    errors += 1
                    continue
                if got:
                    results.append(got)
                # Soft pacing
                time_mod.sleep(0.02)

        tagged = _apply_sector_badges(results)
        for c in tagged:
            c["frozen"] = True

        nifty_cue = _evaluate_nifty_header_cue(upstox, trade_date)
        nifty_ready = _nifty_data_ready(nifty_cue, require_0920=True)

        tagged = sort_screen_rows(tagged)

        incomplete, quality = data_quality_incomplete(
            phase_a_count=len(tagged),
            scanned=len(universe),
            miss_stats=miss_stats,
            nifty_ready=nifty_ready,
        )
        past_deadline = _past_hard_deadline(now)

        # Never replace a non-empty frozen day with an empty force scan
        # (e.g. Upstox historical lag overnight).
        if not tagged:
            existing = list_screen_for_date(trade_date)
            if existing:
                logger.warning(
                    "olh force/screen returned 0 rows; keeping existing %s for %s",
                    len(existing),
                    trade_date,
                )
                meta = get_day_meta(trade_date) or {}
                nifty = (
                    (meta.get("meta") if isinstance(meta.get("meta"), dict) else None) or {}
                )
                return {
                    "ok": True,
                    "status": "frozen",
                    "trade_date": trade_date.isoformat(),
                    "window": win,
                    "frozen": True,
                    "bullish": [r for r in existing if r.get("side") == "BULLISH"],
                    "bearish": [r for r in existing if r.get("side") == "BEARISH"],
                    "meta": meta,
                    "nifty_gap": nifty.get("nifty_gap"),
                    "nifty_bias": nifty.get("nifty_bias"),
                    "nifty_show_on": nifty.get("nifty_show_on"),
                    "message": (
                        f"Screen returned 0 qualifiers — kept existing "
                        f"{len(existing)} rows (data lag?)."
                    ),
                    "kept_existing": True,
                    "data_quality": quality,
                }

        # Empty Phase A + missing equity/Nifty REST → do not freeze (retry window).
        if incomplete and not tagged:
            reason = (
                "nifty_rest_missing"
                if not nifty_ready
                else f"missing_data_rate={quality['missing_rate']:.2%}"
            )
            notes = (
                f"incomplete screened={len(universe)} phase_a=0 "
                f"missing={quality['data_missing']} rate={quality['missing_rate']:.2%} "
                f"nifty_ready={nifty_ready} past_deadline={past_deadline} "
                f"reason={reason} force={force} "
                f"no_bars={miss_stats.get('no_bars', 0)} "
                f"no_0915={miss_stats.get('no_0915', 0)} "
                f"no_prev={miss_stats.get('no_prev', 0)}"
            )
            logger.warning(
                "olh defer empty freeze: %s (retry until hard deadline %s)",
                notes,
                HARD_DEADLINE.isoformat(),
            )
            day_meta = {
                "window": win,
                "expiry": expiry.isoformat(),
                "active": 0,
                "wait": 0,
                "non_active": 0,
                "candidates": 0,
                "incomplete": True,
                "freeze_deferred": True,
                "freeze_reason": reason,
                "data_quality": quality,
                "past_hard_deadline": past_deadline,
                **nifty_cue,
            }
            upsert_day_meta(
                trade_date,
                status="incomplete",
                frozen=False,
                bullish_count=0,
                bearish_count=0,
                notes=notes[:500],
                meta=day_meta,
            )
            payload = {
                "ok": True,
                "status": "incomplete",
                "trade_date": trade_date.isoformat(),
                "window": win,
                "frozen": False,
                "bullish": [],
                "bearish": [],
                "scanned": len(universe),
                "kept": 0,
                "phase_a": 0,
                "active": 0,
                "wait": 0,
                "non_active": 0,
                "candidates": 0,
                "errors": errors,
                "data_quality": quality,
                "freeze_reason": reason,
                "past_hard_deadline": past_deadline,
                "nifty_gap": nifty_cue.get("nifty_gap"),
                "nifty_bias": nifty_cue.get("nifty_bias"),
                "nifty_show_on": nifty_cue.get("nifty_show_on"),
                "message": (
                    "Screen deferred — Phase A empty with missing equity/Nifty REST "
                    f"({reason}). Not freezing; will retry until "
                    f"{HARD_DEADLINE.strftime('%H:%M:%S')} IST."
                    if not past_deadline
                    else (
                        "Screen incomplete past hard deadline — Phase A empty with "
                        f"missing equity/Nifty REST ({reason}). Left unfrozen so a "
                        "later force screen can run when data arrives."
                    )
                ),
            }
            _last_run = dict(payload)
            _last_run["at"] = _now().isoformat()
            return payload

        upsert_screen_rows(trade_date, tagged, frozen=True)
        bullish = [c for c in tagged if c.get("side") == "BULLISH"]
        bearish = [c for c in tagged if c.get("side") == "BEARISH"]
        active_n = sum(1 for c in tagged if c.get("active"))
        wait_n = sum(1 for c in tagged if c.get("wait_list") and not c.get("active"))
        non_active_n = len(tagged) - active_n - wait_n
        candidate_n = wait_n + non_active_n
        phase_ts = _now()
        freeze_reason = (
            "phase_a_qualifiers"
            if tagged
            else "empty_phase_a_data_ok"
        )
        day_meta = {
            "window": win,
            "expiry": expiry.isoformat(),
            "active": active_n,
            "wait": wait_n,
            "non_active": non_active_n,
            "candidates": candidate_n,
            "freeze_reason": freeze_reason,
            "data_quality": quality,
            **nifty_cue,
        }
        upsert_day_meta(
            trade_date,
            status="frozen",
            frozen=True,
            bullish_count=len(bullish),
            bearish_count=len(bearish),
            phase_a_at=phase_ts,
            phase_b_at=phase_ts,
            notes=(
                f"screened={len(universe)} phase_a={len(tagged)} "
                f"active={active_n} wait={wait_n} non_active={non_active_n} "
                f"errors={errors} force={force} freeze_reason={freeze_reason} "
                f"missing_rate={quality['missing_rate']:.2%} nifty_ready={nifty_ready}"
            ),
            meta=day_meta,
        )
        payload = {
            "ok": True,
            "status": "frozen",
            "trade_date": trade_date.isoformat(),
            "window": win,
            "frozen": True,
            "bullish": bullish,
            "bearish": bearish,
            "scanned": len(universe),
            "kept": active_n,
            "phase_a": len(tagged),
            "active": active_n,
            "wait": wait_n,
            "non_active": non_active_n,
            "candidates": candidate_n,
            "errors": errors,
            "freeze_reason": freeze_reason,
            "data_quality": quality,
            "nifty_gap": nifty_cue.get("nifty_gap"),
            "nifty_bias": nifty_cue.get("nifty_bias"),
            "nifty_show_on": nifty_cue.get("nifty_show_on"),
            "message": (
                f"Screen complete — {active_n} active, {wait_n} wait, "
                f"{non_active_n} non-active. Frozen for the day."
            ),
        }
        _last_run = dict(payload)
        _last_run["at"] = phase_ts.isoformat()
        logger.info(
            "olh screen done phase_a=%s active=%s wait=%s non_active=%s "
            "bull=%s bear=%s freeze_reason=%s missing_rate=%.2f nifty_ready=%s",
            len(tagged),
            active_n,
            wait_n,
            non_active_n,
            len(bullish),
            len(bearish),
            freeze_reason,
            quality["missing_rate"],
            nifty_ready,
        )
        return payload
    except Exception as exc:
        logger.exception("olh run_screen failed")
        upsert_day_meta(trade_date, status="error", frozen=False, notes=str(exc)[:500])
        return {
            "ok": False,
            "status": "error",
            "trade_date": trade_date.isoformat(),
            "message": str(exc),
        }
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
    """API payload for the live page. Optionally triggers screen inside the window."""
    ensure_tables()
    trade_date = _today()
    win = window_status()
    meta = get_day_meta(trade_date)
    frozen = bool(meta and meta.get("frozen")) or is_day_frozen(trade_date)

    if auto_run and not frozen and win == "open":
        return run_screen(force=False, session=trade_date)

    rows = _enrich_ltp(list_screen_for_date(trade_date))
    bullish = [r for r in rows if str(r.get("side") or "").upper() == "BULLISH"]
    bearish = [r for r in rows if str(r.get("side") or "").upper() == "BEARISH"]
    active_n = sum(1 for r in rows if r.get("active"))
    wait_n = sum(1 for r in rows if r.get("wait_list") and not r.get("active"))
    non_active_n = len(rows) - active_n - wait_n
    grey_n = wait_n + non_active_n

    meta_status = str((meta or {}).get("status") or "")
    if frozen:
        status = "frozen"
        message = (
            f"Frozen for today — {active_n} active, {wait_n} wait, "
            f"{non_active_n} non-active. Cards and sector tags will not change."
        )
    elif meta_status == "incomplete":
        status = "incomplete"
        message = (
            "Screen deferred — equity/Nifty REST incomplete at freeze time. "
            "Retrying until ~09:29 IST (will not empty-freeze)."
        )
    elif win == "waiting":
        status = "waiting"
        message = "Waiting for 09:15 + 09:20 candles to complete — screen runs ~09:25 IST."
    elif win == "past" and not rows:
        status = "empty"
        message = "No screen stored for today. Use Refresh (force) or wait for tomorrow's window."
    elif rows:
        status = "live"
        message = "Showing stored screen rows."
    else:
        status = "waiting"
        message = "No qualifiers yet."

    nifty = (meta.get("meta") if isinstance(meta, dict) and isinstance(meta.get("meta"), dict) else None) or {}
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
        "meta": meta,
        "nifty_gap": nifty.get("nifty_gap"),
        "nifty_bias": nifty.get("nifty_bias"),
        "nifty_show_on": nifty.get("nifty_show_on"),
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
