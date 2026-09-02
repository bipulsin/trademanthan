"""Breakfast live minute ticks (9:16–9:19 WS/REST 1m) and 9:20:05 freeze — REST 5m."""
from __future__ import annotations

import logging
import threading
import time
from dataclasses import dataclass, field
from datetime import date, datetime, time as dt_time
from typing import Any, Dict, List, Optional, Set, Tuple

import pytz

from backend.config import settings
from backend.services.breakfast_strategy.candles import (
    anchor_bar,
    default_cache_dir,
    ensure_5m_cached,
    fetch_1m_parallel,
    fetch_5m_parallel,
    first_5m_bar,
    forming_bar_from_1m_upto,
    load_cached_1m,
    move_pct_vs_prev_close,
    prev_session_close,
)
from backend.services.breakfast_strategy.config import SL_PCT, TP_PCT
from backend.services.breakfast_prev_close import (
    WICK_NONE,
    filter_live_stocks_by_wick_and_color,
    filter_sector_members_by_first_5m_color,
    filter_sector_members_by_wick,
    load_stored_prev_closes_and_wicks,
)
from backend.services.breakfast_strategy.engine import NIFTY50_KEY
from backend.services.breakfast_strategy.engine_prevclose import (
    nifty_bias_from_bar_vs_prev_close,
    rank_sectors_vs_prev_close,
    select_breakfast_picks_prevclose,
)
from backend.services.breakfast_strategy.live_persist import (
    fetch_session_lock,
    persist_live_signals,
    persist_session_lock,
)
from backend.services.breakfast_strategy.universe import (
    SECTOR_UNIVERSE,
    build_instrument_indexes,
    fo_eligible_sector_keys,
    load_arbitrage_by_sector,
    resolve_stock_instrument,
    sector_index_key_for_label,
)
from backend.services.breakfast_upstox_gate import breakfast_upstox_priority_owner
from backend.services.market_holiday import is_nse_holiday_ist
from backend.services.sector_movers import _index_key_to_sector_label
from backend.services.upstox_service import UpstoxService

logger = logging.getLogger(__name__)
IST = pytz.timezone("Asia/Kolkata")

LIVE_SECTORS_TO_PICK = 2
LIVE_STOCKS_PER_SECTOR = 3
TICK_MINUTES = (16, 17, 18, 19, 20)
SCHEDULER_TICK_MINUTES = (16, 17, 18, 19)
PRE_FREEZE_WARN_MINUTE = 18
INDEX_UNIVERSE_N = 17  # Nifty 50 + 16 sector indices
FREEZE_AT = dt_time(9, 20, 5)
FREEZE_SOURCE_VALUES = ("ws_1m", "rest_1m", "rest_5m", "none")
MAX_TICK_SEC = 20.0
# Per-instrument WS freshness for 9:16–9:20 forming ticks (seconds since last WS tick).
# 90s: liquid names tick every few seconds at the open; brief hiccups tolerated but we
# fall back to REST sooner than UPSTOX_MARKET_FEED_STALE_SEC (120s), which governs the
# global OI/LTP cache used elsewhere. WS warms from 9:10 so staleness here is rare.
BREAKFAST_WS_1M_STALE_SEC = 90.0

_LOCK = threading.Lock()
_SESSION_CACHE: Dict[str, "BreakfastSessionCache"] = {}
_LIVE_TICK_SNAPSHOT: Optional[Dict[str, Any]] = None
_FREEZE_ATTEMPTS: Dict[str, int] = {}
_MAX_FREEZE_RETRIES = 3
_SESSION_MONITOR: Dict[str, Dict[str, Any]] = {}
_LAST_WARMUP: Optional[Dict[str, Any]] = None


@dataclass
class BreakfastSessionCache:
    session_date: str
    sector_keys: List[str] = field(default_factory=list)
    picked_sector_keys: List[str] = field(default_factory=list)
    stock_symbols_by_sector: Dict[str, List[str]] = field(default_factory=dict)
    instrument_keys: List[str] = field(default_factory=list)
    candles_1m: Dict[str, List[Dict[str, Any]]] = field(default_factory=dict)
    locked: bool = False


def _sector_label(sector_key: str) -> str:
    return _index_key_to_sector_label().get(str(sector_key or "").strip()) or str(sector_key)


def _now_ist() -> datetime:
    return datetime.now(IST)


def _is_trading_day(now: datetime) -> bool:
    if now.weekday() >= 5:
        return False
    noon = IST.localize(datetime.combine(now.date(), dt_time(12, 0)))
    return not is_nse_holiday_ist(noon)


def _upto_hhmm_for_tick(minute: int) -> Tuple[int, int]:
    """Minute-close boundary: tick at :MM uses bars through :MM inclusive."""
    if minute < 16:
        return (9, 16)
    if minute >= 20:
        return (9, 21)
    return (9, minute + 1)


def _phase_for_minute(minute: int, *, freezing: bool = False) -> str:
    if freezing:
        return "locking"
    if minute < 20:
        return "forming"
    return "bar_closing"


def _get_cache(session_date: str) -> BreakfastSessionCache:
    with _LOCK:
        if session_date not in _SESSION_CACHE:
            _SESSION_CACHE[session_date] = BreakfastSessionCache(session_date=session_date)
        return _SESSION_CACHE[session_date]


def get_live_tick_snapshot() -> Optional[Dict[str, Any]]:
    with _LOCK:
        return dict(_LIVE_TICK_SNAPSHOT) if _LIVE_TICK_SNAPSHOT else None


def get_breakfast_session_monitor_stats(session_date: str) -> Dict[str, Any]:
    """In-memory tick-source / re-pick stats for post-session Telegram report."""
    sd = str(session_date or "")[:10]
    with _LOCK:
        raw = _SESSION_MONITOR.get(sd)
        warmup = dict(_LAST_WARMUP) if _LAST_WARMUP else None
    if not raw:
        return {"session_date": sd, "tick_sources": [], "repicks": [], "warmup": warmup}
    return {
        "session_date": sd,
        "tick_sources": [dict(r) for r in raw.get("tick_sources") or []],
        "repicks": [dict(r) for r in raw.get("repicks") or []],
        "warmup": warmup or raw.get("warmup"),
    }


def get_last_warmup_result() -> Optional[Dict[str, Any]]:
    with _LOCK:
        return dict(_LAST_WARMUP) if _LAST_WARMUP else None


def _monitor_bucket(session_date: str) -> Dict[str, Any]:
    with _LOCK:
        if session_date not in _SESSION_MONITOR:
            _SESSION_MONITOR[session_date] = {
                "tick_sources": [],
                "repicks": [],
                "warmup": None,
            }
        return _SESSION_MONITOR[session_date]


def _record_tick_sources(session_date: str, tick_source_log: List[Dict[str, Any]]) -> None:
    if not tick_source_log:
        return
    bucket = _monitor_bucket(session_date)
    bucket["tick_sources"].extend(dict(r) for r in tick_source_log)


def _record_sector_repick(
    session_date: str,
    *,
    minute: int,
    prev_picked: List[str],
    picked: List[str],
    stock_count: int,
) -> None:
    bucket = _monitor_bucket(session_date)
    bucket["repicks"].append(
        {
            "minute": minute,
            "from": list(prev_picked),
            "to": list(picked),
            "stocks": stock_count,
        }
    )


def _all_sector_keys() -> List[str]:
    keys: List[str] = []
    for label, _yh in SECTOR_UNIVERSE:
        ik = sector_index_key_for_label(label)
        if ik:
            keys.append(ik)
    return keys


def _merge_today_ws_with_cached(
    ws_bars: List[Dict[str, Any]],
    cached: List[Dict[str, Any]],
    session_date: date,
) -> List[Dict[str, Any]]:
    """Today's session 1m from WS; prior sessions from disk cache."""
    day = session_date.isoformat()
    hist = [c for c in (cached or []) if not str(c.get("timestamp") or "").startswith(day)]
    by_ts: Dict[str, Dict[str, Any]] = {}
    for c in hist + list(ws_bars or []):
        ts = str(c.get("timestamp") or "")
        if ts:
            by_ts[ts] = c
    return [by_ts[k] for k in sorted(by_ts)]


def _ws_usable_for_forming(
    instrument_key: str,
    ws_bars: List[Dict[str, Any]],
    *,
    session_date: date,
    upto_hhmm: Tuple[int, int],
    cache_dir: Any,
) -> Tuple[bool, Optional[str]]:
    """True when WS feed is fresh and yields a forming bar for upto_hhmm."""
    from backend.services.upstox_market_feed import get_ws_feed_row

    row = get_ws_feed_row(instrument_key)
    if not row:
        return False, "no_ws_feed"
    age = float(row.get("age_sec") or 999)
    if age > BREAKFAST_WS_1M_STALE_SEC:
        return False, f"ws_feed_stale_{age:.0f}s"
    cached = load_cached_1m(cache_dir, instrument_key)
    merged = _merge_today_ws_with_cached(ws_bars, cached, session_date)
    if not forming_bar_from_1m_upto(merged, session_date, upto_hhmm):
        return False, "ws_insufficient_1m_bars"
    return True, None


def _composite_data_source(tick_source_log: List[Dict[str, Any]]) -> str:
    srcs = {str(r.get("source") or "") for r in tick_source_log}
    srcs.discard("")
    if not srcs or srcs <= {"ws_1m"}:
        return "ws_1m"
    parts = [s for s in ("ws_1m", "rest_1m", "rest_5m", "none") if s in srcs]
    return "+".join(parts) if parts else "ws_1m"


def _maybe_warn_and_warm_5m_pre_freeze(
    *,
    minute: int,
    index_keys: List[str],
    source_log: List[Dict[str, Any]],
    ux: UpstoxService,
    cache_dir: Any,
    session_date: date,
) -> None:
    """~9:18: if every Nifty+sector key is off WS, warn and start warming minutes/5."""
    if minute != PRE_FREEZE_WARN_MINUTE:
        return
    index_set = set(index_keys)
    rows = [r for r in source_log if r.get("instrument_key") in index_set]
    if not rows:
        return
    ws_ok = any(r.get("source") == "ws_1m" for r in rows)
    if ws_ok:
        return
    logger.warning(
        "breakfast_pre_freeze_ws_dead minute=%s index_keys=%s all rest_fallback/no_ws_feed — "
        "warming REST minutes/5 before 9:20:05 freeze",
        minute,
        len(index_keys),
    )
    for ik in index_keys:
        try:
            ensure_5m_cached(
                ux,
                cache_dir,
                ik,
                range_end=session_date,
                session_dates=[session_date],
                force=True,
            )
        except Exception as e:
            logger.warning("breakfast 5m warm failed %s: %s", ik, e)


def _has_forming_1m_bar(
    candles: List[Dict[str, Any]],
    session_date: date,
    upto_hhmm: Tuple[int, int],
) -> bool:
    return forming_bar_from_1m_upto(candles or [], session_date, upto_hhmm) is not None


def _resolve_candles_ws_primary(
    ux: UpstoxService,
    cache_dir: Any,
    instrument_keys: List[str],
    *,
    session_date: date,
    upto_hhmm: Tuple[int, int],
    tick_minute: int,
) -> Tuple[Dict[str, List[Dict[str, Any]]], List[Dict[str, Any]]]:
    """WS 1m, then REST minutes/1. Never REST 5m (freeze uses ``_resolve_candles_rest_5m``).

    Source per key: ``ws_1m`` / ``rest_1m`` / ``none``.
    """
    from backend.services.upstox_market_feed import get_ws_1m_bars_for_session

    keys = [str(k).strip() for k in instrument_keys if str(k or "").strip()]
    candles_out: Dict[str, List[Dict[str, Any]]] = {}
    source_log: List[Dict[str, Any]] = []
    fallback_keys: List[str] = []
    ws_fail_reason: Dict[str, Optional[str]] = {}

    ws_by_key: Dict[str, List[Dict[str, Any]]] = {
        ik: get_ws_1m_bars_for_session(ik, session_date) for ik in keys
    }

    for ik in keys:
        ok, reason = _ws_usable_for_forming(
            ik, ws_by_key.get(ik, []), session_date=session_date, upto_hhmm=upto_hhmm, cache_dir=cache_dir
        )
        if ok:
            cached = load_cached_1m(cache_dir, ik)
            candles_out[ik] = _merge_today_ws_with_cached(ws_by_key.get(ik, []), cached, session_date)
            source_log.append(
                {"minute": tick_minute, "instrument_key": ik, "source": "ws_1m", "reason": None}
            )
        else:
            fallback_keys.append(ik)
            ws_fail_reason[ik] = reason

    if fallback_keys:
        rest = fetch_1m_parallel(ux, cache_dir, fallback_keys, session_date=session_date)
        for ik in fallback_keys:
            candles_1m = rest.get(ik) or load_cached_1m(cache_dir, ik)
            if _has_forming_1m_bar(candles_1m, session_date, upto_hhmm):
                candles_out[ik] = candles_1m
                source_log.append(
                    {
                        "minute": tick_minute,
                        "instrument_key": ik,
                        "source": "rest_1m",
                        "reason": ws_fail_reason.get(ik),
                    }
                )
            else:
                candles_out[ik] = candles_1m
                source_log.append(
                    {
                        "minute": tick_minute,
                        "instrument_key": ik,
                        "source": "none",
                        "reason": ws_fail_reason.get(ik) or "no_1m_forming_bar",
                    }
                )

    for row in source_log:
        logger.info(
            "breakfast_tick_source minute=%s instrument_key=%s source=%s reason=%s",
            row["minute"],
            row["instrument_key"],
            row["source"],
            row.get("reason") or "",
        )

    return candles_out, source_log


def _resolve_candles_rest_5m(
    ux: UpstoxService,
    cache_dir: Any,
    instrument_keys: List[str],
    *,
    session_date: date,
    tick_minute: int,
) -> Tuple[Dict[str, List[Dict[str, Any]]], List[Dict[str, Any]]]:
    """REST minutes/5 only — freeze / minute-20 lock snapshot. Parallel HTTP."""
    keys = [str(k).strip() for k in instrument_keys if str(k or "").strip()]
    candles_out: Dict[str, List[Dict[str, Any]]] = {}
    source_log: List[Dict[str, Any]] = []
    fetched = fetch_5m_parallel(ux, cache_dir, keys, session_date=session_date, throttle_sec=0.0)
    for ik in keys:
        candles_5m = fetched.get(ik) or []
        bar_5m = first_5m_bar(candles_5m, session_date)
        candles_out[ik] = candles_5m
        if bar_5m:
            source_log.append(
                {"minute": tick_minute, "instrument_key": ik, "source": "rest_5m", "reason": None}
            )
        else:
            source_log.append(
                {
                    "minute": tick_minute,
                    "instrument_key": ik,
                    "source": "none",
                    "reason": "no_5m_bar",
                }
            )
    for row in source_log:
        logger.info(
            "breakfast_tick_source minute=%s instrument_key=%s source=%s reason=%s",
            row["minute"],
            row["instrument_key"],
            row["source"],
            row.get("reason") or "",
        )
    return candles_out, source_log


def _warmup_instrument_keys(session_date: date) -> List[str]:
    """NIFTY + 16 sector indices + full arbitrage_master candidate stock pool."""
    from backend.services.breakfast_strategy.backtest import collect_instrument_keys

    stocks_by_sector = load_arbitrage_by_sector()
    fut_by_und, eq_by_symbol = build_instrument_indexes()
    return sorted(
        collect_instrument_keys(
            [session_date],
            stocks_by_sector,
            fut_by_und,
            eq_by_symbol,
            spot_proxy_fallback=False,
        )
    )


def breakfast_index_instrument_keys() -> List[str]:
    """Nifty 50 + 16 sector indices (17 keys)."""
    return list(dict.fromkeys([NIFTY50_KEY] + _all_sector_keys()))


def run_breakfast_ws_warmup() -> Dict[str, Any]:
    """9:10 IST — ensure WS feed covers breakfast universe before 9:16 ticks."""
    from backend.services.breakfast_upstox_gate import breakfast_upstox_priority_owner
    from backend.services.upstox_market_feed import ensure_market_feed_running

    now = _now_ist()
    if not _is_trading_day(now):
        return {"ok": False, "skipped": "not_trading_day"}
    keys = _warmup_instrument_keys(now.date())
    with breakfast_upstox_priority_owner():
        ensure_market_feed_running(keys)
    logger.info("breakfast WS warmup: subscribed %s instruments", len(keys))
    out = {"ok": True, "instrument_count": len(keys), "session_date": now.date().isoformat()}
    global _LAST_WARMUP
    with _LOCK:
        _LAST_WARMUP = dict(out)
        sd = out["session_date"]
        if sd not in _SESSION_MONITOR:
            _SESSION_MONITOR[sd] = {"tick_sources": [], "repicks": [], "warmup": None}
        _SESSION_MONITOR[sd]["warmup"] = dict(out)
    return out


def run_breakfast_ws_resubscribe_915() -> Dict[str, Any]:
    """9:15 IST — union 17 index keys into the live WS set without dropping 9:10 stocks."""
    from backend.services.breakfast_upstox_gate import breakfast_upstox_priority_owner
    from backend.services.upstox_market_feed import ensure_market_feed_running, feed_status

    now = _now_ist()
    if not _is_trading_day(now):
        out = {"ok": False, "skipped": "not_trading_day", "index_keys_confirmed": 0}
        logger.info("breakfast_ws_resubscribe_915 -> %s", out)
        return out
    index_keys = breakfast_index_instrument_keys()
    with breakfast_upstox_priority_owner():
        ensure_market_feed_running(index_keys, union=True)
    status: Dict[str, Any] = {}
    try:
        status = feed_status() or {}
    except Exception:
        status = {}
    live_n = int(status.get("universe_keys") or 0)
    confirmed = len(index_keys)
    out = {
        "ok": True,
        "index_keys_confirmed": confirmed,
        "index_key_count_expected": INDEX_UNIVERSE_N,
        "universe_keys": live_n,
        "session_date": now.date().isoformat(),
    }
    logger.info("breakfast_ws_resubscribe_915 -> %s", out)
    return out


def _resolve_stock_keys(
    sector_keys: List[str],
    stocks_by_sector: Dict[str, List[Dict[str, str]]],
    session_date: date,
    fut_by_und: Dict[str, List[Dict[str, Any]]],
    eq_by_symbol: Dict[str, Dict[str, Any]],
) -> Tuple[Dict[str, List[str]], List[str]]:
    sym_by_sector: Dict[str, List[str]] = {}
    instrument_keys: List[str] = []
    seen: Set[str] = set()
    for skey in sector_keys:
        syms: List[str] = []
        for m in stocks_by_sector.get(skey, []):
            sym = str(m.get("stock") or "").upper()
            if not sym:
                continue
            ref = resolve_stock_instrument(sym, session_date, fut_by_und=fut_by_und, eq_by_symbol=eq_by_symbol)
            if not ref or not ref.instrument_key:
                continue
            syms.append(sym)
            ik = str(ref.instrument_key)
            if ik not in seen:
                seen.add(ik)
                instrument_keys.append(ik)
        sym_by_sector[skey] = syms
    return sym_by_sector, instrument_keys


def _rank_picked_sectors(
    *,
    session_date: date,
    candles_1m: Dict[str, List[Dict[str, Any]]],
    stocks_by_sector: Dict[str, List[Dict[str, str]]],
    fut_by_und: Dict[str, List[Dict[str, Any]]],
    eq_by_symbol: Dict[str, Dict[str, Any]],
    upto_hhmm: Tuple[int, int],
    nifty_prev_close: Optional[float] = None,
    sector_prev_closes: Optional[Dict[str, float]] = None,
) -> Tuple[List[str], bool]:
    nifty_bar = forming_bar_from_1m_upto(candles_1m.get(NIFTY50_KEY, []), session_date, upto_hhmm)
    if not nifty_bar:
        nifty_bar = first_5m_bar(candles_1m.get(NIFTY50_KEY, []), session_date)
    if not nifty_bar:
        return [], True
    nifty_prev = nifty_prev_close if nifty_prev_close and nifty_prev_close > 0 else prev_session_close(
        candles_1m.get(NIFTY50_KEY, []), session_date
    )
    bias, _ = nifty_bias_from_bar_vs_prev_close(nifty_bar, nifty_prev, missing="unknown")
    if bias == "unknown":
        return [], True
    long_side = bias == "positive"
    eligible = fo_eligible_sector_keys(
        stocks_by_sector, session_date, fut_by_und=fut_by_und, eq_by_symbol=eq_by_symbol
    )
    sector_bars: Dict[str, Dict[str, Any]] = {}
    sector_prev: Dict[str, float] = dict(sector_prev_closes or {})
    for skey in eligible:
        bar = forming_bar_from_1m_upto(candles_1m.get(skey, []), session_date, upto_hhmm)
        if not bar:
            bar = first_5m_bar(candles_1m.get(skey, []), session_date)
        if bar:
            sector_bars[skey] = bar
        if skey not in sector_prev:
            prev = prev_session_close(candles_1m.get(skey, []), session_date)
            if prev is not None:
                sector_prev[skey] = prev
    ranked = rank_sectors_vs_prev_close(
        sector_bars, sector_prev, eligible_keys=eligible, descending=long_side
    )
    take = min(len(ranked), LIVE_SECTORS_TO_PICK)
    return [skey for skey, _, _ in ranked[:take]], long_side


def _gainer_loser_books(
    *,
    session_date: date,
    candles: Dict[str, List[Dict[str, Any]]],
    stocks_by_sector: Dict[str, List[Dict[str, str]]],
    fut_by_und: Dict[str, List[Dict[str, Any]]],
    eq_by_symbol: Dict[str, Dict[str, Any]],
    sector_prev_closes: Optional[Dict[str, float]] = None,
    nifty_prev_close: Optional[float] = None,
) -> List[Tuple[str, bool]]:
    """Top gainer LONG, top loser SHORT. One sector if they collapse or Nifty-bias fallback."""
    eligible = fo_eligible_sector_keys(
        stocks_by_sector, session_date, fut_by_und=fut_by_und, eq_by_symbol=eq_by_symbol
    )
    sector_bars: Dict[str, Dict[str, Any]] = {}
    sector_prev: Dict[str, float] = dict(sector_prev_closes or {})
    for skey in eligible:
        bar = first_5m_bar(candles.get(skey, []), session_date)
        if bar:
            sector_bars[skey] = bar
        if skey not in sector_prev:
            prev = prev_session_close(candles.get(skey, []), session_date)
            if prev is not None:
                sector_prev[skey] = prev
    ranked = rank_sectors_vs_prev_close(
        sector_bars, sector_prev, eligible_keys=eligible, descending=True
    )
    if not ranked:
        logger.warning(
            "breakfast sector rank empty eligible=%s bars=%s prev_closes=%s",
            len(eligible),
            len(sector_bars),
            len(sector_prev),
        )
        return []
    gainer_key = ranked[0][0]
    loser_key = ranked[-1][0]
    if LIVE_SECTORS_TO_PICK >= 2 and gainer_key != loser_key:
        return [(gainer_key, True), (loser_key, False)]
    nifty_bar = first_5m_bar(candles.get(NIFTY50_KEY, []), session_date)
    nifty_prev = nifty_prev_close if nifty_prev_close and nifty_prev_close > 0 else prev_session_close(
        candles.get(NIFTY50_KEY, []), session_date
    )
    bias, _ = nifty_bias_from_bar_vs_prev_close(nifty_bar or {}, nifty_prev, missing="unknown")
    if bias == "negative":
        return [(loser_key, False)]
    return [(gainer_key, True)]


def _members_for_books(
    stocks_by_sector: Dict[str, List[Dict[str, str]]],
    wick_by_symbol: Dict[str, str],
    books: List[Tuple[str, bool]],
) -> Dict[str, List[Dict[str, str]]]:
    out: Dict[str, List[Dict[str, str]]] = {}
    for skey, long_side in books:
        filtered = filter_sector_members_by_wick(
            {skey: stocks_by_sector.get(skey, [])},
            wick_by_symbol,
            long_side=long_side,
        )
        out[skey] = filtered.get(skey, [])
    return out


def _build_stock_overrides_from_1m(
    *,
    symbols: List[str],
    session_date: date,
    candles_1m_by_key: Dict[str, List[Dict[str, Any]]],
    fut_by_und: Dict[str, List[Dict[str, Any]]],
    eq_by_symbol: Dict[str, Dict[str, Any]],
    upto_hhmm: Tuple[int, int],
    stock_prev_closes: Optional[Dict[str, float]] = None,
) -> Tuple[Dict[str, Tuple[Dict[str, Any], float]], Dict[str, Dict[str, Any]]]:
    signal_overrides: Dict[str, Tuple[Dict[str, Any], float]] = {}
    anchor_overrides: Dict[str, Dict[str, Any]] = {}
    prev_map = stock_prev_closes or {}
    for sym in symbols:
        ref = resolve_stock_instrument(sym, session_date, fut_by_und=fut_by_und, eq_by_symbol=eq_by_symbol)
        if not ref or not ref.instrument_key:
            continue
        candles = candles_1m_by_key.get(ref.instrument_key, [])
        partial = forming_bar_from_1m_upto(candles, session_date, upto_hhmm)
        if not partial:
            partial = first_5m_bar(candles, session_date)
        if not partial:
            continue
        prev = prev_map.get(sym) or prev_session_close(candles, session_date)
        if prev is None:
            continue
        pct = move_pct_vs_prev_close(float(partial.get("close") or 0), prev)
        if pct is None:
            continue
        signal_overrides[sym] = (partial, float(pct))
        ab = anchor_bar(candles, session_date)
        if not ab:
            ab = forming_bar_from_1m_upto(candles, session_date, (9, 16))
        if ab:
            anchor_overrides[sym] = ab
    return signal_overrides, anchor_overrides


def _serialize_stock_pick(stk: Any, *, long_side: bool, wick: str = WICK_NONE) -> Dict[str, Any]:
    from backend.services.breakfast_strategy.candles import candle_ohlcv

    anchor = stk.anchor_bar
    _, _, _, anchor_px, _ = candle_ohlcv(anchor)
    lot = int(stk.row.lot_size or 0)
    direction = "LONG" if long_side else "SHORT"
    sl_px = anchor_px * (1.0 - SL_PCT) if long_side else anchor_px * (1.0 + SL_PCT)
    tp_px = anchor_px * (1.0 + TP_PCT) if long_side else anchor_px * (1.0 - TP_PCT)
    risk_inr = round(abs(anchor_px - sl_px) * lot, 2) if lot > 0 else None
    sig_o, _, _, sig_cl, sig_vol = candle_ohlcv(stk.signal_bar)
    labels = ["Pick 1", "Pick 2", "Watch 3rd"]
    label = labels[stk.stock_rank - 1] if 1 <= stk.stock_rank <= len(labels) else f"#{stk.stock_rank}"
    return {
        "rank_label": label,
        "stock_rank": stk.stock_rank,
        "rank_in_sector": stk.stock_rank,
        "symbol": stk.row.stock,
        "display_symbol": stk.row.display_symbol,
        "instrument_label": stk.row.instrument_label,
        "sector": stk.row.sector,
        "direction": direction,
        "move_pct_at_entry": round(stk.move_pct, 3),
        "ltp": sig_cl,
        "signal_open": sig_o,
        "signal_close": sig_cl,
        "volume": sig_vol,
        "lot_size": lot,
        "anchor_price": round(anchor_px, 4),
        "sl_price": round(sl_px, 4),
        "tp_price": round(tp_px, 4),
        "risk_inr": risk_inr,
        "risk_inr_1lot": risk_inr,
        "instrument_key": stk.row.instrument_key,
        "price_source": stk.row.price_source,
        "wick": wick or WICK_NONE,
    }


def _build_payload_from_selection(
    *,
    now: datetime,
    session_date: date,
    phase: str,
    sel: Any,
    nifty_bar: Dict[str, Any],
    tick_minute: int,
    elapsed_sec: float,
    capture_source: str = "live_scheduler",
    data_source: str = "ws_1m",
    tick_source_log: Optional[List[Dict[str, Any]]] = None,
    nifty_prev_close: Optional[float] = None,
    wick_by_symbol: Optional[Dict[str, str]] = None,
) -> Dict[str, Any]:
    nifty_bias = "unknown"
    nifty_pct = None
    if nifty_bar:
        cl = float(nifty_bar.get("close") or 0)
        nifty_pct = move_pct_vs_prev_close(cl, float(nifty_prev_close)) if nifty_prev_close else None
        nifty_bias, nifty_pct = nifty_bias_from_bar_vs_prev_close(
            nifty_bar, nifty_prev_close, missing="unknown"
        )
    if sel is not None:
        nifty_bias = sel.nifty_bias
        nifty_pct = sel.nifty_bias_pct
    if nifty_bias == "negative" or (nifty_pct is not None and nifty_pct < 0):
        nifty_bias = "negative"
        direction = "SHORT"
    elif nifty_bias == "unknown" or nifty_pct is None:
        direction = "UNKNOWN"
    else:
        direction = "LONG"

    wicks = wick_by_symbol or {}
    sectors_out: List[Dict[str, Any]] = []
    if sel:
        for sp in sel.sector_picks:
            sp_long = sel.long_side if getattr(sp, "long_side", None) is None else bool(sp.long_side)
            sp_dir = "LONG" if sp_long else "SHORT"
            raw_stocks = [
                _serialize_stock_pick(
                    s,
                    long_side=sp_long,
                    wick=wicks.get(str(s.row.stock or "").upper(), WICK_NONE),
                )
                for s in sp.stocks
            ]
            stocks = filter_live_stocks_by_wick_and_color(
                raw_stocks, direction=sp_dir, wick_by_symbol=wicks
            )
            sectors_out.append(
                {
                    "sector_key": sp.sector_key,
                    "sector_label": _sector_label(sp.sector_key),
                    "sector_rank": sp.sector_rank,
                    "move_pct": round(sp.sector_move_pct, 3),
                    "direction": sp_dir,
                    "volume": sp.sector_volume,
                    "stocks": stocks,
                }
            )

    banner = "LIVE — FORMING, NOT FINAL" if phase == "forming" else "LOCKED — 9:20 CONFIRMED"
    if phase == "locking":
        banner = "LOCKING — 9:20:05 FREEZE"

    return {
        "ok": True,
        "state": phase if phase != "locking" else "locked",
        "phase": phase,
        "session_date": session_date.isoformat(),
        "server_time": now.isoformat(),
        "banner": banner,
        "refresh_allowed": phase in ("forming", "bar_closing", "locking"),
        "poll_interval_sec": 5 if phase in ("forming", "bar_closing") else 0,
        "nifty": {
            "instrument_key": NIFTY50_KEY,
            "bias": nifty_bias,
            "bias_pct": round(sel.nifty_bias_pct, 3) if sel else (round(nifty_pct, 3) if nifty_pct is not None else None),
            "direction": direction,
            "bar_source": data_source,
            "open": nifty_bar.get("open") if nifty_bar else None,
            "close": nifty_bar.get("close") if nifty_bar else None,
        },
        "sectors": sectors_out,
        "ranked_sector_count": len(sel.ranked_sectors) if sel else 0,
        "mismatch_instruments": [],
        "universe_instruments": len(_get_cache(session_date.isoformat()).instrument_keys),
        "tick_minute": tick_minute,
        "tick_elapsed_sec": round(elapsed_sec, 2),
        "capture_source": capture_source,
        "data_source": data_source,
        "tick_source_log": tick_source_log or [],
    }


def run_breakfast_minute_tick(minute: int) -> Dict[str, Any]:
    """Scheduled tick at 9:MM — fetch scoped 1m bars and refresh live snapshot."""
    global _LIVE_TICK_SNAPSHOT
    t0 = time.monotonic()
    now = _now_ist()
    if not _is_trading_day(now):
        return {"ok": False, "skipped": "not_trading_day"}
    if minute not in TICK_MINUTES:
        return {"ok": False, "skipped": "invalid_minute", "minute": minute}

    session_date = now.date()
    cache_key = session_date.isoformat()
    cache = _get_cache(cache_key)
    if cache.locked:
        return {"ok": True, "skipped": "already_locked", "session_date": cache_key}

    upto_hhmm = _upto_hhmm_for_tick(minute)
    phase = _phase_for_minute(minute)

    with breakfast_upstox_priority_owner():
        ux = UpstoxService(settings.UPSTOX_API_KEY, settings.UPSTOX_API_SECRET)
        ux.reload_token_from_storage()
        cache_dir = default_cache_dir()
        stocks_by_sector = load_arbitrage_by_sector()
        fut_by_und, eq_by_symbol = build_instrument_indexes()
        bench_prev, stock_prev, stock_wicks = load_stored_prev_closes_and_wicks()

        sector_keys = _all_sector_keys()
        cache.sector_keys = sector_keys
        index_keys = list(dict.fromkeys([NIFTY50_KEY] + sector_keys))
        freeze_5m = minute >= 20
        if freeze_5m:
            index_candles, index_source_log = _resolve_candles_rest_5m(
                ux,
                cache_dir,
                index_keys,
                session_date=session_date,
                tick_minute=minute,
            )
        else:
            index_candles, index_source_log = _resolve_candles_ws_primary(
                ux,
                cache_dir,
                index_keys,
                session_date=session_date,
                upto_hhmm=upto_hhmm,
                tick_minute=minute,
            )
        cache.candles_1m.update(index_candles)
        for ik in index_keys:
            if ik not in cache.candles_1m:
                cache.candles_1m[ik] = load_cached_1m(cache_dir, ik)

        tick_source_log: List[Dict[str, Any]] = list(index_source_log)
        data_source = _composite_data_source(tick_source_log)
        if not freeze_5m:
            _maybe_warn_and_warm_5m_pre_freeze(
                minute=minute,
                index_keys=index_keys,
                source_log=index_source_log,
                ux=ux,
                cache_dir=cache_dir,
                session_date=session_date,
            )

        nifty_prev = bench_prev.get(NIFTY50_KEY) or prev_session_close(
            cache.candles_1m.get(NIFTY50_KEY, []), session_date
        )
        prev_picked = list(cache.picked_sector_keys)
        sector_books: List[Tuple[str, bool]] = []
        wick_members: Dict[str, List[Dict[str, str]]] = {}
        if freeze_5m:
            sector_books = _gainer_loser_books(
                session_date=session_date,
                candles=cache.candles_1m,
                stocks_by_sector=stocks_by_sector,
                fut_by_und=fut_by_und,
                eq_by_symbol=eq_by_symbol,
                sector_prev_closes=bench_prev,
                nifty_prev_close=nifty_prev,
            )
            picked = [skey for skey, _ls in sector_books]
            wick_members = _members_for_books(stocks_by_sector, stock_wicks, sector_books)
            cache.picked_sector_keys = picked
            sym_map, stock_iks = _resolve_stock_keys(
                picked, wick_members, session_date, fut_by_und, eq_by_symbol
            )
            cache.stock_symbols_by_sector = sym_map
            cache.instrument_keys = list(dict.fromkeys([NIFTY50_KEY] + sector_keys + stock_iks))
            logger.info(
                "breakfast freeze rest_budget indexes=%s stocks=%s books=%s",
                len(index_keys),
                len(stock_iks),
                [(s, "LONG" if lg else "SHORT") for s, lg in sector_books],
            )
            if picked != prev_picked:
                _record_sector_repick(
                    cache_key,
                    minute=minute,
                    prev_picked=prev_picked,
                    picked=picked,
                    stock_count=sum(len(v) for v in sym_map.values()),
                )
        else:
            picked, _long = _rank_picked_sectors(
                session_date=session_date,
                candles_1m=cache.candles_1m,
                stocks_by_sector=stocks_by_sector,
                fut_by_und=fut_by_und,
                eq_by_symbol=eq_by_symbol,
                upto_hhmm=upto_hhmm,
                nifty_prev_close=nifty_prev,
                sector_prev_closes=bench_prev,
            )
            cache.picked_sector_keys = picked
            sectors_changed = picked != prev_picked
            if sectors_changed or not cache.stock_symbols_by_sector:
                sym_map, stock_iks = _resolve_stock_keys(
                    picked, stocks_by_sector, session_date, fut_by_und, eq_by_symbol
                )
                cache.stock_symbols_by_sector = sym_map
                cache.instrument_keys = list(dict.fromkeys([NIFTY50_KEY] + sector_keys + stock_iks))
                if sectors_changed:
                    stock_n = sum(len(v) for v in sym_map.values())
                    logger.info(
                        "breakfast sector re-pick %s: %s -> %s stocks=%s",
                        cache_key,
                        prev_picked,
                        picked,
                        stock_n,
                    )
                    _record_sector_repick(
                        cache_key,
                        minute=minute,
                        prev_picked=prev_picked,
                        picked=picked,
                        stock_count=stock_n,
                    )
        stock_iks = [
            ik
            for ik in cache.instrument_keys
            if ik != NIFTY50_KEY and ik not in sector_keys
        ]
        if stock_iks:
            if freeze_5m:
                stock_candles, stock_source_log = _resolve_candles_rest_5m(
                    ux,
                    cache_dir,
                    stock_iks,
                    session_date=session_date,
                    tick_minute=minute,
                )
            else:
                stock_candles, stock_source_log = _resolve_candles_ws_primary(
                    ux,
                    cache_dir,
                    stock_iks,
                    session_date=session_date,
                    upto_hhmm=upto_hhmm,
                    tick_minute=minute,
                )
            cache.candles_1m.update(stock_candles)
            tick_source_log.extend(stock_source_log)
            data_source = _composite_data_source(tick_source_log)

        nifty_bar = forming_bar_from_1m_upto(cache.candles_1m.get(NIFTY50_KEY, []), session_date, upto_hhmm)
        if not nifty_bar:
            nifty_bar = first_5m_bar(cache.candles_1m.get(NIFTY50_KEY, []), session_date)
        sector_overrides: Dict[str, Dict[str, Any]] = {}
        for skey in cache.picked_sector_keys or cache.sector_keys:
            bar = forming_bar_from_1m_upto(cache.candles_1m.get(skey, []), session_date, upto_hhmm)
            if not bar:
                bar = first_5m_bar(cache.candles_1m.get(skey, []), session_date)
            if bar:
                sector_overrides[skey] = bar

        all_syms: List[str] = []
        for skey in cache.picked_sector_keys:
            all_syms.extend(cache.stock_symbols_by_sector.get(skey, []))

        stock_signal_overrides, anchor_overrides = _build_stock_overrides_from_1m(
            symbols=all_syms,
            session_date=session_date,
            candles_1m_by_key=cache.candles_1m,
            fut_by_und=fut_by_und,
            eq_by_symbol=eq_by_symbol,
            upto_hhmm=upto_hhmm,
            stock_prev_closes=stock_prev,
        )

        sector_candles_5m_compat: Dict[str, List[Dict[str, Any]]] = {
            ik: cache.candles_1m.get(ik, []) for ik in cache.sector_keys
        }
        stock_candles_by_key = {
            ik: cache.candles_1m.get(ik, []) for ik in cache.instrument_keys if ik != NIFTY50_KEY
        }

        nifty_unknown = False
        _nb = "unknown"
        if nifty_bar:
            _nb, _ = nifty_bias_from_bar_vs_prev_close(nifty_bar, nifty_prev, missing="unknown")
            nifty_unknown = _nb == "unknown"
        else:
            nifty_unknown = True

        sel = None
        bar_map = {
            str(sym).strip().upper(): ov[0]
            for sym, ov in (stock_signal_overrides or {}).items()
            if ov and ov[0]
        }
        if freeze_5m and sector_books:
            stocks_for_pick: Dict[str, List[Dict[str, str]]] = dict(stocks_by_sector)
            for skey, book_long in sector_books:
                colored = filter_sector_members_by_first_5m_color(
                    {skey: wick_members.get(skey, [])},
                    bar_map,
                    long_side=book_long,
                )
                stocks_for_pick[skey] = colored.get(skey, [])
            sel = select_breakfast_picks_prevclose(
                session_date,
                nifty_candles=cache.candles_1m.get(NIFTY50_KEY, []),
                sector_candles=sector_candles_5m_compat,
                stock_candles_by_key=stock_candles_by_key,
                stocks_by_sector=stocks_for_pick,
                fut_by_und=fut_by_und,
                eq_by_symbol=eq_by_symbol,
                upstox=ux,
                nifty_bar=nifty_bar,
                sector_bar_overrides=sector_overrides or None,
                stock_signal_overrides=stock_signal_overrides or None,
                anchor_bar_overrides=anchor_overrides or None,
                nifty_prev_close=nifty_prev,
                sector_prev_closes=bench_prev,
                stock_prev_closes=stock_prev,
                sectors_to_pick=LIVE_SECTORS_TO_PICK,
                stocks_per_sector=LIVE_STOCKS_PER_SECTOR,
                sector_books=sector_books,
            )
        elif not nifty_unknown:
            long_side_pick = _nb == "positive"
            stocks_for_pick = filter_sector_members_by_wick(
                stocks_by_sector, stock_wicks, long_side=long_side_pick
            )
            stocks_for_pick = filter_sector_members_by_first_5m_color(
                stocks_for_pick, bar_map, long_side=long_side_pick
            )
            sel = select_breakfast_picks_prevclose(
                session_date,
                nifty_candles=cache.candles_1m.get(NIFTY50_KEY, []),
                sector_candles=sector_candles_5m_compat,
                stock_candles_by_key=stock_candles_by_key,
                stocks_by_sector=stocks_for_pick,
                fut_by_und=fut_by_und,
                eq_by_symbol=eq_by_symbol,
                upstox=ux,
                nifty_bar=nifty_bar,
                sector_bar_overrides=sector_overrides or None,
                stock_signal_overrides=stock_signal_overrides or None,
                anchor_bar_overrides=anchor_overrides or None,
                nifty_prev_close=nifty_prev,
                sector_prev_closes=bench_prev,
                stock_prev_closes=stock_prev,
                sectors_to_pick=LIVE_SECTORS_TO_PICK,
                stocks_per_sector=LIVE_STOCKS_PER_SECTOR,
            )

        elapsed = time.monotonic() - t0
        payload = _build_payload_from_selection(
            now=now,
            session_date=session_date,
            phase=phase,
            sel=sel,
            nifty_bar=nifty_bar or {},
            tick_minute=minute,
            elapsed_sec=elapsed,
            data_source=data_source,
            tick_source_log=tick_source_log,
            nifty_prev_close=nifty_prev,
            wick_by_symbol=stock_wicks,
        )
        payload["rest_call_budget"] = {
            "indexes": len(index_keys),
            "stocks": len(stock_iks),
        }
        if elapsed > MAX_TICK_SEC:
            payload["tick_slow"] = True
            logger.warning("breakfast tick :%02d took %.1fs (>%.0fs)", minute, elapsed, MAX_TICK_SEC)

        _record_tick_sources(cache_key, tick_source_log)

        with _LOCK:
            _LIVE_TICK_SNAPSHOT = dict(payload)

        return {
            "ok": True,
            "minute": minute,
            "elapsed_sec": round(elapsed, 2),
            "sectors": len(payload.get("sectors") or []),
            "session_date": cache_key,
            "data_source": data_source,
            "rest_call_budget": payload.get("rest_call_budget"),
            "ws_count": sum(1 for r in tick_source_log if r.get("source") == "ws_1m"),
            "rest_fallback_count": sum(
                1 for r in tick_source_log if r.get("source") in ("rest_1m", "rest_5m", "none")
            ),
        }


def _ws_rest_cross_check_observation(
    *,
    session_date: date,
    instrument_keys: List[str],
    upstox: UpstoxService,
) -> Tuple[str, List[Dict[str, Any]]]:
    """Informational WS vs REST 5m delta — does not block freeze."""
    from backend.services.breakfast_strategy.candles import bars_ohlc_close_match, ensure_5m_cached, signal_bar
    from backend.services.breakfast_strategy.live import get_ws_forming_5m_bar

    cache_dir = default_cache_dir()
    rows: List[Dict[str, Any]] = []
    matched = 0
    for ik in instrument_keys:
        if not ik:
            continue
        try:
            candles = ensure_5m_cached(
                upstox, cache_dir, ik, range_end=session_date, session_dates=[session_date], force=False
            )
            rest_bar = signal_bar(candles, session_date)
            ws_bar = get_ws_forming_5m_bar(ik, session_date)
            rest_cl = float((rest_bar or {}).get("close") or 0)
            ws_cl = float((ws_bar or {}).get("close") or 0)
            delta_pct = None
            if rest_cl > 0 and ws_cl > 0:
                delta_pct = round((ws_cl - rest_cl) / rest_cl * 100.0, 4)
            is_match = bars_ohlc_close_match(ws_bar, rest_bar)
            if is_match:
                matched += 1
            rows.append(
                {
                    "instrument_key": ik,
                    "ws_close": ws_cl or None,
                    "rest_close": rest_cl or None,
                    "delta_pct": delta_pct,
                    "matched": is_match,
                }
            )
        except Exception as e:
            rows.append({"instrument_key": ik, "error": str(e), "matched": False})
    total = len(rows)
    status = f"ws_rest:{matched}/{total}_matched" if total else "ws_rest:no_instruments"
    return status, rows


def run_breakfast_freeze_lock(*, retry: bool = False) -> Dict[str, Any]:
    """9:20:05 IST — REST minutes/5 snapshot, persist signals + session lock."""
    global _LIVE_TICK_SNAPSHOT
    now = _now_ist()
    if not _is_trading_day(now):
        return {"ok": False, "skipped": "not_trading_day"}

    session_date = now.date()
    cache_key = session_date.isoformat()
    cache = _get_cache(cache_key)

    existing = fetch_session_lock(cache_key)
    if existing and existing.get("lock_status") == "locked":
        cache.locked = True
        return {"ok": True, "skipped": "already_locked", "session_date": cache_key}

    attempts = _FREEZE_ATTEMPTS.get(cache_key, 0)
    if attempts >= _MAX_FREEZE_RETRIES and not retry:
        failed = _failed_freeze_payload(cache_key, "max_retries_exceeded")
        with _LOCK:
            _LIVE_TICK_SNAPSHOT = failed
        return {"ok": False, "failed": True, "reason": "max_retries_exceeded"}

    _FREEZE_ATTEMPTS[cache_key] = attempts + 1

    tick_out = run_breakfast_minute_tick(20)
    payload = get_live_tick_snapshot() or {}
    payload["phase"] = "frozen"
    payload["state"] = "locked"
    payload["banner"] = "LOCKED — 9:20 CONFIRMED"
    payload["refresh_allowed"] = False
    payload["poll_interval_sec"] = 0
    payload["server_time"] = now.isoformat()

    sectors = payload.get("sectors") or []
    cross_keys: List[str] = [NIFTY50_KEY]
    for sec in sectors:
        sk = str(sec.get("sector_key") or "").strip()
        if sk:
            cross_keys.append(sk)
        for stk in sec.get("stocks") or []:
            ik = str(stk.get("instrument_key") or "").strip()
            if ik:
                cross_keys.append(ik)
    cross_keys = list(dict.fromkeys(cross_keys))
    ux = UpstoxService(settings.UPSTOX_API_KEY, settings.UPSTOX_API_SECRET)
    ux.reload_token_from_storage()
    cross_status, cross_rows = _ws_rest_cross_check_observation(
        session_date=session_date,
        instrument_keys=cross_keys,
        upstox=ux,
    )
    payload["ws_rest_cross_check"] = cross_rows
    payload["cross_check_status"] = cross_status
    logger.info("breakfast WS observation %s: %s", cache_key, cross_status)

    lock_status = "locked"
    failure_reason: Optional[str] = None

    if not sectors:
        lock_status = "failed"
        failure_reason = "no_sectors_at_freeze"
        nifty_dir = str((payload.get("nifty") or {}).get("direction") or "")
        nifty_bias = str((payload.get("nifty") or {}).get("bias") or "")
        if nifty_dir == "UNKNOWN" or nifty_bias == "unknown":
            failure_reason = "no_data"
        payload["state"] = "lock_failed"
        payload["phase"] = "frozen"
        payload["banner"] = "LOCK FAILED — no picks at 9:20; capture manually"
        payload["lock_failed"] = True
        payload["failure_reason"] = failure_reason

    try:
        signal_stats = {"inserted": 0, "skipped": 0}
        if sectors:
            signal_stats = persist_live_signals(payload, cross_status, capture_source="live_scheduler")
        lock_row = persist_session_lock(
            payload,
            lock_status=lock_status,
            failure_reason=failure_reason,
            signal_count=len(sectors),
            capture_source="live_scheduler",
        )
    except Exception as e:
        logger.exception("breakfast freeze persist failed: %s", e)
        lock_status = "failed"
        failure_reason = str(e)
        payload["state"] = "lock_failed"
        payload["phase"] = "frozen"
        payload["banner"] = f"LOCK FAILED — {failure_reason}"
        payload["lock_failed"] = True
        try:
            persist_session_lock(
                payload,
                lock_status="failed",
                failure_reason=failure_reason,
                signal_count=0,
                capture_source="live_scheduler",
            )
        except Exception:
            pass
        lock_row = None
        signal_stats = {"inserted": 0, "skipped": 0}

    cache.locked = lock_status == "locked"
    with _LOCK:
        _LIVE_TICK_SNAPSHOT = dict(payload)

    from backend.services.breakfast_strategy import live as live_mod

    live_mod.ingest_frozen_snapshot(payload)

    return {
        "ok": lock_status == "locked",
        "session_date": cache_key,
        "lock_status": lock_status,
        "failure_reason": failure_reason,
        "signal_stats": signal_stats,
        "lock_row": lock_row,
        "tick": tick_out,
    }


def _failed_freeze_payload(session_date: str, reason: str) -> Dict[str, Any]:
    return {
        "ok": False,
        "state": "lock_failed",
        "phase": "frozen",
        "session_date": session_date,
        "banner": f"LOCK FAILED — {reason}",
        "refresh_allowed": False,
        "poll_interval_sec": 0,
        "lock_failed": True,
        "failure_reason": reason,
        "sectors": [],
        "nifty": {},
    }


def reset_session_cache_for_tests() -> None:
    global _LIVE_TICK_SNAPSHOT, _LAST_WARMUP
    with _LOCK:
        _SESSION_CACHE.clear()
        _LIVE_TICK_SNAPSHOT = None
        _FREEZE_ATTEMPTS.clear()
        _SESSION_MONITOR.clear()
        _LAST_WARMUP = None
