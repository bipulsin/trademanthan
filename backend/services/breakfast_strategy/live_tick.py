"""Breakfast live minute ticks (9:16–9:20) and 9:20:30 freeze — 1m REST path."""
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
    fetch_1m_parallel,
    forming_bar_from_1m_upto,
    load_cached_1m,
    move_pct_vs_prev_close,
    prev_session_close,
)
from backend.services.breakfast_strategy.config import SL_PCT, TP_PCT
from backend.services.breakfast_strategy.engine import NIFTY50_KEY, nifty_bias_from_bar, select_breakfast_picks
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
    rank_sectors,
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
FREEZE_AT = dt_time(9, 20, 30)
MAX_TICK_SEC = 20.0

_LOCK = threading.Lock()
_SESSION_CACHE: Dict[str, "BreakfastSessionCache"] = {}
_LIVE_TICK_SNAPSHOT: Optional[Dict[str, Any]] = None
_FREEZE_ATTEMPTS: Dict[str, int] = {}
_MAX_FREEZE_RETRIES = 3


@dataclass
class BreakfastSessionCache:
    session_date: str
    sector_keys: List[str] = field(default_factory=list)
    picked_sector_keys: List[str] = field(default_factory=list)
    stock_symbols_by_sector: Dict[str, List[str]] = field(default_factory=dict)
    instrument_keys: List[str] = field(default_factory=list)
    candles_1m: Dict[str, List[Dict[str, Any]]] = field(default_factory=dict)
    first_scan_done: bool = False
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


def _all_sector_keys() -> List[str]:
    keys: List[str] = []
    for label, _yh in SECTOR_UNIVERSE:
        ik = sector_index_key_for_label(label)
        if ik:
            keys.append(ik)
    return keys


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
) -> Tuple[List[str], bool]:
    nifty_bar = forming_bar_from_1m_upto(candles_1m.get(NIFTY50_KEY, []), session_date, upto_hhmm)
    if not nifty_bar:
        return [], True
    bias, _ = nifty_bias_from_bar(nifty_bar)
    long_side = bias == "positive"
    eligible = fo_eligible_sector_keys(
        stocks_by_sector, session_date, fut_by_und=fut_by_und, eq_by_symbol=eq_by_symbol
    )
    sector_bars: Dict[str, Dict[str, Any]] = {}
    for skey in eligible:
        bar = forming_bar_from_1m_upto(candles_1m.get(skey, []), session_date, upto_hhmm)
        if bar:
            sector_bars[skey] = bar
    ranked = rank_sectors(sector_bars, eligible_keys=eligible, descending=long_side)
    take = min(len(ranked), LIVE_SECTORS_TO_PICK)
    return [skey for skey, _, _ in ranked[:take]], long_side


def _build_stock_overrides_from_1m(
    *,
    symbols: List[str],
    session_date: date,
    candles_1m_by_key: Dict[str, List[Dict[str, Any]]],
    fut_by_und: Dict[str, List[Dict[str, Any]]],
    eq_by_symbol: Dict[str, Dict[str, Any]],
    upto_hhmm: Tuple[int, int],
) -> Tuple[Dict[str, Tuple[Dict[str, Any], float]], Dict[str, Dict[str, Any]]]:
    signal_overrides: Dict[str, Tuple[Dict[str, Any], float]] = {}
    anchor_overrides: Dict[str, Dict[str, Any]] = {}
    for sym in symbols:
        ref = resolve_stock_instrument(sym, session_date, fut_by_und=fut_by_und, eq_by_symbol=eq_by_symbol)
        if not ref or not ref.instrument_key:
            continue
        candles = candles_1m_by_key.get(ref.instrument_key, [])
        partial = forming_bar_from_1m_upto(candles, session_date, upto_hhmm)
        if not partial:
            continue
        prev = prev_session_close(candles, session_date)
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


def _serialize_stock_pick(stk: Any, *, long_side: bool) -> Dict[str, Any]:
    from backend.services.breakfast_strategy.candles import candle_ohlcv

    anchor = stk.anchor_bar
    _, _, _, anchor_px, _ = candle_ohlcv(anchor)
    lot = int(stk.row.lot_size or 0)
    direction = "LONG" if long_side else "SHORT"
    sl_px = anchor_px * (1.0 - SL_PCT) if long_side else anchor_px * (1.0 + SL_PCT)
    tp_px = anchor_px * (1.0 + TP_PCT) if long_side else anchor_px * (1.0 - TP_PCT)
    risk_inr = round(abs(anchor_px - sl_px) * lot, 2) if lot > 0 else None
    _, _, _, sig_cl, sig_vol = candle_ohlcv(stk.signal_bar)
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
) -> Dict[str, Any]:
    long_side = sel.long_side if sel else True
    nifty_pct = None
    if nifty_bar:
        from backend.services.breakfast_strategy.candles import bar_move_pct

        nifty_pct = bar_move_pct(nifty_bar)

    sectors_out: List[Dict[str, Any]] = []
    if sel:
        for sp in sel.sector_picks:
            sectors_out.append(
                {
                    "sector_key": sp.sector_key,
                    "sector_label": _sector_label(sp.sector_key),
                    "sector_rank": sp.sector_rank,
                    "move_pct": round(sp.sector_move_pct, 3),
                    "direction": "LONG" if long_side else "SHORT",
                    "volume": sp.sector_volume,
                    "stocks": [_serialize_stock_pick(s, long_side=long_side) for s in sp.stocks],
                }
            )

    banner = "LIVE — FORMING, NOT FINAL" if phase == "forming" else "LOCKED — 9:20 CONFIRMED"
    if phase == "locking":
        banner = "LOCKING — 9:20:30 FREEZE"

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
            "bias": sel.nifty_bias if sel else ("positive" if (nifty_pct or 0) >= 0 else "negative"),
            "bias_pct": round(sel.nifty_bias_pct, 3) if sel else (round(nifty_pct, 3) if nifty_pct is not None else None),
            "direction": "LONG" if long_side else "SHORT",
            "bar_source": "rest_1m",
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
        "data_source": "upstox_1m",
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

        fetch_keys: List[str] = [NIFTY50_KEY]
        if not cache.first_scan_done:
            fetch_keys.extend(_all_sector_keys())
        else:
            fetch_keys.extend(cache.instrument_keys)

        fresh = fetch_1m_parallel(ux, cache_dir, fetch_keys, session_date=session_date)
        cache.candles_1m.update(fresh)
        for ik in fetch_keys:
            if ik not in cache.candles_1m:
                cache.candles_1m[ik] = load_cached_1m(cache_dir, ik)

        if not cache.first_scan_done:
            picked, _long = _rank_picked_sectors(
                session_date=session_date,
                candles_1m=cache.candles_1m,
                stocks_by_sector=stocks_by_sector,
                fut_by_und=fut_by_und,
                eq_by_symbol=eq_by_symbol,
                upto_hhmm=upto_hhmm,
            )
            cache.sector_keys = _all_sector_keys()
            cache.picked_sector_keys = picked
            sym_map, stock_iks = _resolve_stock_keys(
                picked, stocks_by_sector, session_date, fut_by_und, eq_by_symbol
            )
            cache.stock_symbols_by_sector = sym_map
            cache.instrument_keys = list(dict.fromkeys([NIFTY50_KEY] + cache.sector_keys + stock_iks))
            if picked and stock_iks:
                stock_fresh = fetch_1m_parallel(ux, cache_dir, stock_iks, session_date=session_date)
                cache.candles_1m.update(stock_fresh)
            cache.first_scan_done = True
            logger.info(
                "breakfast first scan %s: sectors=%s stocks=%s",
                cache_key,
                picked,
                sum(len(v) for v in sym_map.values()),
            )
        else:
            scoped = list(dict.fromkeys(cache.instrument_keys))
            stock_fresh = fetch_1m_parallel(ux, cache_dir, scoped, session_date=session_date)
            cache.candles_1m.update(stock_fresh)

        nifty_bar = forming_bar_from_1m_upto(cache.candles_1m.get(NIFTY50_KEY, []), session_date, upto_hhmm)
        sector_overrides: Dict[str, Dict[str, Any]] = {}
        for skey in cache.picked_sector_keys or cache.sector_keys:
            bar = forming_bar_from_1m_upto(cache.candles_1m.get(skey, []), session_date, upto_hhmm)
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
        )

        sector_candles_5m_compat: Dict[str, List[Dict[str, Any]]] = {
            ik: cache.candles_1m.get(ik, []) for ik in cache.sector_keys
        }
        stock_candles_by_key = {
            ik: cache.candles_1m.get(ik, []) for ik in cache.instrument_keys if ik != NIFTY50_KEY
        }

        sel = select_breakfast_picks(
            session_date,
            nifty_candles=cache.candles_1m.get(NIFTY50_KEY, []),
            sector_candles=sector_candles_5m_compat,
            stock_candles_by_key=stock_candles_by_key,
            stocks_by_sector=stocks_by_sector,
            fut_by_und=fut_by_und,
            eq_by_symbol=eq_by_symbol,
            upstox=ux,
            nifty_bar=nifty_bar,
            sector_bar_overrides=sector_overrides or None,
            stock_signal_overrides=stock_signal_overrides or None,
            anchor_bar_overrides=anchor_overrides or None,
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
        )
        if elapsed > MAX_TICK_SEC:
            payload["tick_slow"] = True
            logger.warning("breakfast tick :%02d took %.1fs (>%.0fs)", minute, elapsed, MAX_TICK_SEC)

        with _LOCK:
            _LIVE_TICK_SNAPSHOT = dict(payload)

        return {
            "ok": True,
            "minute": minute,
            "elapsed_sec": round(elapsed, 2),
            "sectors": len(payload.get("sectors") or []),
            "session_date": cache_key,
        }


def run_breakfast_freeze_lock(*, retry: bool = False) -> Dict[str, Any]:
    """9:20:30 IST — final 1m snapshot, persist signals + session lock."""
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
    cross_status = "matched"
    lock_status = "locked"
    failure_reason: Optional[str] = None

    if not sectors:
        lock_status = "failed"
        failure_reason = "no_sectors_at_freeze"
        payload["state"] = "lock_failed"
        payload["phase"] = "frozen"
        payload["banner"] = "LOCK FAILED — no picks at 9:20; capture manually"
        payload["lock_failed"] = True

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
    global _LIVE_TICK_SNAPSHOT
    with _LOCK:
        _SESSION_CACHE.clear()
        _LIVE_TICK_SNAPSHOT = None
        _FREEZE_ATTEMPTS.clear()
