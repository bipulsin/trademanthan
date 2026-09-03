"""Breakfast Strategy live pre-market state (9:15–9:21 IST)."""
from __future__ import annotations

import json
import logging
import threading
import time
from datetime import date, datetime, time as dt_time, timedelta
from typing import Any, Dict, List, Optional, Set, Tuple

import pytz

from backend.config import settings
from backend.services.breakfast_strategy.backtest import collect_instrument_keys
from backend.services.breakfast_strategy.candles import (
    anchor_bar,
    bar_move_pct,
    bars_ohlc_close_match,
    candle_ohlcv,
    default_cache_dir,
    ensure_5m_cached,
    first_5m_bar,
    first_5m_bar_from_quote,
    first_5m_ohlc_payload,
    ist_ts,
    load_cached_5m,
    move_pct_vs_prev_close,
    prev_session_close,
    signal_bar,
    session_has_stock_bars,
)
from backend.services.breakfast_strategy.config import SL_PCT, STOCK_MOVE_CAP_PCT, TP_PCT
from backend.services.breakfast_strategy.engine import NIFTY50_KEY, nifty_bias_from_bar, select_breakfast_picks
from backend.services.breakfast_strategy.engine_prevclose import (
    nifty_bias_from_bar_vs_prev_close,
    select_breakfast_picks_prevclose,
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
from backend.services.breakfast_prev_close import (
    WICK_NONE,
    filter_live_stocks_by_wick_and_color,
    filter_sector_members_by_first_5m_color,
    filter_sector_members_by_sign_gate,
    filter_sector_members_by_wick,
    first_5m_is_doji,
    ensure_live_stock_wicks,
    load_stored_prev_closes_and_wicks,
    load_stored_wicks,
)
from backend.services.market_holiday import is_nse_holiday_ist
from backend.services.sector_movers import _index_key_to_sector_label
from backend.services.upstox_market_feed import (
    ensure_market_feed_running,
    feed_status,
    get_ws_1m_bars_for_session,
    get_ws_forming_5m_bar,
    get_ws_quote_for_instrument,
)
from backend.services.breakfast_strategy.live_persist import (
    assign_selected_sector_ranks,
    compact_live_sector_cards,
    fetch_live_signals,
    fetch_session_lock,
    live_state_from_persisted_rows,
    persist_live_signals,
)
from backend.services.upstox_service import UpstoxService

logger = logging.getLogger(__name__)
IST = pytz.timezone("Asia/Kolkata")

LIVE_SECTORS_TO_PICK = 2
LIVE_STOCKS_PER_SECTOR = 3
FORMING_FROM = dt_time(9, 16)
LOCK_AT = dt_time(9, 20, 30)
FREEZE_AFTER = LOCK_AT

_LOCK = threading.Lock()
_LIVE_BUILD_LOCK = threading.Lock()
_LIVE_SNAPSHOT: Optional[Dict[str, Any]] = None
_LIVE_SNAPSHOT_AT: float = 0.0
_LIVE_CACHE_TTL_SEC = 4.0
_LIVE_BUILDING = False
_OFF_CYCLE_SNAPSHOT: Optional[Dict[str, Any]] = None
_OFF_CYCLE_SNAPSHOT_AT: float = 0.0
_OFF_CYCLE_CACHE_KEY: str = ""
_FROZEN_STATE: Dict[str, Dict[str, Any]] = {}
_LAST_SESSION_STATE: Optional[Dict[str, Any]] = None
BLANK_SLATE_FROM = dt_time(9, 0)
SESSION_OPEN = dt_time(9, 15)


def _sector_label(sector_key: str) -> str:
    return _index_key_to_sector_label().get(str(sector_key or "").strip()) or str(sector_key)


def _now_ist(replay: Optional[datetime] = None) -> datetime:
    if replay is not None:
        if replay.tzinfo is None:
            return IST.localize(replay)
        return replay.astimezone(IST)
    return datetime.now(IST)


def _live_phase(now: datetime) -> str:
    t = now.time()
    if t < dt_time(9, 15):
        return "waiting"
    if t < FORMING_FROM:
        return "opening"
    if t < dt_time(9, 20):
        return "forming"
    if t < FREEZE_AFTER:
        return "bar_closing"
    return "frozen"


def ingest_frozen_snapshot(payload: Dict[str, Any]) -> None:
    """Called by live_tick freeze job to sync in-memory frozen state.

    The 9:20 snapshot (success or lock_failed) is sticky for the session day.
    Live must not rebuild picks from later off-cycle REST refreshes.
    """
    cache_key = str(payload.get("session_date") or "")[:10]
    if not cache_key:
        return
    with _LOCK:
        _FROZEN_STATE[cache_key] = dict(payload)
        global _LAST_SESSION_STATE
        _LAST_SESSION_STATE = dict(payload)


def _is_trading_day_ist(now: datetime) -> bool:
    if now.weekday() >= 5:
        return False
    noon = IST.localize(datetime.combine(now.date(), dt_time(12, 0)))
    return not is_nse_holiday_ist(noon)


def _is_blank_slate(now: datetime) -> bool:
    """9:00–9:15 IST on a trading day — empty boxes before pre-market window."""
    if not _is_trading_day_ist(now):
        return False
    t = now.time()
    return BLANK_SLATE_FROM <= t < SESSION_OPEN


def _is_pre_live_window(now: datetime) -> bool:
    """Trading day before 9:00 IST — outside the live feed window."""
    return _is_trading_day_ist(now) and now.time() < BLANK_SLATE_FROM


def _off_cycle_banner(now: datetime) -> str:
    return f"Off cycle data as of {now.strftime('%d-%b-%Y %H:%M')}"


def _lock_failed_preview_banner(reason: str, off: Dict[str, Any]) -> str:
    """Banner for a persisted 9:20 lock failure (no off-cycle refresh)."""
    r = str(reason or "see logs")
    as_of = str((off or {}).get("server_time") or (off or {}).get("locked_at") or "").strip()
    head = f"LOCK FAILED — {r}"
    if as_of:
        # Keep a short as-of when payload carries the freeze clock.
        try:
            from datetime import datetime as _dt

            ts = _dt.fromisoformat(as_of.replace("Z", "+00:00")).astimezone(IST)
            return f"{head} · frozen as of {ts.strftime('%d-%b-%Y %H:%M')}"
        except Exception:
            pass
    return head


def _blank_live_payload(
    now: datetime,
    *,
    banner: str,
    state: str = "blank",
    phase: str = "waiting",
    data_missing_reason: Optional[str] = None,
) -> Dict[str, Any]:
    out: Dict[str, Any] = {
        "ok": True,
        "state": state,
        "phase": phase,
        "session_date": now.date().isoformat(),
        "banner": banner,
        "server_time": now.isoformat(),
        "refresh_allowed": _is_trading_day_ist(now) and now.time() < FREEZE_AFTER,
        "poll_interval_sec": 5 if _is_trading_day_ist(now) and now.time() < FREEZE_AFTER else 0,
        "nifty": {},
        "sectors": [],
        "ranked_sector_count": 0,
        "mismatch_instruments": [],
        "universe_instruments": 0,
    }
    if data_missing_reason:
        out["data_missing"] = True
        out["data_missing_reason"] = data_missing_reason
    return out


def _load_persisted_live_state(session_date: str) -> Optional[Dict[str, Any]]:
    try:
        rows = fetch_live_signals(session_date)
        if rows:
            return live_state_from_persisted_rows(session_date, rows)
    except Exception as e:
        logger.warning("breakfast live persist load failed for %s: %s", session_date, e)
    return None


def _frozen_client_payload(payload: Dict[str, Any], now: datetime) -> Dict[str, Any]:
    out = dict(payload)
    out["server_time"] = now.isoformat()
    out["refresh_allowed"] = False
    out["poll_interval_sec"] = 0
    return out


def _payload_from_lock_row(lock_row: Optional[Dict[str, Any]]) -> Optional[Dict[str, Any]]:
    """Full freeze snapshot stored on breakfast_session_lock.payload_json."""
    if not lock_row:
        return None
    raw = lock_row.get("payload_json")
    if raw is None:
        return None
    if isinstance(raw, str):
        try:
            raw = json.loads(raw)
        except Exception:
            return None
    if not isinstance(raw, dict) or not raw:
        return None
    return dict(raw)


def _tick_snapshot_for_session(cache_key: str) -> Optional[Dict[str, Any]]:
    """In-memory forming/freeze snapshot — covers the post-9:20:30 persist gap."""
    try:
        from backend.services.breakfast_strategy.live_tick import get_live_tick_snapshot

        tick = get_live_tick_snapshot()
    except Exception as e:
        logger.debug("breakfast tick snapshot read (frozen): %s", e)
        return None
    if not tick:
        return None
    if str(tick.get("session_date") or "")[:10] != cache_key:
        return None
    return dict(tick)


def _last_session_snapshot(now: datetime, *, banner: str) -> Dict[str, Any]:
    session_key = now.date().isoformat()
    with _LOCK:
        snap = dict(_LAST_SESSION_STATE) if _LAST_SESSION_STATE else None
        if not snap and _FROZEN_STATE:
            latest_key = max(_FROZEN_STATE.keys())
            snap = dict(_FROZEN_STATE[latest_key])
    if not snap:
        persisted = _load_persisted_live_state(session_key)
        if persisted:
            out = dict(persisted)
            out["state"] = "off_session"
            out["phase"] = "frozen"
            out["banner"] = banner
            out["server_time"] = now.isoformat()
            out["refresh_allowed"] = False
            out["poll_interval_sec"] = 0
            return out
        # Never rebuild Live picks off-cycle after the 9:20 freeze window.
        return _blank_live_payload(
            now,
            banner=banner,
            state="off_session",
        )
    out = dict(snap)
    out["state"] = "off_session"
    out["phase"] = "frozen"
    out["banner"] = banner
    out["server_time"] = now.isoformat()
    out["refresh_allowed"] = False
    out["poll_interval_sec"] = 0
    return out


def _banner_for_phase(phase: str, *, mismatch: bool, stale: bool) -> str:
    if mismatch:
        return "DATA STALE / MISMATCH — WS vs REST 5m disagree; verify before trading"
    if stale:
        return "DATA STALE — WebSocket feed quiet; numbers may lag"
    if phase in ("forming", "opening", "bar_closing"):
        return "LIVE — FORMING, NOT FINAL"
    if phase in ("locking", "locked", "frozen"):
        return "LOCKED — 9:20 CONFIRMED"
    return "Pre-market — session opens 9:15 IST"


def _resolve_session_bar(
    instrument_key: str,
    session_date: date,
    candles_5m: List[Dict[str, Any]],
    upstox: UpstoxService,
    *,
    phase: str,
    allow_quote_proxy: bool,
) -> Tuple[Optional[Dict[str, Any]], str]:
    """Return (bar, source) where source is ws | rest | quote | ws+rest."""
    rest_bar = signal_bar(candles_5m, session_date) or first_5m_bar(candles_5m, session_date)
    ws_bar = get_ws_forming_5m_bar(instrument_key, session_date)

    if phase in ("locking", "locked", "frozen") and rest_bar and ws_bar:
        if bars_ohlc_close_match(ws_bar, rest_bar):
            return rest_bar, "ws+rest"
        return ws_bar, "mismatch"

    if phase in ("locking", "locked", "frozen") and rest_bar:
        return rest_bar, "rest"

    if ws_bar:
        return ws_bar, "ws"

    if allow_quote_proxy:
        qbar = first_5m_bar_from_quote(upstox, instrument_key, session_date)
        if qbar:
            return qbar, "quote"

    return rest_bar, "rest" if rest_bar else "none"


def _ws_anchor_bar(instrument_key: str, session_date: date) -> Optional[Dict[str, Any]]:
    """9:15 anchor from WS 1m when REST 5m not yet available."""
    for b in get_ws_1m_bars_for_session(instrument_key, session_date):
        ts = b.get("timestamp") or ""
        try:
            dt = datetime.fromisoformat(str(ts).replace("Z", "+00:00"))
            if dt.tzinfo is None:
                dt = IST.localize(dt)
            else:
                dt = dt.astimezone(IST)
        except (TypeError, ValueError):
            continue
        if dt.time() == dt_time(9, 15):
            return {
                "timestamp": ist_ts(session_date, 9, 15).isoformat(),
                "open": b["open"],
                "high": b["high"],
                "low": b["low"],
                "close": b["close"],
                "volume": b.get("volume", 0),
            }
    return None


def _stock_signal_override(
    sym: str,
    candles: List[Dict[str, Any]],
    session_date: date,
    partial_bar: Optional[Dict[str, Any]],
) -> Optional[Tuple[Dict[str, Any], float]]:
    if not partial_bar:
        return None
    prev = prev_session_close(candles, session_date)
    if prev is None:
        return None
    _, _, _, cl, _ = candle_ohlcv(partial_bar)
    pct = move_pct_vs_prev_close(cl, prev)
    if pct is None:
        return None
    return partial_bar, float(pct)


def _nifty_live_card(
    *,
    nifty_bar: Optional[Dict[str, Any]],
    nifty_prev: Optional[float],
    sel: Any = None,
) -> Tuple[str, Optional[float], str]:
    """Bias + signed % vs prev close, and LONG/SHORT/UNKNOWN matching that %."""
    bias, pct = "unknown", None
    if nifty_bar:
        bias, pct = nifty_bias_from_bar_vs_prev_close(nifty_bar, nifty_prev, missing="unknown")
    if sel is not None:
        bias = sel.nifty_bias
        pct = sel.nifty_bias_pct
    if bias == "negative" or (pct is not None and pct < 0):
        return "negative", (None if pct is None else float(pct)), "SHORT"
    if bias == "unknown" or pct is None:
        return "unknown", (None if pct is None else float(pct)), "UNKNOWN"
    return "positive", float(pct), "LONG"


def _serialize_stock_pick(
    stk: Any,
    *,
    long_side: bool,
    upstox: UpstoxService,
    allow_rest_quote: bool = True,
    wick: str = WICK_NONE,
) -> Dict[str, Any]:
    anchor = stk.anchor_bar
    _, _, _, anchor_px, _ = candle_ohlcv(anchor)
    lot = int(stk.row.lot_size or 0)
    direction = "LONG" if long_side else "SHORT"
    sl_px = anchor_px * (1.0 - SL_PCT) if long_side else anchor_px * (1.0 + SL_PCT)
    tp_px = anchor_px * (1.0 + TP_PCT) if long_side else anchor_px * (1.0 - TP_PCT)
    risk_inr = round(abs(anchor_px - sl_px) * lot, 2) if lot > 0 else None
    wsq = get_ws_quote_for_instrument(stk.row.instrument_key) or {}
    ltp = wsq.get("ltp")
    if ltp is None and allow_rest_quote:
        try:
            q = upstox.get_market_quote_by_key(stk.row.instrument_key) or {}
            ltp = q.get("last_price")
        except Exception:
            ltp = None
    sig_o, _, _, sig_cl, sig_vol = candle_ohlcv(stk.signal_bar)
    labels = ["Pick 1", "Pick 2", "Watch 3rd"]
    label = labels[stk.stock_rank - 1] if 1 <= stk.stock_rank <= len(labels) else f"#{stk.stock_rank}"
    out = {
        "rank_label": label,
        "stock_rank": stk.stock_rank,
        "rank_in_sector": stk.stock_rank,
        "symbol": stk.row.stock,
        "display_symbol": stk.row.display_symbol,
        "instrument_label": stk.row.instrument_label,
        "sector": stk.row.sector,
        "direction": direction,
        "move_pct_at_entry": round(stk.move_pct, 3),
        "ltp": ltp,
        "signal_open": sig_o,
        "signal_close": sig_cl,
        "is_doji": first_5m_is_doji(sig_o, sig_cl),
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
    out.update(first_5m_ohlc_payload(stk.signal_bar))
    return out


def _candidate_sector_keys_for_live(
    *,
    stocks_by_sector: Dict[str, List[Dict[str, str]]],
    session_date: date,
    fut_by_und: Dict[str, List[Dict[str, Any]]],
    eq_by_symbol: Dict[str, Dict[str, Any]],
    sector_overrides: Dict[str, Dict[str, Any]],
    sector_candles: Dict[str, List[Dict[str, Any]]],
    nifty_bar: Dict[str, Any],
    sectors_to_pick: int,
) -> List[str]:
    """Top sector keys likely to be picked — limits WS-only stock scan during live window."""
    bias, _ = nifty_bias_from_bar(nifty_bar)
    long_side = bias == "positive"
    eligible = fo_eligible_sector_keys(
        stocks_by_sector, session_date, fut_by_und=fut_by_und, eq_by_symbol=eq_by_symbol
    )
    sector_bars: Dict[str, Dict[str, Any]] = {}
    for skey in eligible:
        bar = sector_overrides.get(skey) or first_5m_bar(sector_candles.get(skey, []), session_date)
        if bar:
            sector_bars[skey] = bar
    ranked = rank_sectors(sector_bars, eligible_keys=eligible, descending=long_side)
    take = min(len(ranked), max(sectors_to_pick + 2, sectors_to_pick))
    return [skey for skey, _, _ in ranked[:take]]


def _build_ws_stock_overrides(
    *,
    stocks_by_sector: Dict[str, List[Dict[str, str]]],
    candidate_sector_keys: List[str],
    session_date: date,
    stock_candles_by_key: Dict[str, List[Dict[str, Any]]],
    fut_by_und: Dict[str, List[Dict[str, Any]]],
    eq_by_symbol: Dict[str, Dict[str, Any]],
) -> Tuple[Dict[str, Tuple[Dict[str, Any], float]], Dict[str, Dict[str, Any]]]:
    """WS-only partial bars for candidate sector members (no per-stock REST quotes)."""
    stock_signal_overrides: Dict[str, Tuple[Dict[str, Any], float]] = {}
    anchor_overrides: Dict[str, Dict[str, Any]] = {}
    for skey in candidate_sector_keys:
        for m in stocks_by_sector.get(skey, []):
            sym = str(m.get("stock") or "").upper()
            if not sym or sym in stock_signal_overrides:
                continue
            ref = resolve_stock_instrument(sym, session_date, fut_by_und=fut_by_und, eq_by_symbol=eq_by_symbol)
            if not ref or not ref.instrument_key:
                continue
            candles = stock_candles_by_key.get(ref.instrument_key, [])
            partial = get_ws_forming_5m_bar(ref.instrument_key, session_date)
            ov = _stock_signal_override(sym, candles, session_date, partial)
            if ov:
                stock_signal_overrides[sym] = ov
            ab = anchor_bar(candles, session_date) or _ws_anchor_bar(ref.instrument_key, session_date)
            if ab:
                anchor_overrides[sym] = ab
    return stock_signal_overrides, anchor_overrides


def _warm_off_cycle_stock_candles(
    *,
    stocks_by_sector: Dict[str, List[Dict[str, str]]],
    candidate_sector_keys: List[str],
    session_date: date,
    stock_candles_by_key: Dict[str, List[Dict[str, Any]]],
    fut_by_und: Dict[str, List[Dict[str, Any]]],
    eq_by_symbol: Dict[str, Dict[str, Any]],
    upstox: UpstoxService,
    cache_dir: Any,
) -> None:
    """Fetch 5m bars only for stocks in likely-picked sectors (off-cycle preview)."""
    for skey in candidate_sector_keys:
        for m in stocks_by_sector.get(skey, []):
            sym = str(m.get("stock") or "").upper()
            if not sym:
                continue
            ref = resolve_stock_instrument(sym, session_date, fut_by_und=fut_by_und, eq_by_symbol=eq_by_symbol)
            if not ref or not ref.instrument_key:
                continue
            ik = ref.instrument_key
            cached = stock_candles_by_key.get(ik, [])
            if session_has_stock_bars(cached, session_date):
                continue
            stock_candles_by_key[ik] = ensure_5m_cached(
                upstox,
                cache_dir,
                ik,
                range_end=session_date,
                session_dates=[session_date],
                force=True,
            )


def _resort_sector_stocks(sectors_out: List[Dict[str, Any]], *, long_side: bool) -> None:
    """Re-rank stocks within each sector by live move % on every refresh."""
    for sec in sectors_out:
        stocks = list(sec.get("stocks") or [])
        if not stocks:
            continue
        stocks.sort(
            key=lambda s: float(s.get("move_pct_at_entry") or 0),
            reverse=long_side,
        )
        for i, st in enumerate(stocks, start=1):
            st["stock_rank"] = i
            st["rank_in_sector"] = i
            st["rank_label"] = str(i)
        sec["stocks"] = stocks


def build_live_state(*, replay_at: Optional[datetime] = None) -> Dict[str, Any]:
    global _LIVE_SNAPSHOT, _LIVE_SNAPSHOT_AT, _LIVE_BUILDING
    now = _now_ist(replay_at)
    session_date = now.date()
    phase = _live_phase(now)
    cache_key = session_date.isoformat()

    if phase == "frozen" or now.time() >= FREEZE_AFTER:
        # After 9:20, only serve the frozen 9:15–9:20 snapshot. Never rebuild off-cycle.
        with _LOCK:
            cached = _FROZEN_STATE.get(cache_key)
        if cached:
            return _frozen_client_payload(cached, now)

        lock_row = fetch_session_lock(cache_key)
        lock_status = str((lock_row or {}).get("lock_status") or "").lower()

        from_lock = _payload_from_lock_row(lock_row)
        if from_lock:
            if lock_status == "failed" or from_lock.get("lock_failed"):
                reason = (lock_row or {}).get("failure_reason") or from_lock.get("failure_reason") or "see logs"
                from_lock["lock_failed"] = True
                from_lock["failure_reason"] = reason
                from_lock["state"] = "lock_failed"
                from_lock["phase"] = "frozen"
                from_lock["banner"] = _lock_failed_preview_banner(reason, from_lock)
                from_lock["refresh_allowed"] = False
                from_lock["poll_interval_sec"] = 0
            ingest_frozen_snapshot(from_lock)
            return _frozen_client_payload(from_lock, now)

        if lock_status == "locked":
            persisted = _load_persisted_live_state(cache_key)
            if persisted:
                ingest_frozen_snapshot(persisted)
                return _frozen_client_payload(persisted, now)

        tick = _tick_snapshot_for_session(cache_key)
        if tick:
            ingest_frozen_snapshot(tick)
            return _frozen_client_payload(tick, now)

        persisted = _load_persisted_live_state(cache_key)
        if persisted:
            ingest_frozen_snapshot(persisted)
            return _frozen_client_payload(persisted, now)

        if lock_status == "locked":
            return _blank_live_payload(
                now,
                banner="LOCKED — 9:20 CONFIRMED",
                state="locked",
                phase="frozen",
            )

        if lock_status == "failed":
            reason = (lock_row or {}).get("failure_reason") or "see logs"
            out = _blank_live_payload(
                now,
                banner=f"LOCK FAILED — {reason}",
                state="lock_failed",
                phase="frozen",
            )
            out["lock_failed"] = True
            out["failure_reason"] = reason
            out["refresh_allowed"] = False
            out["poll_interval_sec"] = 0
            return out

        # Trading day after freeze with no stored 9:20 snapshot — do not invent off-cycle picks.
        return _blank_live_payload(
            now,
            banner="FROZEN — waiting for 9:20 lock snapshot",
            state="frozen",
            phase="frozen",
        )

    # During scheduler-driven forming window, serve tick snapshot (no heavy rebuild on poll).
    if replay_at is None and phase in ("forming", "bar_closing"):
        try:
            from backend.services.breakfast_strategy.live_tick import get_live_tick_snapshot

            tick_snap = get_live_tick_snapshot()
            if tick_snap and str(tick_snap.get("session_date")) == cache_key:
                out = dict(tick_snap)
                out["server_time"] = now.isoformat()
                out["phase"] = phase
                return out
        except Exception as e:
            logger.debug("breakfast tick snapshot read: %s", e)

    if now.weekday() >= 5:
        return _last_session_snapshot(
            now,
            banner="Weekend — showing last session picks (locked at 9:20)",
        )

    noon = IST.localize(datetime.combine(session_date, dt_time(12, 0)))
    if is_nse_holiday_ist(noon):
        return _last_session_snapshot(
            now,
            banner="NSE holiday — showing last session picks (locked at 9:20)",
        )

    if _is_pre_live_window(now):
        return _last_session_snapshot(
            now,
            banner="Pre-market — live window opens 9:00 IST",
        )

    if _is_blank_slate(now):
        return _blank_live_payload(now, banner="Pre-market — session opens 9:15 IST")

    is_realtime = replay_at is None
    if is_realtime:
        with _LIVE_BUILD_LOCK:
            age = time.monotonic() - _LIVE_SNAPSHOT_AT
            if _LIVE_SNAPSHOT and age < _LIVE_CACHE_TTL_SEC:
                out = dict(_LIVE_SNAPSHOT)
                out["server_time"] = now.isoformat()
                return out
            if _LIVE_BUILDING and _LIVE_SNAPSHOT:
                out = dict(_LIVE_SNAPSHOT)
                out["server_time"] = now.isoformat()
                out["refresh_pending"] = True
                return out
            _LIVE_BUILDING = True

    try:
        payload = _build_live_state_payload(
            now=now,
            session_date=session_date,
            phase=phase,
            cache_key=cache_key,
        )
    finally:
        if is_realtime:
            with _LIVE_BUILD_LOCK:
                _LIVE_BUILDING = False

    if is_realtime:
        with _LIVE_BUILD_LOCK:
            _LIVE_SNAPSHOT = dict(payload)
            _LIVE_SNAPSHOT_AT = time.monotonic()

    return payload


def _build_live_state_payload(
    *,
    now: datetime,
    session_date: date,
    phase: str,
    cache_key: str,
    off_cycle: bool = False,
) -> Dict[str, Any]:
    stocks_by_sector = load_arbitrage_by_sector()
    bench_prev, stock_prev, stock_wicks = load_stored_prev_closes_and_wicks()
    if not stock_wicks:
        stock_wicks = load_stored_wicks()
    fut_by_und, eq_by_symbol = build_instrument_indexes()
    keys = collect_instrument_keys(
        [session_date],
        stocks_by_sector,
        fut_by_und,
        eq_by_symbol,
        spot_proxy_fallback=False,
    )
    from backend.services.breakfast_upstox_gate import breakfast_upstox_priority_owner

    with breakfast_upstox_priority_owner():
        ensure_market_feed_running(sorted(keys))

    ux = UpstoxService(settings.UPSTOX_API_KEY, settings.UPSTOX_API_SECRET)
    ux.reload_token_from_storage()
    cache_dir = default_cache_dir()

    if phase in ("locking", "locked") or off_cycle:
        nifty_candles = ensure_5m_cached(
            ux, cache_dir, NIFTY50_KEY, range_end=session_date, session_dates=[session_date], force=True
        )
    else:
        nifty_candles = load_cached_5m(cache_dir, NIFTY50_KEY)
    sector_candles: Dict[str, List[Dict[str, Any]]] = {}
    for label, _yh in SECTOR_UNIVERSE:
        ik = sector_index_key_for_label(label)
        if ik:
            if off_cycle:
                sector_candles[ik] = ensure_5m_cached(
                    ux, cache_dir, ik, range_end=session_date, session_dates=[session_date], force=True
                )
            else:
                sector_candles[ik] = load_cached_5m(cache_dir, ik)

    stock_candles_by_key: Dict[str, List[Dict[str, Any]]] = {}
    for ik in keys:
        if ik == NIFTY50_KEY or ik in sector_candles:
            continue
        stock_candles_by_key[ik] = load_cached_5m(cache_dir, ik)

    allow_proxy = phase in ("forming", "opening", "bar_closing", "waiting")
    # During 9:16–9:20 forming, rely on WS + disk cache only — no per-instrument REST quotes.
    allow_quote_proxy = phase in ("opening", "waiting") or off_cycle
    need_stock_overrides = allow_proxy or off_cycle
    nifty_bar, nifty_src = _resolve_session_bar(
        NIFTY50_KEY, session_date, nifty_candles, ux, phase=phase, allow_quote_proxy=allow_quote_proxy
    )

    sector_overrides: Dict[str, Dict[str, Any]] = {}
    mismatch_keys: List[str] = []
    for label, _yh in SECTOR_UNIVERSE:
        ik = sector_index_key_for_label(label)
        if not ik:
            continue
        bar, src = _resolve_session_bar(
            ik, session_date, sector_candles.get(ik, []), ux, phase=phase, allow_quote_proxy=allow_quote_proxy
        )
        if bar:
            sector_overrides[ik] = bar
        else:
            logger.warning("breakfast sector 5m missing key=%s src=%s off_cycle=%s", ik, src, off_cycle)
        if src == "mismatch":
            mismatch_keys.append(ik)

    nifty_prev = bench_prev.get(NIFTY50_KEY) or prev_session_close(nifty_candles, session_date)
    sector_books: List[Tuple[str, bool]] = []
    wick_members: Dict[str, List[Dict[str, str]]] = {}
    ranked_keys: List[str] = []
    long_side_off = False
    picked_off: List[str] = []
    if off_cycle:
        from backend.services.breakfast_strategy.live_tick import (
            LIVE_SECTORS_TO_PICK as _LIVE_N,
            _books_same_side,
            _members_for_books,
            _rank_picked_sectors,
            try_one_sector_cascade,
        )

        candles_for_rank: Dict[str, List[Dict[str, Any]]] = {
            ik: list(bars) for ik, bars in sector_candles.items()
        }
        candles_for_rank[NIFTY50_KEY] = nifty_candles
        for ik, bar in sector_overrides.items():
            candles_for_rank[ik] = [bar] + list(candles_for_rank.get(ik) or [])
        ranked_keys, long_side_off = _rank_picked_sectors(
            session_date=session_date,
            candles_1m=candles_for_rank,
            stocks_by_sector=stocks_by_sector,
            fut_by_und=fut_by_und,
            eq_by_symbol=eq_by_symbol,
            upto_hhmm=(9, 20),
            nifty_prev_close=nifty_prev,
            sector_prev_closes=bench_prev,
        )
        picked_off = ranked_keys[:_LIVE_N]
        sector_books = _books_same_side(picked_off, long_side_off)
        wick_members = _members_for_books(stocks_by_sector, stock_wicks, sector_books)
        candidate_sectors = [skey for skey, _ls in sector_books]
        if nifty_bar and candidate_sectors:
            _warm_off_cycle_stock_candles(
                stocks_by_sector=wick_members,
                candidate_sector_keys=candidate_sectors,
                session_date=session_date,
                stock_candles_by_key=stock_candles_by_key,
                fut_by_und=fut_by_und,
                eq_by_symbol=eq_by_symbol,
                upstox=ux,
                cache_dir=cache_dir,
            )
        if not sector_books:
            logger.warning(
                "breakfast off-cycle no sector books bars=%s prev=%s",
                len(sector_overrides),
                len(bench_prev),
            )

    stock_signal_overrides: Dict[str, Tuple[Dict[str, Any], float]] = {}
    anchor_overrides: Dict[str, Dict[str, Any]] = {}
    if need_stock_overrides and nifty_bar:
        candidate_sectors = [skey for skey, _ls in sector_books] if sector_books else _candidate_sector_keys_for_live(
            stocks_by_sector=stocks_by_sector,
            session_date=session_date,
            fut_by_und=fut_by_und,
            eq_by_symbol=eq_by_symbol,
            sector_overrides=sector_overrides,
            sector_candles=sector_candles,
            nifty_bar=nifty_bar,
            sectors_to_pick=LIVE_SECTORS_TO_PICK,
        )
        stock_signal_overrides, anchor_overrides = _build_ws_stock_overrides(
            stocks_by_sector=stocks_by_sector,
            candidate_sector_keys=candidate_sectors,
            session_date=session_date,
            stock_candles_by_key=stock_candles_by_key,
            fut_by_und=fut_by_und,
            eq_by_symbol=eq_by_symbol,
        )

    stale = False
    wsq_n = get_ws_quote_for_instrument(NIFTY50_KEY)
    if phase not in ("waiting", "off_session") and not wsq_n:
        stale = True

    bar_map = {
        str(sym).strip().upper(): ov[0]
        for sym, ov in (stock_signal_overrides or {}).items()
        if ov and ov[0]
    }
    stocks_for_pick = dict(stocks_by_sector)
    if sector_books:
        for skey, book_long in sector_books:
            members = wick_members.get(skey, []) if wick_members else stocks_by_sector.get(skey, [])
            if bar_map:
                move_pcts = {
                    str(sym).strip().upper(): float(ov[1])
                    for sym, ov in (stock_signal_overrides or {}).items()
                    if ov and ov[1] is not None
                }
                members = filter_sector_members_by_sign_gate(
                    {skey: members}, move_pcts, long_side=book_long, move_cap=STOCK_MOVE_CAP_PCT
                ).get(skey, [])
                members = filter_sector_members_by_first_5m_color(
                    {skey: members}, bar_map, long_side=book_long
                ).get(skey, [])
            stocks_for_pick[skey] = members
        if off_cycle and picked_off and ranked_keys:
            new_picked, cascade_from, cascade_to, swapped = try_one_sector_cascade(
                picked_off, ranked_keys, stocks_for_pick
            )
            if swapped and cascade_to:
                extra_books = _books_same_side([cascade_to], long_side_off)
                extra_wick = _members_for_books(stocks_by_sector, stock_wicks, extra_books)
                wick_members[cascade_to] = extra_wick.get(cascade_to, [])
                _warm_off_cycle_stock_candles(
                    stocks_by_sector=wick_members,
                    candidate_sector_keys=[cascade_to],
                    session_date=session_date,
                    stock_candles_by_key=stock_candles_by_key,
                    fut_by_und=fut_by_und,
                    eq_by_symbol=eq_by_symbol,
                    upstox=ux,
                    cache_dir=cache_dir,
                )
                extra_sig, extra_anc = _build_ws_stock_overrides(
                    stocks_by_sector=wick_members,
                    candidate_sector_keys=[cascade_to],
                    session_date=session_date,
                    stock_candles_by_key=stock_candles_by_key,
                    fut_by_und=fut_by_und,
                    eq_by_symbol=eq_by_symbol,
                )
                stock_signal_overrides.update(extra_sig)
                anchor_overrides.update(extra_anc)
                bar_map.update(
                    {str(sym).strip().upper(): ov[0] for sym, ov in extra_sig.items() if ov and ov[0]}
                )
                extra_members = extra_wick.get(cascade_to, [])
                if bar_map:
                    extra_move = {
                        str(sym).strip().upper(): float(ov[1])
                        for sym, ov in (stock_signal_overrides or {}).items()
                        if ov and ov[1] is not None
                    }
                    extra_members = filter_sector_members_by_sign_gate(
                        {cascade_to: extra_members},
                        extra_move,
                        long_side=long_side_off,
                        move_cap=STOCK_MOVE_CAP_PCT,
                    ).get(cascade_to, [])
                    extra_members = filter_sector_members_by_first_5m_color(
                        {cascade_to: extra_members}, bar_map, long_side=long_side_off
                    ).get(cascade_to, [])
                stocks_for_pick[cascade_to] = extra_members
                if cascade_from:
                    stocks_for_pick.pop(cascade_from, None)
                    wick_members.pop(cascade_from, None)
                picked_off = new_picked
                sector_books = _books_same_side(picked_off, long_side_off)
                bar = sector_overrides.get(cascade_to) or first_5m_bar(
                    sector_candles.get(cascade_to, []), session_date
                )
                if bar:
                    sector_overrides[cascade_to] = bar
        sel = select_breakfast_picks_prevclose(
            session_date,
            nifty_candles=nifty_candles,
            sector_candles=sector_candles,
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
    else:
        long_side_pick = (nifty_bias_from_bar_vs_prev_close(nifty_bar or {}, nifty_prev, missing="unknown")[0] == "positive") if nifty_bar else False
        stocks_for_pick = filter_sector_members_by_wick(
            stocks_by_sector,
            stock_wicks,
            long_side=long_side_pick,
        )
        if bar_map:
            move_pcts = {
                str(sym).strip().upper(): float(ov[1])
                for sym, ov in (stock_signal_overrides or {}).items()
                if ov and ov[1] is not None
            }
            stocks_for_pick = filter_sector_members_by_sign_gate(
                stocks_for_pick, move_pcts, long_side=long_side_pick, move_cap=STOCK_MOVE_CAP_PCT
            )
            stocks_for_pick = filter_sector_members_by_first_5m_color(
                stocks_for_pick, bar_map, long_side=long_side_pick
            )
        sel = select_breakfast_picks_prevclose(
            session_date,
            nifty_candles=nifty_candles,
            sector_candles=sector_candles,
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

    mismatch = bool(mismatch_keys) or nifty_src == "mismatch"
    state = "mismatch" if mismatch else ("stale" if stale else phase)
    if phase in ("locked", "frozen"):
        state = "locked" if not mismatch and not stale else state

    nifty_bias, nifty_pct, nifty_dir = _nifty_live_card(
        nifty_bar=nifty_bar, nifty_prev=nifty_prev, sel=sel
    )

    sectors_out: List[Dict[str, Any]] = []
    if sel:
        for sp in sel.sector_picks:
            sp_long = sel.long_side if getattr(sp, "long_side", None) is None else bool(sp.long_side)
            sp_dir = "LONG" if sp_long else "SHORT"
            sectors_out.append(
                {
                    "sector_key": sp.sector_key,
                    "sector_label": _sector_label(sp.sector_key),
                    "sector_rank": sp.sector_rank,
                    "move_pct": round(sp.sector_move_pct, 3),
                    "direction": sp_dir,
                    "volume": sp.sector_volume,
                    "stocks": filter_live_stocks_by_wick_and_color(
                        [
                        _serialize_stock_pick(
                            s,
                            long_side=sp_long,
                            upstox=ux,
                            allow_rest_quote=allow_quote_proxy,
                            wick=stock_wicks.get(str(s.row.stock or "").upper(), WICK_NONE),
                        )
                        for s in sp.stocks
                        ],
                        direction=sp_dir,
                        wick_by_symbol=stock_wicks,
                    ),
                }
            )
            _resort_sector_stocks([sectors_out[-1]], long_side=sp_long)

    sectors_out = compact_live_sector_cards(sectors_out)
    ensure_live_stock_wicks(sectors_out, stock_wicks)
    assign_selected_sector_ranks(sectors_out)

    refresh_allowed = now.time() < FREEZE_AFTER and phase != "frozen"

    payload: Dict[str, Any] = {
        "ok": True,
        "state": state,
        "phase": phase,
        "session_date": cache_key,
        "server_time": now.isoformat(),
        "banner": _banner_for_phase(phase, mismatch=mismatch, stale=stale),
        "refresh_allowed": refresh_allowed,
        "poll_interval_sec": 5 if refresh_allowed else 0,
        "nifty": {
            "instrument_key": NIFTY50_KEY,
            "bias": nifty_bias,
            "bias_pct": round(nifty_pct, 3) if nifty_pct is not None else None,
            "direction": nifty_dir,
            "bar_source": nifty_src,
            "open": nifty_bar.get("open") if nifty_bar else None,
            "close": nifty_bar.get("close") if nifty_bar else None,
            "ltp": (wsq_n or {}).get("ltp"),
        },
        "sectors": sectors_out,
        "ranked_sector_count": len(sel.ranked_sectors) if sel else 0,
        "mismatch_instruments": mismatch_keys,
        "feed": feed_status(),
        "universe_instruments": len(keys),
    }

    if phase in ("locking", "locked", "frozen") and not mismatch and not off_cycle:
        ingest_frozen_snapshot(payload)

    # Persist handled by live_scheduler freeze at 9:20:30 — avoid duplicate rows here.

    return payload


def build_off_cycle_preview_state(now: datetime) -> Dict[str, Any]:
    """Compute breakfast picks from current Upstox data when 9:20 lock was never persisted."""
    session_date = now.date()
    cache_key = session_date.isoformat()
    payload = _build_live_state_payload(
        now=now,
        session_date=session_date,
        phase="locked",
        cache_key=cache_key,
        off_cycle=True,
    )
    payload["state"] = "off_cycle"
    payload["phase"] = "frozen"
    payload["off_cycle"] = True
    payload["banner"] = _off_cycle_banner(now)
    payload["refresh_allowed"] = False
    payload["poll_interval_sec"] = 0
    payload.pop("data_missing", None)
    payload.pop("data_missing_reason", None)
    return payload


def _get_off_cycle_preview_cached(now: datetime) -> Dict[str, Any]:
    global _OFF_CYCLE_SNAPSHOT, _OFF_CYCLE_SNAPSHOT_AT, _OFF_CYCLE_CACHE_KEY
    cache_key = now.date().isoformat()
    age = time.monotonic() - _OFF_CYCLE_SNAPSHOT_AT
    if _OFF_CYCLE_SNAPSHOT and _OFF_CYCLE_CACHE_KEY == cache_key and age < _LIVE_CACHE_TTL_SEC:
        out = dict(_OFF_CYCLE_SNAPSHOT)
        out["server_time"] = now.isoformat()
        out["banner"] = _off_cycle_banner(now)
        return out
    payload = build_off_cycle_preview_state(now)
    _OFF_CYCLE_SNAPSHOT = dict(payload)
    _OFF_CYCLE_SNAPSHOT_AT = time.monotonic()
    _OFF_CYCLE_CACHE_KEY = cache_key
    return payload


def validate_ws_vs_rest(
    instrument_key: str,
    session_date: date,
    *,
    upstox: Optional[UpstoxService] = None,
) -> Dict[str, Any]:
    """Cross-check WS-aggregated 9:15–9:20 vs REST 5m (dry-run / diagnostics)."""
    ux = upstox or UpstoxService(settings.UPSTOX_API_KEY, settings.UPSTOX_API_SECRET)
    ux.reload_token_from_storage()
    cache_dir = default_cache_dir()
    candles = ensure_5m_cached(ux, cache_dir, instrument_key, range_end=session_date, session_dates=[session_date], force=True)
    rest_bar = signal_bar(candles, session_date)
    ws_bar = get_ws_forming_5m_bar(instrument_key, session_date)
    return {
        "instrument_key": instrument_key,
        "session_date": session_date.isoformat(),
        "ws_bar": ws_bar,
        "rest_bar": rest_bar,
        "match": bars_ohlc_close_match(ws_bar, rest_bar),
    }
