"""Breakfast Strategy live pre-market state (9:15–9:21 IST)."""
from __future__ import annotations

import logging
import threading
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
    ist_ts,
    load_cached_5m,
    move_pct_vs_prev_close,
    prev_session_close,
    signal_bar,
)
from backend.services.breakfast_strategy.config import SL_PCT, TP_PCT
from backend.services.breakfast_strategy.engine import NIFTY50_KEY, select_breakfast_picks
from backend.services.breakfast_strategy.universe import (
    SECTOR_UNIVERSE,
    build_instrument_indexes,
    load_arbitrage_by_sector,
    sector_index_key_for_label,
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
from backend.services.breakfast_strategy.live_persist import persist_live_signals
from backend.services.upstox_service import UpstoxService

logger = logging.getLogger(__name__)
IST = pytz.timezone("Asia/Kolkata")

LIVE_SECTORS_TO_PICK = 2
LIVE_STOCKS_PER_SECTOR = 3
FORMING_FROM = dt_time(9, 16)
LOCK_WINDOW_START = dt_time(9, 20, 5)
LOCK_WINDOW_END = dt_time(9, 20, 10)
FREEZE_AFTER = dt_time(9, 21)

_LOCK = threading.Lock()
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
    if t < LOCK_WINDOW_START:
        return "bar_closing"
    if t <= LOCK_WINDOW_END:
        return "locking"
    if t < FREEZE_AFTER:
        return "locked"
    return "frozen"


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


def _blank_live_payload(now: datetime, *, banner: str, state: str = "blank") -> Dict[str, Any]:
    return {
        "ok": True,
        "state": state,
        "phase": "waiting",
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


def _last_session_snapshot(now: datetime, *, banner: str) -> Dict[str, Any]:
    with _LOCK:
        snap = dict(_LAST_SESSION_STATE) if _LAST_SESSION_STATE else None
        if not snap and _FROZEN_STATE:
            latest_key = max(_FROZEN_STATE.keys())
            snap = dict(_FROZEN_STATE[latest_key])
    if not snap:
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


def _serialize_stock_pick(
    stk: Any,
    *,
    long_side: bool,
    upstox: UpstoxService,
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
    if ltp is None:
        try:
            q = upstox.get_market_quote_by_key(stk.row.instrument_key) or {}
            ltp = q.get("last_price")
        except Exception:
            ltp = None
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
        "ltp": ltp,
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
    now = _now_ist(replay_at)
    session_date = now.date()
    phase = _live_phase(now)
    cache_key = session_date.isoformat()

    if phase == "frozen":
        with _LOCK:
            cached = _FROZEN_STATE.get(cache_key)
        if cached:
            out = dict(cached)
            out["server_time"] = now.isoformat()
            out["refresh_allowed"] = False
            return out

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

    if _is_blank_slate(now):
        return _blank_live_payload(now, banner="Pre-market — session opens 9:15 IST")

    stocks_by_sector = load_arbitrage_by_sector()
    fut_by_und, eq_by_symbol = build_instrument_indexes()
    keys = collect_instrument_keys(
        [session_date],
        stocks_by_sector,
        fut_by_und,
        eq_by_symbol,
        spot_proxy_fallback=False,
    )
    ensure_market_feed_running(sorted(keys))

    ux = UpstoxService(settings.UPSTOX_API_KEY, settings.UPSTOX_API_SECRET)
    ux.reload_token_from_storage()
    cache_dir = default_cache_dir()

    nifty_candles = ensure_5m_cached(
        ux, cache_dir, NIFTY50_KEY, range_end=session_date, session_dates=[session_date], force=phase in ("locking", "locked")
    )
    sector_candles: Dict[str, List[Dict[str, Any]]] = {}
    for label, _yh in SECTOR_UNIVERSE:
        ik = sector_index_key_for_label(label)
        if ik:
            sector_candles[ik] = load_cached_5m(cache_dir, ik)

    stock_candles_by_key: Dict[str, List[Dict[str, Any]]] = {}
    for ik in keys:
        if ik == NIFTY50_KEY or ik in sector_candles:
            continue
        stock_candles_by_key[ik] = load_cached_5m(cache_dir, ik)

    allow_proxy = phase in ("forming", "opening", "bar_closing", "waiting")
    nifty_bar, nifty_src = _resolve_session_bar(
        NIFTY50_KEY, session_date, nifty_candles, ux, phase=phase, allow_quote_proxy=allow_proxy
    )

    sector_overrides: Dict[str, Dict[str, Any]] = {}
    mismatch_keys: List[str] = []
    for label, _yh in SECTOR_UNIVERSE:
        ik = sector_index_key_for_label(label)
        if not ik:
            continue
        bar, src = _resolve_session_bar(
            ik, session_date, sector_candles.get(ik, []), ux, phase=phase, allow_quote_proxy=allow_proxy
        )
        if bar:
            sector_overrides[ik] = bar
        if src == "mismatch":
            mismatch_keys.append(ik)

    stock_signal_overrides: Dict[str, Tuple[Dict[str, Any], float]] = {}
    anchor_overrides: Dict[str, Dict[str, Any]] = {}
    if allow_proxy and nifty_bar:
        for members in stocks_by_sector.values():
            for m in members:
                sym = str(m.get("stock") or "").upper()
                if sym in stock_signal_overrides:
                    continue
                from backend.services.breakfast_strategy.universe import resolve_stock_instrument

                ref = resolve_stock_instrument(sym, session_date, fut_by_und=fut_by_und, eq_by_symbol=eq_by_symbol)
                if not ref or not ref.instrument_key:
                    continue
                candles = stock_candles_by_key.get(ref.instrument_key, [])
                partial = get_ws_forming_5m_bar(ref.instrument_key, session_date)
                if not partial:
                    partial, _ = _resolve_session_bar(
                        ref.instrument_key,
                        session_date,
                        candles,
                        ux,
                        phase=phase,
                        allow_quote_proxy=True,
                    )
                ov = _stock_signal_override(sym, candles, session_date, partial)
                if ov:
                    stock_signal_overrides[sym] = ov
                ab = anchor_bar(candles, session_date) or _ws_anchor_bar(ref.instrument_key, session_date)
                if ab:
                    anchor_overrides[sym] = ab

    stale = False
    wsq_n = get_ws_quote_for_instrument(NIFTY50_KEY)
    if phase not in ("waiting", "off_session") and not wsq_n:
        stale = True

    sel = select_breakfast_picks(
        session_date,
        nifty_candles=nifty_candles,
        sector_candles=sector_candles,
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

    mismatch = bool(mismatch_keys) or nifty_src == "mismatch"
    state = "mismatch" if mismatch else ("stale" if stale else phase)
    if phase in ("locked", "frozen"):
        state = "locked" if not mismatch and not stale else state

    nifty_pct = bar_move_pct(nifty_bar) if nifty_bar else None
    long_side = sel.long_side if sel else True

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
                    "stocks": [_serialize_stock_pick(s, long_side=long_side, upstox=ux) for s in sp.stocks],
                }
            )
        _resort_sector_stocks(sectors_out, long_side=long_side)

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
            "bias": sel.nifty_bias if sel else ("positive" if (nifty_pct or 0) >= 0 else "negative"),
            "bias_pct": round(sel.nifty_bias_pct, 3) if sel else (round(nifty_pct, 3) if nifty_pct is not None else None),
            "direction": "LONG" if long_side else "SHORT",
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

    if phase in ("locking", "locked", "frozen") and not mismatch:
        with _LOCK:
            _FROZEN_STATE[cache_key] = dict(payload)
            global _LAST_SESSION_STATE
            _LAST_SESSION_STATE = dict(payload)

    if phase in ("locked", "frozen") and sectors_out and not stale:
        cross_status = "mismatched" if mismatch else "matched"
        try:
            stats = persist_live_signals(payload, cross_status)
            if stats.get("inserted"):
                logger.info("breakfast live signals persisted: %s", stats)
        except Exception as e:
            logger.warning("breakfast live signals persist failed: %s", e)

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
