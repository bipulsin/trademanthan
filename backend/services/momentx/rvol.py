"""Relative volume + post-filter excursion metrics for MomentX Stage-1 backtest.

Informational only — never used to filter or rank Stage-1 qualifiers.

Volume series: futures 5-minute candles (preferred; falls back to 15m if needed).
Day vol / Vol SMA5 on the backtest table remain cash equity and are unchanged.

Contract-roll method (baseline sessions):
  For each prior session D, resolve the front-month and next-month stock futures
  that were live on D (same roll-window rules as arbitrage BOD setup, evaluated
  as-of D). Sum their session volumes up to clock T. This avoids using only the
  *current* front contract's thin next-month history after an expiry roll.
  The trade-day numerator uses the row's front-month contract only (the one
  Stage-1 stored on ``future_instrument_key``).
"""
from __future__ import annotations

import logging
from dataclasses import dataclass, field
from datetime import date, datetime, time as dt_time, timedelta
from typing import Any, Dict, Iterable, List, Optional, Sequence, Tuple

import pytz

from backend.services.momentx.indicators import wilder_atr

logger = logging.getLogger(__name__)
IST = pytz.timezone("Asia/Kolkata")

SESSION_OPEN = dt_time(9, 15)
SESSION_CLOSE = dt_time(15, 30)
DEFAULT_RVOL_SESSIONS = 5
MIN_SESSIONS_FOR_RVOL = 3
ATR_PERIOD = 14
BAR_MINUTES_5 = 5
BAR_MINUTES_15 = 15

RVOL_FIELDS = (
    "rvol_cum",
    "rvol_slot",
    "cum_vol_at_T",
    "baseline_cum_vol",
    "n_sessions_used",
    "atr5_at_T",
    "px_at_T",
    "ret_30m",
    "ret_60m",
    "ret_120m",
    "eod_ret",
    "mfe_atr",
    "mae_atr",
)


@dataclass
class CandleCache:
    """In-memory cache of intraday bars keyed by (instrument_key, interval)."""

    _bars: Dict[Tuple[str, str], List[Dict[str, Any]]] = field(default_factory=dict)

    def get(self, instrument_key: str, interval: str) -> Optional[List[Dict[str, Any]]]:
        return self._bars.get((instrument_key, interval))

    def put(self, instrument_key: str, interval: str, rows: Sequence[Dict[str, Any]]) -> None:
        # Store the list object itself so ensure()/get() share one cache entry.
        self._bars[(instrument_key, interval)] = rows if isinstance(rows, list) else list(rows or [])

    def ensure(
        self,
        instrument_key: str,
        interval: str,
        fetcher,
    ) -> List[Dict[str, Any]]:
        ik = (instrument_key or "").strip()
        if not ik:
            return []
        hit = self.get(ik, interval)
        if hit is not None:
            return hit
        rows = list(fetcher(ik, interval) or [])
        self.put(ik, interval, rows)
        return rows


def _parse_ts(ts: Any) -> Optional[datetime]:
    if ts is None:
        return None
    if isinstance(ts, datetime):
        dt = ts
    else:
        try:
            from backend.services.upstox_service import _parse_ts_to_aware_ist

            dt = _parse_ts_to_aware_ist(ts)
        except Exception:
            dt = None
    if dt is None:
        return None
    if dt.tzinfo is None:
        return IST.localize(dt)
    return dt.astimezone(IST)


def bar_end(ts: datetime, bar_minutes: int) -> datetime:
    return ts + timedelta(minutes=int(bar_minutes))


def clock_on(d: date, hhmm: Tuple[int, int]) -> datetime:
    return IST.localize(datetime.combine(d, dt_time(int(hhmm[0]), int(hhmm[1]), 0)))


def parse_T_from_first_filtered_at(first_filtered_at: Any) -> Optional[Tuple[int, int]]:
    """T is the close clock of the 15m bar when Stage-1 first qualified."""
    dt = _parse_ts(first_filtered_at)
    if dt is None:
        return None
    return dt.hour, dt.minute


def session_bars(
    candles: Sequence[Dict[str, Any]],
    session: date,
    *,
    bar_minutes: int,
    through_T: Optional[Tuple[int, int]] = None,
) -> List[Dict[str, Any]]:
    """Bars whose start is on ``session`` from 09:15, optionally through close at T."""
    open_dt = clock_on(session, (SESSION_OPEN.hour, SESSION_OPEN.minute))
    close_dt = clock_on(session, (SESSION_CLOSE.hour, SESSION_CLOSE.minute))
    t_end = clock_on(session, through_T) if through_T else close_dt
    out: List[Dict[str, Any]] = []
    for c in candles or []:
        ts = _parse_ts(c.get("timestamp"))
        if ts is None or ts.date() != session:
            continue
        if ts < open_dt:
            continue
        end = bar_end(ts, bar_minutes)
        if end > t_end:
            continue
        if end > close_dt:
            continue
        out.append(c)
    return out


def cum_volume(bars: Sequence[Dict[str, Any]]) -> float:
    return float(sum(float(c.get("volume") or 0.0) for c in bars))


def slot_volume(bars: Sequence[Dict[str, Any]], *, bar_minutes: int, T: Tuple[int, int]) -> Optional[float]:
    """Volume of the single bar that closes exactly at T."""
    if not bars:
        return None
    target_h, target_m = T
    for c in bars:
        ts = _parse_ts(c.get("timestamp"))
        if ts is None:
            continue
        end = bar_end(ts, bar_minutes)
        if end.hour == target_h and end.minute == target_m:
            return float(c.get("volume") or 0.0)
    return None


def session_complete_through_T(
    bars: Sequence[Dict[str, Any]],
    session: date,
    T: Tuple[int, int],
    *,
    bar_minutes: int,
) -> bool:
    """True when the session has a continuous grid of bars from open through T."""
    open_dt = clock_on(session, (SESSION_OPEN.hour, SESSION_OPEN.minute))
    t_end = clock_on(session, T)
    if t_end <= open_dt:
        return False
    expected = int((t_end - open_dt).total_seconds() // (bar_minutes * 60))
    if expected <= 0:
        return False
    if len(bars) < expected:
        return False
    # Require last bar closes at T.
    last_ts = _parse_ts(bars[-1].get("timestamp"))
    if last_ts is None:
        return False
    return bar_end(last_ts, bar_minutes) == t_end


def sum_session_volume(
    candle_lists: Sequence[Sequence[Dict[str, Any]]],
    session: date,
    T: Tuple[int, int],
    *,
    bar_minutes: int,
) -> Optional[float]:
    """Sum volumes across contracts for session through T; None if incomplete."""
    if not candle_lists:
        return None
    total = 0.0
    any_complete = False
    for candles in candle_lists:
        bars = session_bars(candles, session, bar_minutes=bar_minutes, through_T=T)
        if session_complete_through_T(bars, session, T, bar_minutes=bar_minutes):
            any_complete = True
            total += cum_volume(bars)
        elif bars:
            # Partial from one contract still counts if another is complete? Prefer strict:
            # require at least one complete series; add whatever bars exist from others.
            total += cum_volume(bars)
    return total if any_complete else None


def sum_slot_volume(
    candle_lists: Sequence[Sequence[Dict[str, Any]]],
    session: date,
    T: Tuple[int, int],
    *,
    bar_minutes: int,
) -> Optional[float]:
    if not candle_lists:
        return None
    total = 0.0
    found = False
    for candles in candle_lists:
        bars = session_bars(candles, session, bar_minutes=bar_minutes, through_T=T)
        sv = slot_volume(bars, bar_minutes=bar_minutes, T=T)
        if sv is not None:
            found = True
            total += sv
    return total if found else None


def prior_sessions(
    trade_date: date,
    *,
    n: int,
    holidays: Optional[Iterable[date]] = None,
) -> List[date]:
    """Previous N full weekdays not in holidays, walking backward from trade_date."""
    hol = set(holidays or [])
    out: List[date] = []
    d = trade_date - timedelta(days=1)
    guard = 0
    while len(out) < int(n) and guard < 60:
        guard += 1
        if d.weekday() < 5 and d not in hol:
            out.append(d)
        d -= timedelta(days=1)
    return list(reversed(out))


def _pct_ret(px0: float, px1: Optional[float]) -> Optional[float]:
    if px1 is None or px0 == 0:
        return None
    return ((float(px1) - float(px0)) / float(px0)) * 100.0


def _close_at_or_after(
    candles: Sequence[Dict[str, Any]],
    session: date,
    target: datetime,
    *,
    bar_minutes: int,
) -> Optional[float]:
    """Close of the bar whose end is >= target, preferring exact end match."""
    best = None
    for c in candles:
        ts = _parse_ts(c.get("timestamp"))
        if ts is None or ts.date() != session:
            continue
        end = bar_end(ts, bar_minutes)
        if end < target:
            continue
        if end == target:
            return float(c.get("close") or 0.0) or None
        if best is None or end < best[0]:
            best = (end, float(c.get("close") or 0.0))
    if best is None:
        return None
    return best[1] if best[1] else None


def _last_session_close(
    candles: Sequence[Dict[str, Any]],
    session: date,
    *,
    bar_minutes: int,
) -> Optional[float]:
    bars = session_bars(candles, session, bar_minutes=bar_minutes, through_T=None)
    if not bars:
        return None
    return float(bars[-1].get("close") or 0.0) or None


def excursion_atr(
    candles: Sequence[Dict[str, Any]],
    session: date,
    T: Tuple[int, int],
    *,
    bar_minutes: int,
    px_at_T: float,
    atr: float,
    side: str,
) -> Tuple[Optional[float], Optional[float]]:
    """MFE/MAE after T through EOD in ATR units (side-aware)."""
    if atr is None or atr <= 0 or px_at_T is None:
        return None, None
    t_end = clock_on(session, T)
    close_dt = clock_on(session, (SESSION_CLOSE.hour, SESSION_CLOSE.minute))
    highs: List[float] = []
    lows: List[float] = []
    for c in candles:
        ts = _parse_ts(c.get("timestamp"))
        if ts is None or ts.date() != session:
            continue
        end = bar_end(ts, bar_minutes)
        if end <= t_end or end > close_dt:
            continue
        highs.append(float(c.get("high") or 0.0))
        lows.append(float(c.get("low") or 0.0))
    if not highs:
        return 0.0, 0.0
    hh = max(highs)
    ll = min(lows)
    bullish = str(side or "bullish").lower() != "bearish"
    if bullish:
        mfe = (hh - px_at_T) / atr
        mae = (ll - px_at_T) / atr  # negative when adverse
    else:
        mfe = (px_at_T - ll) / atr
        mae = (px_at_T - hh) / atr  # negative when adverse (price rose)
    return float(mfe), float(mae)


def compute_rvol_metrics(
    *,
    trade_date: date,
    T: Tuple[int, int],
    side: str,
    trade_day_candles: Sequence[Dict[str, Any]],
    prior_session_candle_lists: Sequence[Tuple[date, Sequence[Sequence[Dict[str, Any]]]]],
    bar_minutes: int = BAR_MINUTES_5,
    atr_period: int = ATR_PERIOD,
    min_sessions: int = MIN_SESSIONS_FOR_RVOL,
) -> Dict[str, Any]:
    """Compute informational RVOL + excursion fields for one backtest row.

    ``prior_session_candle_lists``: list of (session_date, [candle_series_per_contract]).
    Only sessions that are complete through T are included in the baseline.
    """
    empty = {k: None for k in RVOL_FIELDS}
    empty["n_sessions_used"] = 0

    today_bars = session_bars(
        trade_day_candles, trade_date, bar_minutes=bar_minutes, through_T=T
    )
    if not session_complete_through_T(today_bars, trade_date, T, bar_minutes=bar_minutes):
        return empty

    cum_today = cum_volume(today_bars)
    slot_today = slot_volume(today_bars, bar_minutes=bar_minutes, T=T)

    prior_cums: List[float] = []
    prior_slots: List[float] = []
    for sess, series_list in prior_session_candle_lists:
        cv = sum_session_volume(series_list, sess, T, bar_minutes=bar_minutes)
        if cv is None:
            continue
        prior_cums.append(cv)
        sv = sum_slot_volume(series_list, sess, T, bar_minutes=bar_minutes)
        if sv is not None:
            prior_slots.append(sv)

    n_used = len(prior_cums)
    baseline = (sum(prior_cums) / float(n_used)) if n_used else None
    rvol_cum = None
    rvol_slot = None
    if n_used >= int(min_sessions) and baseline and baseline > 0:
        rvol_cum = cum_today / baseline
        if prior_slots and slot_today is not None:
            slot_base = sum(prior_slots) / float(len(prior_slots))
            if slot_base > 0:
                rvol_slot = float(slot_today) / slot_base

    # Price / ATR / returns on the trade-day front-month series.
    full_day = session_bars(trade_day_candles, trade_date, bar_minutes=bar_minutes)
    # Warm ATR with prior bars from the same series (multi-session).
    warm: List[Dict[str, Any]] = []
    for c in trade_day_candles:
        ts = _parse_ts(c.get("timestamp"))
        if ts is None:
            continue
        end = bar_end(ts, bar_minutes)
        if end <= clock_on(trade_date, T):
            warm.append(c)
    highs = [float(c.get("high") or 0.0) for c in warm]
    lows = [float(c.get("low") or 0.0) for c in warm]
    closes = [float(c.get("close") or 0.0) for c in warm]
    atr = wilder_atr(highs, lows, closes, atr_period)

    px_at_T = float(today_bars[-1].get("close") or 0.0) if today_bars else None
    if px_at_T == 0:
        px_at_T = None

    ret_30m = ret_60m = ret_120m = eod_ret = None
    mfe = mae = None
    if px_at_T is not None:
        t0 = clock_on(trade_date, T)
        close_dt = clock_on(trade_date, (SESSION_CLOSE.hour, SESSION_CLOSE.minute))

        def _ret_after(minutes: int) -> Optional[float]:
            tgt = t0 + timedelta(minutes=minutes)
            if tgt > close_dt:
                return None
            px = _close_at_or_after(trade_day_candles, trade_date, tgt, bar_minutes=bar_minutes)
            return _pct_ret(px_at_T, px)

        ret_30m = _ret_after(30)
        ret_60m = _ret_after(60)
        ret_120m = _ret_after(120)
        eod_px = _last_session_close(trade_day_candles, trade_date, bar_minutes=bar_minutes)
        eod_ret = _pct_ret(px_at_T, eod_px)
        if atr is not None and atr > 0:
            mfe, mae = excursion_atr(
                trade_day_candles,
                trade_date,
                T,
                bar_minutes=bar_minutes,
                px_at_T=px_at_T,
                atr=atr,
                side=side,
            )

    return {
        "rvol_cum": None if rvol_cum is None else round(float(rvol_cum), 4),
        "rvol_slot": None if rvol_slot is None else round(float(rvol_slot), 4),
        "cum_vol_at_T": float(cum_today),
        "baseline_cum_vol": None if baseline is None else float(baseline),
        "n_sessions_used": int(n_used),
        "atr5_at_T": None if atr is None else round(float(atr), 4),
        "px_at_T": None if px_at_T is None else float(px_at_T),
        "ret_30m": None if ret_30m is None else round(float(ret_30m), 4),
        "ret_60m": None if ret_60m is None else round(float(ret_60m), 4),
        "ret_120m": None if ret_120m is None else round(float(ret_120m), 4),
        "eod_ret": None if eod_ret is None else round(float(eod_ret), 4),
        "mfe_atr": None if mfe is None else round(float(mfe), 4),
        "mae_atr": None if mae is None else round(float(mae), 4),
    }


# ---------------------------------------------------------------------------
# Historical front / next futures resolution (contract roll)
# ---------------------------------------------------------------------------

_FUT_MAP_CACHE: Optional[Dict[str, List[Dict[str, Any]]]] = None


def _fut_map() -> Dict[str, List[Dict[str, Any]]]:
    global _FUT_MAP_CACHE
    if _FUT_MAP_CACHE is not None:
        return _FUT_MAP_CACHE
    try:
        from backend.services.arbitrage_daily_setup_scheduler import (
            _build_mappings,
            _load_instruments,
        )

        _eq, fut = _build_mappings(_load_instruments())
        _FUT_MAP_CACHE = fut
        return fut
    except Exception as e:
        logger.info("momentx rvol: instruments load failed: %s", e)
        _FUT_MAP_CACHE = {}
        return _FUT_MAP_CACHE


def clear_fut_map_cache() -> None:
    global _FUT_MAP_CACHE
    _FUT_MAP_CACHE = None


def _expiry_date(contract: Dict[str, Any]) -> Optional[date]:
    exp = contract.get("expiry")
    if exp is None:
        return None
    try:
        ms = int(exp)
        return datetime.fromtimestamp(ms / 1000.0, tz=IST).date()
    except Exception:
        return None


def pick_front_next_for_session(
    stock: str,
    session: date,
    *,
    contracts: Optional[Sequence[Dict[str, Any]]] = None,
    apply_roll_window: bool = True,
) -> Tuple[Optional[str], Optional[str]]:
    """Return (front_instrument_key, next_instrument_key) live on ``session``.

    Uses the same roll-window rule as arbitrage BOD (after the 20th of the front
    expiry month, skip the near-expiry contract), evaluated as-of ``session``.
    """
    from backend.services.arbitrage_daily_setup_scheduler import (
        _futures_roll_window_ist,
    )

    pool = list(contracts) if contracts is not None else list(_fut_map().get(str(stock).upper(), []))
    if not pool:
        return None, None
    upcoming = []
    for c in sorted(pool, key=lambda x: x.get("expiry") or 0):
        ed = _expiry_date(c)
        if ed is None:
            continue
        if ed >= session:
            upcoming.append(c)
    if not upcoming:
        return None, None

    asof_dt = IST.localize(datetime.combine(session, dt_time(9, 15)))
    first = upcoming[0]
    if apply_roll_window and _futures_roll_window_ist(asof_dt, first):
        if len(upcoming) < 2:
            return None, None
        cur = upcoming[1]
        nxt = upcoming[2] if len(upcoming) >= 3 else None
    else:
        cur = upcoming[0]
        nxt = upcoming[1] if len(upcoming) >= 2 else None

    def _key(c: Optional[Dict[str, Any]]) -> Optional[str]:
        if not c:
            return None
        return (c.get("instrument_key") or None)

    return _key(cur), _key(nxt)


def baseline_contract_keys(
    stock: str,
    session: date,
    *,
    trade_front_key: Optional[str] = None,
    contracts: Optional[Sequence[Dict[str, Any]]] = None,
) -> List[str]:
    """Keys whose volumes are summed for a baseline session (front + next)."""
    front, nxt = pick_front_next_for_session(stock, session, contracts=contracts)
    keys: List[str] = []
    for k in (front, nxt):
        if k and k not in keys:
            keys.append(k)
    if not keys and trade_front_key:
        keys.append(trade_front_key)
    return keys


def enrich_rvol_for_row(
    row: Dict[str, Any],
    *,
    cache: CandleCache,
    fetch_5m,
    holidays: Optional[Iterable[date]] = None,
    rvol_sessions: int = DEFAULT_RVOL_SESSIONS,
    bar_minutes: int = BAR_MINUTES_5,
    contracts_by_stock: Optional[Dict[str, Sequence[Dict[str, Any]]]] = None,
) -> Dict[str, Any]:
    """Attach RVOL fields onto a qualified backtest row (mutates and returns row)."""
    empty = {k: None for k in RVOL_FIELDS}
    empty["n_sessions_used"] = 0

    trade_date = row.get("trade_date")
    if isinstance(trade_date, str):
        try:
            trade_date = date.fromisoformat(trade_date[:10])
        except ValueError:
            trade_date = None
    if not isinstance(trade_date, date):
        row.update(empty)
        return row

    T = parse_T_from_first_filtered_at(row.get("first_filtered_at"))
    if T is None:
        row.update(empty)
        return row

    stock = str(row.get("stock") or "").upper()
    front_key = str(row.get("future_instrument_key") or "").strip()
    if not front_key:
        row.update(empty)
        return row

    interval = f"minutes/{int(bar_minutes)}"
    trade_candles = cache.ensure(front_key, interval, fetch_5m)

    # Look back enough weekdays for N baseline sessions (+ buffer for holidays).
    look_n = max(int(rvol_sessions) * 3, 10)
    candidates = prior_sessions(trade_date, n=look_n, holidays=holidays)
    prior_lists: List[Tuple[date, List[Sequence[Dict[str, Any]]]]] = []
    used = 0
    for sess in reversed(candidates):  # newest first; collect up to rvol_sessions complete
        if used >= int(rvol_sessions):
            break
        cpool = None
        if contracts_by_stock is not None:
            cpool = contracts_by_stock.get(stock)
        keys = baseline_contract_keys(
            stock, sess, trade_front_key=front_key, contracts=cpool
        )
        series: List[Sequence[Dict[str, Any]]] = []
        for k in keys:
            series.append(cache.ensure(k, interval, fetch_5m))
        cv = sum_session_volume(series, sess, T, bar_minutes=bar_minutes)
        if cv is None:
            continue
        prior_lists.append((sess, series))
        used += 1
    prior_lists.reverse()  # chronological

    metrics = compute_rvol_metrics(
        trade_date=trade_date,
        T=T,
        side=str(row.get("side") or "bullish"),
        trade_day_candles=trade_candles,
        prior_session_candle_lists=prior_lists,
        bar_minutes=bar_minutes,
        min_sessions=MIN_SESSIONS_FOR_RVOL,
    )
    row.update(metrics)
    return row
