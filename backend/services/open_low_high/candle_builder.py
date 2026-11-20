"""OLH WS 1m candle builder + exchange-aligned 5m aggregator.

Shared path for NSE_EQ / NSE_INDEX / NSE_FO. OI and volume are optional and
never gate persistence or aggregation. Buckets use **exchange** timestamps
(floor to minute), not wall clock.
"""
from __future__ import annotations

import logging
import threading
from dataclasses import dataclass, field
from datetime import datetime, timedelta
from typing import Any, Callable, Dict, List, Optional, Sequence, Tuple

import pytz

logger = logging.getLogger(__name__)
IST = pytz.timezone("Asia/Kolkata")

STATE_IN_PROGRESS = "IN_PROGRESS"
STATE_CLOSED = "CLOSED"
STATE_PARTIAL = "PARTIAL"

TRUST_TRUSTED = "TRUSTED"
TRUST_PARTIAL = "PARTIAL"
TRUST_UNTRUSTED = "UNTRUSTED"


def ensure_ist(ts: datetime) -> datetime:
    if ts.tzinfo is None:
        return IST.localize(ts)
    return ts.astimezone(IST)


def floor_minute(ts: datetime) -> datetime:
    t = ensure_ist(ts)
    return t.replace(second=0, microsecond=0)


def bar_end_1m(minute_start: datetime) -> datetime:
    return floor_minute(minute_start) + timedelta(minutes=1)


def exchange_5m_bucket(ts: datetime) -> datetime:
    """Align to NSE cash session 5m opens: 09:15, 09:20, 09:25, …"""
    t = floor_minute(ts)
    # Minutes since midnight; 5m slots offset so :15/:20/:25… align
    # (09:15 → bucket 09:15; 09:19 → 09:15; 09:20 → 09:20).
    total_min = t.hour * 60 + t.minute
    # Session open 09:15 = 555 minutes from midnight.
    session_open = 9 * 60 + 15
    if total_min < session_open:
        # Pre-open: still bucket by wall 5m from :00 for safety.
        m = (t.minute // 5) * 5
        return t.replace(minute=m, second=0, microsecond=0)
    offset = total_min - session_open
    bucket_min = session_open + (offset // 5) * 5
    return t.replace(hour=bucket_min // 60, minute=bucket_min % 60, second=0, microsecond=0)


def expected_1m_minutes(bucket_start: datetime) -> List[datetime]:
    b = floor_minute(bucket_start)
    return [b + timedelta(minutes=i) for i in range(5)]


@dataclass
class TickEvent:
    instrument_key: str
    ltp: float
    exchange_ts: datetime
    oi: Optional[int] = None
    volume: Optional[int] = None
    bid_depth_qty: Optional[int] = None
    ask_depth_qty: Optional[int] = None
    tbq: Optional[int] = None
    tsq: Optional[int] = None
    candle_source: str = "ltp_tick"
    tick_id: Optional[str] = None  # for de-dupe (ik|epoch|ltp)


@dataclass
class Candle1m:
    instrument_key: str
    minute_start: datetime
    open: float
    high: float
    low: float
    close: float
    oi_open: Optional[int] = None
    oi_high: Optional[int] = None
    oi_low: Optional[int] = None
    oi_close: Optional[int] = None
    volume: Optional[int] = None
    bid_depth_qty: Optional[int] = None
    ask_depth_qty: Optional[int] = None
    tbq: Optional[int] = None
    tsq: Optional[int] = None
    candle_source: str = "ltp_tick"
    tick_count: int = 0
    state: str = STATE_IN_PROGRESS
    first_tick_ts: Optional[datetime] = None
    last_tick_ts: Optional[datetime] = None
    seen_tick_ids: set = field(default_factory=set)
    # Official I1 / market OHLC stored alongside tick-built (cross-check).
    i1_open: Optional[float] = None
    i1_high: Optional[float] = None
    i1_low: Optional[float] = None
    i1_close: Optional[float] = None
    i1_volume: Optional[int] = None

    def to_persist_dict(self) -> Dict[str, Any]:
        return {
            "open": float(self.open),
            "high": float(self.high),
            "low": float(self.low),
            "close": float(self.close),
            "oi_open": self.oi_open,
            "oi_high": self.oi_high,
            "oi_low": self.oi_low,
            "oi_close": self.oi_close,
            "volume": self.volume,
            "bid_depth_qty": self.bid_depth_qty,
            "ask_depth_qty": self.ask_depth_qty,
            "tbq": self.tbq,
            "tsq": self.tsq,
            "candle_source": self.candle_source,
            "state": self.state,
            "tick_count": self.tick_count,
            "i1_open": self.i1_open,
            "i1_high": self.i1_high,
            "i1_low": self.i1_low,
            "i1_close": self.i1_close,
            "i1_volume": self.i1_volume,
        }


@dataclass
class Candle5mResult:
    bucket_start: datetime
    open: float
    high: float
    low: float
    close: float
    volume: Optional[float]
    state: str  # TRUSTED / PARTIAL / UNTRUSTED
    missing_minutes: List[datetime]
    present_minutes: List[datetime]
    source: str = "ws"


ClosedBarCallback = Callable[[Candle1m], None]


class MinuteCandleBuilder:
    """In-memory 1m OHLC from LTP ticks; emits only CLOSED bars (bar_end ≤ now)."""

    def __init__(
        self,
        *,
        on_closed: Optional[ClosedBarCallback] = None,
        now_fn: Optional[Callable[[], datetime]] = None,
    ) -> None:
        self._lock = threading.RLock()
        self._state: Dict[Tuple[str, datetime], Candle1m] = {}
        self._on_closed = on_closed
        self._now_fn = now_fn or (lambda: datetime.now(IST))
        self._emitted: set = set()  # (ik, minute_start) already emitted closed

    def reset(self) -> None:
        with self._lock:
            self._state.clear()
            self._emitted.clear()

    def get(self, instrument_key: str, minute_start: datetime) -> Optional[Candle1m]:
        k = (instrument_key, floor_minute(minute_start))
        with self._lock:
            c = self._state.get(k)
            return None if c is None else c

    def mark_partial_on_reconnect(self, instrument_keys: Optional[Sequence[str]] = None) -> List[Candle1m]:
        """Mark in-progress 1m bars PARTIAL after a socket drop (resume continues)."""
        touched: List[Candle1m] = []
        with self._lock:
            for (ik, _ms), c in self._state.items():
                if instrument_keys is not None and ik not in instrument_keys:
                    continue
                if c.state == STATE_IN_PROGRESS:
                    c.state = STATE_PARTIAL
                    touched.append(c)
        for c in touched:
            logger.warning(
                "olh candle_builder reconnect PARTIAL ik=%s minute=%s ticks=%s",
                c.instrument_key,
                c.minute_start.isoformat(),
                c.tick_count,
            )
        return touched

    def ingest_tick(self, tick: TickEvent) -> Optional[Candle1m]:
        """Apply one tick. Returns updated candle (may still be IN_PROGRESS)."""
        if tick.ltp is None or float(tick.ltp) <= 0:
            return None
        ltp = float(tick.ltp)
        ts = ensure_ist(tick.exchange_ts)
        minute = floor_minute(ts)
        ik = str(tick.instrument_key or "").strip()
        if not ik:
            return None

        tick_id = tick.tick_id
        if tick_id is None:
            tick_id = f"{ik}|{ts.isoformat()}|{ltp:.8f}|{tick.oi}|{tick.volume}"

        with self._lock:
            k = (ik, minute)
            c = self._state.get(k)
            if c is not None and tick_id in c.seen_tick_ids:
                return c  # duplicate — ignore

            if c is None:
                c = Candle1m(
                    instrument_key=ik,
                    minute_start=minute,
                    open=ltp,
                    high=ltp,
                    low=ltp,
                    close=ltp,
                    oi_open=tick.oi,
                    oi_high=tick.oi,
                    oi_low=tick.oi,
                    oi_close=tick.oi,
                    volume=tick.volume,
                    bid_depth_qty=tick.bid_depth_qty,
                    ask_depth_qty=tick.ask_depth_qty,
                    tbq=tick.tbq,
                    tsq=tick.tsq,
                    candle_source=tick.candle_source or "ltp_tick",
                    tick_count=1,
                    state=STATE_IN_PROGRESS,
                    first_tick_ts=ts,
                    last_tick_ts=ts,
                )
                c.seen_tick_ids.add(tick_id)
                self._state[k] = c
            else:
                # Out-of-order: still update H/L/C; open stays first-seen by exchange order.
                if c.first_tick_ts is None or ts < c.first_tick_ts:
                    c.open = ltp
                    c.first_tick_ts = ts
                    if tick.oi is not None:
                        c.oi_open = tick.oi
                if c.last_tick_ts is None or ts >= c.last_tick_ts:
                    c.close = ltp
                    c.last_tick_ts = ts
                    if tick.oi is not None:
                        c.oi_close = tick.oi
                c.high = max(float(c.high), ltp)
                c.low = min(float(c.low), ltp)
                if tick.oi is not None:
                    c.oi_high = tick.oi if c.oi_high is None else max(int(c.oi_high), int(tick.oi))
                    c.oi_low = tick.oi if c.oi_low is None else min(int(c.oi_low), int(tick.oi))
                    if c.oi_open is None:
                        c.oi_open = tick.oi
                    if c.oi_close is None or ts >= (c.last_tick_ts or ts):
                        c.oi_close = tick.oi
                if tick.volume is not None:
                    prev = c.volume
                    c.volume = int(tick.volume) if prev is None else max(int(prev), int(tick.volume))
                if tick.bid_depth_qty is not None:
                    c.bid_depth_qty = int(tick.bid_depth_qty)
                if tick.ask_depth_qty is not None:
                    c.ask_depth_qty = int(tick.ask_depth_qty)
                if tick.tbq is not None:
                    c.tbq = int(tick.tbq)
                if tick.tsq is not None:
                    c.tsq = int(tick.tsq)
                if tick.candle_source == "i1":
                    c.candle_source = "i1"
                c.tick_count = int(c.tick_count) + 1
                c.seen_tick_ids.add(tick_id)
                # Cap de-dupe set
                if len(c.seen_tick_ids) > 2000:
                    c.seen_tick_ids = set(list(c.seen_tick_ids)[-500:])

            self._maybe_close_older(ik, minute)
            self._maybe_close_if_ended(c)
            return c

    def attach_i1(
        self,
        instrument_key: str,
        minute_start: datetime,
        *,
        open_: float,
        high: float,
        low: float,
        close: float,
        volume: Optional[int] = None,
    ) -> Optional[Candle1m]:
        """Store official I1 OHLC alongside tick-built candle (does not overwrite OHL)."""
        minute = floor_minute(minute_start)
        ik = str(instrument_key or "").strip()
        with self._lock:
            k = (ik, minute)
            c = self._state.get(k)
            if c is None:
                # Seed from I1 so we still have a bar if ticks were missed.
                c = Candle1m(
                    instrument_key=ik,
                    minute_start=minute,
                    open=float(open_),
                    high=float(high),
                    low=float(low),
                    close=float(close),
                    volume=volume,
                    candle_source="i1",
                    tick_count=0,
                    state=STATE_IN_PROGRESS,
                )
                self._state[k] = c
            c.i1_open = float(open_)
            c.i1_high = float(high)
            c.i1_low = float(low)
            c.i1_close = float(close)
            if volume is not None:
                c.i1_volume = int(volume)
            self._maybe_close_if_ended(c)
            return c

    def flush_closed(self, now: Optional[datetime] = None) -> List[Candle1m]:
        """Close and emit any bars whose bar_end ≤ now."""
        now = ensure_ist(now or self._now_fn())
        closed: List[Candle1m] = []
        with self._lock:
            for c in list(self._state.values()):
                if bar_end_1m(c.minute_start) <= now and c.state != STATE_CLOSED:
                    was = c.state
                    c.state = STATE_CLOSED if was != STATE_PARTIAL else STATE_PARTIAL
                    # PARTIAL stays PARTIAL when closing after reconnect.
                    if was == STATE_PARTIAL:
                        c.state = STATE_PARTIAL
                    else:
                        c.state = STATE_CLOSED
                    closed.append(c)
        for c in closed:
            self._emit_closed(c)
        return closed

    def _maybe_close_older(self, ik: str, current_minute: datetime) -> None:
        now = ensure_ist(self._now_fn())
        for (k_ik, ms), c in list(self._state.items()):
            if k_ik != ik:
                continue
            if ms >= current_minute:
                continue
            if bar_end_1m(ms) <= now and c.state != STATE_CLOSED:
                if c.state != STATE_PARTIAL:
                    c.state = STATE_CLOSED
                self._emit_closed(c)

    def _maybe_close_if_ended(self, c: Candle1m) -> None:
        now = ensure_ist(self._now_fn())
        if bar_end_1m(c.minute_start) <= now and c.state == STATE_IN_PROGRESS:
            c.state = STATE_CLOSED
            self._emit_closed(c)

    def _emit_closed(self, c: Candle1m) -> None:
        key = (c.instrument_key, c.minute_start)
        if key in self._emitted:
            # Re-emit allowed for OOO updates after close — still call persist callback.
            pass
        self._emitted.add(key)
        if self._on_closed is not None:
            try:
                self._on_closed(c)
            except Exception:
                logger.exception(
                    "olh candle_builder on_closed failed ik=%s minute=%s",
                    c.instrument_key,
                    c.minute_start.isoformat(),
                )


def aggregate_5m_from_1m(
    candles_1m: Sequence[Candle1m] | Sequence[Dict[str, Any]],
    *,
    bucket_start: Optional[datetime] = None,
) -> List[Candle5mResult]:
    """Aggregate 1m → exchange-aligned 5m with TRUSTED / PARTIAL / UNTRUSTED.

    COMPLETE (TRUSTED) only if all five 1m present.
    Else PARTIAL + missing-minute list.
    If the first minute of the bucket is missing → UNTRUSTED.
    """
    by_minute: Dict[datetime, Dict[str, Any]] = {}
    for raw in candles_1m:
        if isinstance(raw, Candle1m):
            ms = floor_minute(raw.minute_start)
            row = {
                "open": raw.open,
                "high": raw.high,
                "low": raw.low,
                "close": raw.close,
                "volume": raw.volume,
            }
        else:
            ts = raw.get("minute_start") or raw.get("timestamp") or raw.get("candle_time")
            if isinstance(ts, str):
                ts = datetime.fromisoformat(ts.replace("Z", "+00:00"))
            if not isinstance(ts, datetime):
                continue
            ms = floor_minute(ts)
            try:
                row = {
                    "open": float(raw["open"]),
                    "high": float(raw["high"]),
                    "low": float(raw["low"]),
                    "close": float(raw["close"]),
                    "volume": raw.get("volume"),
                }
            except (KeyError, TypeError, ValueError):
                continue
        by_minute[ms] = row

    if not by_minute:
        return []

    if bucket_start is not None:
        buckets = [floor_minute(bucket_start)]
    else:
        buckets = sorted({exchange_5m_bucket(ms) for ms in by_minute})

    out: List[Candle5mResult] = []
    for bucket in buckets:
        needed = expected_1m_minutes(bucket)
        present = [m for m in needed if m in by_minute]
        missing = [m for m in needed if m not in by_minute]
        first_missing = needed[0] not in by_minute
        if not present:
            out.append(
                Candle5mResult(
                    bucket_start=bucket,
                    open=0.0,
                    high=0.0,
                    low=0.0,
                    close=0.0,
                    volume=None,
                    state=TRUST_UNTRUSTED,
                    missing_minutes=missing,
                    present_minutes=[],
                )
            )
            continue
        ordered = sorted(present)
        first = by_minute[ordered[0]]
        last = by_minute[ordered[-1]]
        highs = [by_minute[m]["high"] for m in ordered]
        lows = [by_minute[m]["low"] for m in ordered]
        vol_sum = 0.0
        any_vol = False
        for m in ordered:
            v = by_minute[m].get("volume")
            if v is not None:
                any_vol = True
                vol_sum += float(v)
        if first_missing:
            state = TRUST_UNTRUSTED
        elif missing:
            state = TRUST_PARTIAL
        else:
            state = TRUST_TRUSTED
        # Prefer open from the *bucket's* first minute when present.
        if needed[0] in by_minute:
            open_px = float(by_minute[needed[0]]["open"])
        else:
            open_px = float(first["open"])
        # Prefer close from the last expected minute when present.
        if needed[-1] in by_minute:
            close_px = float(by_minute[needed[-1]]["close"])
        else:
            close_px = float(last["close"])
        out.append(
            Candle5mResult(
                bucket_start=bucket,
                open=open_px,
                high=max(highs),
                low=min(lows),
                close=close_px,
                volume=vol_sum if any_vol else None,
                state=state,
                missing_minutes=missing,
                present_minutes=ordered,
            )
        )
    return out


def compare_official_open(
    tick_built: Dict[str, Any],
    i1: Dict[str, Any],
    *,
    tolerance_pct: float = 0.05,
    label: str = "",
) -> Dict[str, Any]:
    """Compare tick-built vs I1/market 09:15 O/H/L; log when beyond tolerance.

    ``tolerance_pct`` is percent of price (default 0.05% = 5 bps).
    """
    mismatches: List[str] = []
    details: Dict[str, Any] = {"label": label, "ok": True, "mismatches": mismatches}

    def _pct(a: float, b: float) -> float:
        base = max(abs(b), abs(a), 1e-9)
        return abs(a - b) / base * 100.0

    for field_name in ("open", "high", "low"):
        try:
            a = float(tick_built[field_name])
            b = float(i1[field_name])
        except (KeyError, TypeError, ValueError):
            continue
        pct = _pct(a, b)
        details[f"{field_name}_tick"] = a
        details[f"{field_name}_i1"] = b
        details[f"{field_name}_pct"] = round(pct, 6)
        if pct > float(tolerance_pct):
            mismatches.append(field_name)

    details["ok"] = len(mismatches) == 0
    if mismatches:
        logger.warning(
            "olh official_open mismatch label=%s fields=%s detail=%s",
            label or "-",
            ",".join(mismatches),
            details,
        )
    else:
        logger.info(
            "olh official_open match label=%s open_tick=%s open_i1=%s",
            label or "-",
            details.get("open_tick"),
            details.get("open_i1"),
        )
    return details


def compare_ws_vs_rest_0915(
    ws_bar: Dict[str, Any],
    rest_bar: Dict[str, Any],
    *,
    symbol: str = "",
    tolerance_pct: float = 0.05,
) -> Dict[str, Any]:
    """Compare WS-built 09:15 5m vs REST for reporting / scheduled hook."""
    return compare_official_open(
        ws_bar,
        rest_bar,
        tolerance_pct=tolerance_pct,
        label=f"ws_vs_rest_0915:{symbol}",
    )


# Process-wide builder used by the Upstox feed (EQ+INDEX+FUT).
_GLOBAL_BUILDER: Optional[MinuteCandleBuilder] = None
_GLOBAL_LOCK = threading.Lock()


def get_global_builder() -> MinuteCandleBuilder:
    global _GLOBAL_BUILDER
    with _GLOBAL_LOCK:
        if _GLOBAL_BUILDER is None:
            _GLOBAL_BUILDER = MinuteCandleBuilder()
        return _GLOBAL_BUILDER


def set_global_on_closed(cb: Optional[ClosedBarCallback]) -> None:
    get_global_builder()._on_closed = cb
