"""One-session HA-VWAP simulation: max 2 concurrent, top-2 volume entries."""
from __future__ import annotations

import logging
from dataclasses import dataclass
from datetime import date, time
from typing import Any, Dict, List

import pytz

from backend.services.ha_vwap.config import (
    FORCE_EXIT_TIME,
    MAX_CONCURRENT,
    SIGNAL_FROM,
    SIGNAL_TO,
    SLIPPAGE,
    TOP_N_BY_VOLUME,
    TP_PCT,
)
from backend.services.ha_vwap.indicators import crossed_above

logger = logging.getLogger(__name__)
IST = pytz.timezone("Asia/Kolkata")


def _t(bar: Dict[str, Any]) -> time:
    start = bar.get("bar_start")
    if start is None:
        return time(0, 0)
    if hasattr(start, "astimezone"):
        start = start.astimezone(IST)
    return time(int(start.hour), int(start.minute))


def in_signal_window(bar: Dict[str, Any]) -> bool:
    tm = _t(bar)
    return SIGNAL_FROM <= tm <= SIGNAL_TO


def is_eod_bar(bar: Dict[str, Any]) -> bool:
    return _t(bar) >= FORCE_EXIT_TIME


@dataclass
class OpenPos:
    symbol: str
    instrument_key: str
    instrument: str
    qty: int
    entry: float
    tp: float
    entry_time: str
    volume: float
    date: str


def annotate_bars(bars: List[Dict[str, Any]], ha_c, vwap, ema20, macd_h) -> List[Dict[str, Any]]:
    out = []
    for i, b in enumerate(bars):
        row = dict(b)
        row["ha_close"] = ha_c[i]
        row["vwap"] = vwap[i]
        row["ema20"] = ema20[i]
        row["macd_hist"] = macd_h[i]
        out.append(row)
    return out


def is_signal(prev: Dict[str, Any], cur: Dict[str, Any]) -> bool:
    if not in_signal_window(cur):
        return False
    if not crossed_above(prev["ha_close"], prev["vwap"], cur["ha_close"], cur["vwap"]):
        return False
    if float(cur["ha_close"]) <= float(cur["ema20"]):
        return False
    if float(cur["macd_hist"]) <= 0:
        return False
    return True


def dual_exit(bar: Dict[str, Any]) -> bool:
    return float(bar["ha_close"]) < float(bar["vwap"]) and float(bar["ha_close"]) < float(bar["ema20"])


def select_top_volume(signals: List[Dict[str, Any]], n: int) -> List[Dict[str, Any]]:
    ranked = sorted(signals, key=lambda s: float(s.get("volume") or 0), reverse=True)
    return ranked[: max(0, n)]


def simulate_session(
    by_symbol: Dict[str, List[Dict[str, Any]]],
    *,
    lots: Dict[str, int],
    instruments: Dict[str, str],
    keys: Dict[str, str],
    session_date: date,
) -> List[Dict[str, Any]]:
    """Walk all symbols in time. by_symbol values are annotated session bars (today only)."""
    times = sorted({_t(b) for bars in by_symbol.values() for b in bars})
    idx: Dict[str, Dict[time, Dict[str, Any]]] = {}
    prev_bar: Dict[str, Dict[str, Any]] = {}
    for sym, bars in by_symbol.items():
        idx[sym] = {_t(b): b for b in bars}
        # prev within session is handled per time step
    last_seen: Dict[str, Dict[str, Any]] = {}
    open_pos: Dict[str, OpenPos] = {}
    trades: List[Dict[str, Any]] = []
    ds = session_date.isoformat()

    def close_pos(pos: OpenPos, exit_px: float, exit_time: str, reason: str) -> None:
        pnl = (exit_px - pos.entry) * pos.qty
        r_unit = pos.entry * TP_PCT
        r_val = ((exit_px - pos.entry) / r_unit) if r_unit else None
        trades.append(
            {
                "date": pos.date,
                "symbol": pos.symbol,
                "instrument_key": pos.instrument_key,
                "instrument": pos.instrument,
                "entry_time": pos.entry_time,
                "entry": round(pos.entry, 4),
                "tp": round(pos.tp, 4),
                "exit": round(exit_px, 4),
                "exit_time": exit_time,
                "reason": reason,
                "volume": pos.volume,
                "qty": pos.qty,
                "pnl": round(pnl, 2),
                "R": round(r_val, 4) if r_val is not None else None,
            }
        )
        open_pos.pop(pos.symbol, None)

    for tm in times:
        # exits first
        for sym in list(open_pos.keys()):
            bar = idx.get(sym, {}).get(tm)
            if not bar:
                continue
            pos = open_pos[sym]
            ts = bar["bar_start"].strftime("%H:%M") if hasattr(bar.get("bar_start"), "strftime") else str(tm)[:5]
            high = float(bar.get("high") or 0)
            close = float(bar.get("close") or 0)
            if high >= pos.tp:
                close_pos(pos, pos.tp, ts, "tp")
                continue
            if dual_exit(bar):
                close_pos(pos, close, ts, "vwap_ema_exit")
                continue
            if is_eod_bar(bar):
                close_pos(pos, close, ts, "time")
                continue

        # entries
        slots = MAX_CONCURRENT - len(open_pos)
        if slots > 0:
            cands: List[Dict[str, Any]] = []
            for sym, tmap in idx.items():
                if sym in open_pos:
                    continue
                bar = tmap.get(tm)
                if not bar:
                    continue
                prev = last_seen.get(sym)
                if prev is None:
                    continue
                if is_signal(prev, bar):
                    cands.append({"symbol": sym, "bar": bar, "volume": float(bar.get("volume") or 0)})
            for pick in select_top_volume(cands, min(slots, TOP_N_BY_VOLUME)):
                bar = pick["bar"]
                raw_close = float(bar.get("close") or 0)
                if raw_close <= 0:
                    continue
                entry = raw_close * (1.0 + SLIPPAGE)
                qty = int(lots.get(pick["symbol"]) or 0)
                if qty <= 0:
                    logger.warning("ha_vwap skip entry %s: missing lot size (not using qty=1)", pick["symbol"])
                    continue
                ts = bar["bar_start"].strftime("%H:%M") if hasattr(bar.get("bar_start"), "strftime") else str(tm)[:5]
                open_pos[pick["symbol"]] = OpenPos(
                    symbol=pick["symbol"],
                    instrument_key=keys.get(pick["symbol"]) or "",
                    instrument=instruments.get(pick["symbol"]) or "fut",
                    qty=qty,
                    entry=entry,
                    tp=entry * (1.0 + TP_PCT),
                    entry_time=ts,
                    volume=pick["volume"],
                    date=ds,
                )

        for sym, tmap in idx.items():
            bar = tmap.get(tm)
            if bar:
                last_seen[sym] = bar

    # leftover (no 15:15 bar)
    for pos in list(open_pos.values()):
        bars = by_symbol.get(pos.symbol) or []
        last = bars[-1] if bars else None
        px = float(last.get("close") or pos.entry) if last else pos.entry
        ts = "15:15"
        if last and hasattr(last.get("bar_start"), "strftime"):
            ts = last["bar_start"].strftime("%H:%M")
        close_pos(pos, px, ts, "time")

    return trades
