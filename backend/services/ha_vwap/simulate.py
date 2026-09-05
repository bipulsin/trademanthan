"""CSV-timed HA-VWAP simulation: TP, HA<VWAP+EMA20, raw close < SuperTrend, 15:15 time."""
from __future__ import annotations

import logging
from dataclasses import dataclass
from datetime import date, time
from typing import Any, Dict, Iterable, List, Optional, Set, Tuple

import pytz

from backend.services.ha_vwap.config import FORCE_EXIT_TIME, SLIPPAGE, TP_PCT

logger = logging.getLogger(__name__)
IST = pytz.timezone("Asia/Kolkata")


def _t(bar: Dict[str, Any]) -> time:
    start = bar.get("bar_start")
    if start is None:
        return time(0, 0)
    if hasattr(start, "astimezone"):
        start = start.astimezone(IST)
    return time(int(start.hour), int(start.minute))


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


def annotate_bars(bars: List[Dict[str, Any]], ha_c, vwap, ema20, st) -> List[Dict[str, Any]]:
    out = []
    for i, b in enumerate(bars):
        row = dict(b)
        row["ha_close"] = ha_c[i]
        row["vwap"] = vwap[i]
        row["ema20"] = ema20[i]
        row["st"] = st[i]
        out.append(row)
    return out


def dual_exit(bar: Dict[str, Any]) -> bool:
    return float(bar["ha_close"]) < float(bar["vwap"]) and float(bar["ha_close"]) < float(bar["ema20"])


def st_exit(bar: Dict[str, Any]) -> bool:
    """RAW 10m close vs SuperTrend(10,3) on raw OHLC (not HA)."""
    st = bar.get("st")
    if st is None:
        return False
    return float(bar.get("close") or 0) < float(st)


def simulate_session(
    by_symbol: Dict[str, List[Dict[str, Any]]],
    *,
    lots: Dict[str, int],
    instruments: Dict[str, str],
    keys: Dict[str, str],
    session_date: date,
    csv_entries: Optional[Iterable[Tuple[str, time]]] = None,
) -> List[Dict[str, Any]]:
    """Walk symbols in time. Entries only at CSV (symbol, bar_start) pairs."""
    wanted: Dict[str, List[time]] = {}
    for sym, tm in csv_entries or []:
        wanted.setdefault(sym, []).append(tm)
    used: Set[Tuple[str, time]] = set()

    times = sorted({_t(b) for bars in by_symbol.values() for b in bars})
    idx: Dict[str, Dict[time, Dict[str, Any]]] = {}
    for sym, bars in by_symbol.items():
        idx[sym] = {_t(b): b for b in bars}
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
                close_pos(pos, close, ts, "vwap_ema")
                continue
            if st_exit(bar):
                close_pos(pos, close, ts, "supertrend")
                continue
            if is_eod_bar(bar):
                close_pos(pos, close, ts, "time")
                continue

        for sym, tlist in wanted.items():
            if tm not in tlist:
                continue
            if (sym, tm) in used:
                continue
            if sym in open_pos:
                logger.warning("ha_vwap skip entry %s %s: already open", sym, tm)
                used.add((sym, tm))
                continue
            bar = idx.get(sym, {}).get(tm)
            if not bar:
                logger.warning("ha_vwap skip entry %s %s: no matching 10m bar", sym, tm.strftime("%H:%M"))
                used.add((sym, tm))
                continue
            raw_close = float(bar.get("close") or 0)
            if raw_close <= 0:
                logger.warning("ha_vwap skip entry %s %s: bad close", sym, tm)
                used.add((sym, tm))
                continue
            qty = int(lots.get(sym) or 0)
            if qty <= 0:
                logger.warning("ha_vwap skip entry %s: missing lot size", sym)
                used.add((sym, tm))
                continue
            entry = raw_close * (1.0 + SLIPPAGE)
            ts = bar["bar_start"].strftime("%H:%M") if hasattr(bar.get("bar_start"), "strftime") else str(tm)[:5]
            used.add((sym, tm))
            open_pos[sym] = OpenPos(
                symbol=sym,
                instrument_key=keys.get(sym) or "",
                instrument=instruments.get(sym) or "fut",
                qty=qty,
                entry=entry,
                tp=entry * (1.0 + TP_PCT),
                entry_time=ts,
                volume=float(bar.get("volume") or 0),
                date=ds,
            )

    for pos in list(open_pos.values()):
        bars = by_symbol.get(pos.symbol) or []
        last = bars[-1] if bars else None
        px = float(last.get("close") or pos.entry) if last else pos.entry
        ts = "15:15"
        if last and hasattr(last.get("bar_start"), "strftime"):
            ts = last["bar_start"].strftime("%H:%M")
        close_pos(pos, px, ts, "time")

    for sym, tlist in wanted.items():
        for tm in tlist:
            if (sym, tm) not in used:
                logger.warning("ha_vwap skip entry %s %s: no FUT/candle", sym, tm.strftime("%H:%M"))

    return trades
