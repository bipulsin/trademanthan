"""
NKS intraday momentum backtest service.

Takes a list of (date, symbol) rows and, for each row, looks up the intraday
1-minute candles of the **front-month equity future** on that date via Upstox
V2 historical candle API. Extracts the price at 09:45, 12:30, 14:00 and 15:15
IST and computes which of 12:30/14:00/15:15 had the largest absolute move from
09:45, plus the signed PnL points for that slot.

Instrument resolution uses the current ``nse_instruments.json`` to find the
FUT with the smallest expiry on/after the session date (the front-month at
that time). When no matching FUT is available (typical for session dates whose
front-month contract has already expired and is no longer listed in the
instruments snapshot), the service falls back to the underlying equity cash
instrument (``NSE_EQ|<ISIN>``) as a proxy — intraday moves track the future
within the trading session.
"""
from __future__ import annotations

import csv
import json
import logging
import time
from dataclasses import dataclass, field
from datetime import date, datetime, time as dt_time, timedelta, timezone
from pathlib import Path
from typing import Any, Dict, List, Optional, Tuple

import pytz

from backend.config import get_instruments_file_path, settings
from backend.services.upstox_service import UpstoxService

logger = logging.getLogger(__name__)

IST = pytz.timezone("Asia/Kolkata")

# Slot schedule (hour, minute). The 09:45 mark is the reference; the other
# three slots are compared against it to find the largest move.
ANCHOR_HHMM = (9, 45)
TARGET_SLOTS: List[Tuple[int, int]] = [(12, 30), (14, 0), (15, 15)]
SLOT_LABELS: Dict[Tuple[int, int], str] = {
    (9, 45): "09:45",
    (12, 30): "12:30",
    (14, 0): "14:00",
    (15, 15): "15:15",
}

# Max forward days between session_date and FUT expiry to be considered the
# "front-month" contract. Monthly expiries are ~30d apart; if the nearest
# listed expiry is >45 days after the session date, the front-month for that
# session has already expired and the EQ fallback is used instead.
FRONT_MONTH_MAX_FORWARD_DAYS = 45

# Pro-grade entry filters (applied on top of the raw backtest):
# 1. VWAP entry: the simulated long entry happens at 10:15 IST, priced at
#    the session VWAP computed from 09:15 through 10:15 (typical-price * V,
#    cumulative). This keeps the entry close to the volume-weighted average
#    while waiting for the breakout to confirm.
# 2. Risk cap: skip the trade when the worst-case drawdown from the VWAP
#    entry to the lowest traded price FROM 10:15 ONWARDS, priced per-lot,
#    exceeds this rupee threshold. Only the post-entry window is counted.
# 3. Opening-range-breakout (ORB) confirmation: the classical 15-minute
#    opening range high is the max high from 09:15 through 09:30 inclusive;
#    the trade is confirmed only when the 10:15 candle opens at/above that
#    level (i.e. the breakout is still holding at the 10:15 entry check).
MARKET_OPEN_HHMM = (9, 15)
OR_RANGE_END_HHMM = (9, 30)          # 15-minute opening range (09:15 -> 09:30)
ENTRY_HHMM = (10, 15)                # entry + ORB confirmation time
RISK_CAP_RUPEES = 10_000.0


@dataclass
class InstrumentRef:
    """Resolved instrument for a (symbol, session_date)."""

    source: str  # "FUT" or "EQ"
    trading_symbol: str
    instrument_key: str
    expiry_date: Optional[date] = None
    lot_size: Optional[int] = None
    # Always the current front-month FUT lot size for the underlying (used for
    # PnL-in-rupees calculation even when ``source == "EQ"``).
    fut_lot_size: Optional[int] = None


@dataclass
class BacktestRow:
    """Computed backtest result for one CSV row."""

    csv_date: str  # original date from the CSV (the shortlist date)
    session_date: str  # the actual date we fetched candles for (same or next trading day)
    symbol: str
    marketcapname: str
    sector: str
    source: Optional[str] = None
    trading_symbol: Optional[str] = None
    instrument_key: Optional[str] = None
    expiry_date: Optional[str] = None
    lot_size: Optional[int] = None
    fut_lot_size: Optional[int] = None
    price_0945: Optional[float] = None
    price_1230: Optional[float] = None
    price_1400: Optional[float] = None
    price_1515: Optional[float] = None
    best_slot: Optional[str] = None
    best_diff_points: Optional[float] = None
    best_abs_diff: Optional[float] = None
    pnl_rupees: Optional[float] = None
    # Max intraday drawdown: lowest traded price from 09:45 onward minus
    # the 09:45 price (so drawdown_points <= 0 in practice). Rupee amount is
    # drawdown_points * fut_lot_size. ``min_price_at`` is the HH:MM of the
    # minute-candle whose ``low`` produced the minimum (for UI tooltip).
    min_price: Optional[float] = None
    min_price_at: Optional[str] = None
    drawdown_points: Optional[float] = None
    drawdown_rupees: Optional[float] = None
    # Pro-grade filter fields --------------------------------------------------
    vwap_entry: Optional[float] = None               # session VWAP 09:15 -> 10:15
    or_high_0930: Optional[float] = None             # 15-minute OR high (09:15 -> 09:30)
    orb_price_at_1015: Optional[float] = None        # 10:15 candle open (entry time)
    orb_pass: Optional[bool] = None                  # 10:15 price >= OR high
    min_price_from_entry: Optional[float] = None     # min low from 10:15 onward
    min_price_from_entry_at: Optional[str] = None    # HH:MM of that low
    risk_rupees: Optional[float] = None              # (vwap_entry - min_from_entry) * lot
    risk_pass: Optional[bool] = None                 # risk_rupees <= RISK_CAP_RUPEES
    pnl_points_vwap: Optional[float] = None          # best_slot_price - vwap_entry
    pnl_rupees_vwap: Optional[float] = None          # pnl_points_vwap * lot
    drawdown_points_vwap: Optional[float] = None     # min_price_from_entry - vwap_entry
    drawdown_rupees_vwap: Optional[float] = None     # drawdown_points_vwap * lot
    trade_taken: Optional[bool] = None               # all gates pass -> TAKEN
    skip_reasons: List[str] = field(default_factory=list)
    error: Optional[str] = None
    notes: List[str] = field(default_factory=list)

    def to_dict(self) -> Dict[str, Any]:
        return {
            "csv_date": self.csv_date,
            "session_date": self.session_date,
            "symbol": self.symbol,
            "marketcapname": self.marketcapname,
            "sector": self.sector,
            "source": self.source,
            "trading_symbol": self.trading_symbol,
            "instrument_key": self.instrument_key,
            "expiry_date": self.expiry_date,
            "lot_size": self.lot_size,
            "fut_lot_size": self.fut_lot_size,
            "price_0945": self.price_0945,
            "price_1230": self.price_1230,
            "price_1400": self.price_1400,
            "price_1515": self.price_1515,
            "best_slot": self.best_slot,
            "best_diff_points": self.best_diff_points,
            "best_abs_diff": self.best_abs_diff,
            "pnl_rupees": self.pnl_rupees,
            "min_price": self.min_price,
            "min_price_at": self.min_price_at,
            "drawdown_points": self.drawdown_points,
            "drawdown_rupees": self.drawdown_rupees,
            "vwap_entry": self.vwap_entry,
            "or_high_0930": self.or_high_0930,
            "orb_price_at_1015": self.orb_price_at_1015,
            "orb_pass": self.orb_pass,
            "min_price_from_entry": self.min_price_from_entry,
            "min_price_from_entry_at": self.min_price_from_entry_at,
            "risk_rupees": self.risk_rupees,
            "risk_pass": self.risk_pass,
            "pnl_points_vwap": self.pnl_points_vwap,
            "pnl_rupees_vwap": self.pnl_rupees_vwap,
            "drawdown_points_vwap": self.drawdown_points_vwap,
            "drawdown_rupees_vwap": self.drawdown_rupees_vwap,
            "trade_taken": self.trade_taken,
            "skip_reasons": self.skip_reasons,
            "error": self.error,
            "notes": self.notes,
        }


def _parse_csv_date(s: str) -> Optional[date]:
    s = (s or "").strip()
    for fmt in ("%d-%m-%Y", "%Y-%m-%d", "%d/%m/%Y"):
        try:
            return datetime.strptime(s, fmt).date()
        except ValueError:
            continue
    return None


def load_stocks_csv(csv_path: Path) -> List[Dict[str, Any]]:
    """Read the NKS intraday stocks CSV, returning normalized rows."""
    rows: List[Dict[str, Any]] = []
    with open(csv_path, newline="", encoding="utf-8-sig") as f:
        reader = csv.DictReader(f)
        for raw in reader:
            d = _parse_csv_date(str(raw.get("date") or ""))
            sym = str(raw.get("symbol") or "").strip().upper()
            if not d or not sym:
                continue
            rows.append(
                {
                    "session_date": d,
                    "symbol": sym,
                    "marketcapname": str(raw.get("marketcapname") or "").strip(),
                    "sector": str(raw.get("sector") or "").strip(),
                }
            )
    return rows


def _expiry_ms_to_ist_date(ms: Any) -> Optional[date]:
    try:
        n = int(ms)
    except (TypeError, ValueError):
        return None
    if n > 1_000_000_000_000:
        n //= 1000
    try:
        return datetime.fromtimestamp(n, tz=timezone.utc).astimezone(IST).date()
    except (OSError, OverflowError, ValueError):
        return None


def _load_instruments() -> List[Dict[str, Any]]:
    path = get_instruments_file_path()
    if not path.is_file():
        logger.warning("nks_intraday_backtest: instruments file missing: %s", path)
        return []
    with open(path, encoding="utf-8") as f:
        data = json.load(f)
    return data if isinstance(data, list) else []


def _index_instruments(
    instruments: List[Dict[str, Any]],
) -> Tuple[Dict[str, List[Dict[str, Any]]], Dict[str, Dict[str, Any]]]:
    """Build two indexes: FUT-by-underlying and EQ-by-trading-symbol."""
    fut_by_und: Dict[str, List[Dict[str, Any]]] = {}
    eq_by_symbol: Dict[str, Dict[str, Any]] = {}
    for inst in instruments:
        if not isinstance(inst, dict):
            continue
        itype = str(inst.get("instrument_type") or "").upper()
        seg = str(inst.get("segment") or "").upper()
        if itype == "FUT" and ("NSE_FO" in seg or "NFO" in seg):
            und = (inst.get("underlying_symbol") or "").strip().upper()
            if und:
                fut_by_und.setdefault(und, []).append(inst)
        elif itype == "EQ" and "NSE_EQ" in seg:
            ts = (inst.get("trading_symbol") or "").strip().upper()
            if ts:
                eq_by_symbol[ts] = inst
    return fut_by_und, eq_by_symbol


def _current_fut_lot_size(
    symbol: str, *, fut_by_und: Dict[str, List[Dict[str, Any]]]
) -> Optional[int]:
    """Return the lot size of the nearest-expiry currently-listed FUT for ``symbol``.

    This is used to compute PnL in rupees for both FUT- and EQ-sourced rows, so
    the EQ fallback (for older session dates whose own contract is no longer
    listed) still reports a realistic futures position size.
    """
    sym_u = (symbol or "").strip().upper()
    lst = fut_by_und.get(sym_u) or []
    best_exp: Optional[date] = None
    best_lot: Optional[int] = None
    for inst in lst:
        exp = _expiry_ms_to_ist_date(inst.get("expiry"))
        if exp is None:
            continue
        ls_raw = inst.get("lot_size")
        try:
            ls = int(ls_raw) if ls_raw is not None else None
        except (TypeError, ValueError):
            ls = None
        if not ls:
            continue
        if best_exp is None or exp < best_exp:
            best_exp = exp
            best_lot = ls
    return best_lot


def resolve_instrument(
    symbol: str,
    session_date: date,
    *,
    fut_by_und: Dict[str, List[Dict[str, Any]]],
    eq_by_symbol: Dict[str, Dict[str, Any]],
) -> Optional[InstrumentRef]:
    """Return front-month FUT for the date, or EQ fallback, or None if unknown."""
    sym_u = (symbol or "").strip().upper()
    if not sym_u:
        return None
    fut_list = fut_by_und.get(sym_u, [])
    best_fut: Optional[Dict[str, Any]] = None
    best_exp: Optional[date] = None
    for inst in fut_list:
        exp = _expiry_ms_to_ist_date(inst.get("expiry"))
        if exp is None or exp < session_date:
            continue
        gap = (exp - session_date).days
        if gap > FRONT_MONTH_MAX_FORWARD_DAYS:
            continue
        if best_exp is None or exp < best_exp:
            best_exp = exp
            best_fut = inst
    cur_fut_lot = _current_fut_lot_size(sym_u, fut_by_und=fut_by_und)
    if best_fut and best_exp:
        fut_lot = int(best_fut.get("lot_size") or 0) or None
        return InstrumentRef(
            source="FUT",
            trading_symbol=str(best_fut.get("trading_symbol") or best_fut.get("tradingsymbol") or ""),
            instrument_key=str(best_fut.get("instrument_key") or ""),
            expiry_date=best_exp,
            lot_size=fut_lot,
            fut_lot_size=fut_lot or cur_fut_lot,
        )
    eq = eq_by_symbol.get(sym_u)
    if eq:
        return InstrumentRef(
            source="EQ",
            trading_symbol=str(eq.get("trading_symbol") or ""),
            instrument_key=str(eq.get("instrument_key") or ""),
            expiry_date=None,
            lot_size=int(eq.get("lot_size") or 0) or None,
            fut_lot_size=cur_fut_lot,
        )
    return None


def _candle_ts_to_ist(ts: Any) -> Optional[datetime]:
    if ts is None:
        return None
    if isinstance(ts, (int, float)):
        v = float(ts)
        if v > 1_000_000_000_000:
            v /= 1000.0
        try:
            return datetime.fromtimestamp(v, tz=IST)
        except (OSError, OverflowError, ValueError):
            return None
    s = str(ts).strip()
    if not s:
        return None
    if s.isdigit():
        v = float(s)
        if v > 1_000_000_000_000:
            v /= 1000.0
        try:
            return datetime.fromtimestamp(v, tz=IST)
        except (OSError, OverflowError, ValueError):
            return None
    try:
        dt = datetime.fromisoformat(s.replace("Z", "+00:00").replace(" ", "T"))
        return dt.astimezone(IST) if dt.tzinfo else IST.localize(dt)
    except ValueError:
        return None


def _price_at_slot(
    candles_by_hhmm: Dict[Tuple[int, int], Dict[str, Any]],
    hhmm: Tuple[int, int],
) -> Optional[float]:
    """Return the **open** price of the minute candle at HH:MM IST, falling back
    to the close of HH:MM-1 or the open of HH:MM+1 if the exact minute is
    missing. This mirrors "price as at HH:MM" for a second-precision user.
    """
    exact = candles_by_hhmm.get(hhmm)
    if exact is not None:
        px = exact.get("open")
        if isinstance(px, (int, float)) and px > 0:
            return float(px)
    h, m = hhmm
    prev_m = m - 1
    prev_h = h
    if prev_m < 0:
        prev_m = 59
        prev_h -= 1
    prev = candles_by_hhmm.get((prev_h, prev_m))
    if prev is not None:
        px = prev.get("close")
        if isinstance(px, (int, float)) and px > 0:
            return float(px)
    nxt_m = m + 1
    nxt_h = h
    if nxt_m > 59:
        nxt_m = 0
        nxt_h += 1
    nxt = candles_by_hhmm.get((nxt_h, nxt_m))
    if nxt is not None:
        px = nxt.get("open")
        if isinstance(px, (int, float)) and px > 0:
            return float(px)
    return None


def _bucket_candles_by_hhmm(
    candles: List[Dict[str, Any]], session_date: date
) -> Dict[Tuple[int, int], Dict[str, Any]]:
    out: Dict[Tuple[int, int], Dict[str, Any]] = {}
    for c in candles or []:
        dt_ist = _candle_ts_to_ist(c.get("timestamp"))
        if dt_ist is None or dt_ist.date() != session_date:
            continue
        out[(dt_ist.hour, dt_ist.minute)] = c
    return out


def _candle_ohlcv(c: Any) -> Tuple[Optional[float], Optional[float], Optional[float], Optional[float], Optional[float]]:
    """Extract (open, high, low, close, volume) from a Upstox minute candle.

    Upstox V2 candles are typically returned as lists ``[ts, o, h, l, c, v, ...]``
    but the service normalises them to dicts in some paths — we tolerate both.
    """
    if isinstance(c, dict):
        op = c.get("open"); hi = c.get("high"); lo = c.get("low")
        cl = c.get("close"); v = c.get("volume")
    elif isinstance(c, (list, tuple)) and len(c) >= 6:
        op, hi, lo, cl, v = c[1], c[2], c[3], c[4], c[5]
    else:
        return None, None, None, None, None

    def _f(x: Any) -> Optional[float]:
        try:
            return float(x)
        except (TypeError, ValueError):
            return None

    return _f(op), _f(hi), _f(lo), _f(cl), _f(v)


def _intraday_min_low_from(
    buckets: Dict[Tuple[int, int], Dict[str, Any]],
    start_hhmm: Tuple[int, int],
    end_hhmm: Optional[Tuple[int, int]] = None,
) -> Tuple[Optional[float], Optional[Tuple[int, int]]]:
    """Return the lowest candle ``low`` between ``start_hhmm`` and
    ``end_hhmm`` (inclusive, both endpoints) together with the HH:MM of the
    candle that produced it. ``end_hhmm=None`` means "through end of day".
    """
    best_low: Optional[float] = None
    best_at: Optional[Tuple[int, int]] = None
    for (h, m), c in buckets.items():
        if (h, m) < start_hhmm:
            continue
        if end_hhmm is not None and (h, m) > end_hhmm:
            continue
        _, _, lo, _, _ = _candle_ohlcv(c)
        if lo is None or lo <= 0:
            continue
        if best_low is None or lo < best_low:
            best_low = lo
            best_at = (h, m)
    return best_low, best_at


def _range_high(
    buckets: Dict[Tuple[int, int], Dict[str, Any]],
    start_hhmm: Tuple[int, int],
    end_hhmm: Tuple[int, int],
) -> Optional[float]:
    """Maximum ``high`` across candles in the inclusive HH:MM window."""
    best: Optional[float] = None
    for (h, m), c in buckets.items():
        if (h, m) < start_hhmm or (h, m) > end_hhmm:
            continue
        _, hi, _, _, _ = _candle_ohlcv(c)
        if hi is None or hi <= 0:
            continue
        if best is None or hi > best:
            best = hi
    return best


def _session_vwap_upto(
    buckets: Dict[Tuple[int, int], Dict[str, Any]],
    start_hhmm: Tuple[int, int],
    end_hhmm: Tuple[int, int],
) -> Optional[float]:
    """Volume-weighted average price over the inclusive HH:MM window,
    computed as ``Σ((H+L+C)/3 * V) / Σ(V)`` across 1-minute candles.
    Returns ``None`` when the cumulative volume is zero.
    """
    num = 0.0
    vol_sum = 0.0
    for (h, m), c in buckets.items():
        if (h, m) < start_hhmm or (h, m) > end_hhmm:
            continue
        _, hi, lo, cl, v = _candle_ohlcv(c)
        if hi is None or lo is None or cl is None or v is None:
            continue
        if not (hi > 0 and lo > 0 and cl > 0) or v <= 0:
            continue
        typical = (hi + lo + cl) / 3.0
        num += typical * v
        vol_sum += v
    if vol_sum <= 0:
        return None
    return num / vol_sum


def fetch_intraday_1m_candles(
    upstox: UpstoxService, instrument_key: str, session_date: date
) -> Optional[List[Dict[str, Any]]]:
    """Fetch 1-minute candles for the given session date via Upstox V2 API."""
    day_str = session_date.strftime("%Y-%m-%d")
    return upstox._fetch_historical_v2_candles(
        instrument_key, "1minute", day_str, day_str
    )


def _find_next_trading_day_with_candles(
    upstox: UpstoxService,
    instrument_key: str,
    base_date: date,
    *,
    max_forward_days: int = 7,
) -> Tuple[Optional[date], Optional[List[Dict[str, Any]]]]:
    """Return the first calendar day > ``base_date`` that returns intraday
    candles, skipping weekends and market holidays. Falls back to returning
    ``(None, None)`` when no candles are found within ``max_forward_days``.
    """
    for i in range(1, max_forward_days + 1):
        candidate = base_date + timedelta(days=i)
        if candidate.weekday() >= 5:  # Sat/Sun
            continue
        candles = fetch_intraday_1m_candles(upstox, instrument_key, candidate)
        if candles:
            return candidate, candles
    return None, None


def compute_backtest_row(
    upstox: UpstoxService,
    row: Dict[str, Any],
    *,
    fut_by_und: Dict[str, List[Dict[str, Any]]],
    eq_by_symbol: Dict[str, Dict[str, Any]],
    day_mode: str = "same",
) -> BacktestRow:
    csv_date: date = row["session_date"]
    sym: str = row["symbol"]
    br = BacktestRow(
        csv_date=csv_date.isoformat(),
        session_date=csv_date.isoformat(),
        symbol=sym,
        marketcapname=str(row.get("marketcapname") or ""),
        sector=str(row.get("sector") or ""),
    )
    # Resolve the contract using the ORIGINAL csv date so the expiry/front-month
    # lookup matches the timeframe of the shortlist. The next-day mode re-uses
    # the same contract and just fetches candles for the following session.
    ref = resolve_instrument(
        sym, csv_date, fut_by_und=fut_by_und, eq_by_symbol=eq_by_symbol
    )
    if ref is None or not ref.instrument_key:
        br.error = "instrument_not_resolved"
        return br
    br.source = ref.source
    br.trading_symbol = ref.trading_symbol
    br.instrument_key = ref.instrument_key
    br.expiry_date = ref.expiry_date.isoformat() if ref.expiry_date else None
    br.lot_size = ref.lot_size
    br.fut_lot_size = ref.fut_lot_size
    if ref.source == "EQ":
        br.notes.append("EQ cash proxy (front-month FUT not in instruments snapshot)")

    if day_mode == "next":
        eff_date, candles = _find_next_trading_day_with_candles(
            upstox, ref.instrument_key, csv_date
        )
        if eff_date is None or not candles:
            br.error = "no_next_day_candles"
            return br
        br.session_date = eff_date.isoformat()
    else:
        eff_date = csv_date
        candles = fetch_intraday_1m_candles(upstox, ref.instrument_key, eff_date)
        if not candles:
            br.error = "no_candles"
            return br

    buckets = _bucket_candles_by_hhmm(candles, eff_date)
    if not buckets:
        br.error = "no_session_candles"
        return br

    p0945 = _price_at_slot(buckets, ANCHOR_HHMM)
    p1230 = _price_at_slot(buckets, (12, 30))
    p1400 = _price_at_slot(buckets, (14, 0))
    p1515 = _price_at_slot(buckets, (15, 15))

    br.price_0945 = p0945
    br.price_1230 = p1230
    br.price_1400 = p1400
    br.price_1515 = p1515

    if p0945 is None:
        br.error = "no_0945_price"
        return br

    best_slot: Optional[str] = None
    best_abs: Optional[float] = None
    best_signed: Optional[float] = None
    for hhmm, price in (
        ((12, 30), p1230),
        ((14, 0), p1400),
        ((15, 15), p1515),
    ):
        if price is None:
            continue
        diff = float(price) - float(p0945)
        absd = abs(diff)
        if best_abs is None or absd > best_abs:
            best_abs = absd
            best_signed = diff
            best_slot = SLOT_LABELS[hhmm]
    br.best_slot = best_slot
    br.best_diff_points = round(best_signed, 2) if best_signed is not None else None
    br.best_abs_diff = round(best_abs, 2) if best_abs is not None else None
    if best_signed is not None and br.fut_lot_size:
        br.pnl_rupees = round(float(best_signed) * int(br.fut_lot_size), 2)

    # Intraday max drawdown from the 09:45 anchor: use the minimum ``low`` of
    # any 1m candle from 09:45 IST onward. Drawdown_points is signed; it will
    # be 0 only if 09:45 itself was the lowest traded price of the session.
    min_low, min_at = _intraday_min_low_from(buckets, ANCHOR_HHMM)
    if min_low is not None:
        br.min_price = round(float(min_low), 2)
        if min_at is not None:
            br.min_price_at = f"{min_at[0]:02d}:{min_at[1]:02d}"
        dd_pts = float(min_low) - float(p0945)
        br.drawdown_points = round(dd_pts, 2)
        if br.fut_lot_size:
            br.drawdown_rupees = round(dd_pts * int(br.fut_lot_size), 2)

    # -- Pro-grade entry filters ---------------------------------------------
    # Entry at 10:15 IST, priced at the session VWAP from 09:15 through 10:15
    # (typical-price * volume, cumulative). Waiting till 10:15 lets us confirm
    # the 15-minute opening-range breakout has held, and VWAP keeps the entry
    # close to the market's volume-weighted average during that window.
    vwap_entry = _session_vwap_upto(buckets, MARKET_OPEN_HHMM, ENTRY_HHMM)
    if vwap_entry is not None:
        br.vwap_entry = round(float(vwap_entry), 2)

    # Classical 15-minute opening range high (09:15 -> 09:30 inclusive).
    orh = _range_high(buckets, MARKET_OPEN_HHMM, OR_RANGE_END_HHMM)
    if orh is not None:
        br.or_high_0930 = round(float(orh), 2)

    # 10:15 entry-check price (open of the 10:15 1-minute candle).
    br.orb_price_at_1015 = _price_at_slot(buckets, ENTRY_HHMM)

    # ORB pass: breakout is still holding at 10:15 -- i.e. the 10:15 candle
    # opens at/above the 15-minute OR high.
    if orh is not None and br.orb_price_at_1015 is not None:
        br.orb_pass = bool(float(br.orb_price_at_1015) >= orh)
    else:
        br.orb_pass = None

    # Drawdown is measured from the 10:15 entry onwards -- that's the window
    # the strategy is actually exposed in.
    min_low_entry, min_low_entry_at = _intraday_min_low_from(buckets, ENTRY_HHMM)
    if min_low_entry is not None:
        br.min_price_from_entry = round(float(min_low_entry), 2)
        if min_low_entry_at is not None:
            br.min_price_from_entry_at = f"{min_low_entry_at[0]:02d}:{min_low_entry_at[1]:02d}"

    # Risk cap in rupees: (vwap_entry - min_from_entry) * lot_size. Positive
    # means we'd take on a drawdown; negative means price never retraced
    # below VWAP after 10:15 (which is favourable -> passes automatically).
    if (
        vwap_entry is not None
        and min_low_entry is not None
        and br.fut_lot_size
    ):
        risk_rs = (float(vwap_entry) - float(min_low_entry)) * int(br.fut_lot_size)
        br.risk_rupees = round(risk_rs, 2)
        br.risk_pass = bool(risk_rs <= RISK_CAP_RUPEES)

    # VWAP-anchored PnL / drawdown. PnL reference is the VWAP entry; the best
    # slot (based on the 09:45 abs-move anchor) is still the candidate exit --
    # 12:30 / 14:00 / 15:15 all land after the 10:15 entry so they remain
    # valid exit points for this simulation.
    if vwap_entry is not None:
        if best_signed is not None:
            best_slot_price = float(p0945) + float(best_signed)
            pnl_v = best_slot_price - float(vwap_entry)
            br.pnl_points_vwap = round(pnl_v, 2)
            if br.fut_lot_size:
                br.pnl_rupees_vwap = round(pnl_v * int(br.fut_lot_size), 2)
        if min_low_entry is not None:
            dd_v = float(min_low_entry) - float(vwap_entry)
            br.drawdown_points_vwap = round(dd_v, 2)
            if br.fut_lot_size:
                br.drawdown_rupees_vwap = round(dd_v * int(br.fut_lot_size), 2)

    # Composite decision. Missing VWAP or unresolved gates fail safe.
    reasons: List[str] = []
    if vwap_entry is None:
        reasons.append("no_vwap")
    if br.risk_pass is False:
        reasons.append("risk_gt_10k")
    elif br.risk_pass is None:
        reasons.append("risk_unknown")
    if br.orb_pass is False:
        reasons.append("orb_failed")
    elif br.orb_pass is None:
        reasons.append("orb_unknown")
    br.skip_reasons = reasons
    br.trade_taken = bool(not reasons)
    return br


def run_backtest(
    rows: List[Dict[str, Any]],
    *,
    throttle_sec: float = 0.08,
    progress_every: int = 25,
    logger_fn=None,
    day_mode: str = "same",
) -> List[Dict[str, Any]]:
    """Run the NKS intraday backtest across all rows and return serialized results.

    ``day_mode`` is ``"same"`` (default) to price the same day as the CSV date
    or ``"next"`` to price the **next trading session** after the CSV date
    (weekends and holidays are skipped automatically).
    """
    log = logger_fn or (lambda msg: logger.info(msg))
    mode = (day_mode or "same").lower()
    if mode not in ("same", "next"):
        raise ValueError(f"day_mode must be 'same' or 'next' (got {day_mode!r})")

    instruments = _load_instruments()
    fut_by_und, eq_by_symbol = _index_instruments(instruments)
    log(
        f"nks_intraday_backtest[{mode}]: loaded {len(instruments)} instruments "
        f"({len(fut_by_und)} FUT underlyings, {len(eq_by_symbol)} EQ symbols)"
    )

    upstox = UpstoxService(settings.UPSTOX_API_KEY, settings.UPSTOX_API_SECRET)

    out: List[Dict[str, Any]] = []
    total = len(rows)
    for idx, row in enumerate(rows, start=1):
        try:
            br = compute_backtest_row(
                upstox,
                row,
                fut_by_und=fut_by_und,
                eq_by_symbol=eq_by_symbol,
                day_mode=mode,
            )
        except Exception as e:
            logger.exception("nks_intraday_backtest[%s] row %s/%s failed", mode, idx, total)
            csv_iso = row["session_date"].isoformat() if row.get("session_date") else ""
            br = BacktestRow(
                csv_date=csv_iso,
                session_date=csv_iso,
                symbol=str(row.get("symbol") or ""),
                marketcapname=str(row.get("marketcapname") or ""),
                sector=str(row.get("sector") or ""),
                error=f"exception:{type(e).__name__}",
            )
        out.append(br.to_dict())
        if idx % progress_every == 0 or idx == total:
            log(f"nks_intraday_backtest[{mode}]: {idx}/{total} rows processed")
        if throttle_sec > 0:
            time.sleep(throttle_sec)
    return out


def build_output_document(
    results: List[Dict[str, Any]], *, day_mode: str = "same"
) -> Dict[str, Any]:
    """Wrap per-row results with summary metadata for the public JSON artifact."""
    total = len(results)
    with_prices = sum(
        1
        for r in results
        if r.get("price_0945") is not None and r.get("best_slot")
    )
    fut_count = sum(1 for r in results if r.get("source") == "FUT")
    eq_count = sum(1 for r in results if r.get("source") == "EQ")
    # slot winner counts (when a best_slot was selected)
    slot_wins: Dict[str, int] = {"12:30": 0, "14:00": 0, "15:15": 0}
    pos_pnl = neg_pnl = 0
    sum_pnl = 0.0
    sum_pnl_rupees = 0.0
    sum_dd_points = 0.0
    sum_dd_rupees = 0.0
    worst_dd_rupees: Optional[float] = None
    taken = skipped = 0
    skip_by_reason: Dict[str, int] = {
        "risk_gt_10k": 0,
        "orb_failed": 0,
        "no_vwap": 0,
        "other": 0,
    }
    taken_pos_pnl = taken_neg_pnl = 0
    sum_taken_pnl_rupees = 0.0
    sum_taken_dd_rupees = 0.0
    worst_taken_dd_rupees: Optional[float] = None
    for r in results:
        slot = r.get("best_slot")
        if slot in slot_wins:
            slot_wins[slot] += 1
        pnl = r.get("best_diff_points")
        if isinstance(pnl, (int, float)):
            sum_pnl += float(pnl)
            if pnl > 0:
                pos_pnl += 1
            elif pnl < 0:
                neg_pnl += 1
        pnl_r = r.get("pnl_rupees")
        if isinstance(pnl_r, (int, float)):
            sum_pnl_rupees += float(pnl_r)
        dd_p = r.get("drawdown_points")
        if isinstance(dd_p, (int, float)):
            sum_dd_points += float(dd_p)
        dd_r = r.get("drawdown_rupees")
        if isinstance(dd_r, (int, float)):
            sum_dd_rupees += float(dd_r)
            if worst_dd_rupees is None or dd_r < worst_dd_rupees:
                worst_dd_rupees = float(dd_r)

        if r.get("trade_taken") is True:
            taken += 1
            pnl_v = r.get("pnl_rupees_vwap")
            if isinstance(pnl_v, (int, float)):
                sum_taken_pnl_rupees += float(pnl_v)
                if pnl_v > 0:
                    taken_pos_pnl += 1
                elif pnl_v < 0:
                    taken_neg_pnl += 1
            dd_v = r.get("drawdown_rupees_vwap")
            if isinstance(dd_v, (int, float)):
                sum_taken_dd_rupees += float(dd_v)
                if worst_taken_dd_rupees is None or dd_v < worst_taken_dd_rupees:
                    worst_taken_dd_rupees = float(dd_v)
        else:
            skipped += 1
            for rsn in (r.get("skip_reasons") or []):
                if rsn in skip_by_reason:
                    skip_by_reason[rsn] += 1
                else:
                    skip_by_reason["other"] += 1
    return {
        "generated_at": datetime.now(IST).isoformat(),
        "day_mode": day_mode,
        "anchor_time": "09:45 IST",
        "target_slots": ["12:30", "14:00", "15:15"],
        "summary": {
            "day_mode": day_mode,
            "total_rows": total,
            "rows_with_prices": with_prices,
            "rows_fut_source": fut_count,
            "rows_eq_source": eq_count,
            "slot_wins": slot_wins,
            "positive_pnl_rows": pos_pnl,
            "negative_pnl_rows": neg_pnl,
            "sum_pnl_points": round(sum_pnl, 2),
            "sum_pnl_rupees": round(sum_pnl_rupees, 2),
            "sum_drawdown_points": round(sum_dd_points, 2),
            "sum_drawdown_rupees": round(sum_dd_rupees, 2),
            "worst_drawdown_rupees": (
                round(worst_dd_rupees, 2) if worst_dd_rupees is not None else None
            ),
            # Pro-grade filtered-book metrics ----------------------------------
            "taken_rows": taken,
            "skipped_rows": skipped,
            "skipped_by_reason": skip_by_reason,
            "taken_positive_pnl_rows": taken_pos_pnl,
            "taken_negative_pnl_rows": taken_neg_pnl,
            "taken_sum_pnl_rupees": round(sum_taken_pnl_rupees, 2),
            "taken_sum_drawdown_rupees": round(sum_taken_dd_rupees, 2),
            "taken_worst_drawdown_rupees": (
                round(worst_taken_dd_rupees, 2)
                if worst_taken_dd_rupees is not None else None
            ),
            "risk_cap_rupees": RISK_CAP_RUPEES,
        },
        "rows": results,
    }
