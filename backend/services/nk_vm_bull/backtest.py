"""NK VM Bull backtest — per-row LONG futures simulation from CSV signals."""
from __future__ import annotations

import csv
import json
import logging
import time
from datetime import date, datetime, timedelta
from pathlib import Path
from typing import Any, Dict, List, Optional, Tuple

import pytz

from backend.config import settings
from backend.services.nks_intraday_backtest import (
    _bucket_candles_by_hhmm,
    _candle_ohlcv,
    fetch_intraday_1m_candles,
)
from backend.services.smart_futures_picker.position_sizing import (
    get_futures_lot_size_by_instrument_key,
)
from backend.services.upstox_service import UpstoxService
from backend.services.volume_mismatch.backtest_universe import (
    load_volume_mismatch_universe_for_session,
)

logger = logging.getLogger(__name__)

IST = pytz.timezone("Asia/Kolkata")

ENTRY_OFFSET_MIN = 5
EXIT_1230_HHMM = (12, 30)
EXIT_1515_HHMM = (15, 15)
PNL_MILESTONE_RUPEES = 5000.0
STOP_LOSS_PCT = 0.005

ARTIFACT_NAME = "nk_vm_bull_backtest.json"
SOURCE_NAME = "nk_vm_bull_backtest_source.csv"


def _parse_signal_dt(s: str) -> Optional[datetime]:
    s = (s or "").strip()
    if not s:
        return None
    for fmt in (
        "%d-%m-%Y %H:%M",
        "%d-%m-%Y %H:%M:%S",
        "%d-%m-%Y %I:%M %p",
        "%d-%m-%Y %I:%M:%S %p",
    ):
        try:
            dt = datetime.strptime(s, fmt)
            return IST.localize(dt)
        except ValueError:
            continue
    return None


def load_source_csv(csv_path: Path) -> List[Dict[str, Any]]:
    """Parse source CSV into normalized signal rows."""
    rows: List[Dict[str, Any]] = []
    with open(csv_path, newline="", encoding="utf-8-sig") as f:
        reader = csv.DictReader(f)
        for raw in reader:
            dt = _parse_signal_dt(str(raw.get("date") or ""))
            sym = str(raw.get("symbol") or "").strip().upper()
            if dt is None or not sym:
                continue
            rows.append(
                {
                    "signal_time": dt,
                    "symbol": sym,
                    "marketcapname": str(raw.get("marketcapname") or "").strip(),
                    "sector": str(raw.get("sector") or "").strip(),
                }
            )
    rows.sort(key=lambda r: r["signal_time"])
    return rows


def _add_minutes(dt: datetime, mins: int) -> datetime:
    return dt + timedelta(minutes=mins)


def _hhmm(dt: datetime) -> Tuple[int, int]:
    return (dt.hour, dt.minute)


def _fmt_dt(dt: Optional[datetime]) -> Optional[str]:
    if dt is None:
        return None
    return dt.astimezone(IST).strftime("%Y-%m-%d %H:%M:%S")


def _ltp_close_at_slot(
    buckets: Dict[Tuple[int, int], Dict[str, Any]],
    hhmm: Tuple[int, int],
) -> Optional[float]:
    """LTP = close of the 1m candle at HH:MM, with nearest-minute fallback."""
    exact = buckets.get(hhmm)
    if exact is not None:
        _, _, _, cl, _ = _candle_ohlcv(exact)
        if cl is not None and float(cl) > 0:
            return float(cl)
    h, m = hhmm
    for delta in (1, -1, 2, -2, 3, -3, 4, -4, 5, -5):
        probe_m = m + delta
        probe_h = h
        while probe_m < 0:
            probe_m += 60
            probe_h -= 1
        while probe_m > 59:
            probe_m -= 60
            probe_h += 1
        if probe_h < 0 or probe_h > 23:
            continue
        c = buckets.get((probe_h, probe_m))
        if c is None:
            continue
        _, _, _, cl, _ = _candle_ohlcv(c)
        if cl is not None and float(cl) > 0:
            return float(cl)
    return None


def _resolve_future_for_symbol(
    symbol: str,
    session_date: date,
    universe_cache: Dict[date, List[Dict[str, str]]],
) -> Optional[Dict[str, str]]:
    if session_date not in universe_cache:
        universe_cache[session_date] = load_volume_mismatch_universe_for_session(session_date)
    sym_u = symbol.strip().upper()
    for row in universe_cache.get(session_date) or []:
        if str(row.get("symbol") or "").upper() == sym_u:
            return row
    return None


def _next_trading_session(
    base: date,
    *,
    max_forward_days: int = 7,
) -> date:
    """Shift weekend/holiday CSV dates to the next weekday (Mon if Sun)."""
    d = base
    for _ in range(max_forward_days + 1):
        if d.weekday() < 5:
            return d
        d += timedelta(days=1)
    return base


def _session_candles(
    upstox: UpstoxService,
    instrument_key: str,
    session_date: date,
    cache: Dict[Tuple[str, date], Dict[Tuple[int, int], Dict[str, Any]]],
    *,
    throttle_sec: float,
) -> Tuple[Optional[date], Dict[Tuple[int, int], Dict[str, Any]], Optional[str]]:
    """Fetch 1m candles; if empty on weekend date, try next trading day."""
    notes: List[str] = []
    actual_date = session_date
    if session_date.weekday() >= 5:
        shifted = _next_trading_session(session_date)
        if shifted != session_date:
            notes.append(f"weekend_csv_shifted_to_{shifted.isoformat()}")
            actual_date = shifted

    key = (instrument_key, actual_date)
    if key in cache:
        return actual_date, cache[key], ("; ".join(notes) if notes else None)

    candles = fetch_intraday_1m_candles(upstox, instrument_key, actual_date)
    if throttle_sec > 0:
        time.sleep(throttle_sec)

    if not candles and actual_date == session_date and session_date.weekday() < 5:
        for i in range(1, 6):
            candidate = session_date + timedelta(days=i)
            if candidate.weekday() >= 5:
                continue
            candles = fetch_intraday_1m_candles(upstox, instrument_key, candidate)
            if throttle_sec > 0:
                time.sleep(throttle_sec)
            if candles:
                notes.append(f"no_data_shifted_to_{candidate.isoformat()}")
                actual_date = candidate
                key = (instrument_key, actual_date)
                break

    if not candles:
        return actual_date, {}, ("; ".join(notes) if notes else None)

    buckets = _bucket_candles_by_hhmm(candles, actual_date)
    cache[key] = buckets
    return actual_date, buckets, ("; ".join(notes) if notes else None)


def _candle_at_slot_with_fallback(
    buckets: Dict[Tuple[int, int], Dict[str, Any]],
    hhmm: Tuple[int, int],
) -> Optional[Dict[str, Any]]:
    """1m candle at HH:MM with nearest-minute fallback (same as entry LTP)."""
    exact = buckets.get(hhmm)
    if exact is not None:
        return exact
    h, m = hhmm
    for delta in (1, -1, 2, -2, 3, -3, 4, -4, 5, -5):
        probe_m = m + delta
        probe_h = h
        while probe_m < 0:
            probe_m += 60
            probe_h -= 1
        while probe_m > 59:
            probe_m -= 60
            probe_h += 1
        if probe_h < 0 or probe_h > 23:
            continue
        c = buckets.get((probe_h, probe_m))
        if c is not None:
            return c
    return None


def _entry_stop_loss_price(
    buckets: Dict[Tuple[int, int], Dict[str, Any]],
    entry_hhmm: Tuple[int, int],
) -> Optional[float]:
    """0.5% below min(open, close) of the entry 1m candle."""
    c = _candle_at_slot_with_fallback(buckets, entry_hhmm)
    if c is None:
        return None
    op, _, _, cl, _ = _candle_ohlcv(c)
    if op is None or cl is None:
        return None
    o_f, c_f = float(op), float(cl)
    if o_f <= 0 or c_f <= 0:
        return None
    ref = min(o_f, c_f)
    return round(ref * (1.0 - STOP_LOSS_PCT), 2)


def _scan_sl_and_milestone(
    buckets: Dict[Tuple[int, int], Dict[str, Any]],
    entry_hhmm: Tuple[int, int],
    entry_price: float,
    lot_size: int,
    stop_loss_price: float,
    *,
    end_hhmm: Tuple[int, int] = EXIT_1515_HHMM,
) -> Tuple[bool, Optional[str], Optional[float], Optional[str], Optional[float]]:
    """Minute scan from entry: SL (low) before Rs 5000 milestone (close LTP)."""
    if lot_size <= 0 or entry_price <= 0 or stop_loss_price <= 0:
        return False, None, None, None, None

    sl_hit = False
    sl_hit_time: Optional[str] = None
    pnl_5000_time: Optional[str] = None
    pnl_5000_ltp: Optional[float] = None

    keys = sorted(k for k in buckets if entry_hhmm <= k <= end_hhmm)
    for k in keys:
        if not sl_hit:
            c = buckets.get(k)
            if c is not None:
                _, _, lo, _, _ = _candle_ohlcv(c)
                if lo is not None and float(lo) <= stop_loss_price:
                    sl_hit = True
                    sl_hit_time = f"{k[0]:02d}:{k[1]:02d}"

        if pnl_5000_time is None:
            ltp = _ltp_close_at_slot(buckets, k)
            if ltp is not None:
                pnl = (ltp - entry_price) * lot_size
                if pnl >= PNL_MILESTONE_RUPEES:
                    pnl_5000_time = f"{k[0]:02d}:{k[1]:02d}"
                    pnl_5000_ltp = round(ltp, 2)

    pnl_at_sl: Optional[float] = None
    if sl_hit:
        pnl_at_sl = round((stop_loss_price - entry_price) * lot_size, 2)
    return sl_hit, sl_hit_time, pnl_at_sl, pnl_5000_time, pnl_5000_ltp


def _reached_profit(
    sl_hit: bool,
    sl_hit_time: Optional[str],
    pnl_5000_time: Optional[str],
) -> bool:
    """Rs 5000 milestone hit before SL, or milestone hit when SL never triggered."""
    if not pnl_5000_time:
        return False
    if not sl_hit:
        return True
    if not sl_hit_time:
        return True
    return pnl_5000_time < sl_hit_time


def compute_trade_row(
    signal_row: Dict[str, Any],
    *,
    upstox: UpstoxService,
    universe_cache: Dict[date, List[Dict[str, str]]],
    candle_cache: Dict[Tuple[str, date], Dict[Tuple[int, int], Dict[str, Any]]],
    throttle_sec: float = 0.15,
) -> Dict[str, Any]:
    signal_dt: datetime = signal_row["signal_time"]
    symbol = signal_row["symbol"]
    session_date = signal_dt.date()
    entry_dt = _add_minutes(signal_dt, ENTRY_OFFSET_MIN)

    out: Dict[str, Any] = {
        "signal_time": _fmt_dt(signal_dt),
        "trade_date": session_date.isoformat(),
        "symbol": symbol,
        "marketcapname": signal_row.get("marketcapname") or "",
        "sector": signal_row.get("sector") or "",
        "future_symbol": None,
        "instrument_key": None,
        "lot_size": None,
        "entry_time": _fmt_dt(entry_dt),
        "entry_price": None,
        "exit_1230_price": None,
        "pnl_1230": None,
        "exit_1515_price": None,
        "pnl_1515": None,
        "pnl_5000_time": None,
        "pnl_5000_ltp": None,
        "stop_loss_price": None,
        "sl_hit": False,
        "sl_hit_time": None,
        "pnl_at_sl": None,
        "reached_profit": False,
        "reached_profit_1230": False,
        "reached_profit_1515": False,
        "error": None,
        "notes": None,
    }

    if session_date.weekday() >= 5:
        out["notes"] = "csv_date_is_weekend"

    fut = _resolve_future_for_symbol(symbol, session_date, universe_cache)
    if not fut or not fut.get("instrument_key"):
        out["error"] = "future_not_resolved"
        return out

    ik = str(fut["instrument_key"]).strip()
    out["future_symbol"] = fut.get("future_symbol") or symbol
    out["instrument_key"] = ik

    lot_size = int(get_futures_lot_size_by_instrument_key(ik) or 0)
    if lot_size <= 0:
        lot_size = int(fut.get("lot_size") or 0) if fut.get("lot_size") else 0
    out["lot_size"] = lot_size if lot_size > 0 else None
    if not lot_size or lot_size <= 0:
        out["error"] = "lot_size_unavailable"
        return out

    actual_date, buckets, shift_note = _session_candles(
        upstox, ik, session_date, candle_cache, throttle_sec=throttle_sec
    )
    if shift_note:
        out["notes"] = shift_note if not out.get("notes") else f"{out['notes']}; {shift_note}"
    if actual_date != session_date:
        out["trade_date"] = actual_date.isoformat()

    if not buckets:
        out["error"] = "no_session_candles"
        return out

    entry_hhmm = _hhmm(entry_dt.astimezone(IST))
    entry_price = _ltp_close_at_slot(buckets, entry_hhmm)
    if entry_price is None:
        out["error"] = "entry_price_unavailable"
        return out
    out["entry_price"] = round(entry_price, 2)

    exit_1230 = _ltp_close_at_slot(buckets, EXIT_1230_HHMM)
    exit_1515 = _ltp_close_at_slot(buckets, EXIT_1515_HHMM)
    out["exit_1230_price"] = round(exit_1230, 2) if exit_1230 is not None else None
    out["exit_1515_price"] = round(exit_1515, 2) if exit_1515 is not None else None
    if exit_1230 is not None:
        out["pnl_1230"] = round((exit_1230 - entry_price) * lot_size, 2)
    if exit_1515 is not None:
        out["pnl_1515"] = round((exit_1515 - entry_price) * lot_size, 2)

    sl_price = _entry_stop_loss_price(buckets, entry_hhmm)
    if sl_price is None:
        out["error"] = "stop_loss_price_unavailable"
        return out
    out["stop_loss_price"] = sl_price

    sl_hit, sl_hit_time, pnl_at_sl, pnl_time, pnl_ltp = _scan_sl_and_milestone(
        buckets, entry_hhmm, entry_price, lot_size, sl_price
    )
    out["sl_hit"] = sl_hit
    out["sl_hit_time"] = sl_hit_time
    out["pnl_at_sl"] = pnl_at_sl
    out["pnl_5000_time"] = pnl_time
    out["pnl_5000_ltp"] = pnl_ltp
    out["reached_profit"] = _reached_profit(sl_hit, sl_hit_time, pnl_time)
    if out.get("pnl_1230") is not None:
        out["reached_profit_1230"] = float(out["pnl_1230"]) > 0
    if out.get("pnl_1515") is not None:
        out["reached_profit_1515"] = float(out["pnl_1515"]) > 0
    if pnl_time is None:
        out["notes"] = (
            f"{out['notes']}; pnl_5000_not_hit" if out.get("notes") else "pnl_5000_not_hit"
        )

    return out


def run_nk_vm_bull_backtest(
    source_path: Path,
    *,
    out_path: Optional[Path] = None,
    throttle_sec: float = 0.15,
) -> Dict[str, Any]:
    """Run backtest for all CSV rows and optionally write JSON artifact."""
    signals = load_source_csv(source_path)
    if not signals:
        return {"error": "no_signals_in_csv", "rows": []}

    upstox = UpstoxService(settings.UPSTOX_API_KEY, settings.UPSTOX_API_SECRET)
    if not getattr(upstox, "access_token", None):
        return {"error": "Upstox token unavailable", "rows": []}

    universe_cache: Dict[date, List[Dict[str, str]]] = {}
    candle_cache: Dict[Tuple[str, date], Dict[Tuple[int, int], Dict[str, Any]]] = {}
    rows: List[Dict[str, Any]] = []
    errors = 0

    for idx, sig in enumerate(signals, start=1):
        try:
            row = compute_trade_row(
                sig,
                upstox=upstox,
                universe_cache=universe_cache,
                candle_cache=candle_cache,
                throttle_sec=throttle_sec,
            )
        except Exception as e:
            logger.exception("nk_vm_bull row %s failed: %s", sig.get("symbol"), e)
            row = {
                "signal_time": _fmt_dt(sig.get("signal_time")),
                "trade_date": (
                    sig["signal_time"].date().isoformat()
                    if sig.get("signal_time")
                    else None
                ),
                "symbol": sig.get("symbol"),
                "error": f"compute_error: {e}",
            }
        if row.get("error"):
            errors += 1
        rows.append(row)
        if out_path is not None:
            _write_artifact(out_path, rows, errors=errors, partial=True)
        if idx % 10 == 0 or idx == len(signals):
            logger.info("nk_vm_bull_backtest: %s/%s rows", idx, len(signals))

    doc = build_output_document(rows, errors=errors, partial=False)
    if out_path is not None:
        _write_artifact(out_path, rows, errors=errors, partial=False)
    return doc


def build_output_document(
    rows: List[Dict[str, Any]],
    *,
    errors: int = 0,
    partial: bool = False,
) -> Dict[str, Any]:
    sorted_rows = sorted(rows, key=lambda r: str(r.get("signal_time") or ""))
    ok_rows = [r for r in sorted_rows if not r.get("error")]
    sl_hit_before_profit = 0
    for r in ok_rows:
        if not r.get("sl_hit"):
            continue
        pnl_t = r.get("pnl_5000_time")
        sl_t = r.get("sl_hit_time")
        if not pnl_t or (sl_t and sl_t <= pnl_t):
            sl_hit_before_profit += 1
    return {
        "algo": "nk_vm_bull_backtest",
        "generated_at": datetime.now(IST).isoformat(),
        "partial": partial,
        "summary": {
            "total_trades": len(sorted_rows),
            "errors": errors,
            "sl_hit_count": sum(1 for r in ok_rows if r.get("sl_hit")),
            "reached_profit_count": sum(1 for r in ok_rows if r.get("reached_profit")),
            "sl_hit_before_profit_count": sl_hit_before_profit,
        },
        "rows": sorted_rows,
    }


def _write_artifact(
    path: Path,
    rows: List[Dict[str, Any]],
    *,
    errors: int,
    partial: bool,
) -> None:
    doc = build_output_document(rows, errors=errors, partial=partial)
    path.parent.mkdir(parents=True, exist_ok=True)
    tmp = path.with_suffix(".json.tmp")
    with open(tmp, "w", encoding="utf-8") as f:
        json.dump(doc, f, indent=2, default=str)
    tmp.replace(path)


def default_source_path() -> Path:
    root = Path(__file__).resolve().parents[3]
    for rel in ("data", "backend/data"):
        p = root / rel / SOURCE_NAME
        if p.is_file():
            return p
    return root / "data" / SOURCE_NAME


def default_out_path() -> Path:
    ec2 = Path("/home/ubuntu/trademanthan/data") / ARTIFACT_NAME
    if ec2.parent.is_dir():
        return ec2
    root = Path(__file__).resolve().parents[3]
    for rel in ("data", "backend/data"):
        p = root / rel / ARTIFACT_NAME
        p.parent.mkdir(parents=True, exist_ok=True)
        return p
    return root / "data" / ARTIFACT_NAME
