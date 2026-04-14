#!/usr/bin/env python3
"""
SMARTFUTURE futures-only backtester.

Strictly uses current/front-month FUT contracts (NSE_FO FUT) from arbitrage_master symbols.
No spot/equity/options are used in calculations.
"""

from __future__ import annotations

import argparse
import csv
import json
import math
from dataclasses import dataclass, asdict
from datetime import date, datetime, time as dt_time
from pathlib import Path
from statistics import median
from typing import Any, Dict, List, Optional, Sequence, Tuple

import pytz
from sqlalchemy import text

from backend.config import get_instruments_file_path, settings
from backend.database import SessionLocal
from backend.services.smart_futures_exit import evaluate_exit_with_profit_protection
from backend.services.upstox_service import UpstoxService

IST = pytz.timezone("Asia/Kolkata")
OUT_ROOT = Path("backtest_data")
INDEX_UNDERLYINGS = {"NIFTY", "BANKNIFTY", "FINNIFTY", "MIDCPNIFTY", "NIFTYNXT50", "SENSEX", "BANKEX"}


@dataclass
class ContractInfo:
    stock: str
    instrument_key: str
    expiry: int
    lot_size: int
    futures_symbol: str
    contract_month: str


@dataclass
class BacktestRow:
    symbol: str
    futures_symbol: str
    expiry: str
    cms_score: float
    cms_direction: str
    cms_tier: str
    price_change_0915_to_1330_pct: float
    oi_change_0915_to_1330_pct: float
    oi_signal: str
    combo_score: float
    final_decision: str
    entry_time: Optional[str]
    entry_price: Optional[float]
    entry_qty: int
    hard_stop_loss: Optional[float]
    breakeven_activated: bool
    breakeven_activation_time: Optional[str]
    profit_locking_activated: bool
    profit_locking_activation_time: Optional[str]
    profit_locking_stop_level: Optional[float]
    trailing_stop_activated: bool
    trailing_stop_activation_time: Optional[str]
    initial_trailing_stop_level: Optional[float]
    current_trailing_stop_level: Optional[float]
    current_active_stop_loss_level: Optional[float]
    final_exit_price: Optional[float]
    final_exit_time: Optional[str]
    final_exit_profit: Optional[float]
    total_roi_pct: Optional[float]
    holding_time_minutes: Optional[float]
    max_profit_achieved: Optional[float]
    final_exit_reason: str
    mfe: Optional[float]
    mae: Optional[float]
    pnl: Optional[float]
    roi_pct: Optional[float]
    hit: str
    lot_size: int
    error: str = ""


def parse_dt_ist(ts: Any) -> Optional[datetime]:
    if ts is None:
        return None
    s = str(ts).strip()
    try:
        if "T" in s:
            dt = datetime.fromisoformat(s.replace("Z", "+00:00"))
            return dt.astimezone(IST) if dt.tzinfo else IST.localize(dt)
    except ValueError:
        return None
    return None


def sort_candles(candles: Optional[List[dict]]) -> List[dict]:
    return sorted(candles or [], key=lambda c: str(c.get("timestamp") or ""))


def find_candle(candles: Sequence[dict], session_d: date, hh: int, mm: int) -> Optional[dict]:
    for c in candles:
        dt = parse_dt_ist(c.get("timestamp"))
        if dt and dt.date() == session_d and dt.hour == hh and dt.minute == mm:
            return c
    return None


def find_candle_at_or_after(
    candles: Sequence[dict], session_d: date, hh: int, mm: int, max_delay_min: int = 20
) -> Optional[dict]:
    target = IST.localize(datetime.combine(session_d, dt_time(hh, mm)))
    best = None
    best_delay = None
    for c in candles:
        dt = parse_dt_ist(c.get("timestamp"))
        if not dt or dt.date() != session_d or dt < target:
            continue
        delay = int((dt - target).total_seconds() // 60)
        if delay < 0 or delay > max_delay_min:
            continue
        if best is None or delay < (best_delay or 10**9):
            best = c
            best_delay = delay
    return best


def find_candle_at_or_before(
    candles: Sequence[dict], session_d: date, hh: int, mm: int, max_lookback_min: int = 20
) -> Optional[dict]:
    target = IST.localize(datetime.combine(session_d, dt_time(hh, mm)))
    best = None
    best_back = None
    for c in candles:
        dt = parse_dt_ist(c.get("timestamp"))
        if not dt or dt.date() != session_d or dt > target:
            continue
        back = int((target - dt).total_seconds() // 60)
        if back < 0 or back > max_lookback_min:
            continue
        if best is None or back < (best_back or 10**9):
            best = c
            best_back = back
    return best


def candles_for_day(candles: Sequence[dict], session_d: date) -> List[dict]:
    return [c for c in candles if (parse_dt_ist(c.get("timestamp")) or datetime.min.replace(tzinfo=IST)).date() == session_d]


def load_arbitrage_symbols() -> List[str]:
    db = SessionLocal()
    try:
        rows = db.execute(
            text(
                """
                SELECT DISTINCT stock
                FROM arbitrage_master
                WHERE stock IS NOT NULL AND TRIM(stock) <> ''
                ORDER BY stock
                """
            )
        ).fetchall()
    finally:
        db.close()
    out, seen = [], set()
    for (s,) in rows:
        u = str(s or "").strip().upper()
        if u and u not in seen:
            seen.add(u)
            out.append(u)
    return out


def load_instruments() -> List[Dict[str, Any]]:
    p = get_instruments_file_path()
    if not p.exists():
        raise FileNotFoundError(f"instruments file missing: {p}")
    return json.loads(p.read_text(encoding="utf-8"))


def is_stock_future(inst: Dict[str, Any]) -> bool:
    seg = str(inst.get("segment") or "").upper()
    if "NSE_FO" not in seg and "NFO" not in seg:
        return False
    if str(inst.get("instrument_type") or "").upper() != "FUT":
        return False
    und = str(inst.get("underlying_symbol") or inst.get("name") or "").strip().upper()
    return bool(und) and und not in INDEX_UNDERLYINGS


def front_month_resolver(session_d: date, symbols: Sequence[str]) -> Dict[str, ContractInfo]:
    target_ms = int(IST.localize(datetime.combine(session_d, dt_time(13, 30))).timestamp() * 1000)
    rows = [r for r in load_instruments() if isinstance(r, dict) and is_stock_future(r)]
    by_sym: Dict[str, List[Dict[str, Any]]] = {}
    for r in rows:
        und = str(r.get("underlying_symbol") or "").strip().upper()
        if und:
            by_sym.setdefault(und, []).append(r)
    out: Dict[str, ContractInfo] = {}
    for sym in symbols:
        contracts = by_sym.get(sym) or []
        if not contracts:
            continue
        contracts.sort(key=lambda x: int(x.get("expiry") or 0))
        active = [c for c in contracts if int(c.get("expiry") or 0) >= target_ms]
        c = active[0] if active else contracts[0]
        tsym = str(c.get("trading_symbol") or c.get("tradingsymbol") or "").strip()
        ik = str(c.get("instrument_key") or "").strip()
        if not ik:
            continue
        expiry = int(c.get("expiry") or 0)
        lot = int(float(c.get("lot_size") or c.get("quantity") or c.get("minimum_lot") or 1))
        cm = ""
        if tsym:
            parts = tsym.split()
            cm = parts[-1] if parts else ""
        out[sym] = ContractInfo(sym, ik, expiry, max(1, lot), tsym or sym, cm)
    return out


def fetch_futures_day_candles(
    ux: UpstoxService, ci: ContractInfo, session_d: date, cache_dir: Path
) -> Tuple[List[dict], str]:
    cache_dir.mkdir(parents=True, exist_ok=True)
    f1 = cache_dir / f"{ci.stock}_1m.json"
    f5 = cache_dir / f"{ci.stock}_5m.json"

    raw_1m = ux.get_historical_candles_by_instrument_key(ci.instrument_key, "minutes/1", 0, range_end_date=session_d) or []
    if not raw_1m:
        raw_1m = ux.get_historical_candles_by_instrument_key(ci.instrument_key, "minutes/1", 1, range_end_date=session_d) or []
    day_1m = candles_for_day(sort_candles(raw_1m), session_d)
    if day_1m:
        f1.write_text(json.dumps(day_1m), encoding="utf-8")
        return day_1m, "1m"

    raw_5m = ux.get_historical_candles_by_instrument_key(ci.instrument_key, "minutes/5", 0, range_end_date=session_d) or []
    if not raw_5m:
        raw_5m = ux.get_historical_candles_by_instrument_key(ci.instrument_key, "minutes/5", 1, range_end_date=session_d) or []
    day_5m = candles_for_day(sort_candles(raw_5m), session_d)
    if day_5m:
        f5.write_text(json.dumps(day_5m), encoding="utf-8")
    return day_5m, "5m"


def tanh_norm(x: float, scale: float) -> float:
    if scale <= 0:
        return 0.0
    return math.tanh(x / scale)


def cms_from_futures(candles: Sequence[dict], o915: dict, s1330: dict) -> Tuple[float, str, str]:
    closes = [float(c.get("close") or 0) for c in candles if c.get("close") is not None]
    vols = [float(c.get("volume") or 0) for c in candles if c.get("volume") is not None]
    c0, c1 = float(o915.get("close") or 0), float(s1330.get("close") or 0)
    if c0 <= 0 or c1 <= 0:
        return 0.5, "SHORT", "TIER3"
    trend = (c1 - c0) / c0
    prev = closes[max(0, len(closes) - 30)] if closes else c0
    momentum = (c1 - prev) / prev if prev > 0 else 0.0
    v_med = median(vols) if vols else 0.0
    v_now = float(s1330.get("volume") or 0.0)
    vol_dev = ((v_now - v_med) / max(1.0, v_med)) if v_med > 0 else 0.0
    raw = 0.5 * tanh_norm(trend, 0.01) + 0.3 * tanh_norm(momentum, 0.008) + 0.2 * tanh_norm(vol_dev, 0.7)
    score = max(0.0, min(1.0, (raw + 1.0) / 2.0))
    direction = "LONG" if score >= 0.5 else "SHORT"
    tier = "TIER1" if score >= 0.75 else ("TIER2" if score >= 0.60 else "TIER3")
    return score, direction, tier


def oi_signal_rules(pct_p: float, pct_oi: float) -> str:
    if abs(pct_p) < 0.1 or abs(pct_oi) < 1.0:
        return "NEUTRAL"
    if pct_p > 0 and pct_oi > 0:
        return "LONG_BUILDUP"
    if pct_p < 0 and pct_oi > 0:
        return "SHORT_BUILDUP"
    if pct_p > 0 and pct_oi < 0:
        return "SHORT_COVERING"
    if pct_p < 0 and pct_oi < 0:
        return "LONG_UNWINDING"
    return "NEUTRAL"


def final_decision(cms_dir: str, oi_sig: str) -> str:
    if cms_dir == "LONG" and oi_sig == "LONG_BUILDUP":
        return "ENTER_LONG"
    if cms_dir == "SHORT" and oi_sig == "SHORT_BUILDUP":
        return "ENTER_SHORT"
    if cms_dir == "LONG" and oi_sig == "SHORT_BUILDUP":
        return "BLOCK"
    if cms_dir == "SHORT" and oi_sig == "LONG_BUILDUP":
        return "BLOCK"
    if cms_dir == "LONG" and oi_sig == "NEUTRAL":
        return "ENTER_LONG"
    if cms_dir == "SHORT" and oi_sig == "NEUTRAL":
        return "ENTER_SHORT"
    return "NO_TRADE"


def final_decision_buildup_only(oi_sig: str) -> str:
    """Trade direction strictly from OI buildup only."""
    if oi_sig == "LONG_BUILDUP":
        return "ENTER_LONG"
    if oi_sig == "SHORT_BUILDUP":
        return "ENTER_SHORT"
    return "NO_TRADE"


def combo_score_cms_oi(cms_score: float, oi_change_pct: float) -> float:
    """Combined ranking score for top-N selection."""
    oi_component = min(abs(float(oi_change_pct or 0.0)) / 20.0, 1.0)
    return 0.7 * float(cms_score or 0.0) + 0.3 * oi_component


def atr14(candles: Sequence[dict]) -> float:
    if len(candles) < 2:
        return 0.0
    trs: List[float] = []
    prev_close = float(candles[0].get("close") or 0)
    for c in candles[1:]:
        h = float(c.get("high") or 0)
        l = float(c.get("low") or 0)
        tr = max(h - l, abs(h - prev_close), abs(l - prev_close))
        trs.append(tr)
        prev_close = float(c.get("close") or prev_close)
    if not trs:
        return 0.0
    return sum(trs[-14:]) / min(14, len(trs))


def evaluate_outcome(
    decision: str, entry: float, exit_close: float, lot: int, post: Sequence[dict], atr: float
) -> Tuple[Optional[float], Optional[float], str]:
    if decision not in {"ENTER_LONG", "ENTER_SHORT"}:
        return None, None, "NA"
    is_long = decision == "ENTER_LONG"
    sl_dist, tp_dist = 1.2 * atr, 2.0 * atr
    sl = entry - sl_dist if is_long else entry + sl_dist
    tp = entry + tp_dist if is_long else entry - tp_dist
    exit_px, hit = exit_close, "loss"
    for c in post:
        h = float(c.get("high") or 0)
        l = float(c.get("low") or 0)
        if is_long:
            if l <= sl:
                exit_px, hit = sl, "loss"
                break
            if h >= tp:
                exit_px, hit = tp, "win"
                break
        else:
            if h >= sl:
                exit_px, hit = sl, "loss"
                break
            if l <= tp:
                exit_px, hit = tp, "win"
                break
    pnl = (exit_px - entry) * lot if is_long else (entry - exit_px) * lot
    roi = ((exit_px - entry) / entry * 100.0) if is_long else ((entry - exit_px) / entry * 100.0)
    if pnl > 0:
        hit = "win"
    elif pnl < 0 and hit == "NA":
        hit = "loss"
    return round(pnl, 2), round(roi, 4), hit


def evaluate_outcome_fixed_exit(decision: str, entry: float, exit_close: float, lot: int) -> Tuple[Optional[float], Optional[float], str]:
    """Fixed-time exit evaluation (no SL/TP path checks)."""
    if decision not in {"ENTER_LONG", "ENTER_SHORT"}:
        return None, None, "NA"
    is_long = decision == "ENTER_LONG"
    pnl = (exit_close - entry) * lot if is_long else (entry - exit_close) * lot
    roi = ((exit_close - entry) / entry * 100.0) if is_long else ((entry - exit_close) / entry * 100.0)
    hit = "win" if pnl > 0 else ("loss" if pnl < 0 else "flat")
    return round(pnl, 2), round(roi, 4), hit


def run(args: argparse.Namespace) -> Tuple[List[BacktestRow], Dict[str, Any]]:
    sd = date.fromisoformat(args.date)
    th, tm, _ = [int(x) for x in args.time.split(":")]
    bh, bm, _ = [int(x) for x in args.baseline.split(":")]
    eh, em = 15, 30

    symbols = load_arbitrage_symbols()
    contracts = front_month_resolver(sd, symbols)
    ux = UpstoxService(settings.UPSTOX_API_KEY, settings.UPSTOX_API_SECRET)
    cache_dir = OUT_ROOT / args.date / "candles"
    rows: List[BacktestRow] = []
    failures: List[Dict[str, str]] = []
    candles_by_symbol: Dict[str, List[dict]] = {}
    m5_by_symbol: Dict[str, List[dict]] = {}

    for sym in symbols:
        ci = contracts.get(sym)
        if not ci:
            failures.append({"symbol": sym, "error": "no_front_month_future"})
            continue
        try:
            candles, tf = fetch_futures_day_candles(ux, ci, sd, cache_dir)
            if not candles:
                raise ValueError("no_candles_for_day")
            c0915 = find_candle(candles, sd, bh, bm) or find_candle_at_or_after(candles, sd, bh, bm, max_delay_min=20)
            c1330 = find_candle(candles, sd, th, tm) or find_candle_at_or_after(candles, sd, th, tm, max_delay_min=20)
            c1530 = find_candle(candles, sd, eh, em) or find_candle_at_or_before(candles, sd, eh, em, max_lookback_min=20)
            if not c0915 or not c1330 or not c1530:
                raise ValueError("missing_baseline_or_signal_or_exit_candle")
            if c0915.get("oi") is None or c1330.get("oi") is None:
                raise ValueError("missing_oi_for_baseline_or_signal")

            upto_signal = [c for c in candles if (parse_dt_ist(c.get("timestamp")) or datetime.min.replace(tzinfo=IST)).time() <= dt_time(th, tm)]
            cms_score, cms_dir, cms_tier = cms_from_futures(upto_signal, c0915, c1330)
            p0, p1 = float(c0915["close"]), float(c1330["close"])
            oi0, oi1 = float(c0915["oi"]), float(c1330["oi"])
            price_pct = ((p1 - p0) / p0 * 100.0) if p0 else 0.0
            oi_pct = ((oi1 - oi0) / oi0 * 100.0) if oi0 else 0.0
            oi_sig = oi_signal_rules(price_pct, oi_pct)
            dec = final_decision_buildup_only(oi_sig)
            combo = combo_score_cms_oi(cms_score, oi_pct)
            entry_dt = parse_dt_ist(c1330.get("timestamp")) or IST.localize(datetime.combine(sd, dt_time(th, tm)))
            entry_ts = entry_dt.isoformat()
            candles_by_symbol[sym] = candles
            raw_5m = ux.get_historical_candles_by_instrument_key(ci.instrument_key, "minutes/5", 0, range_end_date=sd) or []
            if not raw_5m:
                raw_5m = ux.get_historical_candles_by_instrument_key(ci.instrument_key, "minutes/5", 1, range_end_date=sd) or []
            m5_by_symbol[sym] = candles_for_day(sort_candles(raw_5m), sd)

            rows.append(
                BacktestRow(
                    symbol=sym,
                    futures_symbol=ci.futures_symbol,
                    expiry=str(ci.expiry),
                    cms_score=round(cms_score, 4),
                    cms_direction=cms_dir,
                    cms_tier=cms_tier,
                    price_change_0915_to_1330_pct=round(price_pct, 4),
                    oi_change_0915_to_1330_pct=round(oi_pct, 4),
                    oi_signal=oi_sig,
                    combo_score=round(combo, 6),
                    final_decision=dec,
                    entry_time=entry_ts if dec.startswith("ENTER") else None,
                    entry_price=round(p1, 4) if dec.startswith("ENTER") else None,
                    entry_qty=ci.lot_size,
                    hard_stop_loss=None,
                    breakeven_activated=False,
                    breakeven_activation_time=None,
                    profit_locking_activated=False,
                    profit_locking_activation_time=None,
                    profit_locking_stop_level=None,
                    trailing_stop_activated=False,
                    trailing_stop_activation_time=None,
                    initial_trailing_stop_level=None,
                    current_trailing_stop_level=None,
                    current_active_stop_loss_level=None,
                    final_exit_price=None,
                    final_exit_time=None,
                    final_exit_profit=None,
                    total_roi_pct=None,
                    holding_time_minutes=None,
                    max_profit_achieved=None,
                    final_exit_reason="",
                    mfe=None,
                    mae=None,
                    pnl=None,
                    roi_pct=None,
                    hit="NA",
                    lot_size=ci.lot_size,
                    error="",
                )
            )
            # cache metadata per symbol
            (cache_dir / f"{sym}_meta.json").write_text(
                json.dumps({"timeframe": tf, **asdict(ci)}, indent=2), encoding="utf-8"
            )
        except Exception as e:
            failures.append({"symbol": sym, "error": str(e)})
            rows.append(
                BacktestRow(
                    symbol=sym,
                    futures_symbol=ci.futures_symbol,
                    expiry=str(ci.expiry),
                    cms_score=0.0,
                    cms_direction="NA",
                    cms_tier="NA",
                    price_change_0915_to_1330_pct=0.0,
                    oi_change_0915_to_1330_pct=0.0,
                    oi_signal="NA",
                    combo_score=0.0,
                    final_decision="NO_TRADE",
                    entry_time=None,
                    entry_price=None,
                    entry_qty=ci.lot_size if ci else 1,
                    hard_stop_loss=None,
                    breakeven_activated=False,
                    breakeven_activation_time=None,
                    profit_locking_activated=False,
                    profit_locking_activation_time=None,
                    profit_locking_stop_level=None,
                    trailing_stop_activated=False,
                    trailing_stop_activation_time=None,
                    initial_trailing_stop_level=None,
                    current_trailing_stop_level=None,
                    current_active_stop_loss_level=None,
                    final_exit_price=None,
                    final_exit_time=None,
                    final_exit_profit=None,
                    total_roi_pct=None,
                    holding_time_minutes=None,
                    max_profit_achieved=None,
                    final_exit_reason="",
                    mfe=None,
                    mae=None,
                    pnl=None,
                    roi_pct=None,
                    hit="NA",
                    lot_size=ci.lot_size if ci else 1,
                    error=str(e),
                )
            )

    candidates = [
        r for r in rows
        if r.oi_signal in {"LONG_BUILDUP", "SHORT_BUILDUP"} and r.final_decision in {"ENTER_LONG", "ENTER_SHORT"}
    ]
    candidates.sort(key=lambda r: float(r.combo_score or 0.0), reverse=True)
    selected_keys = {r.symbol for r in candidates[: int(getattr(args, "top_n", 5) or 5)]}

    for r in rows:
        if r.symbol not in selected_keys:
            r.final_decision = "NO_TRADE"
            r.entry_time = None
            r.entry_price = None
            r.pnl = None
            r.roi_pct = None
            r.hit = "NA"

    for r in rows:
        if r.final_decision not in {"ENTER_LONG", "ENTER_SHORT"} or r.entry_price is None or not r.entry_time:
            continue
        side = "LONG" if r.final_decision == "ENTER_LONG" else "SHORT"
        seq = candles_by_symbol.get(r.symbol) or []
        seq5 = m5_by_symbol.get(r.symbol) or []
        entry_dt = parse_dt_ist(r.entry_time)
        post = [c for c in seq5 if (parse_dt_ist(c.get("timestamp")) and parse_dt_ist(c.get("timestamp")) >= entry_dt)] if entry_dt else seq5
        pre = (
            [c for c in seq5 if (parse_dt_ist(c.get("timestamp")) and parse_dt_ist(c.get("timestamp")) < entry_dt)]
            if entry_dt
            else []
        )
        if not post:
            r.error = "missing_post_entry_candles"
            continue
        try:
            ex = evaluate_exit_with_profit_protection(
                side, float(r.entry_price), r.entry_time, int(r.lot_size), post, m5_pre_entry=pre
            )
            st = ex.get("state", {}) if isinstance(ex, dict) else {}
            r.hard_stop_loss = st.get("hard_stop_loss")
            r.breakeven_activated = bool(st.get("breakeven_activated"))
            r.breakeven_activation_time = st.get("breakeven_activation_time")
            r.profit_locking_activated = bool(st.get("profit_locking_activated"))
            r.profit_locking_activation_time = st.get("profit_locking_activation_time")
            r.profit_locking_stop_level = st.get("profit_locking_stop_level")
            r.trailing_stop_activated = bool(st.get("trailing_stop_activated"))
            r.trailing_stop_activation_time = st.get("trailing_stop_activation_time")
            r.initial_trailing_stop_level = st.get("initial_trailing_stop_level")
            r.current_trailing_stop_level = st.get("current_trailing_stop_level")
            r.current_active_stop_loss_level = st.get("current_active_stop_loss_level")
            r.max_profit_achieved = st.get("max_profit_achieved")
            r.final_exit_price = ex.get("final_exit_price")
            r.final_exit_time = ex.get("final_exit_time")
            r.final_exit_profit = ex.get("final_exit_profit")
            r.total_roi_pct = ex.get("total_roi_pct")
            r.final_exit_reason = str(ex.get("final_exit_reason") or "")
            r.pnl = r.final_exit_profit
            r.roi_pct = r.total_roi_pct
            r.hit = "win" if (r.pnl or 0) > 0 else ("loss" if (r.pnl or 0) < 0 else "flat")
            ex_dt = parse_dt_ist(r.final_exit_time) if r.final_exit_time else None
            if entry_dt and ex_dt:
                r.holding_time_minutes = round(max(0.0, (ex_dt - entry_dt).total_seconds() / 60.0), 2)
            highs = [float(c.get("high") or c.get("close") or 0.0) for c in post]
            lows = [float(c.get("low") or c.get("close") or 0.0) for c in post]
            if highs and lows:
                if side == "LONG":
                    r.mfe = round((max(highs) - float(r.entry_price)) * float(r.lot_size), 2)
                    r.mae = round((min(lows) - float(r.entry_price)) * float(r.lot_size), 2)
                else:
                    r.mfe = round((float(r.entry_price) - min(lows)) * float(r.lot_size), 2)
                    r.mae = round((float(r.entry_price) - max(highs)) * float(r.lot_size), 2)
        except Exception as ex_err:
            r.error = f"exit_eval_error: {ex_err}"

    trades = [r for r in rows if r.final_decision in {"ENTER_LONG", "ENTER_SHORT"} and r.final_exit_profit is not None]
    wins = [r for r in trades if (r.pnl or 0) > 0]
    losses = [r for r in trades if (r.pnl or 0) < 0]
    sum_wins = sum(float(r.pnl or 0.0) for r in wins)
    sum_losses_abs = abs(sum(float(r.pnl or 0.0) for r in losses))
    tier1 = [r for r in trades if r.breakeven_activated]
    tier2 = [r for r in trades if r.profit_locking_activated]
    tier3 = [r for r in trades if r.trailing_stop_activated]
    exit_trailing = [r for r in trades if "Trailing Stop Hit" in str(r.final_exit_reason or "")]
    exit_primary = [r for r in trades if str(r.final_exit_reason or "").startswith("15-min")]
    exit_emergency = [r for r in trades if "Emergency Stop" in str(r.final_exit_reason or "")]
    exit_stoploss = [r for r in trades if "Stop Loss Hit" in str(r.final_exit_reason or "")]
    avg_holding = round(sum(float(r.holding_time_minutes or 0.0) for r in trades) / len(trades), 2) if trades else 0.0
    avg_profit = round(sum(float(r.pnl or 0.0) for r in trades) / len(trades), 2) if trades else 0.0
    avg_mfe = round(sum(float(r.mfe or 0.0) for r in trades) / len(trades), 2) if trades else 0.0
    avg_mae = round(sum(float(r.mae or 0.0) for r in trades) / len(trades), 2) if trades else 0.0
    tier3_avg = round(sum(float(r.pnl or 0.0) for r in tier3) / len(tier3), 2) if tier3 else 0.0
    not_tier3 = [r for r in trades if not r.trailing_stop_activated]
    non_tier3_avg = round(sum(float(r.pnl or 0.0) for r in not_tier3) / len(not_tier3), 2) if not_tier3 else 0.0
    summary = {
        "date": args.date,
        "signal_time": args.time,
        "baseline_time": args.baseline,
        "symbols_from_arbitrage_master": len(symbols),
        "contracts_resolved": len(contracts),
        "rows_generated": len(rows),
        "selection_rule": "Top-N by combo score among LONG_BUILDUP/SHORT_BUILDUP only",
        "top_n": int(getattr(args, "top_n", 5) or 5),
        "exit_time": "multi-timeframe with profit protection tiers",
        "trade_count": len(trades),
        "wins": len(wins),
        "win_rate_pct": round((len(wins) / len(trades) * 100.0), 2) if trades else 0.0,
        "total_pnl": round(sum(float(r.pnl or 0.0) for r in trades), 2),
        "avg_roi_pct": round(sum(float(r.roi_pct or 0.0) for r in trades) / len(trades), 4) if trades else 0.0,
        "avg_holding_time_minutes": avg_holding,
        "avg_profit_per_trade": avg_profit,
        "profit_factor": round((sum_wins / sum_losses_abs), 4) if sum_losses_abs > 0 else (999.0 if sum_wins > 0 else 0.0),
        "avg_mfe": avg_mfe,
        "avg_mae": avg_mae,
        "pct_reach_tier1": round((len(tier1) / len(trades) * 100.0), 2) if trades else 0.0,
        "pct_reach_tier2": round((len(tier2) / len(trades) * 100.0), 2) if trades else 0.0,
        "pct_reach_tier3": round((len(tier3) / len(trades) * 100.0), 2) if trades else 0.0,
        "pct_exit_trailing": round((len(exit_trailing) / len(trades) * 100.0), 2) if trades else 0.0,
        "pct_exit_primary_15m": round((len(exit_primary) / len(trades) * 100.0), 2) if trades else 0.0,
        "pct_exit_emergency": round((len(exit_emergency) / len(trades) * 100.0), 2) if trades else 0.0,
        "pct_exit_stoploss": round((len(exit_stoploss) / len(trades) * 100.0), 2) if trades else 0.0,
        "avg_profit_tier3": tier3_avg,
        "avg_profit_non_tier3": non_tier3_avg,
        "failures": failures[:200],
    }
    return rows, summary


def write_outputs(rows: List[BacktestRow], summary: Dict[str, Any], d: str, t: str) -> None:
    OUT_ROOT.mkdir(parents=True, exist_ok=True)
    csv_path = Path("futures_backtest_trades_detailed.csv")
    js_path = Path("futures_backtest_profit_protection_summary.json")
    md_path = Path("futures_backtest_performance_report.md")
    html_path = Path("futures_backtest_dashboard_profit_protection.html")
    public_backtests_dir = Path("frontend/public/backtests")
    public_backtests_dir.mkdir(parents=True, exist_ok=True)
    public_html_path = public_backtests_dir / "futures_backtest.html"

    with csv_path.open("w", newline="", encoding="utf-8") as f:
        w = csv.DictWriter(f, fieldnames=list(asdict(rows[0]).keys()))
        w.writeheader()
        for r in rows:
            w.writerow(asdict(r))

    js_path.write_text(json.dumps(summary, indent=2), encoding="utf-8")
    md_path.write_text(
        "\n".join(
            [
                f"# Futures Backtest Profit Protection Report ({d})",
                "",
                f"- Signal time: `{summary.get('signal_time')}`",
                f"- Trades: `{summary.get('trade_count')}` | Win rate: `{summary.get('win_rate_pct')}%`",
                f"- Total PnL: `{summary.get('total_pnl')}` | Profit factor: `{summary.get('profit_factor')}`",
                f"- Tier reach: T1 `{summary.get('pct_reach_tier1')}%`, T2 `{summary.get('pct_reach_tier2')}%`, T3 `{summary.get('pct_reach_tier3')}%`",
                f"- Exit mix: Trailing `{summary.get('pct_exit_trailing')}%`, Primary15m `{summary.get('pct_exit_primary_15m')}%`, Emergency `{summary.get('pct_exit_emergency')}%`, StopLoss `{summary.get('pct_exit_stoploss')}%`",
            ]
        ),
        encoding="utf-8",
    )
    html_path.write_text(
        f"""<!doctype html><html><head><meta charset='utf-8'><title>Futures Backtest Profit Protection</title></head>
<body><h2>Futures Backtest Dashboard Profit Protection ({d})</h2>
<pre>{json.dumps(summary, indent=2)}</pre>
<p>CSV: {csv_path.name}</p></body></html>""",
        encoding="utf-8",
    )

    # Public overwrite report: always latest run at fixed URL/path.
    top_n = int(summary.get("top_n") or 5)
    top_rows = [r for r in rows if r.final_decision in {"ENTER_LONG", "ENTER_SHORT"}][:top_n]
    rows_html = "".join(
        "<tr>"
        f"<td>{r.futures_symbol}</td>"
        f"<td>{r.cms_score:.4f}</td>"
        f"<td>{r.oi_signal}</td>"
        f"<td>{r.combo_score:.4f}</td>"
        f"<td>{r.final_decision}</td>"
        f"<td>{r.entry_time if r.entry_time else '—'}</td>"
        f"<td>{r.entry_price if r.entry_price is not None else '—'}</td>"
        f"<td>{r.entry_qty}</td>"
        f"<td>{'Activated' if r.breakeven_activated else 'Not Activated'}</td>"
        f"<td>{'Activated' if r.profit_locking_activated else 'Not Activated'}</td>"
        f"<td>{'Activated' if r.trailing_stop_activated else 'Not Activated'}</td>"
        f"<td>{r.current_active_stop_loss_level if r.current_active_stop_loss_level is not None else '—'}</td>"
        f"<td>{r.current_trailing_stop_level if r.current_trailing_stop_level is not None else '—'}</td>"
        f"<td>{r.final_exit_time if r.final_exit_time else '—'}</td>"
        f"<td>{r.final_exit_price if r.final_exit_price is not None else '—'}</td>"
        f"<td>{r.final_exit_reason if r.final_exit_reason else '—'}</td>"
        f"<td>{r.pnl if r.pnl is not None else '—'}</td>"
        f"<td>{r.roi_pct if r.roi_pct is not None else '—'}</td>"
        f"<td>{r.hit if r.hit else '—'}</td>"
        "</tr>"
        for r in top_rows
    )
    public_html = f"""<!doctype html>
<html>
<head>
  <meta charset="utf-8">
  <meta name="viewport" content="width=device-width, initial-scale=1">
  <title>Futures Backtest Report</title>
  <style>
    body {{ font-family: Arial, sans-serif; margin: 20px; color: #0f172a; }}
    h1 {{ margin-bottom: 8px; }}
    .sub {{ color: #475569; margin-bottom: 16px; }}
    .summary {{ background:#f8fafc; border:1px solid #e2e8f0; border-radius:8px; padding:12px 14px; margin-bottom:16px; }}
    .summary-grid {{ display:grid; grid-template-columns:repeat(auto-fit,minmax(220px,1fr)); gap:8px 14px; }}
    .k {{ color:#64748b; font-size:12px; text-transform:uppercase; letter-spacing:.04em; }}
    .v {{ font-weight:700; font-size:15px; }}
    table {{ width:100%; border-collapse: collapse; margin-top: 12px; }}
    th, td {{ border:1px solid #cbd5e1; padding:8px; text-align:left; font-size:13px; }}
    th {{ background:#f1f5f9; }}
    .muted {{ color:#64748b; font-size:12px; margin-top:10px; }}
  </style>
</head>
<body>
  <h1>Futures Backtest Profit Protection Report</h1>
  <div class="sub">Date {summary.get('date')} | Signal {summary.get('signal_time')} | Baseline {summary.get('baseline_time')}</div>
  <div class="summary">
    <div class="summary-grid">
      <div><div class="k">Symbols from arbitrage_master</div><div class="v">{summary.get('symbols_from_arbitrage_master')}</div></div>
      <div><div class="k">Contracts resolved</div><div class="v">{summary.get('contracts_resolved')}</div></div>
      <div><div class="k">Trade count</div><div class="v">{summary.get('trade_count')}</div></div>
      <div><div class="k">Wins</div><div class="v">{summary.get('wins')}</div></div>
      <div><div class="k">Win rate %</div><div class="v">{summary.get('win_rate_pct')}</div></div>
      <div><div class="k">Total PnL</div><div class="v">{summary.get('total_pnl')}</div></div>
      <div><div class="k">Avg ROI %</div><div class="v">{summary.get('avg_roi_pct')}</div></div>
      <div><div class="k">Profit Factor</div><div class="v">{summary.get('profit_factor')}</div></div>
      <div><div class="k">Tier3 Reach %</div><div class="v">{summary.get('pct_reach_tier3')}</div></div>
      <div><div class="k">Selection</div><div class="v">Top {top_n} (LONG/SHORT BUILDUP)</div></div>
    </div>
  </div>
  <h3>Top {top_n} Futures</h3>
  <table>
    <thead>
      <tr>
        <th>Futures Symbol</th>
        <th>CMS</th>
        <th>OI Signal</th>
        <th>CMS+OI score</th>
        <th>Decision</th>
        <th>Entry time</th>
        <th>Entry price</th>
        <th>Position Size</th>
        <th>Tier 1</th>
        <th>Tier 2</th>
        <th>Tier 3</th>
        <th>Active Stop</th>
        <th>Trailing Stop</th>
        <th>Exit time</th>
        <th>Exit price</th>
        <th>Exit reason</th>
        <th>PnL</th>
        <th>ROI%</th>
        <th>Result</th>
      </tr>
    </thead>
    <tbody>{rows_html if rows_html else '<tr><td colspan="18">No selected trades.</td></tr>'}</tbody>
  </table>
  <div class="muted">This file is overwritten on every futures_backtester execution.</div>
</body>
</html>"""
    public_html_path.write_text(public_html, encoding="utf-8")
    print(f"Wrote: {csv_path}, {js_path}, {md_path}, {html_path}")
    print(f"Wrote public report: {public_html_path}")


def main() -> None:
    p = argparse.ArgumentParser(description="SmartFuture futures-only backtester")
    p.add_argument("--date", required=True, help="YYYY-MM-DD")
    p.add_argument("--time", required=True, help="HH:MM:SS")
    p.add_argument("--baseline", required=True, help="HH:MM:SS")
    p.add_argument("--top-n", type=int, default=5, help="Top N buildup symbols to trade by combo score")
    args = p.parse_args()
    rows, summary = run(args)
    write_outputs(rows, summary, args.date, args.time)
    print(json.dumps(summary, indent=2))


if __name__ == "__main__":
    main()
