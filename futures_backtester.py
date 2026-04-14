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
    final_decision: str
    entry_price_1330: Optional[float]
    exit_price_1530: Optional[float]
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
    day_1m = candles_for_day(sort_candles(raw_1m), session_d)
    if day_1m:
        f1.write_text(json.dumps(day_1m), encoding="utf-8")
        return day_1m, "1m"

    raw_5m = ux.get_historical_candles_by_instrument_key(ci.instrument_key, "minutes/5", 0, range_end_date=session_d) or []
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

    for sym in symbols:
        ci = contracts.get(sym)
        if not ci:
            failures.append({"symbol": sym, "error": "no_front_month_future"})
            continue
        try:
            candles, tf = fetch_futures_day_candles(ux, ci, sd, cache_dir)
            if not candles:
                raise ValueError("no_candles_for_day")
            c0915 = find_candle(candles, sd, bh, bm)
            c1330 = find_candle(candles, sd, th, tm)
            c1530 = find_candle(candles, sd, eh, em)
            if not c0915 or not c1330 or not c1530:
                raise ValueError("missing_baseline_or_signal_or_exit_candle")
            if c0915.get("oi") is None or c1330.get("oi") is None:
                raise ValueError("missing_oi_for_baseline_or_signal")

            upto_signal = [c for c in candles if (parse_dt_ist(c.get("timestamp")) or datetime.min.replace(tzinfo=IST)).time() <= dt_time(th, tm)]
            post = [c for c in candles if (parse_dt_ist(c.get("timestamp")) or datetime.min.replace(tzinfo=IST)).time() >= dt_time(th, tm)]

            cms_score, cms_dir, cms_tier = cms_from_futures(upto_signal, c0915, c1330)
            p0, p1 = float(c0915["close"]), float(c1330["close"])
            oi0, oi1 = float(c0915["oi"]), float(c1330["oi"])
            price_pct = ((p1 - p0) / p0 * 100.0) if p0 else 0.0
            oi_pct = ((oi1 - oi0) / oi0 * 100.0) if oi0 else 0.0
            oi_sig = oi_signal_rules(price_pct, oi_pct)
            dec = final_decision(cms_dir, oi_sig)
            atr = atr14(upto_signal)
            entry, exit_close = p1, float(c1530["close"] or p1)
            pnl, roi, hit = evaluate_outcome(dec, entry, exit_close, ci.lot_size, post, atr)

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
                    final_decision=dec,
                    entry_price_1330=round(entry, 4) if dec.startswith("ENTER") else None,
                    exit_price_1530=round(exit_close, 4) if dec.startswith("ENTER") else None,
                    pnl=pnl,
                    roi_pct=roi,
                    hit=hit,
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
                    final_decision="NO_TRADE",
                    entry_price_1330=None,
                    exit_price_1530=None,
                    pnl=None,
                    roi_pct=None,
                    hit="NA",
                    lot_size=ci.lot_size if ci else 1,
                    error=str(e),
                )
            )

    trades = [r for r in rows if r.final_decision in {"ENTER_LONG", "ENTER_SHORT"} and r.pnl is not None]
    wins = [r for r in trades if (r.pnl or 0) > 0]
    summary = {
        "date": args.date,
        "signal_time": args.time,
        "baseline_time": args.baseline,
        "symbols_from_arbitrage_master": len(symbols),
        "contracts_resolved": len(contracts),
        "rows_generated": len(rows),
        "trade_count": len(trades),
        "wins": len(wins),
        "win_rate_pct": round((len(wins) / len(trades) * 100.0), 2) if trades else 0.0,
        "total_pnl": round(sum(float(r.pnl or 0.0) for r in trades), 2),
        "avg_roi_pct": round(sum(float(r.roi_pct or 0.0) for r in trades) / len(trades), 4) if trades else 0.0,
        "failures": failures[:200],
    }
    return rows, summary


def write_outputs(rows: List[BacktestRow], summary: Dict[str, Any], d: str, t: str) -> None:
    OUT_ROOT.mkdir(parents=True, exist_ok=True)
    t_compact = t.replace(":", "")[:4]
    csv_path = Path(f"futures_backtest_{d}_{t_compact}.csv")
    js_path = Path(f"futures_backtest_summary_{d}.json")
    md_path = Path(f"futures_backtest_report_{d}.md")
    html_path = Path(f"futures_backtest_dashboard_{d}.html")

    with csv_path.open("w", newline="", encoding="utf-8") as f:
        w = csv.DictWriter(f, fieldnames=list(asdict(rows[0]).keys()))
        w.writeheader()
        for r in rows:
            w.writerow(asdict(r))

    js_path.write_text(json.dumps(summary, indent=2), encoding="utf-8")
    md_path.write_text(
        "\n".join(
            [
                f"# Futures Backtest Report ({d})",
                "",
                f"- Signal time: `{summary['signal_time']}`",
                f"- Symbols from arbitrage_master: `{summary['symbols_from_arbitrage_master']}`",
                f"- Contracts resolved: `{summary['contracts_resolved']}`",
                f"- Trades: `{summary['trade_count']}`",
                f"- Win rate: `{summary['win_rate_pct']}%`",
                f"- Total PnL: `{summary['total_pnl']}`",
                f"- Avg ROI: `{summary['avg_roi_pct']}%`",
            ]
        ),
        encoding="utf-8",
    )
    html_path.write_text(
        f"""<!doctype html><html><head><meta charset='utf-8'><title>Futures Backtest {d}</title></head>
<body><h2>Futures Backtest Dashboard ({d})</h2>
<pre>{json.dumps(summary, indent=2)}</pre>
<p>CSV: {csv_path.name}</p></body></html>""",
        encoding="utf-8",
    )
    print(f"Wrote: {csv_path}, {js_path}, {md_path}, {html_path}")


def main() -> None:
    p = argparse.ArgumentParser(description="SmartFuture futures-only backtester")
    p.add_argument("--date", required=True, help="YYYY-MM-DD")
    p.add_argument("--time", required=True, help="HH:MM:SS")
    p.add_argument("--baseline", required=True, help="HH:MM:SS")
    args = p.parse_args()
    rows, summary = run(args)
    write_outputs(rows, summary, args.date, args.time)
    print(json.dumps(summary, indent=2))


if __name__ == "__main__":
    main()
