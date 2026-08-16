"""Forensic audit: ASHOKLEY (ASHOKLEYLAND) trade on 2026-08-14.

Reconstructs entry sizing, ATR levels, bar-by-bar price action, and ML
meta-feature state from cached 5m candles + walk-forward meta scores.

Usage:
  PYTHONPATH=. python -m rocket.diagnostics.audit_ashokley
  PYTHONPATH=. python -m rocket.diagnostics.audit_ashokley --start-date 2026-08-01 --end-date 2026-08-14
"""

from __future__ import annotations

import argparse
import json
from datetime import date, datetime
from pathlib import Path
from typing import Any, Dict, List, Optional, Tuple

import numpy as np
import pandas as pd
import pytz
from rich.console import Console
from rich.table import Table

from rocket.config.settings import get_settings
from rocket.engine.backtester import RocketBacktester, enrich_features
from rocket.engine.costs import FuturesCostModel
from rocket.ml.feature_extractor import RocketFeatureExtractor
from rocket.ml.meta_filter import MetaModelConfig, RocketMetaFilter
from rocket.ml.pipeline import harvest_raw_signals
from rocket.ml.trade_selector import (
    TIER1_PROB,
    TIER2_PROB,
    apply_tiered_sizing,
)
from rocket.strategies.ml_institutional import MLInstitutionalStrategy

IST = pytz.timezone("Asia/Kolkata")
console = Console()

SYMBOL_ALIASES = ("ASHOKLEY", "ASHOKLEYLAND")
STOP_ATR = 1.8
TARGET_ATR = 3.2
TRADE_DATE = "2026-08-14"


def _parse_date(value: str) -> date:
    return datetime.strptime(value, "%Y-%m-%d").date()


def _resolve_symbol(bt: RocketBacktester) -> str:
    by_upper = {c.symbol.upper(): c.symbol for c in bt.contracts}
    for alias in SYMBOL_ALIASES:
        if alias.upper() in by_upper:
            return by_upper[alias.upper()]
    for sym in by_upper:
        if "ASHOK" in sym:
            return by_upper[sym]
    raise SystemExit(
        f"ASHOKLEY not in universe ({len(bt.contracts)} contracts). "
        "Check arbitrage_master / --limit."
    )


def _to_ist(ts: Any) -> pd.Timestamp:
    t = pd.Timestamp(ts)
    if t.tzinfo is None:
        return t.tz_localize(IST)
    return t.tz_convert(IST)


def _find_trade_from_html(html_path: Path) -> Optional[Dict[str, Any]]:
    if not html_path.exists():
        return None
    import re

    html = html_path.read_text(encoding="utf-8")
    pat = (
        r"<tr class='[^']*'><td>(ASHOKLEY(?:LAND)?)</td><td>(BUY|SELL)</td>"
        r"<td class='num'>([^<]+)</td><td class='num'>([^<]+)</td>"
        r"<td class='num'>([^<]+)</td><td>([^<]*2026-08-14[^<]*)</td>"
        r"<td>([^<]+)</td><td class='num'>([^<]+)</td>"
        r"<td class='num'>([^<]+)</td><td>([^<]+)</td></tr>"
    )
    m = re.search(pat, html, flags=re.I)
    if not m:
        return None
    return {
        "symbol": m.group(1),
        "side": m.group(2).upper(),
        "qty": int(float(m.group(3).replace(",", ""))),
        "entry": float(m.group(4).replace(",", "")),
        "exit": float(m.group(5).replace(",", "")),
        "entry_time": m.group(6),
        "exit_time": m.group(7),
        "pnl": float(m.group(8).replace(",", "")),
        "costs": float(m.group(9).replace(",", "")),
        "reason": m.group(10),
        "source": "rocket.html",
    }


def _locate_entry_idx(df: pd.DataFrame, entry_time: str) -> int:
    target = _to_ist(entry_time)
    ts = pd.to_datetime(df["timestamp"], utc=True).dt.tz_convert(IST)
    # Exact match preferred
    hits = np.where(ts == target)[0]
    if len(hits):
        return int(hits[0])
    # Nearest open-time match
    deltas = (ts - target).abs()
    return int(deltas.argmin())


def _bar_anatomy(
    df: pd.DataFrame,
    entry_idx: int,
    exit_idx: int,
    *,
    entry_price: float,
    stop_loss: float,
    take_profit: float,
    side: str,
) -> List[Dict[str, Any]]:
    start = max(0, entry_idx - 3)
    end = min(len(df) - 1, max(exit_idx, entry_idx) + 1)
    rows: List[Dict[str, Any]] = []
    for i in range(start, end + 1):
        r = df.iloc[i]
        ts = _to_ist(r["timestamp"])
        o, h, l, c = float(r["open"]), float(r["high"]), float(r["low"]), float(r["close"])
        vol = float(r.get("volume") or 0.0)
        tag = []
        if i == entry_idx - 1:
            tag.append("signal")
        if i == entry_idx:
            tag.append("ENTRY")
        if i == exit_idx:
            tag.append("EXIT")
        # Relative to entry
        if i > entry_idx:
            tag.append(f"post+{i - entry_idx}")
        elif i < entry_idx:
            tag.append(f"pre-{entry_idx - i}")

        stop_hit = False
        tp_hit = False
        if side == "BUY":
            stop_hit = l <= stop_loss
            tp_hit = h >= take_profit
            adverse = entry_price - l
            favorable = h - entry_price
        else:
            stop_hit = h >= stop_loss
            tp_hit = l <= take_profit
            adverse = h - entry_price
            favorable = entry_price - l

        rows.append(
            {
                "i": i,
                "timestamp": ts.isoformat(),
                "tag": ",".join(tag) or "—",
                "open": o,
                "high": h,
                "low": l,
                "close": c,
                "volume": vol,
                "stop_hit": stop_hit,
                "tp_hit": tp_hit,
                "adverse_from_entry": adverse if i >= entry_idx else None,
                "favorable_from_entry": favorable if i >= entry_idx else None,
            }
        )
    return rows


def _reconstruct_meta_row(
    bt: RocketBacktester,
    symbol: str,
    side: str,
    entry_time: str,
    start: date,
    end: date,
) -> Tuple[Optional[Dict[str, Any]], Optional[Dict[str, float]], pd.DataFrame]:
    """Harvest + walk-forward score; return selected/sized row near entry + features."""
    bt.series = RocketFeatureExtractor.enrich_universe(bt.series)
    raw = harvest_raw_signals(bt, start, end, min_confidence=0.58)
    if raw.empty:
        return None, None, pd.DataFrame()

    sym_raw = raw[raw["symbol"].astype(str).str.upper() == symbol.upper()].copy()
    meta = RocketMetaFilter(MetaModelConfig(scoring_threshold=0.55))
    scored = meta.score_walk_forward(raw)
    scored_sym = scored[scored["symbol"].astype(str).str.upper() == symbol.upper()].copy()
    if scored_sym.empty:
        return None, None, sym_raw

    entry_ts = _to_ist(entry_time)
    # Signal is typically the bar before fill (next-bar open execution)
    scored_sym = scored_sym.copy()
    scored_sym["_ts"] = pd.to_datetime(scored_sym["timestamp"], utc=True).dt.tz_convert(IST)
    scored_sym["_side"] = scored_sym["side"].astype(str).str.upper()
    cand = scored_sym[scored_sym["_side"] == side.upper()]
    if cand.empty:
        cand = scored_sym

    # Prefer signal timestamp == entry_time - 5m, else nearest before entry
    target_signal = entry_ts - pd.Timedelta(minutes=5)
    cand = cand.copy()
    cand["_dt"] = (cand["_ts"] - target_signal).abs()
    best = cand.sort_values("_dt").iloc[0].to_dict()
    sized = apply_tiered_sizing(best)
    feats = {k: best.get(k) for k in (
        "directional_ema20_dist",
        "directional_vwap_dist",
        "rsi_14",
        "rsi_slope_3",
        "rvol",
        "vol_surge",
        "high_breakout",
        "low_breakout",
        "atr_pct",
        "win_probability",
        "strategy_confidence",
    )}
    return sized or best, feats, scored_sym


def run_audit(
    *,
    start: date,
    end: date,
    interval: str = "5minute",
    limit: int = 200,
    html_path: Optional[Path] = None,
) -> Dict[str, Any]:
    settings = get_settings()
    html_path = html_path or settings.rocket_output_html
    html_trade = _find_trade_from_html(Path(html_path))

    bt = RocketBacktester(interval=interval, max_symbols=limit, time_exit_bars=4)
    bt.load_universe()
    symbol = _resolve_symbol(bt)
    contract = next(c for c in bt.contracts if c.symbol == symbol)
    bt.fetch_data(start, end)
    if symbol not in bt.series:
        raise SystemExit(f"No candle series for {symbol}")

    raw_df = bt.series[symbol].copy()
    df = RocketFeatureExtractor.calculate_indicators(enrich_features(raw_df))

    # Ground-truth trade from tearsheet when available
    if html_trade:
        side = html_trade["side"]
        entry_time = html_trade["entry_time"]
        exit_time = html_trade["exit_time"]
        entry_price = float(html_trade["entry"])
        exit_price = float(html_trade["exit"])
        qty = int(html_trade["qty"])
        pnl = float(html_trade["pnl"])
        costs = float(html_trade["costs"])
        reason = str(html_trade["reason"])
    else:
        raise SystemExit(
            f"Could not find ASHOKLEY 2026-08-14 trade in {html_path}. "
            "Re-run compare-time-exits / backtest first."
        )

    entry_idx = _locate_entry_idx(df, entry_time)
    exit_idx = _locate_entry_idx(df, exit_time)
    signal_idx = max(0, entry_idx - 1)

    # ATR at signal / entry context
    atr = float(df.iloc[signal_idx].get("atr") or df.iloc[signal_idx].get("safe_atr") or 0.0)
    if atr <= 0:
        atr = float(df.iloc[entry_idx].get("atr") or entry_price * 0.005)

    if side == "BUY":
        stop_loss = entry_price - STOP_ATR * atr
        take_profit = entry_price + TARGET_ATR * atr
    else:
        stop_loss = entry_price + STOP_ATR * atr
        take_profit = entry_price - TARGET_ATR * atr

    lots = max(1, int(round(qty / max(1, contract.lot_size))))
    notional = abs(entry_price * qty)

    sized, feats, _ = _reconstruct_meta_row(bt, symbol, side, entry_time, start, end)
    win_p = None
    tier = None
    if sized:
        win_p = float(sized.get("win_probability") or 0.0)
        tier = int(sized.get("tier") or (1 if win_p >= TIER1_PROB else 2))
        # Prefer sized SL/TP if present (same ATR path)
        if sized.get("stop_loss") is not None:
            stop_loss = float(sized["stop_loss"])
        if sized.get("take_profit") is not None:
            take_profit = float(sized["take_profit"])
        if sized.get("atr") is not None and float(sized["atr"]) > 0:
            atr = float(sized["atr"])
        if sized.get("lots") is not None:
            lots = int(sized["lots"])

    bars = _bar_anatomy(
        df,
        entry_idx,
        exit_idx,
        entry_price=entry_price,
        stop_loss=stop_loss,
        take_profit=take_profit,
        side=side,
    )

    # Cost split estimate (entry + exit legs)
    cost_model = FuturesCostModel(settings.rocket_brokerage_per_order)
    slip_per = entry_price * (settings.rocket_slippage_bps / 10_000.0)
    entry_side = "BUY" if side == "BUY" else "SELL"
    exit_side = "SELL" if side == "BUY" else "BUY"
    c_entry = cost_model.compute(
        side=entry_side, price=entry_price, quantity=qty, slippage_rupees=slip_per * qty
    )
    c_exit = cost_model.compute(
        side=exit_side, price=exit_price, quantity=qty, slippage_rupees=slip_per * qty
    )
    cost_est = {
        "entry": c_entry.as_dict(),
        "exit": c_exit.as_dict(),
        "total_est": round(c_entry.total + c_exit.total, 2),
        "tearsheet_costs": costs,
    }

    # Exit mechanism
    exit_bar = df.iloc[exit_idx]
    eh, el, ec = float(exit_bar["high"]), float(exit_bar["low"]), float(exit_bar["close"])
    if side == "BUY":
        intra_stop = el <= stop_loss
        adverse_atr = (entry_price - el) / atr if atr else float("nan")
    else:
        intra_stop = eh >= stop_loss
        adverse_atr = (eh - entry_price) / atr if atr else float("nan")

    pct_move = 100.0 * (exit_price - entry_price) / entry_price * (1 if side == "BUY" else -1)
    bars_held = max(0, exit_idx - entry_idx)

    # False-breakout heuristic
    rvol = float((feats or {}).get("rvol") or df.iloc[signal_idx].get("rvol") or 0.0)
    signal_bar = df.iloc[signal_idx]
    wick_up = float(signal_bar["high"]) - max(float(signal_bar["open"]), float(signal_bar["close"]))
    wick_dn = min(float(signal_bar["open"]), float(signal_bar["close"])) - float(signal_bar["low"])
    trap_notes = []
    if rvol >= 1.5:
        trap_notes.append(f"elevated RVOL={rvol:.2f}")
    if side == "SELL" and wick_dn > wick_up * 1.2:
        trap_notes.append("signal bar had dominant downside wick (short-friendly)")
    if side == "SELL" and bars_held <= 2 and intra_stop:
        trap_notes.append("immediate adverse spike into short stop (squeeze / false breakdown risk)")
    if side == "BUY" and bars_held <= 2 and intra_stop:
        trap_notes.append("immediate adverse dump into long stop (false breakout risk)")

    if reason == "stop_loss" and intra_stop:
        verdict = (
            f"Hard stop triggered on Bar {bars_held} after entry: "
            f"{adverse_atr:.2f}×ATR adverse extreme vs {STOP_ATR}×ATR stop "
            f"on {qty:,} qty ({lots} lot{'s' if lots != 1 else ''}, "
            f"Tier {tier or '?'}). Exit fill {exit_price:.2f} ≈ stop {stop_loss:.2f}."
        )
    elif reason == "stop_loss":
        verdict = (
            f"Recorded as stop_loss but exit bar H/L did not clearly cross stop "
            f"({stop_loss:.2f}); exit={exit_price:.2f} close={ec:.2f} — check slippage/priority."
        )
    else:
        verdict = f"Exit reason={reason} after {bars_held} bars; adverse excursion {adverse_atr:.2f}×ATR."

    if trap_notes:
        verdict += " Trap signals: " + "; ".join(trap_notes) + "."

    report = {
        "symbol": symbol,
        "contract": {
            "instrument_key": contract.instrument_key,
            "futures_symbol": contract.futures_symbol,
            "lot_size": contract.lot_size,
            "tick_size": contract.tick_size,
        },
        "trade": {
            "side": side,
            "entry_time": entry_time,
            "exit_time": exit_time,
            "entry_price": entry_price,
            "exit_price": exit_price,
            "qty": qty,
            "lots": lots,
            "tier": tier,
            "win_probability": win_p,
            "notional": round(notional, 2),
            "pnl": pnl,
            "pct_move_signed": round(pct_move, 4),
            "reason": reason,
            "bars_held": bars_held,
        },
        "levels": {
            "atr_14": round(atr, 6),
            "stop_atr_mult": STOP_ATR,
            "target_atr_mult": TARGET_ATR,
            "stop_loss": round(stop_loss, 4),
            "take_profit": round(take_profit, 4),
            "stop_distance": round(abs(stop_loss - entry_price), 4),
            "adverse_atr_on_exit_bar": round(float(adverse_atr), 4),
            "intra_bar_stop_breach": bool(intra_stop),
        },
        "features": feats,
        "costs": cost_est,
        "bars": bars,
        "verdict": verdict,
    }
    return report


def print_report(report: Dict[str, Any]) -> None:
    t = report["trade"]
    lv = report["levels"]
    c = report["contract"]
    feats = report.get("features") or {}

    console.print("\n[bold cyan]ASHOKLEY Forensic Trade Audit — 2026-08-14[/bold cyan]")
    console.print(
        f"Contract {c['instrument_key']} · lot_size={c['lot_size']} · tick={c['tick_size']}"
    )

    overview = Table(title="Trade Overview", show_header=True)
    overview.add_column("Field")
    overview.add_column("Value", justify="right")
    for k, v in [
        ("Side", t["side"]),
        ("Entry Time", t["entry_time"]),
        ("Exit Time", t["exit_time"]),
        ("Entry Price", f"{t['entry_price']:.2f}"),
        ("Exit Price", f"{t['exit_price']:.2f}"),
        ("Qty", f"{t['qty']:,}"),
        ("Lots", t["lots"]),
        ("Tier", t["tier"]),
        ("Win P", f"{t['win_probability']:.4f}" if t["win_probability"] is not None else "—"),
        ("Notional ₹", f"{t['notional']:,.0f}"),
        ("PnL ₹", f"{t['pnl']:,.2f}"),
        ("% Move (signed)", f"{t['pct_move_signed']:.3f}%"),
        ("Exit Reason", t["reason"]),
        ("Bars Held", t["bars_held"]),
        ("ATR(14)", f"{lv['atr_14']:.4f}"),
        ("Stop (1.8×ATR)", f"{lv['stop_loss']:.4f}"),
        ("Target (3.2×ATR)", f"{lv['take_profit']:.4f}"),
        ("Intra-bar SL breach", str(lv["intra_bar_stop_breach"])),
        ("Adverse ×ATR (exit bar)", f"{lv['adverse_atr_on_exit_bar']:.2f}"),
        ("Tearsheet costs ₹", f"{report['costs']['tearsheet_costs']:,.2f}"),
        ("Est. slip+statutory ₹", f"{report['costs']['total_est']:,.2f}"),
        ("  · slippage est", f"{report['costs']['entry']['slippage'] + report['costs']['exit']['slippage']:,.2f}"),
    ]:
        overview.add_row(k, str(v))
    console.print(overview)

    feat_tbl = Table(title="ML Feature State @ Signal", show_header=True)
    feat_tbl.add_column("Feature")
    feat_tbl.add_column("Value", justify="right")
    for key in (
        "win_probability",
        "strategy_confidence",
        "directional_ema20_dist",
        "directional_vwap_dist",
        "rsi_14",
        "rsi_slope_3",
        "rvol",
        "vol_surge",
        "high_breakout",
        "low_breakout",
        "atr_pct",
    ):
        val = feats.get(key)
        if val is None:
            feat_tbl.add_row(key, "—")
        else:
            try:
                feat_tbl.add_row(key, f"{float(val):.6f}")
            except (TypeError, ValueError):
                feat_tbl.add_row(key, str(val))
    console.print(feat_tbl)

    bars = Table(title="Bar Anatomy (3 pre → exit)", show_header=True)
    for col in ("Tag", "Timestamp", "O", "H", "L", "C", "Vol", "SL?", "Adv", "Fav"):
        bars.add_column(col, justify="right" if col not in ("Tag", "Timestamp") else "left")
    for b in report["bars"]:
        bars.add_row(
            b["tag"],
            b["timestamp"],
            f"{b['open']:.2f}",
            f"{b['high']:.2f}",
            f"{b['low']:.2f}",
            f"{b['close']:.2f}",
            f"{b['volume']:.0f}",
            "YES" if b["stop_hit"] else "",
            "" if b["adverse_from_entry"] is None else f"{b['adverse_from_entry']:.2f}",
            "" if b["favorable_from_entry"] is None else f"{b['favorable_from_entry']:.2f}",
        )
    console.print(bars)

    console.print(f"\n[bold red]Root-Cause Verdict[/bold red]\n{report['verdict']}\n")


def main() -> None:
    ap = argparse.ArgumentParser(description="ASHOKLEY 2026-08-14 forensic audit")
    ap.add_argument("--start-date", default="2026-08-01")
    ap.add_argument("--end-date", default="2026-08-14")
    ap.add_argument("--interval", default="5minute")
    ap.add_argument("--limit", type=int, default=200)
    ap.add_argument("--html", default=None, help="Path to rocket.html tearsheet")
    ap.add_argument("--json-out", default=None, help="Optional JSON report path")
    args = ap.parse_args()

    report = run_audit(
        start=_parse_date(args.start_date),
        end=_parse_date(args.end_date),
        interval=args.interval,
        limit=args.limit,
        html_path=Path(args.html) if args.html else None,
    )
    print_report(report)
    if args.json_out:
        Path(args.json_out).write_text(json.dumps(report, indent=2, default=str), encoding="utf-8")
        console.print(f"[green]Wrote[/green] {args.json_out}")


if __name__ == "__main__":
    main()
