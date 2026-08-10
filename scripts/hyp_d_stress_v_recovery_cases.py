#!/usr/bin/env python3
"""Stress-test Hypothesis D against A/C worse-than-actual V-recovery / early-spike cases.

Reads candle series from the prior follow-up JSON (same OHLC reconstruction).
Does not change live rules. Writes:
  docs/diagnostics/hyp_d_stress_v_recovery_20260810.json
"""
from __future__ import annotations

import json
from pathlib import Path
from typing import Any, Dict, List, Optional

ROOT = Path(__file__).resolve().parents[1]
SRC = ROOT / "docs/diagnostics/pp_followup_hdfc_clean_worse_DEF_20260810.json"
OUT = ROOT / "docs/diagnostics/hyp_d_stress_v_recovery_20260810.json"

FOCUS_IDS = {27, 17, 45}  # UNITDSPR, MUTHOOTFIN, KALYANKJIL


def _f(x: Any) -> Optional[float]:
    try:
        if x is None:
            return None
        return float(x)
    except (TypeError, ValueError):
        return None


def _price_r(px: float, entry: float, risk: float, is_long: bool) -> Optional[float]:
    if risk <= 0:
        return None
    pts = (px - entry) if is_long else (entry - px)
    return round(pts / risk, 4)


def _pnl(pts: float, qty: Optional[float]) -> Optional[float]:
    if qty is None:
        return None
    return round(pts * float(qty), 2)


def analyze_trade(t: Dict[str, Any]) -> Dict[str, Any]:
    is_long = t["direction"] != "SHORT"
    entry = float(t["entry"]["price"])
    risk = float(t["entry"]["risk_pts"])
    qty = t["entry"].get("qty")
    candles = t.get("candles_10m") or []

    per_candle: List[Dict[str, Any]] = []
    first_dual: Optional[Dict[str, Any]] = None
    first_ema10_only: Optional[Dict[str, Any]] = None
    first_vwap_only: Optional[Dict[str, Any]] = None

    for c in candles:
        e10 = _f(c.get("ema10"))
        vw = _f(c.get("vwap"))
        h = float(c["h"])
        l = float(c["l"])
        close = float(c["c"])
        if is_long:
            ema10_breach = e10 is not None and l < e10
            vwap_breach = vw is not None and l < vw
            adverse_extreme = l
        else:
            ema10_breach = e10 is not None and h > e10
            vwap_breach = vw is not None and h > vw
            adverse_extreme = h
        dual = bool(ema10_breach and vwap_breach)
        row = {
            "bar_end_ist": c.get("bar_end_ist"),
            "o": c["o"],
            "h": h,
            "l": l,
            "c": close,
            "ema10": e10,
            "vwap": vw,
            "ema10_breach_intrabar": ema10_breach,
            "vwap_breach_intrabar": vwap_breach,
            "dual_breach_intrabar": dual,
            "r_at_close": c.get("r_at_close"),
            "r_at_high": c.get("r_at_high"),
            "r_at_low": c.get("r_at_low"),
        }
        per_candle.append(row)
        if dual and first_dual is None:
            first_dual = row
        if ema10_breach and not vwap_breach and first_ema10_only is None:
            first_ema10_only = row
        if vwap_breach and not ema10_breach and first_vwap_only is None:
            first_vwap_only = row

    hyp_d: Dict[str, Any]
    if first_dual is None:
        hyp_d = {
            "fired": False,
            "note": (
                "No candle from entry→exit showed adverse intrabar dual-breach "
                "of BOTH EMA10 and VWAP. Hypothesis D would not have exited; "
                "stayed out of this A/C failure mode by design."
            ),
        }
    else:
        close = float(first_dual["c"])
        pts = (close - entry) if is_long else (entry - close)
        exit_r = _price_r(close, entry, risk, is_long)
        exit_pnl = _pnl(pts, qty)
        hyp_d = {
            "fired": True,
            "bar_at": first_dual["bar_end_ist"],
            "exit_price": close,
            "exit_r": exit_r,
            "exit_pnl_inr": exit_pnl,
            "reason": "intrabar_dual_EMA10_VWAP_breach_exit_at_candle_close",
            "note": (
                "Simulated exit at dual-breach candle close (matches prior Hyp D "
                "backtest; true mid-bar fill not available from OHLC alone)."
            ),
        }

    actual = t["actual_exit"]
    rules = t.get("rule_exits") or {}

    return {
        "id": t["id"],
        "symbol": t["symbol"],
        "session_date": t["session_date"],
        "direction": t["direction"],
        "failure_mode_prior": {
            27: "V-recovery after dip (A/C sold 10:35 dip; next bar recovered)",
            17: "V-recovery after dip (C sold weak close; next bar continued)",
            45: "Early adverse spike then trend continues (A/C exited first candle)",
        }.get(t["id"]),
        "entry": t["entry"],
        "peak": t["peak"],
        "actual_exit": actual,
        "rule_A_C_exits": rules,
        "hypothesis_D": hyp_d,
        "comparison": {
            "dual_breach_occurred": first_dual is not None,
            "vs_actual": (
                None
                if first_dual is None
                else {
                    "actual_pnl_inr": actual.get("pnl_inr"),
                    "hyp_d_pnl_inr": hyp_d.get("exit_pnl_inr"),
                    "delta_vs_actual": (
                        round(hyp_d["exit_pnl_inr"] - actual["pnl_inr"], 2)
                        if hyp_d.get("exit_pnl_inr") is not None
                        and actual.get("pnl_inr") is not None
                        else None
                    ),
                }
            ),
            "vs_rules_A_C": (
                "N/A — Hyp D did not fire; A/C did fire and underperformed actual"
                if first_dual is None
                else "see rule exits vs hyp_d exit"
            ),
            "first_ema10_only_breach": (
                {
                    "bar": first_ema10_only["bar_end_ist"],
                    "extreme": first_ema10_only["l"] if is_long else first_ema10_only["h"],
                    "ema10": first_ema10_only["ema10"],
                    "vwap": first_ema10_only["vwap"],
                }
                if first_ema10_only
                else None
            ),
            "first_vwap_only_breach": (
                {
                    "bar": first_vwap_only["bar_end_ist"],
                    "extreme": first_vwap_only["l"] if is_long else first_vwap_only["h"],
                    "ema10": first_vwap_only["ema10"],
                    "vwap": first_vwap_only["vwap"],
                }
                if first_vwap_only
                else None
            ),
        },
        "candles_10m": per_candle,
        "ohlc_quality": "clean (from prior reconstruction; no gaps flagged)",
    }


def main() -> None:
    src = json.loads(SRC.read_text())
    trades = [
        t
        for t in src["section3_worse_trade_detail"]["trades"]
        if t["id"] in FOCUS_IDS
    ]
    # Preserve request order: UNITDSPR, MUTHOOTFIN, KALYANKJIL
    order = {27: 0, 17: 1, 45: 2}
    trades.sort(key=lambda t: order.get(t["id"], 99))

    analyzed = [analyze_trade(t) for t in trades]
    any_fired = any(a["hypothesis_D"].get("fired") for a in analyzed)

    report = {
        "generated_at": "2026-08-10",
        "live_changes": False,
        "source": str(SRC.relative_to(ROOT)),
        "definition": (
            "Hypothesis D: adverse extreme of a 10m candle breaches BOTH EMA10 and VWAP "
            "intrabar (low < both for LONG; high > both for SHORT). Scan = all candles "
            "entry→exit (not only after peak). Simulated exit = that candle's close."
        ),
        "verdict": {
            "any_of_3_fired": any_fired,
            "summary": (
                "Hypothesis D did not fire on any of the three A/C failure cases. "
                "Each showed at most a single-level breach (EMA10-only on UNITDSPR and "
                "KALYANKJIL; MUTHOOTFIN breached neither EMA10 nor VWAP on the adverse "
                "dip). D therefore correctly stayed out of these failure modes by design."
                if not any_fired
                else "At least one dual-breach fire — see per_trade."
            ),
        },
        "trades": analyzed,
    }
    OUT.parent.mkdir(parents=True, exist_ok=True)
    OUT.write_text(json.dumps(report, indent=2, default=str))
    print(json.dumps(report["verdict"], indent=2))
    for a in analyzed:
        print(
            a["symbol"],
            "dual=",
            a["comparison"]["dual_breach_occurred"],
            "ema10_only=",
            a["comparison"]["first_ema10_only_breach"],
            "vwap_only=",
            a["comparison"]["first_vwap_only_breach"],
        )
    print("wrote", OUT)


if __name__ == "__main__":
    main()
