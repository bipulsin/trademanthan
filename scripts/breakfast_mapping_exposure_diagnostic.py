#!/usr/bin/env python3
"""Read-only: Breakfast Strategy NSE sector-mapping change exposure diagnostic.

Queries breakfast_strategy_trades (mode=backtest_oos_spot) and scores mapping
exposure for the 8 affected months. Sector ranking is NOT recomputed — only
stock-level candidate-pool effects within an already-selected sector.

Run on prod:
  ./scripts/paperclip-ssh.sh 'cd /home/ubuntu/twcto && docker exec twcto-app-1 python /app/scripts/breakfast_mapping_exposure_diagnostic.py'
"""
from __future__ import annotations

import json
import sys
from collections import Counter, defaultdict
from datetime import date
from pathlib import Path
from typing import Any, Dict, List, Set, Tuple

ROOT = Path(__file__).resolve().parents[1]
if str(ROOT) not in sys.path:
    sys.path.insert(0, str(ROOT))

from sqlalchemy import text

from backend.database import SessionLocal
from backend.services.breakfast_strategy.history import load_history

AFFECTED_MONTHS = [
    "2025-09",
    "2025-10",
    "2025-11",
    "2025-12",
    "2026-01",
    "2026-02",
    "2026-03",
    "2026-04",
]
CONTROL_MONTH = "2026-05"
MODE = "backtest_oos_spot"

RECLASSIFIED: Dict[str, Tuple[str, str]] = {
    "BHARTIARTL": ("SERVICES", "SERVICES"),
    "ADANIENT": ("ENERGY", "METAL"),
    "AUROPHARMA": ("HEALTHCARE", "HEALTHCARE"),
    "DIVISLAB": ("HEALTHCARE", "HEALTHCARE"),
    "DRREDDY": ("HEALTHCARE", "HEALTHCARE"),
    "GLENMARK": ("HEALTHCARE", "HEALTHCARE"),
    "LAURUSLABS": ("HEALTHCARE", "HEALTHCARE"),
    "LUPIN": ("HEALTHCARE", "HEALTHCARE"),
    "SUNPHARMA": ("HEALTHCARE", "HEALTHCARE"),
    "TORNTPHARM": ("HEALTHCARE", "HEALTHCARE"),
    "ZYDUSLIFE": ("HEALTHCARE", "HEALTHCARE"),
    "APOLLOHOSP": ("HEALTHCARE", "HEALTHCARE"),
    "MAXHEALTH": ("HEALTHCARE", "HEALTHCARE"),
    "IDEA": ("SERVICES", "SERVICES"),
    "INDUSTOWER": ("SERVICES", "SERVICES"),
    "INDIGO": ("SERVICES", "SERVICES"),
    "INDHOTEL": ("SERVICES", "SERVICES"),
    "SWIGGY": ("SERVICES", "SERVICES"),
}

ADDED: Dict[str, str] = {
    "WIPRO": "IT",
    "RADICO": "FMCG",
    "ANGELONE": "FINANCIALS",
    "MOTILALOFS": "FINANCIALS",
    "NAM-INDIA": "FINANCIALS",
    "GVT&D": "ENERGY",
}

ALL_AFFECTED_SYMBOLS: Set[str] = set(RECLASSIFIED) | set(ADDED)
CROSS_SECTOR = {sym: pair for sym, pair in RECLASSIFIED.items() if pair[0] != pair[1]}
SERVICES_SUBPOOL = {sym for sym, (o, n) in RECLASSIFIED.items() if o == n == "SERVICES"}
HEALTHCARE_SAME_LABEL = {sym for sym, (o, n) in RECLASSIFIED.items() if o == n == "HEALTHCARE"}


def session_days_from_history() -> Dict[str, int]:
    doc = load_history()
    out: Dict[str, int] = {}
    for m in doc.get("months") or []:
        pl = str(m.get("period_label") or "")
        summary = m.get("summary") or {}
        sd = summary.get("session_days")
        if pl and isinstance(sd, int):
            out[pl] = sd
    return out


def load_trades() -> List[Dict[str, Any]]:
    db = SessionLocal()
    try:
        rows = db.execute(
            text(
                """
                SELECT session_date, symbol, underlying_symbol, direction, sector,
                       sector_index, stock_move_pct_at_entry, mode, period_label
                FROM breakfast_strategy_trades
                WHERE mode = :mode
                ORDER BY session_date, underlying_symbol
                """
            ),
            {"mode": MODE},
        ).mappings().all()
        return [dict(r) for r in rows]
    finally:
        db.close()


def period_label(sd: date) -> str:
    return f"{sd.year:04d}-{sd.month:02d}"


def near_miss_reasons(day_trades: List[Dict[str, Any]]) -> List[Tuple[str, str, str]]:
    syms = {str(t["symbol"]).upper() for t in day_trades}
    secs = {str(t.get("sector") or "").upper() for t in day_trades}
    reasons: List[Tuple[str, str, str]] = []
    for sym, (old_sec, new_sec) in CROSS_SECTOR.items():
        if sym not in syms and new_sec.upper() in secs:
            reasons.append(("cross_sector", sym, f"{old_sec}->{new_sec}"))
    for sym, new_sec in ADDED.items():
        if sym not in syms and new_sec.upper() in secs:
            reasons.append(("added_pool", sym, new_sec))
    if "SERVICES" in secs:
        for sym in SERVICES_SUBPOOL:
            if sym not in syms:
                reasons.append(("services_subpool", sym, "SERVICES"))
    return reasons


def analyze_month(
    trades: List[Dict[str, Any]],
    period: str,
    session_days: Dict[str, int],
) -> Dict[str, Any]:
    month_trades = [t for t in trades if t["period"] == period]
    n_trades = len(month_trades)
    trade_days = {t["session_date"] for t in month_trades}
    n_session = session_days.get(period) or len(trade_days)

    direct = [t for t in month_trades if t["symbol"] in ALL_AFFECTED_SYMBOLS]
    direct_days = {t["session_date"] for t in direct}

    by_day: Dict[date, List[Dict[str, Any]]] = defaultdict(list)
    for t in month_trades:
        by_day[t["session_date"]].append(t)

    near_days: Set[date] = set()
    near_type_counts: Counter = Counter()
    for sd, dtr in by_day.items():
        rs = near_miss_reasons(dtr)
        if rs:
            near_days.add(sd)
            for typ, sym, _detail in rs:
                near_type_counts[(typ, sym)] += 1

    exposed_days = direct_days | near_days
    combined_pct = (len(exposed_days) / n_session * 100) if n_session else 0.0
    decision = "candidate re-run" if combined_pct >= 30 else "pass-with-disclosure"

    return {
        "period_label": period,
        "session_days": n_session,
        "trade_active_days": len(trade_days),
        "total_trades": n_trades,
        "direct_trades": len(direct),
        "direct_trade_pct": round(len(direct) / n_trades * 100, 1) if n_trades else 0.0,
        "direct_days": len(direct_days),
        "direct_day_pct": round(len(direct_days) / n_session * 100, 1) if n_session else 0.0,
        "near_miss_days": len(near_days),
        "near_miss_day_pct": round(len(near_days) / n_session * 100, 1) if n_session else 0.0,
        "combined_exposed_days": len(exposed_days),
        "combined_exposure_pct": round(combined_pct, 1),
        "decision": decision,
        "borderline": 25 <= combined_pct <= 35,
        "direct_symbols": sorted({t["symbol"] for t in direct}),
        "near_miss_types": {f"{k[0]}:{k[1]}": v for k, v in near_type_counts.items()},
    }


def main() -> int:
    raw = load_trades()
    if not raw:
        print("ERROR: no trades", file=sys.stderr)
        return 1

    session_days = session_days_from_history()
    trades: List[Dict[str, Any]] = []
    for r in raw:
        sd = r["session_date"]
        if not isinstance(sd, date):
            sd = date.fromisoformat(str(sd)[:10])
        sym = str(r.get("underlying_symbol") or r.get("symbol") or "").upper()
        trades.append(
            {
                "session_date": sd,
                "period": period_label(sd),
                "symbol": sym,
                "sector": str(r.get("sector") or "").upper(),
                "move_pct": r.get("stock_move_pct_at_entry"),
            }
        )

    results = [analyze_month(trades, pl, session_days) for pl in AFFECTED_MONTHS]
    control = analyze_month(trades, CONTROL_MONTH, session_days)

    print(f"Loaded {len(trades)} trades | mode={MODE} | period_label mostly NULL in DB")
    print()

    hdr = (
        "| Month | Sess days | Trades | Direct tr | Direct % | Direct days | "
        "Near-miss days | Near % | Combined days | Combined % | Decision |"
    )
    sep = "|---|---:|---:|---:|---:|---:|---:|---:|---:|---:|---|"
    print("## Mapping exposure (denominator = history session_days)")
    print(hdr)
    print(sep)
    for r in results:
        bl = " ⚠️" if r["borderline"] else ""
        print(
            f"| {r['period_label']} | {r['session_days']} | {r['total_trades']} | "
            f"{r['direct_trades']} | {r['direct_trade_pct']}% | {r['direct_days']} ({r['direct_day_pct']}%) | "
            f"{r['near_miss_days']} | {r['near_miss_day_pct']}% | "
            f"{r['combined_exposed_days']} | {r['combined_exposure_pct']}%{bl} | {r['decision']} |"
        )

    r = control
    print(
        f"| {r['period_label']} (ctrl) | {r['session_days']} | {r['total_trades']} | "
        f"{r['direct_trades']} | {r['direct_trade_pct']}% | {r['direct_days']} | "
        f"{r['near_miss_days']} | {r['near_miss_day_pct']}% | "
        f"{r['combined_exposed_days']} | {r['combined_exposure_pct']}% | {r['decision']} |"
    )

    flagged = [r["period_label"] for r in results if r["decision"] == "candidate re-run"]
    borderline = [r["period_label"] for r in results if r["borderline"]]
    est = 100
    print()
    print(f"Flagged (≥30%): {', '.join(flagged) or 'none'}")
    print(f"Borderline (25–35%): {', '.join(borderline) or 'none'}")
    print(f"Cost est all 8 @ ~{est} min: ~{8 * est} min ({8 * est / 60:.1f} h)")
    print(f"Cost est flagged ({len(flagged)}): ~{len(flagged) * est} min")
    print()
    print(json.dumps({"affected": results, "control": control}, indent=2))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
