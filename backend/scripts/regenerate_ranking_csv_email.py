#!/usr/bin/env python3
"""
Rebuild ranking CSV + trigger ChartInk ranking email (same as webhook pipeline).

Parses today's "Parsing stocks: ... value='A,B,C'" lines from scan_st1_algo.log,
builds minimal enriched rows for the ranker, writes logs/scan_rankings/*.csv + JSON,
and sends the CSV to tradentical@gmail.com (async SMTP).

  PYTHONPATH=/home/ubuntu/trademanthan python3 backend/scripts/regenerate_ranking_csv_email.py

  python3 backend/scripts/regenerate_ranking_csv_email.py --symbols "A,B,C" --option-type PE
"""

from __future__ import annotations

import argparse
import re
import sys
from datetime import date
from pathlib import Path

ROOT = Path(__file__).resolve().parent.parent.parent
if str(ROOT) not in sys.path:
    sys.path.insert(0, str(ROOT))

import backend.env_bootstrap  # noqa: F401


def _symbols_from_log(log_path: Path) -> str:
    if not log_path.is_file():
        return ""
    text = log_path.read_text(encoding="utf-8", errors="ignore")
    today_str = date.today().strftime("%Y-%m-%d")
    pat = re.compile(r"value='([^']+)'")
    seen: list[str] = []
    for line in text.splitlines():
        if not line.startswith(today_str):
            continue
        if "Parsing stocks" not in line or "value=" not in line:
            continue
        m = pat.search(line)
        if not m:
            continue
        for s in m.group(1).split(","):
            s = s.strip().upper()
            if s and s not in seen:
                seen.append(s)
    return ",".join(seen)


def _minimal_enriched(symbol: str, option_type: str) -> dict:
    """Synthetic enriched row (bearish PE: ltp < vwap)."""
    sym = symbol.strip().upper()
    ltp = 500.0 + (hash(sym) % 200) / 10.0
    vwap = ltp + (15.0 if option_type.upper() == "PE" else -15.0)
    return {
        "stock_name": sym,
        "trigger_price": ltp,
        "last_traded_price": ltp,
        "stock_vwap": vwap,
        "stock_vwap_previous_hour": None,
        "stock_vwap_previous_hour_time": None,
        "option_type": option_type.upper(),
        "option_contract": f"{sym}{option_type.upper()[:1]}E",
        "otm1_strike": round(ltp / 100) * 100,
        "option_ltp": 8.0 + (hash(sym) % 50) / 10.0,
        "option_vwap": 0.0,
        "qty": 250 + (hash(sym) % 400),
        "instrument_key": f"NSE_FO|{sym}",
        "option_candles": [],
        "_enrichment_failed": False,
        "_enrichment_error": None,
    }


def main() -> int:
    p = argparse.ArgumentParser()
    p.add_argument("--symbols", default="", help="Comma-separated (default: today's webhook symbols from log)")
    p.add_argument(
        "--log",
        default="/home/ubuntu/trademanthan/logs/scan_st1_algo.log",
        help="scan_st1_algo.log path",
    )
    p.add_argument("--option-type", default="PE", choices=("CE", "PE"))
    p.add_argument("--no-email", action="store_true", help="CSV/JSON only")
    args = p.parse_args()

    syms = args.symbols.strip() or _symbols_from_log(Path(args.log))
    if not syms:
        print(
            "No symbols: use --symbols A,B or ensure today's log has 'Parsing stocks' lines.",
            file=sys.stderr,
        )
        return 1

    stocks = [_minimal_enriched(s.strip(), args.option_type) for s in syms.split(",") if s.strip()]
    meta = {
        "triggered_at_display": date.today().strftime("%Y-%m-%d") + " (regenerated)",
        "scan_name": "Webhook symbols replay — ranking CSV + email",
        "alert_name": "regenerate_ranking_csv_email.py",
    }
    try:
        from services.stock_ranker import export_full_ranking_only
    except ImportError:
        from backend.services.stock_ranker import export_full_ranking_only

    export_full_ranking_only(
        stocks,
        alert_type=args.option_type,
        export_meta=meta,
        send_email=not args.no_email,
    )
    print(f"OK: ranked {len(stocks)} symbols → logs/scan_rankings/ (email={'on' if not args.no_email else 'off'})")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
