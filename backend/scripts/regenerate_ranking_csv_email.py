#!/usr/bin/env python3
"""
Rebuild ranking CSV + trigger ChartInk ranking email (same as webhook pipeline).

Parses "Parsing stocks: ... value='A,B,C'" lines from scan_st1_algo.log for a given day,
or loads distinct symbols from intraday_stock_options for --date (real LTP/VWAP when present),
builds enriched rows for the ranker, writes logs/scan_rankings/*.csv + JSON,
and sends the CSV to CHARTINK_RANKING_EMAIL / tradentical@gmail.com (async SMTP).

  PYTHONPATH=/home/ubuntu/trademanthan python3 backend/scripts/regenerate_ranking_csv_email.py

  python3 backend/scripts/regenerate_ranking_csv_email.py --symbols "A,B,C" --option-type PE

  # March 23, 2026 — DB first, then log lines dated 2026-03-23
  python3 backend/scripts/regenerate_ranking_csv_email.py --date 2026-03-23
"""

from __future__ import annotations

import argparse
import re
import sys
from datetime import date, datetime, timedelta
from pathlib import Path
from typing import List

ROOT = Path(__file__).resolve().parent.parent.parent
if str(ROOT) not in sys.path:
    sys.path.insert(0, str(ROOT))

import backend.env_bootstrap  # noqa: F401


def _symbols_from_log(log_path: Path, line_date_prefix: str) -> str:
    """line_date_prefix: YYYY-MM-DD — must match start of log lines."""
    if not log_path.is_file():
        return ""
    text = log_path.read_text(encoding="utf-8", errors="ignore")
    pat = re.compile(r"value='([^']+)'")
    seen: list[str] = []
    for line in text.splitlines():
        if not line.startswith(line_date_prefix):
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


def _stocks_from_db(target: date) -> List[dict]:
    """One row per distinct stock_name for target trade_date (latest alert_time wins)."""
    from sqlalchemy import desc

    from backend.database import SessionLocal
    from backend.models.trading import IntradayStockOption

    day_start = datetime.combine(target, datetime.min.time())
    day_end = day_start + timedelta(days=1)
    db = SessionLocal()
    try:
        rows = (
            db.query(IntradayStockOption)
            .filter(
                IntradayStockOption.trade_date >= day_start,
                IntradayStockOption.trade_date < day_end,
            )
            .order_by(desc(IntradayStockOption.alert_time))
            .all()
        )
    finally:
        db.close()

    seen: set[str] = set()
    out: List[dict] = []
    for r in rows:
        sym = (r.stock_name or "").strip().upper()
        if not sym or sym in seen:
            continue
        seen.add(sym)
        out.append(_row_to_enriched(r))
    return out


def _row_to_enriched(r) -> dict:
    """Map DB row to ranker input (same keys as webhook enrichment)."""
    sym = (r.stock_name or "").strip().upper()
    ot = (r.option_type or "").strip().upper() or (
        "CE" if (r.alert_type or "").strip() == "Bullish" else "PE"
    )
    ltp = float(r.stock_ltp or 0)
    vwap = float(r.stock_vwap or 0)
    if ltp <= 0 or vwap <= 0:
        return _minimal_enriched(sym, ot)
    return {
        "stock_name": sym,
        "trigger_price": ltp,
        "last_traded_price": ltp,
        "stock_vwap": vwap,
        "stock_vwap_previous_hour": r.stock_vwap_previous_hour,
        "stock_vwap_previous_hour_time": r.stock_vwap_previous_hour_time,
        "option_type": ot,
        "option_contract": (r.option_contract or f"{sym}{ot[:1]}E"),
        "otm1_strike": round(ltp / 100) * 100,
        "option_ltp": float(r.option_ltp or 0) or 8.0,
        "option_vwap": float(r.option_vwap or 0) or 0.0,
        "qty": int(r.qty or 0) or 150,
        "instrument_key": (r.instrument_key or f"NSE_FO|{sym}"),
        "option_candles": [],
        "_enrichment_failed": False,
        "_enrichment_error": None,
    }


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
    p.add_argument("--symbols", default="", help="Comma-separated (overrides --date / log)")
    p.add_argument(
        "--date",
        default="",
        help="Trade date YYYY-MM-DD (e.g. 2026-03-23): load from DB, else symbols from log for that day",
    )
    p.add_argument(
        "--log",
        default="/home/ubuntu/trademanthan/logs/scan_st1_algo.log",
        help="scan_st1_algo.log path",
    )
    p.add_argument("--option-type", default="PE", choices=("CE", "PE"), help="Used for synthetic rows from --symbols or log fallback")
    p.add_argument("--no-email", action="store_true", help="CSV/JSON only")
    args = p.parse_args()

    if args.date:
        try:
            target_d = datetime.strptime(args.date.strip(), "%Y-%m-%d").date()
        except ValueError:
            print("Invalid --date; use YYYY-MM-DD", file=sys.stderr)
            return 1
    else:
        target_d = date.today()

    date_prefix = target_d.strftime("%Y-%m-%d")
    stocks: List[dict] = []

    if args.symbols.strip():
        stocks = [
            _minimal_enriched(s.strip(), args.option_type)
            for s in args.symbols.split(",")
            if s.strip()
        ]
    else:
        stocks = _stocks_from_db(target_d)
        if not stocks:
            syms = _symbols_from_log(Path(args.log), date_prefix)
            if not syms:
                print(
                    f"No data for {date_prefix}: no DB rows and no 'Parsing stocks' lines in log.",
                    file=sys.stderr,
                )
                return 1
            stocks = [_minimal_enriched(s.strip(), args.option_type) for s in syms.split(",") if s.strip()]

    if not stocks:
        print("No symbols to rank.", file=sys.stderr)
        return 1

    meta = {
        "triggered_at_display": f"{date_prefix} (regenerated)",
        "scan_name": "Webhook / DB replay — ranking CSV + email",
        "alert_name": "regenerate_ranking_csv_email.py",
    }
    # Filename tag: explicit --symbols uses CE/PE; DB/log replay uses date (may mix CE/PE per row)
    export_alert_type = args.option_type if args.symbols.strip() else f"D_{date_prefix}"

    try:
        from services.stock_ranker import export_full_ranking_only
    except ImportError:
        from backend.services.stock_ranker import export_full_ranking_only

    export_full_ranking_only(
        stocks,
        alert_type=export_alert_type,
        export_meta=meta,
        send_email=not args.no_email,
    )
    print(f"OK: ranked {len(stocks)} symbols → logs/scan_rankings/ (email={'on' if not args.no_email else 'off'})")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
