#!/usr/bin/env python3
"""
Fetch 5-minute historical candles from Upstox V3 for a given calendar date.

Uses the same path as the app: UpstoxService.get_historical_candles_by_instrument_key.

Examples (from repo root):

  PYTHONPATH=. python3 scripts/upstox_fetch_5m_candles.py --date 2026-04-15

  PYTHONPATH=. python3 scripts/upstox_fetch_5m_candles.py --date 2026-04-15 \\
      --instrument-key 'NSE_FO|67003'

  PYTHONPATH=. python3 scripts/upstox_fetch_5m_candles.py --date 2026-04-15 --stock RELIANCE
"""

from __future__ import annotations

import argparse
import json
import sys
from datetime import date, datetime
from pathlib import Path
from typing import Any, List, Optional

import pytz
from sqlalchemy import text

# Repo root = parent of scripts/
_PROJECT_ROOT = Path(__file__).resolve().parent.parent
if str(_PROJECT_ROOT) not in sys.path:
    sys.path.insert(0, str(_PROJECT_ROOT))

from backend.config import settings  # noqa: E402
from backend.database import SessionLocal  # noqa: E402
from backend.services.upstox_service import UpstoxService  # noqa: E402

IST = pytz.timezone("Asia/Kolkata")


def parse_dt_ist(ts: Any) -> Optional[datetime]:
    if ts is None:
        return None
    if isinstance(ts, (int, float)):
        try:
            v = float(ts)
            if v > 1_000_000_000_000:
                v = v / 1000.0
            return datetime.fromtimestamp(v, tz=IST)
        except Exception:
            return None
    s = str(ts).strip()
    if not s:
        return None
    if s.isdigit():
        try:
            v = float(s)
            if v > 1_000_000_000_000:
                v = v / 1000.0
            return datetime.fromtimestamp(v, tz=IST)
        except Exception:
            return None
    try:
        dt = datetime.fromisoformat(s.replace("Z", "+00:00").replace(" ", "T"))
        return dt.astimezone(IST) if dt.tzinfo else IST.localize(dt)
    except ValueError:
        pass
    try:
        d = datetime.strptime(s[:10], "%Y-%m-%d")
        return IST.localize(d)
    except Exception:
        return None


def candles_for_session_day(candles: List[dict], session_d: date) -> List[dict]:
    out = []
    for c in candles or []:
        dt = parse_dt_ist(c.get("timestamp"))
        if dt and dt.date() == session_d:
            out.append(c)
    return out


def resolve_instrument_key_from_arbitrage(stock: str) -> Optional[str]:
    sym = (stock or "").strip().upper()
    if not sym:
        return None
    db = SessionLocal()
    try:
        row = db.execute(
            text(
                """
                SELECT currmth_future_instrument_key
                FROM arbitrage_master
                WHERE UPPER(TRIM(stock)) = :s
                  AND currmth_future_instrument_key IS NOT NULL
                  AND LENGTH(TRIM(currmth_future_instrument_key)) > 0
                LIMIT 1
                """
            ),
            {"s": sym},
        ).fetchone()
        if row and row[0]:
            return str(row[0]).strip()
    finally:
        db.close()
    return None


def main() -> int:
    p = argparse.ArgumentParser(description="Fetch Upstox 5m candles for a session date")
    p.add_argument("--date", default="2026-04-15", help="Session date YYYY-MM-DD (IST calendar day)")
    p.add_argument("--stock", default="RELIANCE", help="Underlying stock; uses arbitrage_master FUT key if --instrument-key omitted")
    p.add_argument("--instrument-key", default=None, help="Full Upstox instrument key, e.g. NSE_FO|67003")
    p.add_argument(
        "--days-back",
        type=int,
        default=5,
        help="Historical window length passed to API (clamped by Upstox for minutes/5)",
    )
    p.add_argument("--json-out", default=None, help="Optional path to write full candle list JSON")
    args = p.parse_args()

    session_d = date.fromisoformat(args.date)
    ik = (args.instrument_key or "").strip() or resolve_instrument_key_from_arbitrage(args.stock)
    if not ik:
        print("ERROR: No instrument key. Pass --instrument-key or --stock present in arbitrage_master.", file=sys.stderr)
        return 1

    ux = UpstoxService(settings.UPSTOX_API_KEY, settings.UPSTOX_API_SECRET)
    ux.reload_token_from_storage()

    raw = ux.get_historical_candles_by_instrument_key(
        ik,
        interval="minutes/5",
        days_back=max(1, int(args.days_back)),
        range_end_date=session_d,
    )
    candles = raw or []
    day_only = candles_for_session_day(candles, session_d)

    first_raw = parse_dt_ist(candles[0]["timestamp"]) if candles else None
    last_raw = parse_dt_ist(candles[-1]["timestamp"]) if candles else None
    first_day = parse_dt_ist(day_only[0]["timestamp"]) if day_only else None
    last_day = parse_dt_ist(day_only[-1]["timestamp"]) if day_only else None

    summary = {
        "session_date": args.date,
        "instrument_key": ik,
        "stock_fallback": args.stock if not args.instrument_key else None,
        "raw_candle_count": len(candles),
        "session_day_ist_candle_count": len(day_only),
        "first_raw_timestamp_ist": first_raw.isoformat() if first_raw else None,
        "last_raw_timestamp_ist": last_raw.isoformat() if last_raw else None,
        "first_session_day_timestamp_ist": first_day.isoformat() if first_day else None,
        "last_session_day_timestamp_ist": last_day.isoformat() if last_day else None,
    }

    print(json.dumps(summary, indent=2))

    if args.json_out:
        out_path = Path(args.json_out)
        out_path.parent.mkdir(parents=True, exist_ok=True)
        payload = {
            "summary": summary,
            "candles_raw": candles,
            "candles_session_day_ist": day_only,
        }
        out_path.write_text(json.dumps(payload, indent=2), encoding="utf-8")
        print(f"Wrote: {out_path}", file=sys.stderr)

    return 0


if __name__ == "__main__":
    raise SystemExit(main())
