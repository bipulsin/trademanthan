"""Run the study and write the JSON the EMA scan page reads."""
from __future__ import annotations

import json
import logging
from datetime import date, datetime, timedelta
from pathlib import Path
from typing import Any, Dict, List, Optional

import pytz

from backend.services.ema_scan.calendar import expiry_by_day, holidays, trading_days
from backend.services.ema_scan.engine import build_report, default_params, scan_contract
from backend.services.ema_scan.fetch import (
    fetch_candles,
    load_future_index,
    make_upstox,
    repo_root,
    resolve_contract,
    universe_rows,
)
from backend.services.mtf_swing.data import monthly_expiry

logger = logging.getLogger(__name__)
IST = pytz.timezone("Asia/Kolkata")


def result_candidates() -> List[Path]:
    root = repo_root()
    return [
        root / "frontend" / "public" / "ema-scan-result.json",
        root / "data" / "ema_scan" / "result.json",
        Path("/home/ubuntu/trademanthan/data/ema_scan/result.json"),
        Path("/home/ubuntu/twcto/data/ema_scan/result.json"),
    ]


def find_result() -> Optional[Path]:
    for path in result_candidates():
        try:
            if path.is_file() and path.stat().st_size > 20:
                return path
        except OSError:
            continue
    return None


def _contract_rule(hols) -> str:
    october = monthly_expiry(2026, 10, hols)
    november = monthly_expiry(2026, 11, hols)
    nxt = october + timedelta(days=1)
    return (
        f"Front-month future through the expiry session ({october.day} "
        f"{october.strftime('%b %Y')}). Next contract from the following session "
        f"({nxt.day} {nxt.strftime('%b %Y')}). October expiry {october.isoformat()}, "
        f"November expiry {november.isoformat()}. Each contract is scored on its own "
        f"5-minute and daily candles."
    )


def run_scan(params: Optional[Dict[str, Any]] = None, symbols: Optional[List[str]] = None) -> Dict[str, Any]:
    params = dict(default_params() if params is None else params)
    hols = holidays()
    start = date.fromisoformat(str(params["date_from"]))
    end = date.fromisoformat(str(params["date_to"]))
    days = trading_days(start, end, hols)
    by_expiry = expiry_by_day(days, hols)
    expiries = sorted(set(by_expiry.values()))
    universe = universe_rows()
    if symbols:
        wanted = {s.strip().upper() for s in symbols if s and s.strip()}
        universe = [row for row in universe if row["symbol"] in wanted]
    fut_index = load_future_index()
    upstox = make_upstox()
    signals: List[Dict[str, Any]] = []
    skipped: List[Dict[str, Any]] = []
    logger.info(
        "ema_scan start symbols=%s days=%s expiries=%s",
        len(universe),
        len(days),
        [e.isoformat() for e in expiries],
    )
    for idx, row in enumerate(universe, start=1):
        symbol = row["symbol"]
        underlying = row.get("underlying_key") or ""
        if not underlying:
            for day in days:
                skipped.append(
                    {
                        "date": day.isoformat(),
                        "symbol": symbol,
                        "contract": "",
                        "reason": "no underlying instrument key on arbitrage_master",
                    }
                )
            continue
        for expiry in expiries:
            exp_days = [day for day in days if by_expiry[day] == expiry]
            if not exp_days:
                continue
            resolved = resolve_contract(upstox, symbol, expiry, underlying, fut_index)
            key = resolved.get("instrument_key") or ""
            label = resolved.get("trading_symbol") or ""
            if not key:
                for day in exp_days:
                    skipped.append(
                        {
                            "date": day.isoformat(),
                            "symbol": symbol,
                            "contract": label,
                            "reason": f"front contract for expiry {expiry.isoformat()} was not resolved",
                        }
                    )
                continue
            hist_start = expiry - timedelta(days=100)
            hist_end = min(expiry, end)
            bars_5m = fetch_candles(upstox, key, hist_start, hist_end, "5m")
            bars_1d = fetch_candles(upstox, key, hist_start, hist_end, "1d")
            sigs, skips = scan_contract(
                symbol=symbol,
                contract=label,
                instrument_key=key,
                bars_5m=bars_5m,
                bars_daily=bars_1d,
                days=exp_days,
                params=params,
            )
            signals.extend(sigs)
            skipped.extend(skips)
        if idx % 25 == 0 or idx == len(universe):
            logger.info("ema_scan progress %s/%s signals=%s", idx, len(universe), len(signals))
    report = build_report(
        signals=signals,
        skipped=skipped,
        params=params,
        universe=len(universe),
        contract_rule=_contract_rule(hols),
    )
    report["generated_at"] = datetime.now(IST).isoformat(timespec="seconds")
    _write_report(report)
    return report


def _write_report(report: Dict[str, Any]) -> None:
    payload = json.dumps(report)
    for path in result_candidates():
        try:
            if not path.parent.exists() and path.parent.parent.exists():
                path.parent.mkdir(parents=True, exist_ok=True)
            if not path.parent.exists():
                continue
            path.write_text(payload, encoding="utf-8")
            logger.info("ema_scan wrote %s", path)
        except OSError as exc:
            logger.info("ema_scan write skipped %s: %s", path, exc)
