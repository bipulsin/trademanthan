"""Build the stock-day CSV one month at a time, then the page's analysis.

A finished symbol is recorded in progress.json. Starting the job again
continues at the next symbol. ``retry_missing`` redoes symbols that came back
with no 5-minute candles.
"""
from __future__ import annotations

import json
import logging
import time
from datetime import date, timedelta
from pathlib import Path
from typing import Any, Dict, List, Optional, Sequence, Tuple

from backend.services.big_move.analyze import build_analysis
from backend.services.big_move.features import build_stock_day
from backend.services.big_move.fetch import data_dir, fetch_candles, make_upstox, repo_root
from backend.services.big_move.store import SKIP_COLUMNS, read_csv, write_csv
from backend.services.ema_scan.calendar import expiry_by_day, holidays, trading_days
from backend.services.ema_scan.fetch import load_future_index, resolve_contract, universe_rows
from backend.services.mtf_swing.data import NIFTY50_KEY, monthly_expiry
from backend.services.upstox_rate_limiter import set_backtest_bulk_prefetch_mode

logger = logging.getLogger(__name__)

STUDY_FROM = "2025-11-01"
STUDY_TO = "2026-11-13"
TRAIN_END = "2026-06-30"


def default_params() -> Dict[str, Any]:
    return {
        "date_from": STUDY_FROM,
        "date_to": STUDY_TO,
        "reference": "09:45",
        "thresholds": [2, 3, 4],
        "train_end": TRAIN_END,
        "min_rule_n": 30,
        "min_bucket_n": 30,
        "atr_period": 14,
        "bar_minutes": 5,
    }


def result_candidates() -> List[Path]:
    root = repo_root()
    base = data_dir()
    return [
        base / "result.json",
        root / "frontend" / "public" / "big-move-result.json",
        Path("/home/ubuntu/twcto/data/big_move/result.json"),
    ]


def rows_candidates() -> List[Path]:
    root = repo_root()
    base = data_dir()
    return [
        base / "rows.csv",
        root / "frontend" / "public" / "big-move-rows.csv",
        Path("/home/ubuntu/twcto/data/big_move/rows.csv"),
    ]


def skips_candidates() -> List[Path]:
    root = repo_root()
    base = data_dir()
    return [
        base / "skips.csv",
        root / "frontend" / "public" / "big-move-skips.csv",
        Path("/home/ubuntu/twcto/data/big_move/skips.csv"),
    ]


def _first(paths: Sequence[Path]) -> Optional[Path]:
    for path in paths:
        try:
            if path.is_file() and path.stat().st_size > 20:
                return path
        except OSError:
            continue
    return None


def find_result() -> Optional[Path]:
    return _first(result_candidates())


def find_rows() -> Optional[Path]:
    return _first(rows_candidates())


def find_skips() -> Optional[Path]:
    return _first(skips_candidates())


def _write_all(paths: Sequence[Path], text: str) -> None:
    for path in paths:
        try:
            path.parent.mkdir(parents=True, exist_ok=True)
            path.write_text(text, encoding="utf-8")
        except OSError as exc:
            logger.info("big_move write %s: %s", path, exc)


def _progress_path() -> Path:
    return data_dir() / "progress.json"


def _load_progress() -> Dict[str, Any]:
    path = _progress_path()
    if not path.is_file():
        return {"complete_months": [], "symbols": {}}
    try:
        data = json.loads(path.read_text(encoding="utf-8"))
    except Exception:
        return {"complete_months": [], "symbols": {}}
    data.setdefault("complete_months", [])
    data.setdefault("symbols", {})
    return data


def _save_progress(progress: Dict[str, Any]) -> None:
    path = _progress_path()
    path.write_text(json.dumps(progress), encoding="utf-8")


def _months(start: date, end: date) -> List[str]:
    out = []
    year, month = start.year, start.month
    while date(year, month, 1) <= end:
        out.append(f"{year:04d}-{month:02d}")
        month += 1
        if month == 13:
            year, month = year + 1, 1
    return out


def _contract_rule(hols) -> str:
    first = monthly_expiry(2025, 11, hols)
    last = monthly_expiry(2026, 11, hols)
    return (
        "Front-month future through its expiry session, then the next contract "
        f"from the following session (first expiry {first.isoformat()}, last expiry "
        f"{last.isoformat()}). Each stock-day uses that contract's own 5-minute and "
        "daily candles. Two contracts are never joined."
    )


def _sectors() -> Dict[str, str]:
    out: Dict[str, str] = {}
    try:
        from sqlalchemy import text

        from backend.database import SessionLocal

        db = SessionLocal()
        try:
            rows = db.execute(
                text(
                    """
                    SELECT UPPER(TRIM(stock)), MAX(NULLIF(TRIM(sector_index), ''))
                    FROM arbitrage_master
                    WHERE stock IS NOT NULL AND TRIM(stock) <> ''
                    GROUP BY 1
                    """
                )
            ).fetchall()
        finally:
            db.close()
        for row in rows:
            out[str(row[0] or "").strip().upper()] = str(row[1] or "").strip()
    except Exception as exc:
        logger.info("big_move sector column unavailable: %s", exc)
    try:
        from backend.services.fno_sector_mapping_csv import load_fno_sector_index_map

        for sym, key in load_fno_sector_index_map().items():
            if key and not out.get(sym):
                out[str(sym).strip().upper()] = str(key).strip()
    except Exception as exc:
        logger.info("big_move sector map unavailable: %s", exc)
    return out


def _index_window(study_from: date, study_to: date) -> Tuple[date, date, date, date]:
    return study_from - timedelta(days=10), study_to, study_from - timedelta(days=90), study_to


def _contract_window(expiry: date, study_from: date, study_to: date) -> Tuple[date, date, date, date]:
    end = min(expiry, study_to)
    start_5m = max(expiry - timedelta(days=75), study_from - timedelta(days=45))
    start_1d = max(expiry - timedelta(days=210), study_from - timedelta(days=200))
    if start_5m > end:
        start_5m = end
    if start_1d > end:
        start_1d = end
    return start_5m, end, start_1d, end


def _month_path(month: str, kind: str) -> Path:
    folder = data_dir() / "months"
    folder.mkdir(parents=True, exist_ok=True)
    name = f"{month}.csv" if kind == "rows" else f"{month}-skips.csv"
    return folder / name


def _replace_symbol(path: Path, symbol: str, rows: List[Dict[str, Any]], columns: List[str]) -> None:
    kept = [row for row in read_csv(path) if str(row.get("symbol") or "") != symbol]
    kept.extend(rows)
    write_csv(path, kept, columns)


def _combine(kind: str) -> List[Dict[str, Any]]:
    rows: List[Dict[str, Any]] = []
    folder = data_dir() / "months"
    if not folder.is_dir():
        return rows
    suffix = ".csv" if kind == "rows" else "-skips.csv"
    for path in sorted(folder.glob(f"*{suffix}")):
        if kind == "rows" and path.name.endswith("-skips.csv"):
            continue
        rows.extend(read_csv(path))
    return rows


def publish(params: Dict[str, Any], contract_rule: str) -> Dict[str, Any]:
    rows = _combine("rows")
    skipped = _combine("skips")
    write_csv(data_dir() / "rows.csv", rows)
    write_csv(data_dir() / "skips.csv", skipped, ["date", "symbol", "contract", "reason"])
    public = repo_root() / "frontend" / "public"
    if public.is_dir():
        write_csv(public / "big-move-rows.csv", rows)
        write_csv(public / "big-move-skips.csv", skipped, ["date", "symbol", "contract", "reason"])
    analysis = build_analysis(
        rows,
        train_end=str(params["train_end"]),
        params=params,
        skipped=skipped,
        contract_rule=contract_rule,
    )
    text = json.dumps(analysis, default=str)
    _write_all(result_candidates(), text)
    logger.info("big_move published rows=%s skipped=%s", len(rows), len(skipped))
    return analysis


def run_study(
    params: Optional[Dict[str, Any]] = None,
    symbols: Optional[Sequence[str]] = None,
    months: Optional[Sequence[str]] = None,
    retry_missing: bool = False,
) -> Dict[str, Any]:
    set_backtest_bulk_prefetch_mode(True)
    params = dict(default_params() if params is None else {**default_params(), **params})
    hols = holidays()
    start = date.fromisoformat(str(params["date_from"]))
    end = date.fromisoformat(str(params["date_to"]))
    days = trading_days(start, end, hols)
    by_expiry = expiry_by_day(days, hols)
    universe = universe_rows()
    if symbols:
        wanted = {s.strip().upper() for s in symbols if s and str(s).strip()}
        universe = [row for row in universe if row["symbol"] in wanted]
    sectors = _sectors()
    fut_index = load_future_index()
    upstox = make_upstox()
    rule = _contract_rule(hols)
    i5_a, i5_b, i1_a, i1_b = _index_window(start, end)
    indexes: Dict[str, Dict[str, List[Dict[str, Any]]]] = {}

    def index_bars(key: str) -> Dict[str, List[Dict[str, Any]]]:
        key = (key or "").strip()
        if not key:
            return {"5m": [], "1d": []}
        cached = indexes.get(key)
        if cached and cached.get("5m") and cached.get("1d"):
            return cached
        logger.info("big_move index %s", key)
        loaded = {"5m": [], "1d": []}
        for attempt in range(3):
            loaded = {
                "5m": fetch_candles(upstox, key, i5_a, i5_b, "5m"),
                "1d": fetch_candles(upstox, key, i1_a, i1_b, "1d"),
            }
            if len(loaded["5m"]) >= 1000 and len(loaded["1d"]) >= 40:
                break
            time.sleep(5 * (attempt + 1))
        indexes[key] = loaded
        return loaded

    nifty = index_bars(NIFTY50_KEY)
    progress = _load_progress()
    month_list = list(months) if months else _months(start, end)
    for month in month_list:
        month_days = [day for day in days if day.isoformat().startswith(month)]
        if not month_days:
            continue
        if retry_missing and month not in (months or []):
            pass
        if retry_missing:
            _drop_missing(month, progress)
        if month in progress["complete_months"] and not symbols:
            continue
        done = set(progress["symbols"].get(month, []))
        logger.info("big_move month %s days=%s done=%s", month, len(month_days), len(done))
        for index, item in enumerate(universe, start=1):
            symbol = item["symbol"]
            if symbol in done and not symbols:
                continue
            sector_key = sectors.get(symbol, "")
            sector = index_bars(sector_key) if sector_key and sector_key != NIFTY50_KEY else {"5m": [], "1d": []}
            built, skipped = _symbol_month(
                upstox,
                fut_index,
                item,
                month_days,
                by_expiry,
                start,
                end,
                nifty,
                sector,
            )
            _replace_symbol(_month_path(month, "rows"), symbol, built, list(read_header_rows()))
            _replace_symbol(_month_path(month, "skips"), symbol, skipped, ["date", "symbol", "contract", "reason"])
            done.add(symbol)
            progress["symbols"][month] = sorted(done)
            _save_progress(progress)
            if index == 1 or index % 10 == 0 or index == len(universe):
                logger.info("big_move %s %s/%s %s rows=%s skips=%s", month, index, len(universe), symbol, len(built), len(skipped))
        if not symbols:
            if month not in progress["complete_months"]:
                progress["complete_months"].append(month)
            _save_progress(progress)
        publish(params, rule)
    return publish(params, rule)


def read_header_rows():
    from backend.services.big_move.store import COLUMNS

    return COLUMNS


def _drop_missing(month: str, progress: Dict[str, Any]) -> None:
    have = {str(row.get("symbol") or "") for row in read_csv(_month_path(month, "rows"))}
    skips = read_csv(_month_path(month, "skips"))
    missing = {
        str(row.get("symbol") or "")
        for row in skips
        if "no 5-minute" in str(row.get("reason") or "") and str(row.get("symbol") or "") not in have
    }
    if not missing:
        return
    progress["symbols"][month] = [sym for sym in progress["symbols"].get(month, []) if sym not in missing]
    if month in progress["complete_months"]:
        progress["complete_months"].remove(month)
    kept = [row for row in skips if str(row.get("symbol") or "") not in missing]
    write_csv(_month_path(month, "skips"), kept, ["date", "symbol", "contract", "reason"])
    _save_progress(progress)
    logger.info("big_move retry %s symbols=%s", month, len(missing))


def _symbol_month(
    upstox,
    fut_index,
    item: Dict[str, str],
    month_days: Sequence[date],
    by_expiry: Dict[date, date],
    study_from: date,
    study_to: date,
    nifty: Dict[str, List[Dict[str, Any]]],
    sector: Dict[str, List[Dict[str, Any]]],
) -> Tuple[List[Dict[str, Any]], List[Dict[str, Any]]]:
    symbol = item["symbol"]
    underlying = item.get("underlying_key") or ""
    grouped: Dict[date, List[date]] = {}
    for day in month_days:
        grouped.setdefault(by_expiry[day], []).append(day)
    built: List[Dict[str, Any]] = []
    skipped: List[Dict[str, Any]] = []
    for expiry, expiry_days in grouped.items():
        resolved = resolve_contract(upstox, symbol, expiry, underlying, fut_index)
        key = str(resolved.get("instrument_key") or "")
        contract = str(resolved.get("trading_symbol") or f"{symbol} {expiry.isoformat()}")
        if not key:
            for day in expiry_days:
                skipped.append(
                    {"date": day.isoformat(), "symbol": symbol, "contract": contract, "reason": "front-month contract not resolved"}
                )
            continue
        start_5m, end_5m, start_1d, end_1d = _contract_window(expiry, study_from, study_to)
        bars_5m = fetch_candles(upstox, key, start_5m, end_5m, "5m")
        if len(bars_5m) < 50:
            time.sleep(5)
            bars_5m = fetch_candles(upstox, key, start_5m, end_5m, "5m")
        bars_1d = fetch_candles(upstox, key, start_1d, end_1d, "1d")
        if len(bars_5m) < 6:
            for day in expiry_days:
                skipped.append(
                    {
                        "date": day.isoformat(),
                        "symbol": symbol,
                        "contract": contract,
                        "reason": "no 5-minute candles for this contract",
                    }
                )
            continue
        for day in expiry_days:
            row, reason = build_stock_day(
                symbol=symbol,
                contract=contract,
                instrument_key=key,
                day=day,
                bars_5m=bars_5m,
                bars_daily=bars_1d,
                nifty_5m=nifty.get("5m"),
                nifty_daily=nifty.get("1d"),
                sector_5m=sector.get("5m"),
                sector_daily=sector.get("1d"),
            )
            if row is None:
                skipped.append({"date": day.isoformat(), "symbol": symbol, "contract": contract, "reason": reason})
            else:
                built.append(row)
    return built, skipped
