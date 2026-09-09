#!/usr/bin/env python3
"""Backfill Stock Options live rows from a ChartInk past-month CSV.

Replays ChartInk triggers as webhook data into ``stock_option_signals``, then
walks the same 11:15 / 13:15 / 15:15 IST EMA clock as the live scheduler.

Arming uses 2h bars that completed strictly after the trigger (not a single
future snapshot). An Active row that later loses the EMA condition, with no
user Trade click, becomes Executed with the live invalidate remark and blank
strikes. Current Active rows then get ~28Δ / ~18Δ spreads via the existing
Upstox option-chain path.

Does not delete other tables. Aborts if ``stock_option_signals`` is not empty
unless ``--allow-nonempty`` is passed (still never truncates).
"""
from __future__ import annotations

import argparse
import csv
import json
import sys
import time
from collections import defaultdict
from datetime import date, datetime, timedelta
from pathlib import Path
from typing import Any, Dict, List, Optional, Sequence, Tuple

ROOT = Path(__file__).resolve().parents[1]
if str(ROOT) not in sys.path:
    sys.path.insert(0, str(ROOT))

import pytz
from sqlalchemy import text

from backend.database import SessionLocal
from backend.services.ist_datetime import naive_ist
from backend.services.market_holiday import should_skip_scheduled_market_jobs_ist
from backend.services.stock_option_signals import (
    BAR_MINUTES,
    EMA_SLOW,
    FETCH_SLEEP_SEC,
    INVALIDATE_REMARKS,
    STATUS_ACTIVE,
    STATUS_EXECUTED,
    STATUS_RADAR,
    _fill_spreads_if_blank,
    _norm_symbol,
    aggregate_intraday_to_2h,
    completed_2h_bars,
    ema_snapshot,
    ensure_stock_option_tables,
    next_ema_action,
    resolve_equity_instrument_key,
    should_insert_new_signal,
    side_from_williamsr,
)

IST = pytz.timezone("Asia/Kolkata")
SCAN_NAME = "HA-stock option"
ALERT_NAME = "ChartInk CSV backfill"
SOURCE_IP = "chartink-csv"
TICK_TIMES = ((11, 15), (13, 15), (15, 15))
# arbitrage_master / instruments JSON store a non-trading CHOLAFIN ISIN (no hour candles).
INSTRUMENT_OVERRIDES = {"CHOLAFIN": "NSE_EQ|INE121A01024"}


def log(msg: str) -> None:
    print(msg, flush=True)


def parse_csv_dt(raw: str) -> datetime:
    """ChartInk export is day-first IST (``31-07-2026 09:15``)."""
    s = (raw or "").strip()
    for fmt in ("%d-%m-%Y %H:%M", "%d-%m-%Y %H:%M:%S", "%d/%m/%Y %H:%M", "%Y-%m-%d %H:%M:%S"):
        try:
            return datetime.strptime(s, fmt)
        except ValueError:
            continue
    raise ValueError(f"unrecognized Date: {raw!r}")


def load_csv(path: Path) -> List[Dict[str, Any]]:
    rows: List[Dict[str, Any]] = []
    with path.open(newline="", encoding="utf-8-sig") as fh:
        reader = csv.DictReader(fh)
        if not reader.fieldnames or "Date" not in reader.fieldnames or "Symbol" not in reader.fieldnames:
            raise SystemExit(f"CSV missing Date/Symbol columns: {reader.fieldnames}")
        wr_key = "williamsr" if "williamsr" in reader.fieldnames else None
        if wr_key is None:
            for name in reader.fieldnames:
                if name and name.strip().lower().replace(" ", "") in ("williamsr", "williams_r"):
                    wr_key = name
                    break
        if wr_key is None:
            raise SystemExit(f"CSV missing williamsr column: {reader.fieldnames}")
        for i, rec in enumerate(reader, start=2):
            dt = parse_csv_dt(rec["Date"])
            rows.append(
                {
                    "line": i,
                    "dt": dt,
                    "symbol": _norm_symbol(rec.get("Symbol")),
                    "williamsr": rec.get(wr_key),
                    "triggered_at_raw": (rec.get("Date") or "").strip(),
                }
            )
    rows.sort(key=lambda r: (r["dt"], r["line"]))
    return rows


def tick_times(start: datetime, end: datetime) -> List[datetime]:
    """Scheduler clock from the day of the first trigger through ``end``."""
    out: List[datetime] = []
    day = start.date()
    last = end.date()
    while day <= last:
        for hh, mm in TICK_TIMES:
            tick = datetime(day.year, day.month, day.day, hh, mm)
            if tick < start or tick > end:
                continue
            aware = IST.localize(tick)
            if should_skip_scheduled_market_jobs_ist(aware):
                continue
            out.append(tick)
        day += timedelta(days=1)
    return out


def _bar_end(bar: Dict[str, Any]) -> Optional[datetime]:
    ts = bar.get("timestamp")
    if ts is None:
        return None
    if isinstance(ts, datetime):
        start = ts.astimezone(IST) if ts.tzinfo else IST.localize(ts)
    else:
        from backend.services.stock_option_signals import _parse_candle_ts

        start = _parse_candle_ts(ts)
    if start is None:
        return None
    end = start + timedelta(minutes=BAR_MINUTES)
    return naive_ist(end)


def _merge_raw(chunks: Sequence[Sequence[Dict[str, Any]]]) -> List[Dict[str, Any]]:
    by_ts: Dict[str, Dict[str, Any]] = {}
    for chunk in chunks:
        for c in chunk or []:
            key = str(c.get("timestamp") or "")
            if key:
                by_ts[key] = c
    return [by_ts[k] for k in sorted(by_ts)]


def fetch_2h_bars(instrument_key: str, asof: datetime) -> List[Dict[str, Any]]:
    """hours/1 (chunked) aggregated to 09:15 2h bars; hours/2 fallback.

    Upstox hours windows are capped at 60 days, so two overlapping windows cover
    EMA100 warmup before the first ChartInk trigger.
    """
    from backend.config import settings
    from backend.services.upstox_service import UpstoxService

    u = UpstoxService(settings.UPSTOX_API_KEY, settings.UPSTOX_API_SECRET)
    asof_d = asof.date() if asof.tzinfo is None else asof.astimezone(IST).date()
    older_end = asof_d - timedelta(days=55)
    chunks: List[List[Dict[str, Any]]] = []
    for label, kwargs in (
        ("recent", {"interval": "hours/1", "days_back": 60}),
        ("older", {"interval": "hours/1", "days_back": 60, "range_end_date": older_end}),
    ):
        raw = None
        last_err: Optional[Exception] = None
        for attempt in range(3):
            try:
                raw = u.get_historical_candles_by_instrument_key(instrument_key, **kwargs)
                last_err = None
                break
            except Exception as e:
                last_err = e
                time.sleep(1.5 * (attempt + 1))
        if last_err is not None:
            log(f"  hours/1 {label} failed {instrument_key}: {last_err}")
        chunks.append(raw or [])
        time.sleep(FETCH_SLEEP_SEC)

    merged = _merge_raw(chunks)
    agg = aggregate_intraday_to_2h(merged_now_safe(merged, asof), now=asof_aware(asof))
    if len(agg) >= EMA_SLOW:
        return agg

    native = None
    try:
        native = u.get_historical_candles_by_instrument_key(
            instrument_key, interval="hours/2", days_back=60
        )
    except Exception as e:
        log(f"  hours/2 failed {instrument_key}: {e}")
        native = None
    time.sleep(FETCH_SLEEP_SEC)
    bars = completed_2h_bars(native or [], now=asof_aware(asof))
    if len(bars) > len(agg):
        return bars
    return agg


def asof_aware(asof: datetime) -> datetime:
    if asof.tzinfo is None:
        return IST.localize(asof)
    return asof.astimezone(IST)


def merged_now_safe(candles: List[Dict[str, Any]], asof: datetime) -> List[Dict[str, Any]]:
    return candles


def ema_lookup(bars: Sequence[Dict[str, Any]]) -> List[Tuple[datetime, Dict[str, Optional[float]]]]:
    closes: List[float] = []
    out: List[Tuple[datetime, Dict[str, Optional[float]]]] = []
    ordered = []
    for bar in bars:
        end = _bar_end(bar)
        close = bar.get("close")
        if end is None or close is None:
            continue
        try:
            c = float(close)
        except (TypeError, ValueError):
            continue
        ordered.append((end, c))
    ordered.sort(key=lambda x: x[0])
    for end, close in ordered:
        closes.append(close)
        out.append((end, ema_snapshot(closes)))
    return out


def snap_asof(snaps: Sequence[Tuple[datetime, Dict[str, Optional[float]]]], tick: datetime) -> Dict[str, Optional[float]]:
    chosen: Optional[Dict[str, Optional[float]]] = None
    for end, snap in snaps:
        if end <= tick:
            chosen = snap
        else:
            break
    return chosen or {"ema9": None, "ema30": None, "ema100": None}


def resolve_keys(symbols: Sequence[str]) -> Dict[str, Optional[str]]:
    db = SessionLocal()
    found: Dict[str, Optional[str]] = {}
    try:
        rows = db.execute(
            text(
                """
                SELECT UPPER(TRIM(stock)) AS sym, stock_instrument_key
                FROM arbitrage_master
                WHERE stock_instrument_key IS NOT NULL
                  AND TRIM(stock_instrument_key) <> ''
                """
            )
        ).fetchall()
        master = {str(r[0]): str(r[1]).strip() for r in rows if r[0] and r[1]}
        for sym in symbols:
            ik = master.get(sym)
            if not ik:
                ik = resolve_equity_instrument_key(sym, db)
            found[sym] = ik or None
    finally:
        db.close()
    return found


def group_payload(dt: datetime, members: Sequence[Dict[str, Any]]) -> Dict[str, Any]:
    return {
        "stocks": ",".join(m["symbol"] for m in members),
        "triggered_at": dt.strftime("%Y-%m-%d %H:%M:%S"),
        "scan_name": SCAN_NAME,
        "alert_name": ALERT_NAME,
        "columns": [{"symbol": m["symbol"], "williamsr": m["williamsr"]} for m in members],
        "source": SOURCE_IP,
    }


def persist(signals: Sequence[Dict[str, Any]], webhook_groups: Sequence[Dict[str, Any]]) -> None:
    now = naive_ist(datetime.now(IST))
    db = SessionLocal()
    try:
        existing = db.execute(text("SELECT COUNT(*) FROM stock_option_signals")).scalar()
        if int(existing or 0) != 0:
            raise SystemExit(f"stock_option_signals is not empty ({existing}); aborting write")
        for grp in webhook_groups:
            db.execute(
                text(
                    """
                    INSERT INTO stock_option_webhook_log (
                        received_at, source_ip, scan_name, alert_name, triggered_at_raw,
                        raw_payload, parse_status
                    ) VALUES (
                        :received_at, :source_ip, :scan_name, :alert_name, :triggered_at_raw,
                        CAST(:raw_payload AS jsonb), :parse_status
                    )
                    """
                ),
                {
                    "received_at": grp["received_at"],
                    "source_ip": SOURCE_IP,
                    "scan_name": SCAN_NAME,
                    "alert_name": ALERT_NAME,
                    "triggered_at_raw": grp["triggered_at_raw"],
                    "raw_payload": json.dumps(grp["payload"]),
                    "parse_status": grp["parse_status"],
                },
            )
        for sig in signals:
            db.execute(
                text(
                    """
                    INSERT INTO stock_option_signals (
                        symbol, williamsr, instrument_key, status, side,
                        trigger_at, triggered_at_raw, scan_name, alert_name, raw_payload,
                        ema9, ema30, ema100, ema_updated_at, armed_at,
                        hard_stop_placed, remarks, created_at, updated_at
                    ) VALUES (
                        :symbol, :williamsr, :instrument_key, :status, :side,
                        :trigger_at, :triggered_at_raw, :scan_name, :alert_name,
                        CAST(:raw_payload AS jsonb),
                        :ema9, :ema30, :ema100, :ema_updated_at, :armed_at,
                        FALSE, :remarks, :created_at, :updated_at
                    )
                    """
                ),
                {
                    "symbol": sig["symbol"],
                    "williamsr": sig["williamsr"],
                    "instrument_key": sig["instrument_key"],
                    "status": sig["status"],
                    "side": sig["side"],
                    "trigger_at": sig["trigger_at"],
                    "triggered_at_raw": sig["triggered_at_raw"],
                    "scan_name": SCAN_NAME,
                    "alert_name": ALERT_NAME,
                    "raw_payload": json.dumps(sig["raw_payload"]),
                    "ema9": sig.get("ema9"),
                    "ema30": sig["ema30"],
                    "ema100": sig["ema100"],
                    "ema_updated_at": sig.get("ema_updated_at"),
                    "armed_at": sig.get("armed_at"),
                    "remarks": sig.get("remarks"),
                    "created_at": sig["trigger_at"],
                    "updated_at": sig.get("updated_at") or now,
                },
            )
        db.commit()
    except Exception:
        db.rollback()
        raise
    finally:
        db.close()


def fill_active_spreads(sleep_sec: float) -> List[Dict[str, Any]]:
    db = SessionLocal()
    try:
        rows = db.execute(
            text(
                """
                SELECT id, symbol, side, sell_strike, buy_strike
                FROM stock_option_signals
                WHERE status = :active
                ORDER BY id
                """
            ),
            {"active": STATUS_ACTIVE},
        ).mappings().all()
        pending = [dict(r) for r in rows]
    finally:
        db.close()
    now = naive_ist(datetime.now(IST))
    for row in pending:
        _fill_spreads_if_blank(row, now)
        time.sleep(sleep_sec)
    db = SessionLocal()
    try:
        rows = db.execute(
            text(
                """
                SELECT id, symbol, side, sell_strike, sell_delta, buy_strike, buy_delta,
                       sell_instrument_key, buy_instrument_key
                FROM stock_option_signals
                WHERE status = :active
                ORDER BY id
                """
            ),
            {"active": STATUS_ACTIVE},
        ).mappings().all()
        return [dict(r) for r in rows]
    finally:
        db.close()


def simulate(
    csv_rows: Sequence[Dict[str, Any]],
    keys: Dict[str, Optional[str]],
    bars_by_sym: Dict[str, List[Tuple[datetime, Dict[str, Optional[float]]]]],
    asof: datetime,
) -> Tuple[List[Dict[str, Any]], List[Dict[str, Any]], Dict[str, int]]:
    """Chronological ingest + EMA ticks. Ticks apply only to trigger_at < tick."""
    start = csv_rows[0]["dt"] if csv_rows else asof
    ticks = tick_times(start, asof)
    events: List[Tuple[datetime, int, Any]] = []
    by_dt: Dict[datetime, List[Dict[str, Any]]] = defaultdict(list)
    for row in csv_rows:
        by_dt[row["dt"]].append(row)
    for dt, members in by_dt.items():
        events.append((dt, 0, members))
    for tick in ticks:
        events.append((tick, 1, None))
    events.sort(key=lambda e: (e[0], e[1]))

    signals: List[Dict[str, Any]] = []
    open_by_sym: Dict[str, Dict[str, Any]] = {}
    webhook_groups: List[Dict[str, Any]] = []
    stats = {
        "csv_rows": len(csv_rows),
        "discarded_williamsr": 0,
        "ignored_duplicate": 0,
        "inserted": 0,
        "same_batch_dupes": 0,
    }

    for dt, kind, payload in events:
        if kind == 0:
            members: List[Dict[str, Any]] = payload
            usable = 0
            for m in members:
                if side_from_williamsr(m["williamsr"]) is not None:
                    usable += 1
            parse_status = "success" if usable == len(members) and usable else ("partial" if usable else "failed")
            payload_json = group_payload(dt, members)
            webhook_groups.append(
                {
                    "received_at": dt,
                    "triggered_at_raw": members[0]["triggered_at_raw"],
                    "payload": payload_json,
                    "parse_status": parse_status,
                }
            )
            seen: set[str] = set()
            for m in members:
                sym = m["symbol"]
                if not sym or sym in seen:
                    stats["same_batch_dupes"] += 1
                    continue
                seen.add(sym)
                side = side_from_williamsr(m["williamsr"])
                if side is None:
                    stats["discarded_williamsr"] += 1
                    continue
                existing = [s["status"] for s in signals if s["symbol"] == sym]
                if not should_insert_new_signal(existing):
                    stats["ignored_duplicate"] += 1
                    continue
                try:
                    wr = float(m["williamsr"])
                except (TypeError, ValueError):
                    wr = None
                sig = {
                    "symbol": sym,
                    "williamsr": float(wr) if wr is not None else None,
                    "instrument_key": keys.get(sym),
                    "status": STATUS_RADAR,
                    "side": side,
                    "trigger_at": dt,
                    "triggered_at_raw": m["triggered_at_raw"],
                    "raw_payload": payload_json,
                    "ema9": None,
                    "ema30": None,
                    "ema100": None,
                    "ema_updated_at": None,
                    "armed_at": None,
                    "remarks": None,
                    "updated_at": dt,
                    "trade_submitted": False,
                }
                signals.append(sig)
                open_by_sym[sym] = sig
                stats["inserted"] += 1
            continue

        tick: datetime = dt
        for sym, sig in list(open_by_sym.items()):
            if sig["trigger_at"] >= tick:
                continue
            snaps = bars_by_sym.get(sym) or []
            if not snaps:
                continue
            snap = snap_asof(snaps, tick)
            action = next_ema_action(
                sig["status"],
                sig["side"],
                snap["ema9"],
                snap["ema30"],
                snap["ema100"],
                sig["trade_submitted"],
            )
            if snap["ema9"] is not None:
                sig["ema9"] = snap["ema9"]
            if snap["ema30"] is not None:
                sig["ema30"] = snap["ema30"]
            if snap["ema100"] is not None:
                sig["ema100"] = snap["ema100"]
            sig["ema_updated_at"] = tick
            sig["updated_at"] = tick
            if action == "arm":
                sig["status"] = STATUS_ACTIVE
                if sig.get("armed_at") is None:
                    sig["armed_at"] = tick
            elif action == "invalidate":
                sig["status"] = STATUS_EXECUTED
                sig["remarks"] = INVALIDATE_REMARKS
                open_by_sym.pop(sym, None)

    return signals, webhook_groups, stats


def main() -> None:
    ap = argparse.ArgumentParser(description="Ingest ChartInk CSV into stock_option_signals")
    ap.add_argument("csv_path", type=Path)
    ap.add_argument("--sleep", type=float, default=FETCH_SLEEP_SEC)
    ap.add_argument("--allow-nonempty", action="store_true")
    ap.add_argument("--dry-run", action="store_true", help="Simulate and print counts; do not write")
    ap.add_argument("--cache", type=Path, default=Path("/tmp/stock_option_chartink_bars.json"))
    args = ap.parse_args()

    asof = naive_ist(datetime.now(IST))
    log(f"asof IST {asof}")
    csv_rows = load_csv(args.csv_path)
    if not csv_rows:
        raise SystemExit("CSV has no rows")
    log(f"csv_rows {len(csv_rows)} range {csv_rows[0]['dt']} .. {csv_rows[-1]['dt']}")

    ensure_stock_option_tables()
    db = SessionLocal()
    try:
        n = int(db.execute(text("SELECT COUNT(*) FROM stock_option_signals")).scalar() or 0)
    finally:
        db.close()
    if n and not args.allow_nonempty and not args.dry_run:
        raise SystemExit(f"stock_option_signals already has {n} rows; refusing to mix")

    symbols = sorted({r["symbol"] for r in csv_rows if r["symbol"]})
    log(f"resolving instrument keys for {len(symbols)} symbols")
    keys = resolve_keys(symbols)
    for sym, ik in INSTRUMENT_OVERRIDES.items():
        if sym in keys:
            keys[sym] = ik
    missing = sorted(s for s in symbols if not keys.get(s))
    log(f"no_instrument {len(missing)}: {', '.join(missing) if missing else '-'}")

    cache: Dict[str, Any] = {}
    if args.cache.is_file():
        try:
            cache = json.loads(args.cache.read_text(encoding="utf-8"))
        except Exception:
            cache = {}

    bars_by_sym: Dict[str, List[Tuple[datetime, Dict[str, Optional[float]]]]] = {}
    fetch_failed: List[str] = []
    need = [s for s in symbols if keys.get(s)]
    for i, sym in enumerate(need, start=1):
        ik = keys[sym]
        cached = cache.get(ik)
        if isinstance(cached, list) and cached:
            bars = cached
            log(f"[{i}/{len(need)}] {sym} cache {len(bars)} 2h bars")
        else:
            log(f"[{i}/{len(need)}] {sym} fetch {ik}")
            try:
                bars = fetch_2h_bars(ik, asof)
            except Exception as e:
                log(f"  fetch error {sym}: {e}")
                bars = []
            cache[ik] = bars
            try:
                args.cache.write_text(json.dumps(cache), encoding="utf-8")
            except Exception as e:
                log(f"  cache write failed: {e}")
        snaps = ema_lookup(bars)
        bars_by_sym[sym] = snaps
        if not snaps:
            fetch_failed.append(sym)
        if i % 10 == 0 or i == len(need):
            log(f"  fetched {i}/{len(need)} last={sym} bars={len(snaps)}")

    signals, webhook_groups, stats = simulate(csv_rows, keys, bars_by_sym, asof)
    counts = {"Radar": 0, "Active": 0, "Executed": 0}
    for sig in signals:
        counts[sig["status"]] = counts.get(sig["status"], 0) + 1
    no_ik_inserted = sorted({s["symbol"] for s in signals if not s.get("instrument_key")})

    log(
        "simulate "
        + json.dumps(
            {
                **stats,
                "radar": counts.get("Radar", 0),
                "active": counts.get("Active", 0),
                "executed": counts.get("Executed", 0),
                "webhook_groups": len(webhook_groups),
                "fetch_failed": fetch_failed,
                "no_instrument": missing,
            },
            default=str,
        )
    )

    if args.dry_run:
        log("dry-run; no database write")
        return

    log(f"writing {len(signals)} signals and {len(webhook_groups)} webhook groups")
    persist(signals, webhook_groups)
    log("filling option spreads for Active rows")
    # Align sleep with CLI without changing the imported constant for other jobs.
    active = fill_active_spreads(args.sleep)
    with_strikes = [r for r in active if r.get("sell_strike") is not None and r.get("buy_strike") is not None]
    sample = active[:8]
    log(
        json.dumps(
            {
                "csv_rows": stats["csv_rows"],
                "discarded_williamsr": stats["discarded_williamsr"],
                "ignored_duplicate": stats["ignored_duplicate"],
                "inserted": stats["inserted"],
                "radar": counts.get("Radar", 0),
                "active": counts.get("Active", 0),
                "executed": counts.get("Executed", 0),
                "active_with_strikes": len(with_strikes),
                "active_sample": sample,
                "no_instrument": missing,
                "fetch_failed": fetch_failed,
                "no_instrument_inserted": no_ik_inserted,
            },
            default=str,
        )
    )


if __name__ == "__main__":
    main()
