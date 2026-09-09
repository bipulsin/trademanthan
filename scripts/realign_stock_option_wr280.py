#!/usr/bin/env python3
"""Recompute Stock Options Williams %R(280) on completed 2h bars and realign sides.

ChartInk HA-stock option triggers sit on 09:15 / 11:15 / 13:15 / 15:15, the same
2-hour clock as the live EMA series. %R uses the last completed 2h bar whose
end is at or before trigger_at (never a later bar):

    %R = (Highest High(280) - Close) / (Highest High(280) - Lowest Low(280)) * -100

Gates: WR > -3 → BEAR CALL; WR < -97 → BULL PUT; else REJECTED.
Rows are walked in trigger-time order per symbol so an open Radar/Active row
still blocks a later trigger. EMA arm/invalidate and 72h Active expiry use the
existing helpers. User-submitted Executed rows (both costs filled) are left
untouched.
"""
from __future__ import annotations

import argparse
import json
import sys
import time
from collections import defaultdict
from datetime import datetime, timedelta
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
    EXPIRY_REMARKS,
    FETCH_SLEEP_SEC,
    INVALIDATE_REMARKS,
    STATUS_ACTIVE,
    STATUS_EXECUTED,
    STATUS_RADAR,
    STATUS_REJECTED,
    _fill_spreads_if_blank,
    _parse_candle_ts,
    active_past_max_age,
    aggregate_intraday_to_2h,
    ema_snapshot,
    expiry_remarks,
    invalidate_outcome,
    next_ema_action,
    resolve_equity_instrument_key,
    side_from_williamsr,
)

IST = pytz.timezone("Asia/Kolkata")
WR_PERIOD = 280
WR_OUTSIDE = "Williams %R(280) outside gate"
WR_SHORT = "Williams %R(280) insufficient 2h history"
IGNORED_OPEN = "Ignored: open row already exists for symbol"
TICK_TIMES = ((11, 15), (13, 15), (15, 15))
INSTRUMENT_OVERRIDES = {"CHOLAFIN": "NSE_EQ|INE121A01024"}
OPEN_STATUSES = {STATUS_RADAR, STATUS_ACTIVE}


def log(msg: str) -> None:
    print(msg, flush=True)


def user_submitted(row: Dict[str, Any]) -> bool:
    return row.get("sell_cost") is not None and row.get("buy_cost") is not None


def tick_times(start: datetime, end: datetime) -> List[datetime]:
    out: List[datetime] = []
    day = start.date()
    last = end.date()
    while day <= last:
        for hh, mm in TICK_TIMES:
            tick = datetime(day.year, day.month, day.day, hh, mm)
            if tick < start or tick > end:
                continue
            if should_skip_scheduled_market_jobs_ist(IST.localize(tick)):
                continue
            out.append(tick)
        day += timedelta(days=1)
    return out


def bar_end(bar: Dict[str, Any]) -> Optional[datetime]:
    ts = _parse_candle_ts(bar.get("timestamp"))
    if ts is None:
        return None
    return naive_ist(ts + timedelta(minutes=BAR_MINUTES))


def ohlc_bars(raw: Sequence[Dict[str, Any]]) -> List[Dict[str, Any]]:
    out: List[Dict[str, Any]] = []
    for bar in raw or []:
        end = bar_end(bar)
        high = bar.get("high")
        low = bar.get("low")
        close = bar.get("close")
        if end is None or high is None or low is None or close is None:
            continue
        try:
            h, lo, c = float(high), float(low), float(close)
        except (TypeError, ValueError):
            continue
        out.append({"end": end, "high": h, "low": lo, "close": c})
    out.sort(key=lambda b: b["end"])
    dedup: Dict[datetime, Dict[str, Any]] = {}
    for b in out:
        dedup[b["end"]] = b
    return [dedup[k] for k in sorted(dedup)]


def williams_r_at(bars: Sequence[Dict[str, Any]], asof: datetime, period: int = WR_PERIOD) -> Optional[float]:
    """%R of the last completed bar with end <= asof. Needs ``period`` bars."""
    idx = -1
    for i, bar in enumerate(bars):
        if bar["end"] <= asof:
            idx = i
        else:
            break
    if idx < period - 1:
        return None
    window = bars[idx - period + 1 : idx + 1]
    if len(window) < period:
        return None
    hh = max(b["high"] for b in window)
    ll = min(b["low"] for b in window)
    if hh <= ll:
        return None
    close = window[-1]["close"]
    return (hh - close) / (hh - ll) * -100.0


def ema_snaps(bars: Sequence[Dict[str, Any]]) -> List[Tuple[datetime, Dict[str, Optional[float]]]]:
    closes: List[float] = []
    out: List[Tuple[datetime, Dict[str, Optional[float]]]] = []
    for bar in bars:
        closes.append(float(bar["close"]))
        out.append((bar["end"], ema_snapshot(closes)))
    return out


def snap_asof(
    snaps: Sequence[Tuple[datetime, Dict[str, Optional[float]]]],
    tick: datetime,
) -> Dict[str, Optional[float]]:
    chosen: Optional[Dict[str, Optional[float]]] = None
    for end, snap in snaps:
        if end <= tick:
            chosen = snap
        else:
            break
    return chosen or {"ema9": None, "ema30": None, "ema100": None}


def fetch_2h_ohlc(instrument_key: str, asof: datetime, sleep_sec: float) -> List[Dict[str, Any]]:
    from backend.config import settings
    from backend.services.upstox_service import UpstoxService

    u = UpstoxService(settings.UPSTOX_API_KEY, settings.UPSTOX_API_SECRET)
    asof_d = asof.date()
    # hours/1 windows are capped at 60 days. Five overlapping chunks cover
    # WR(280) before the July 2026 triggers and EMA replay through today.
    ends = [asof_d - timedelta(days=55 * i) for i in range(5)]
    chunks: List[List[Dict[str, Any]]] = []
    for end_d in ends:
        kwargs: Dict[str, Any] = {"interval": "hours/1", "days_back": 60}
        if end_d < asof_d:
            kwargs["range_end_date"] = end_d
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
            log(f"  hours/1 end={end_d} failed {instrument_key}: {last_err}")
        chunks.append(raw or [])
        time.sleep(sleep_sec)

    by_ts: Dict[str, Dict[str, Any]] = {}
    for chunk in chunks:
        for c in chunk or []:
            key = str(c.get("timestamp") or "")
            if key:
                by_ts[key] = c
    merged = [by_ts[k] for k in sorted(by_ts)]
    agg = aggregate_intraday_to_2h(merged, now=IST.localize(asof))
    return ohlc_bars(agg)


def load_rows() -> List[Dict[str, Any]]:
    db = SessionLocal()
    try:
        rows = db.execute(
            text(
                """
                SELECT id, symbol, williamsr, instrument_key, status, side,
                       trigger_at, armed_at, date_traded, sell_cost, buy_cost,
                       sell_strike, buy_strike, remarks, scan_name, alert_name
                FROM stock_option_signals
                ORDER BY symbol, trigger_at, id
                """
            )
        ).mappings().all()
        return [dict(r) for r in rows]
    finally:
        db.close()


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
            ik = INSTRUMENT_OVERRIDES.get(sym) or master.get(sym)
            if not ik:
                ik = resolve_equity_instrument_key(sym, db)
            found[sym] = ik or None
    finally:
        db.close()
    return found


def counts(rows: Sequence[Dict[str, Any]], key: str = "status") -> Dict[str, int]:
    out = {STATUS_RADAR: 0, STATUS_ACTIVE: 0, STATUS_EXECUTED: 0, STATUS_REJECTED: 0}
    for row in rows:
        st = row.get(key) or row.get("status")
        out[st] = out.get(st, 0) + 1
    return out


def clear_arm(row: Dict[str, Any]) -> None:
    row["ema9"] = None
    row["ema30"] = None
    row["ema100"] = None
    row["ema_updated_at"] = None
    row["armed_at"] = None
    row["sell_strike"] = None
    row["buy_strike"] = None
    row["date_traded"] = None


def reject(row: Dict[str, Any], remarks: str, side: Optional[str] = None) -> None:
    clear_arm(row)
    row["status"] = STATUS_REJECTED
    row["side"] = side
    row["remarks"] = remarks


def apply_expiry(row: Dict[str, Any], tick: datetime) -> bool:
    if row.get("status") != STATUS_ACTIVE:
        return False
    if user_submitted(row):
        return False
    if not active_past_max_age(row.get("armed_at"), tick):
        return False
    armed = row.get("armed_at")
    row["status"] = STATUS_EXECUTED
    row["date_traded"] = armed.date() if isinstance(armed, datetime) else None
    row["remarks"] = expiry_remarks(None)
    return True


def replay_symbol(
    rows: Sequence[Dict[str, Any]],
    bars: Sequence[Dict[str, Any]],
    asof: datetime,
) -> None:
    snaps = ema_snaps(bars)
    start = min(r["trigger_at"] for r in rows)
    events: List[Tuple[datetime, int, Optional[Dict[str, Any]]]] = []
    for row in rows:
        events.append((row["trigger_at"], 0, row))
    for tick in tick_times(start, asof):
        events.append((tick, 1, None))
    events.sort(key=lambda e: (e[0], e[1], (e[2] or {}).get("id") or 0))

    open_row: Optional[Dict[str, Any]] = None
    for dt, kind, row in events:
        if kind == 0:
            assert row is not None
            if user_submitted(row):
                row["skipped_user"] = True
                if row.get("status") in OPEN_STATUSES:
                    open_row = row
                continue
            wr = williams_r_at(bars, row["trigger_at"])
            row["new_wr"] = wr
            if wr is None:
                reject(row, WR_SHORT)
                continue
            side = side_from_williamsr(wr)
            if side is None:
                reject(row, WR_OUTSIDE)
                continue
            if open_row is not None and open_row.get("status") in OPEN_STATUSES:
                reject(row, IGNORED_OPEN, side=side)
                continue
            clear_arm(row)
            row["status"] = STATUS_RADAR
            row["side"] = side
            row["remarks"] = None
            open_row = row
            continue

        tick = dt
        if open_row is None or open_row.get("skipped_user") or open_row.get("status") not in OPEN_STATUSES:
            continue
        if open_row["trigger_at"] >= tick:
            continue
        if apply_expiry(open_row, tick):
            open_row = None
            continue
        snap = snap_asof(snaps, tick)
        action = next_ema_action(
            open_row.get("status") or "",
            open_row.get("side"),
            snap.get("ema9"),
            snap.get("ema30"),
            snap.get("ema100"),
            False,
        )
        if snap.get("ema9") is not None:
            open_row["ema9"] = snap["ema9"]
        if snap.get("ema30") is not None:
            open_row["ema30"] = snap["ema30"]
        if snap.get("ema100") is not None:
            open_row["ema100"] = snap["ema100"]
        open_row["ema_updated_at"] = tick
        if action == "arm":
            open_row["status"] = STATUS_ACTIVE
            if open_row.get("armed_at") is None:
                open_row["armed_at"] = tick
        elif action == "invalidate":
            dest = invalidate_outcome(
                open_row.get("armed_at"),
                open_row.get("sell_strike"),
                open_row.get("buy_strike"),
            )
            open_row["status"] = dest
            open_row["remarks"] = INVALIDATE_REMARKS
            open_row = None

    if open_row is not None and open_row.get("status") == STATUS_ACTIVE:
        apply_expiry(open_row, asof)


def persist(rows: Sequence[Dict[str, Any]], now: datetime) -> int:
    db = SessionLocal()
    n = 0
    try:
        for row in rows:
            if row.get("skipped_user"):
                continue
            db.execute(
                text(
                    """
                    UPDATE stock_option_signals
                    SET williamsr = :williamsr,
                        instrument_key = COALESCE(:instrument_key, instrument_key),
                        status = :status,
                        side = :side,
                        ema9 = :ema9,
                        ema30 = :ema30,
                        ema100 = :ema100,
                        ema_updated_at = :ema_updated_at,
                        armed_at = :armed_at,
                        sell_strike = :sell_strike,
                        sell_delta = NULL,
                        sell_instrument_key = NULL,
                        buy_strike = :buy_strike,
                        buy_delta = NULL,
                        buy_instrument_key = NULL,
                        date_traded = :date_traded,
                        sell_ltp = NULL,
                        buy_ltp = NULL,
                        ltp_updated_at = NULL,
                        user_sell_strike = NULL,
                        user_buy_strike = NULL,
                        remarks = :remarks,
                        updated_at = :updated_at
                    WHERE id = :id
                      AND sell_cost IS NULL
                      AND buy_cost IS NULL
                    """
                ),
                {
                    "williamsr": row.get("new_wr"),
                    "instrument_key": row.get("instrument_key"),
                    "status": row["status"],
                    "side": row.get("side"),
                    "ema9": row.get("ema9"),
                    "ema30": row.get("ema30"),
                    "ema100": row.get("ema100"),
                    "ema_updated_at": row.get("ema_updated_at"),
                    "armed_at": row.get("armed_at"),
                    "sell_strike": row.get("sell_strike"),
                    "buy_strike": row.get("buy_strike"),
                    "date_traded": row.get("date_traded"),
                    "remarks": row.get("remarks"),
                    "updated_at": now,
                    "id": row["id"],
                },
            )
            n += 1
        db.commit()
        return n
    except Exception:
        db.rollback()
        raise
    finally:
        db.close()


def fill_active(sleep_sec: float) -> int:
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
    n = 0
    for row in pending:
        before = row.get("sell_strike")
        _fill_spreads_if_blank(row, now)
        time.sleep(sleep_sec)
        n += 1
        if before is None:
            log(f"  spread fill attempted {row['symbol']} id={row['id']}")
    return n


def main() -> None:
    ap = argparse.ArgumentParser(description="Realign stock_option_signals with WR(280) on 2h bars")
    ap.add_argument("--sleep", type=float, default=FETCH_SLEEP_SEC)
    ap.add_argument("--dry-run", action="store_true")
    ap.add_argument("--cache", type=Path, default=Path("/tmp/stock_option_wr280_bars.json"))
    args = ap.parse_args()

    asof = naive_ist(datetime.now(IST))
    log(f"asof IST {asof} period={WR_PERIOD} tf=2h ema_slow={EMA_SLOW}")
    rows = load_rows()
    if not rows:
        raise SystemExit("stock_option_signals is empty")
    before = counts(rows)
    log(f"before {json.dumps(before)} n={len(rows)}")

    symbols = sorted({r["symbol"] for r in rows if r.get("symbol")})
    keys = resolve_keys(symbols)
    for row in rows:
        ik = keys.get(row["symbol"]) or row.get("instrument_key")
        if row["symbol"] in INSTRUMENT_OVERRIDES:
            ik = INSTRUMENT_OVERRIDES[row["symbol"]]
        row["instrument_key"] = ik

    cache: Dict[str, Any] = {}
    if args.cache.is_file():
        try:
            cache = json.loads(args.cache.read_text(encoding="utf-8"))
        except Exception:
            cache = {}

    bars_by_ik: Dict[str, List[Dict[str, Any]]] = {}
    need = sorted({r["instrument_key"] for r in rows if r.get("instrument_key")})
    missing_sym = sorted({r["symbol"] for r in rows if not r.get("instrument_key")})
    log(f"symbols {len(symbols)} instruments {len(need)} missing_key {missing_sym or '-'}")

    for i, ik in enumerate(need, start=1):
        cached = cache.get(ik)
        if isinstance(cached, list) and cached:
            bars = []
            for rec in cached:
                end = datetime.fromisoformat(rec["end"])
                bars.append(
                    {"end": end, "high": float(rec["high"]), "low": float(rec["low"]), "close": float(rec["close"])}
                )
            log(f"[{i}/{len(need)}] cache {ik} {len(bars)} 2h bars")
        else:
            log(f"[{i}/{len(need)}] fetch {ik}")
            try:
                bars = fetch_2h_ohlc(ik, asof, args.sleep)
            except Exception as e:
                log(f"  fetch error {ik}: {e}")
                bars = []
            cache[ik] = [
                {"end": b["end"].isoformat(sep=" "), "high": b["high"], "low": b["low"], "close": b["close"]}
                for b in bars
            ]
            try:
                args.cache.write_text(json.dumps(cache), encoding="utf-8")
            except Exception as e:
                log(f"  cache write failed: {e}")
        bars_by_ik[ik] = bars

    by_sym: Dict[str, List[Dict[str, Any]]] = defaultdict(list)
    for row in rows:
        by_sym[row["symbol"]].append(row)

    short = outside = ignored = flipped = kept_user = no_bars = 0
    for sym, members in by_sym.items():
        members.sort(key=lambda r: (r["trigger_at"], r["id"]))
        ik = members[0].get("instrument_key")
        bars = bars_by_ik.get(ik or "") or []
        if not bars:
            no_bars += 1
            for row in members:
                if user_submitted(row):
                    row["skipped_user"] = True
                    kept_user += 1
                    continue
                row["new_wr"] = None
                reject(row, WR_SHORT)
                short += 1
            continue
        old_sides = {r["id"]: r.get("side") for r in members}
        replay_symbol(members, bars, asof)
        for row in members:
            if row.get("skipped_user"):
                kept_user += 1
                continue
            if row.get("remarks") == WR_SHORT:
                short += 1
            elif row.get("remarks") == WR_OUTSIDE:
                outside += 1
            elif row.get("remarks") == IGNORED_OPEN:
                ignored += 1
            if old_sides.get(row["id"]) != row.get("side"):
                flipped += 1

    after = counts(rows)
    gate_bear = sum(1 for r in rows if r.get("new_wr") is not None and r["new_wr"] > -3)
    gate_bull = sum(1 for r in rows if r.get("new_wr") is not None and r["new_wr"] < -97)
    sample = next((r for r in rows if r["id"] == 423), None)
    summary = {
        "period": WR_PERIOD,
        "timeframe": "2h",
        "before": before,
        "after": after,
        "sides_flipped": flipped,
        "wr_outside": outside,
        "wr_short_history": short,
        "ignored_open_row": ignored,
        "kept_user_submitted": kept_user,
        "symbols_without_bars": no_bars,
        "gate_bear_gt_m3": gate_bear,
        "gate_bull_lt_m97": gate_bull,
        "adanient_423": None
        if sample is None
        else {
            "id": 423,
            "symbol": sample.get("symbol"),
            "old_williamsr": sample.get("williamsr"),
            "williamsr": sample.get("new_wr"),
            "side": sample.get("side"),
            "status": sample.get("status"),
            "armed_at": str(sample.get("armed_at")),
            "remarks": sample.get("remarks"),
            "trigger_at": str(sample.get("trigger_at")),
            "bars": len(bars_by_ik.get(sample.get("instrument_key") or "") or []),
        },
    }
    log("plan " + json.dumps(summary, default=str))

    evaluated = sum(1 for r in rows if r.get("skipped_user") or "new_wr" in r)
    if no_bars > max(3, int(0.1 * len(symbols))):
        raise SystemExit(f"abort write: no 2h bars for {no_bars} symbols")
    if evaluated < len(rows):
        raise SystemExit(f"abort write: evaluated {evaluated}/{len(rows)} rows")

    if args.dry_run:
        log("dry-run; no database write")
        return

    written = persist(rows, asof)
    log(f"updated {written} rows")
    active_n = fill_active(args.sleep)
    log(f"active_spread_attempts {active_n}")

    fresh = load_rows()
    after_db = counts(fresh)
    row423 = next((r for r in fresh if r["id"] == 423), None)
    log(
        "done "
        + json.dumps(
            {
                "period": WR_PERIOD,
                "timeframe": "2h",
                "before": before,
                "after": after_db,
                "sides_flipped": flipped,
                "written": written,
                "adanient_423": None if row423 is None else dict(row423),
            },
            default=str,
        )
    )


if __name__ == "__main__":
    main()
