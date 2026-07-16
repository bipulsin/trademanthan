#!/usr/bin/env python3
"""Read-only retrospective READY NOW windows under post-Change-1/2/3 logic.

Does not write to any DB tables. Prints a chronological CSV-like report.
"""
from __future__ import annotations

import json
import sys
from datetime import date, datetime, time, timedelta
from pathlib import Path
from typing import Any, Dict, List, Optional, Set, Tuple

import pytz

ROOT = Path(__file__).resolve().parents[1]
if str(ROOT) not in sys.path:
    sys.path.insert(0, str(ROOT))

IST = pytz.timezone("Asia/Kolkata")
SESSION = "2026-07-16"  # overridden by --session


def _parse_ts(val) -> Optional[datetime]:
    from backend.services.rs_vwap_quality import _parse_ts as p

    return p(val)


def _regime_asof(nifty_candles: List[Dict], now: datetime) -> Dict[str, Any]:
    """Approximate live market_regime using NIFTY candles only up to `now`."""
    from backend.services.daily_checklist_chop_gates import (
        CHOP_ADX_MAX,
        CHOP_RANGE_RATIO,
        CHOP_VIX_DELTA_PCT,
        NIFTY_FLAT_PCT,
        REGIME_CHOP,
        REGIME_TRANSITION,
        REGIME_TREND,
        RANGE_LOOKBACK_DAYS,
        TREND_ADX_MIN,
        _daily_ranges,
        _nifty_adx,
        _vix_snapshot,
    )

    sliced = [c for c in nifty_candles if (_parse_ts(c.get("timestamp")) or now) <= now]
    today_range, avg_range, day_chg = _daily_ranges(sliced, RANGE_LOOKBACK_DAYS)
    adx = _nifty_adx(sliced)
    vix, vix_prev = _vix_snapshot()
    vix_delta = None
    if vix is not None and vix_prev and vix_prev > 0:
        vix_delta = (vix - vix_prev) / vix_prev * 100.0
    chop_reasons: List[str] = []
    if today_range is not None and avg_range and avg_range > 0:
        if today_range < CHOP_RANGE_RATIO * avg_range:
            chop_reasons.append("range")
    if adx is not None and adx < CHOP_ADX_MAX:
        chop_reasons.append("adx")
    if (
        vix_delta is not None
        and vix_delta > CHOP_VIX_DELTA_PCT
        and day_chg is not None
        and abs(day_chg) <= NIFTY_FLAT_PCT
    ):
        chop_reasons.append("vix")
    trend_ok = (
        adx is not None
        and adx >= TREND_ADX_MIN
        and today_range is not None
        and avg_range is not None
        and avg_range > 0
        and today_range > avg_range
        and not chop_reasons
    )
    if chop_reasons:
        regime = REGIME_CHOP
    elif trend_ok:
        regime = REGIME_TREND
    else:
        regime = REGIME_TRANSITION
    return {"market_regime": regime, "market_regime_label": regime}


def _lock_timeline(db) -> Tuple[List[Dict], Dict[str, str]]:
    """Return (events sorted, morning direction map)."""
    from sqlalchemy import text

    rows = db.execute(
        text(
            """
            SELECT event_at AT TIME ZONE 'Asia/Kolkata' AS ist,
                   UPPER(symbol) AS symbol,
                   UPPER(COALESCE(event_type, '')) AS event_type,
                   UPPER(COALESCE(direction, '')) AS direction
            FROM rs_lock_membership_audit
            WHERE (event_at AT TIME ZONE 'Asia/Kolkata')::date = CAST(:d AS date)
            ORDER BY event_at ASC, id ASC
            """
        ),
        {"d": SESSION},
    ).fetchall()
    events = [
        {
            "at": r.ist if getattr(r.ist, "tzinfo", None) else IST.localize(r.ist),
            "symbol": r.symbol,
            "event_type": r.event_type,
            "direction": (
                "SHORT"
                if r.direction in ("BEAR", "SHORT")
                else "LONG"
            ),
        }
        for r in rows
    ]
    morn = {}
    try:
        mrows = db.execute(
            text(
                """
                SELECT UPPER(symbol) AS symbol,
                       UPPER(COALESCE(direction, '')) AS direction
                FROM daily_snapshot
                WHERE snapshot_date = CAST(:d AS date)
                """
            ),
            {"d": SESSION},
        ).fetchall()
        for r in mrows:
            morn[r.symbol] = (
                "SHORT" if r.direction in ("BEAR", "SHORT") else "LONG"
            )
    except Exception:
        pass
    return events, morn


def _locked_at(
    events: List[Dict],
    morning: Dict[str, str],
    now: datetime,
) -> Dict[str, str]:
    """Symbol → direction for names on the lock list at `now`."""
    locked = dict(morning)
    for e in events:
        at = e["at"]
        if at.tzinfo is None:
            at = IST.localize(at)
        else:
            at = at.astimezone(IST)
        if at > now:
            break
        et = e["event_type"]
        sym = e["symbol"]
        if et in ("ENTRY", "ADD", "PROMOTE") or et == "ENTRY":
            locked[sym] = e["direction"]
        elif "ENTRY" in et or et in ("entry",):
            locked[sym] = e["direction"]
        elif et in ("REMOVE", "EXIT", "DROP") or "REMOVE" in et:
            locked.pop(sym, None)
        else:
            # rs_lock uses event_type entry/remove lowercase often
            if et.lower() == "entry":
                locked[sym] = e["direction"]
            elif et.lower() == "remove":
                locked.pop(sym, None)
    return locked


def main() -> int:
    global SESSION
    import argparse

    ap = argparse.ArgumentParser(description="Read-only READY NOW replay for one session")
    ap.add_argument("--session", default=SESSION, help="YYYY-MM-DD (IST session)")
    args = ap.parse_args()
    SESSION = args.session

    from backend.config import settings
    from backend.database import SessionLocal
    from backend.services.daily_checklist_chop_gates import (
        _load_nifty_candles,
        count_pullback_attempts,
        count_whipsaw_reversals,
        stopped_out_today,
    )
    from backend.services.daily_checklist_live import _latest_nifty_pct, _ranking_for_direction
    from backend.services.daily_checklist_trade_state import (
        apply_warning_stack_downgrades,
        compute_trade_state_for_stock,
        overlay_live_momentum_from_candles,
        session_day_levels_from_candles,
        _lot_for_symbol,
        _recent_removals,
        STATE_READY,
        STATE_READY_RECHECK,
    )
    from backend.services.daily_checklist_zones import annotate_regime_context
    from backend.services.kavach_10m import metrics_from_10m_candles
    from backend.services.kavach_universe_vwap_scan import _atr_map
    from backend.services.relative_strength_scanner import (
        CANDLE_DAYS_BACK,
        CANDLE_INTERVAL,
        MIN_BARS,
        _sorted_candles,
    )
    from backend.services.rs_conviction_candles import load_instrument_atr_maps
    from backend.services.rs_conviction_config import get_config
    from backend.services.upstox_service import UpstoxService

    d = date.fromisoformat(SESSION)
    start = IST.localize(datetime.combine(d, time(9, 45)))
    end = IST.localize(datetime.combine(d, time(15, 25)))

    db = SessionLocal()
    try:
        events, morning = _lock_timeline(db)
        # Normalize event types from DB
        for e in events:
            e["event_type"] = (e["event_type"] or "").lower()

        all_syms: Set[str] = set(morning)
        for e in events:
            all_syms.add(e["symbol"])
        symbols = sorted(all_syms)
        print(f"# symbols_touched={len(symbols)} morning={len(morning)} events={len(events)}", flush=True)

        ikey_map, _ = load_instrument_atr_maps(db, set(symbols))
        atr_map = _atr_map(db, symbols) or {}
        nifty_pct = _latest_nifty_pct(db)
        cfg = get_config()
        near_atr = float(cfg.get("convergence_atr") or 0.35)
        removals_all = _recent_removals(db, SESSION)
        stopped_map = stopped_out_today(db, SESSION, symbols)
        nifty_candles = _load_nifty_candles()
        upstox = UpstoxService(settings.UPSTOX_API_KEY, settings.UPSTOX_API_SECRET)

        candle_cache: Dict[str, List] = {}
        lot_cache: Dict[str, int] = {}
        for sym in symbols:
            ikey = ikey_map.get(sym)
            if not ikey:
                continue
            try:
                raw = upstox.get_historical_candles_by_instrument_key(
                    ikey,
                    interval=CANDLE_INTERVAL,
                    days_back=max(CANDLE_DAYS_BACK, 5),
                    range_end_date=d,
                )
            except Exception as exc:
                print(f"# fetch_fail {sym}: {exc}", flush=True)
                continue
            if not raw or len(raw) < MIN_BARS:
                continue
            candle_cache[sym] = _sorted_candles(raw)
            lot_cache[sym], _ = _lot_for_symbol(db, sym)
            print(f"# loaded {sym} bars={len(candle_cache[sym])} lot={lot_cache[sym]}", flush=True)
    finally:
        db.close()

    hits: List[Dict[str, Any]] = []
    cur = start
    while cur <= end:
        # rebuild lock set with lowercase event types
        locked: Dict[str, str] = dict(morning)
        for e in events:
            at = e["at"]
            if at.tzinfo is None:
                at = IST.localize(at)
            else:
                at = at.astimezone(IST)
            if at > cur:
                break
            et = e["event_type"]
            if et == "entry":
                locked[e["symbol"]] = e["direction"]
            elif et == "remove":
                locked.pop(e["symbol"], None)

        mkt = _regime_asof(nifty_candles, cur)
        rem_asof = [
            r
            for r in (removals_all or [])
            if (_parse_ts(r.get("at") or r.get("event_at")) or cur) <= cur
        ]
        # only last-hour removals for CHURN — annotate_regime uses removal_counts_last_hour(now=)
        stocks_batch: List[Dict[str, Any]] = []
        meta_batch: List[Tuple[str, Dict, Dict]] = []

        for sym, direction in sorted(locked.items()):
            candles = candle_cache.get(sym)
            if not candles:
                continue
            sliced = [c for c in candles if (_parse_ts(c.get("timestamp")) or cur) <= cur]
            if len(sliced) < 30:
                continue
            ranking = _ranking_for_direction(direction)
            stock: Dict[str, Any] = {
                "symbol": sym,
                "direction": direction,
                "confidence": None,
            }
            overlay_live_momentum_from_candles(stock, sliced, nifty_pct=nifty_pct)
            m = metrics_from_10m_candles(
                sliced, ranking_type=ranking, nifty_pct=nifty_pct, now=cur
            )
            if not m:
                continue
            # Stock hard-block uses TREND/TRANSITION only — index CHOP is applied
            # via market_regime_idx (tier-down), matching live enrich separation.
            stock_regime = (m.get("market_regime") or "TRANSITION").upper()
            if stock_regime == "CHOP":
                stock_regime = "TRANSITION"
            levels = {
                "price": m.get("price"),
                "ema5": m.get("ema5"),
                "ema10": m.get("ema10_10m") or m.get("ema10"),
                "vwap": m.get("vwap"),
                "adx": m.get("adx"),
                "confidence_grade": m.get("confidence_grade")
                or m.get("dashboard_kavach"),
                "market_regime": stock_regime,
                "source": "replay_10m",
            }
            stock["confidence"] = levels["confidence_grade"]
            stock["dashboard_kavach_live"] = m.get("kavach_state")
            atr_pct = float(atr_map.get(sym) or 1.0)
            price = float(m.get("price") or 0)
            atr = (price * atr_pct / 100.0) if price else None
            is_long = direction != "SHORT"
            whip = count_whipsaw_reversals(
                sliced,
                session_date=SESSION,
                is_long=is_long,
                near_atr=near_atr,
                atr=atr,
            )
            pb = count_pullback_attempts(
                sliced,
                session_date=SESSION,
                is_long=is_long,
                near_atr=near_atr,
                atr=atr,
            )
            smeta = session_day_levels_from_candles(sliced, SESSION)
            ts = compute_trade_state_for_stock(
                stock,
                levels=levels,
                atr_pct=atr_pct,
                lot=int(lot_cache.get(sym) or 1),
                session_hi=smeta.get("session_hi"),
                session_lo=smeta.get("session_lo"),
                open_pos=None,
                promo=None,
                cfg=cfg,
                market_regime_idx=mkt.get("market_regime"),
                direction_unstable=False,
                whipsaw_count=whip,
                pullback_count=pb,
                stopped=stopped_map.get(sym),
                now=cur,
                session_open=smeta.get("session_open"),
                opening_candle_high=smeta.get("opening_candle_high"),
                opening_candle_low=smeta.get("opening_candle_low"),
            )
            stock.update(ts)
            stocks_batch.append(stock)
            meta_batch.append((sym, levels, ts))

        annotate_regime_context(
            stocks_batch,
            market_regime=mkt.get("market_regime"),
            market_regime_label=mkt.get("market_regime_label"),
            imbalance=None,
            removals=rem_asof,
            now=cur,
        )
        apply_warning_stack_downgrades(stocks_batch)

        for stock in stocks_batch:
            st = stock.get("trade_state")
            if st not in (STATE_READY, STATE_READY_RECHECK):
                continue
            take = bool(stock.get("trade_take_enabled"))
            hits.append(
                {
                    "time": cur.strftime("%H:%M"),
                    "symbol": stock.get("symbol"),
                    "direction": stock.get("direction"),
                    "state": st,
                    "take_enabled": take,
                    "entry": stock.get("trade_entry"),
                    "sl": stock.get("trade_sl"),
                    "rr": stock.get("trade_rr"),
                    "rr_label": stock.get("trade_rr_label"),
                    "risk_inr": stock.get("trade_risk_inr"),
                    "disable_reason": stock.get("trade_take_disable_reason"),
                    "state_reason": stock.get("trade_state_reason"),
                    "confidence": stock.get("confidence"),
                    "whipsaw": stock.get("whipsaw_count"),
                    "badges": stock.get("gate_badges"),
                    "regime": mkt.get("market_regime"),
                }
            )

        if cur.minute in (45, 15) and cur.hour in (10, 12, 14):
            print(f"# progress {cur.strftime('%H:%M')} locked={len(locked)} hits_so_far={len(hits)}", flush=True)
        cur += timedelta(minutes=10)

    print("\n=== CHRONOLOGICAL READY WINDOWS (new logic) ===", flush=True)
    print(
        "time\tsymbol\tdir\tstate\ttake\tentry\tsl\trr\trisk\tdisable_or_note\tconf\twhip\tregime",
        flush=True,
    )
    for h in hits:
        note = ""
        if h["take_enabled"]:
            note = "TAKE_OK"
        else:
            note = h.get("disable_reason") or h.get("state_reason") or "take_disabled"
        print(
            f"{h['time']}\t{h['symbol']}\t{h['direction']}\t{h['state']}\t"
            f"{h['take_enabled']}\t{h['entry']}\t{h['sl']}\t{h['rr_label'] or h['rr']}\t"
            f"{h['risk_inr']}\t{note}\t{h['confidence']}\t{h['whipsaw']}\t{h['regime']}",
            flush=True,
        )

    # UPL summary
    upl = [h for h in hits if h["symbol"] == "UPL"]
    upl_ok = [h for h in upl if h["take_enabled"]]
    print("\n=== UPL SUMMARY ===", flush=True)
    print(
        json.dumps(
            {
                "ready_rows": len(upl),
                "take_enabled_rows": len(upl_ok),
                "windows": upl,
            },
            indent=2,
            default=str,
        ),
        flush=True,
    )

    by_sym: Dict[str, List] = {}
    for h in hits:
        by_sym.setdefault(h["symbol"], []).append(h)
    print("\n=== PER-SYMBOL TAKE_OK COUNT ===", flush=True)
    for sym in sorted(by_sym):
        rows = by_sym[sym]
        ok = sum(1 for r in rows if r["take_enabled"])
        times = [r["time"] for r in rows if r["take_enabled"]]
        print(f"{sym}\tready={len(rows)}\ttake_ok={ok}\ttimes={times}", flush=True)

    print(
        "\n"
        + json.dumps(
            {
                "ok": True,
                "session": SESSION,
                "total_ready_rows": len(hits),
                "total_take_ok": sum(1 for h in hits if h["take_enabled"]),
                "symbols_with_take_ok": sorted(
                    {h["symbol"] for h in hits if h["take_enabled"]}
                ),
            },
            indent=2,
        ),
        flush=True,
    )
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
