"""Orchestrate Kavach 22-Aug BT-1..4 research run over trade_log."""
from __future__ import annotations

import logging
from datetime import date, datetime, time, timedelta
from typing import Any, Dict, List, Optional

import pytz
from sqlalchemy import text

from backend.database import SessionLocal
from backend.services.kavach_bt_checkpoint.candles import (
    day_bars_10m_with_indicators,
    fetch_5m_candles,
)
from backend.services.kavach_bt_checkpoint.config import DATE_FROM, DATE_TO, RUN_ID_PREFIX
from backend.services.kavach_bt_checkpoint.db import (
    ensure_bt_checkpoint_tables,
    replace_summaries,
    upsert_pullback_bar,
    upsert_trade_detail,
)
from backend.services.kavach_bt_checkpoint.exits import (
    exit_c_actual,
    path_mfe_mae,
    pick_best_exit,
    simulate_dynamic_trail_exit,
    simulate_exit_a_baseline,
)
from backend.services.kavach_bt_checkpoint.garuda import classify_garuda
from backend.services.kavach_bt_checkpoint.pullback import (
    count_pullbacks_legacy_on_10m,
    count_pullbacks_v2_on_10m,
    pullback_at_entry,
)
from backend.services.kavach_bt_checkpoint.report import build_summary_rows
from backend.services.kavach_bt_checkpoint.resistance import evaluate_resistance_confluence
from backend.services.kavach_bt_checkpoint.universe import resolve_instrument_key
from backend.services.rule27_trade_log import ensure_trade_log_table

logger = logging.getLogger(__name__)
IST = pytz.timezone("Asia/Kolkata")


def _f(v: Any) -> Optional[float]:
    if v is None or v == "":
        return None
    try:
        return float(v)
    except (TypeError, ValueError):
        return None


def _as_dt(session_date: date, tval: Any) -> Optional[datetime]:
    if tval is None:
        return None
    if isinstance(tval, datetime):
        dt = tval
        if dt.tzinfo is None:
            return IST.localize(dt)
        return dt.astimezone(IST)
    if isinstance(tval, time):
        return IST.localize(datetime.combine(session_date, tval))
    s = str(tval).strip()
    # ISO datetime
    try:
        dt = datetime.fromisoformat(s.replace("Z", "+00:00"))
        if dt.tzinfo is None:
            return IST.localize(dt)
        return dt.astimezone(IST)
    except Exception:
        pass
    for fmt in ("%H:%M:%S", "%H:%M"):
        try:
            tm = datetime.strptime(s, fmt).time()
            return IST.localize(datetime.combine(session_date, tm))
        except ValueError:
            continue
    return None


def _risk_pts(row: Dict[str, Any]) -> Optional[float]:
    pts = _f(row.get("planned_risk_pts"))
    if pts and pts > 0:
        return pts
    ep = _f(row.get("entry_price"))
    e10 = _f(row.get("ema10_at_entry"))
    vwap = _f(row.get("vwap_at_entry"))
    cands = []
    if ep is not None and e10 is not None:
        cands.append(abs(ep - e10))
    if ep is not None and vwap is not None:
        cands.append(abs(ep - vwap))
    pos = [x for x in cands if x and x > 0]
    if pos:
        return min(pos)
    # 0.3% fallback so exit sims can run
    if ep and ep > 0:
        return ep * 0.003
    return None


def _pnl(row: Dict[str, Any]) -> Optional[float]:
    pts = _f(row.get("points_captured"))
    qty = _f(row.get("qty"))
    if pts is not None and qty is not None:
        return pts * qty
    ep = _f(row.get("entry_price"))
    xp = _f(row.get("exit_price"))
    qty = qty or 1
    if ep is None or xp is None:
        return None
    d = str(row.get("direction") or "").upper()
    raw = (xp - ep) if d in ("LONG", "BUY", "B") else (ep - xp)
    return raw * qty


def load_closed_trades(
    *,
    date_from: date = DATE_FROM,
    date_to: date = DATE_TO,
) -> List[Dict[str, Any]]:
    ensure_trade_log_table()
    db = SessionLocal()
    try:
        rows = (
            db.execute(
                text(
                    """
                    SELECT *
                    FROM trade_log
                    WHERE session_date >= :df AND session_date <= :dt
                      AND exit_price IS NOT NULL
                    ORDER BY session_date, entry_time
                    """
                ),
                {"df": date_from.isoformat(), "dt": date_to.isoformat()},
            )
            .mappings()
            .all()
        )
        return [dict(r) for r in rows]
    finally:
        db.close()


def _hold_bars(
    bars: List[Dict[str, Any]],
    entry_dt: datetime,
    exit_dt: Optional[datetime],
) -> List[Dict[str, Any]]:
    out = []
    for b in bars:
        be = b.get("bar_end")
        if be is None:
            continue
        if be.tzinfo is None:
            be = IST.localize(be)
        else:
            be = be.astimezone(IST)
        if be < entry_dt:
            continue
        if exit_dt is not None and be > exit_dt + timedelta(minutes=10):
            # allow a little slack past recorded exit for sims
            break
        out.append({**b, "bar_end": be})
    # If exit cuts early, still need bars for sim until force exit — use rest of day
    if len(out) < 2:
        out = []
        for b in bars:
            be = b.get("bar_end")
            if be is None:
                continue
            if be.tzinfo is None:
                be = IST.localize(be)
            else:
                be = be.astimezone(IST)
            if be >= entry_dt:
                out.append({**b, "bar_end": be})
    return out


def process_trade(
    row: Dict[str, Any],
    *,
    run_id: str,
    candle_cache: Dict[str, List[Dict[str, Any]]],
) -> Optional[Dict[str, Any]]:
    db = SessionLocal()
    try:
        symbol = (row.get("symbol") or "").strip().upper()
        direction = (row.get("direction") or "").strip().upper()
        sd = row.get("session_date")
        if isinstance(sd, datetime):
            session_date = sd.date()
        elif isinstance(sd, date):
            session_date = sd
        else:
            session_date = date.fromisoformat(str(sd)[:10])

        ikey = resolve_instrument_key(db, symbol)
        if not ikey:
            logger.warning("No instrument key for %s", symbol)
            return None

        cache_key = f"{ikey}|{session_date.isoformat()}"
        if cache_key not in candle_cache:
            raw5 = fetch_5m_candles(ikey, session_date)
            candle_cache[cache_key] = day_bars_10m_with_indicators(raw5, session_date)
        bars = candle_cache[cache_key]
        if not bars:
            logger.warning("No 10m bars for %s %s", symbol, session_date)
            return None

        entry_dt = _as_dt(session_date, row.get("entry_time"))
        if entry_dt is None:
            entry_dt = IST.localize(datetime.combine(session_date, time(9, 30)))
        exit_dt = _as_dt(session_date, row.get("exit_time"))
        entry_price = _f(row.get("entry_price")) or 0.0
        risk = _risk_pts(row) or (entry_price * 0.003)

        pb = pullback_at_entry(bars, entry_dt, direction)
        entry_idx = pb.get("bar_idx") if pb.get("bar_idx") is not None else 0
        res = evaluate_resistance_confluence(
            bars,
            entry_idx=int(entry_idx),
            entry_price=entry_price,
            direction=direction,
        )

        hold = _hold_bars(bars, entry_dt, exit_dt)
        path = path_mfe_mae(hold, entry=entry_price, risk_pts=risk, direction=direction)
        exit_a = simulate_exit_a_baseline(
            hold, entry=entry_price, risk_pts=risk, direction=direction, symbol=symbol
        )
        exit_b = simulate_dynamic_trail_exit(
            hold, entry=entry_price, risk_pts=risk, direction=direction
        )
        exit_c = exit_c_actual(row)
        best = pick_best_exit(exit_a, exit_b, exit_c)

        g = classify_garuda(
            symbol=symbol,
            direction=direction,
            entry_time=entry_dt,
            session_date=session_date.isoformat(),
        )

        mfe = _f(row.get("mfe_r"))
        mae = _f(row.get("mae_r"))
        if mfe is None:
            mfe = path.get("mfe_r")
        if mae is None:
            mae = path.get("mae_r")

        detail = {
            "run_id": run_id,
            "trade_log_id": int(row["id"]),
            "session_date": session_date.isoformat(),
            "symbol": symbol,
            "direction": direction,
            "entry_time": entry_dt,
            "entry_price": entry_price,
            "exit_time": exit_dt,
            "exit_price": _f(row.get("exit_price")),
            "grade": row.get("confidence_at_entry") or row.get("grade"),
            "r_realized": _f(row.get("r_realized")),
            "mfe_r": mfe,
            "mae_r": mae,
            "pnl": _pnl(row),
            "pb_legacy": pb.get("pb_legacy"),
            "pb_v2": pb.get("pb_v2"),
            "pb_hard_blocked": bool(pb.get("pb_hard_blocked")),
            "res_confluence": bool(res.get("res_confluence")),
            "nearest_pivot": res.get("nearest_pivot"),
            "pivot_kind": res.get("pivot_kind"),
            "pivot_zone_pct": res.get("pivot_zone_pct"),
            "cluster_n": res.get("cluster_n"),
            "exit_a_price": (exit_a or {}).get("exit_price"),
            "exit_a_time": (exit_a or {}).get("exit_time"),
            "exit_a_r": (exit_a or {}).get("exit_r"),
            "exit_a_reason": (exit_a or {}).get("reason"),
            "exit_b_price": (exit_b or {}).get("exit_price"),
            "exit_b_time": (exit_b or {}).get("exit_time"),
            "exit_b_r": (exit_b or {}).get("exit_r"),
            "exit_b_reason": (exit_b or {}).get("reason"),
            "exit_c_price": exit_c.get("exit_price"),
            "exit_c_time": exit_c.get("exit_time"),
            "exit_c_r": _f(exit_c.get("exit_r")),
            "exit_c_reason": exit_c.get("reason"),
            "exit_c_trigger_type": exit_c.get("exit_trigger_type"),
            "best_exit_method": best,
            "garuda_confluence": g.get("garuda_confluence"),
            "garuda_rank": g.get("garuda_rank"),
            "garuda_direction": g.get("garuda_direction"),
            "components": {
                "pullback": pb,
                "resistance": res,
                "risk_pts": risk,
                "path": path,
            },
        }
        upsert_trade_detail(detail)
        return detail
    finally:
        db.close()


def sample_fo_pullback_bars(
    *,
    run_id: str,
    symbols: List[str],
    session_dates: List[date],
    max_symbols: int = 15,
) -> int:
    """FO-wide pullback distribution sample (not full 200×all days — bounded)."""
    db = SessionLocal()
    n_written = 0
    try:
        for sym in symbols[:max_symbols]:
            ikey = resolve_instrument_key(db, sym)
            if not ikey:
                continue
            for sd in session_dates:
                try:
                    raw5 = fetch_5m_candles(ikey, sd)
                    bars = day_bars_10m_with_indicators(raw5, sd)
                except Exception as e:
                    logger.debug("FO sample skip %s %s: %s", sym, sd, e)
                    continue
                if not bars:
                    continue
                leg_l, leg_s = count_pullbacks_legacy_on_10m(bars)
                v2_l, v2_s, flags = count_pullbacks_v2_on_10m(bars)
                # store every 3rd bar to bound size
                for i, b in enumerate(bars):
                    if i % 3 != 0:
                        continue
                    fl = flags[i] if i < len(flags) else {}
                    upsert_pullback_bar(
                        {
                            "run_id": run_id,
                            "session_date": sd.isoformat(),
                            "bar_end": b["bar_end"],
                            "symbol": sym,
                            "pb_legacy": max(leg_l[i], leg_s[i]),
                            "pb_v2": max(v2_l[i], v2_s[i]),
                            "touched_ema5": fl.get("touched_ema5"),
                            "touched_ema10": fl.get("touched_ema10"),
                            "touched_vwap": fl.get("touched_vwap"),
                            "dual_reset": fl.get("dual_reset"),
                        }
                    )
                    n_written += 1
    finally:
        db.close()
    return n_written


def run_checkpoint(
    *,
    date_from: date = DATE_FROM,
    date_to: date = DATE_TO,
    run_id: Optional[str] = None,
    fo_sample: bool = True,
) -> Dict[str, Any]:
    ensure_bt_checkpoint_tables()
    rid = run_id or f"{RUN_ID_PREFIX}_{datetime.now(IST).strftime('%Y%m%d_%H%M%S')}"
    trades = load_closed_trades(date_from=date_from, date_to=date_to)
    candle_cache: Dict[str, List[Dict[str, Any]]] = {}
    details: List[Dict[str, Any]] = []
    errors = []

    for row in trades:
        try:
            d = process_trade(row, run_id=rid, candle_cache=candle_cache)
            if d:
                details.append(d)
        except Exception as e:
            logger.exception("trade %s failed", row.get("id"))
            errors.append({"trade_log_id": row.get("id"), "error": str(e)})

    summaries = build_summary_rows(details)
    replace_summaries(rid, summaries)

    fo_bars = 0
    if fo_sample and details:
        syms = sorted({d["symbol"] for d in details})
        dates = sorted({date.fromisoformat(str(d["session_date"])[:10]) for d in details})
        try:
            fo_bars = sample_fo_pullback_bars(run_id=rid, symbols=syms, session_dates=dates)
        except Exception as e:
            logger.exception("FO pullback sample failed")
            errors.append({"fo_sample": str(e)})

    return {
        "ok": True,
        "run_id": rid,
        "n_trades_loaded": len(trades),
        "n_details": len(details),
        "n_summaries": len(summaries),
        "fo_pullback_bars": fo_bars,
        "errors": errors,
        "summaries": summaries,
        "details": details,
    }
