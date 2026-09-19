"""EOD reconstruction: futures underlying, Black-76 IV/delta, liquidity bands."""
from __future__ import annotations

import json
import uuid
from collections import defaultdict
from datetime import date, datetime, time, timezone
from typing import Any, Dict, List, Optional, Sequence, Tuple
from zoneinfo import ZoneInfo

from sqlalchemy import text

from backend.database import SessionLocal
from backend.services.tarang.calendar import MCX_CLOSE_MINUTES
from backend.services.tarang.greeks import black76_delta, implied_vol_black76
from backend.services.tarang.schema import ensure_tarang_tables

IST = ZoneInfo("Asia/Kolkata")
MIN_VOLUME_DEFAULT = 5
# Expiry-day options: skip IV/delta. T would be a stub of remaining session and IVs are unstable.
EXPIRY_DAY_POLICY = "skip_expiry_day_options"
YEARS = 365.25


def session_close_ist(d: date) -> datetime:
    h, m = divmod(MCX_CLOSE_MINUTES, 60)
    return datetime.combine(d, time(h, m), tzinfo=IST)


def years_to_expiry(trade_date: date, expiry_date: date) -> Optional[float]:
    """T from 23:30 IST on observation date to 23:30 IST on expiry date. T>0; expiry day skipped."""
    if trade_date >= expiry_date:
        return None
    t0 = session_close_ist(trade_date)
    t1 = session_close_ist(expiry_date)
    seconds = (t1 - t0).total_seconds()
    if seconds <= 0:
        return None
    return seconds / (YEARS * 24 * 3600)


def put_call_parity_F(calls: Sequence[Dict[str, Any]], puts: Sequence[Dict[str, Any]]) -> Optional[float]:
    by_k: Dict[float, Dict[str, float]] = {}
    for r in calls:
        if r.get("close") is None:
            continue
        by_k.setdefault(float(r["strike"]), {})["C"] = float(r["close"])
    for r in puts:
        if r.get("close") is None:
            continue
        by_k.setdefault(float(r["strike"]), {})["P"] = float(r["close"])
    est: List[float] = []
    for k, pair in by_k.items():
        if "C" in pair and "P" in pair:
            est.append(k + pair["C"] - pair["P"])
    if not est:
        return None
    est.sort()
    return est[len(est) // 2]


def _is_otm(right: str, strike: float, F: float) -> bool:
    if right == "CE":
        return strike > F
    if right == "PE":
        return strike < F
    return False


def reconstruct_slice(
    rows: Sequence[Dict[str, Any]],
    *,
    min_volume: int = MIN_VOLUME_DEFAULT,
) -> List[Dict[str, Any]]:
    """IV from OTM only; volume>=min and OI>0; skip untraded and placeholder closes."""
    fut = next((r for r in rows if r.get("option_type") == "FUT" and r.get("close")), None)
    opts = [r for r in rows if r.get("option_type") in ("CE", "PE")]
    calls = [r for r in opts if r.get("option_type") == "CE"]
    puts = [r for r in opts if r.get("option_type") == "PE"]
    estimated = False
    F = float(fut["close"]) if fut and fut.get("close") else None
    if F is None:
        F = put_call_parity_F(calls, puts)
        estimated = True
    out: List[Dict[str, Any]] = []
    if F is None:
        for r in opts:
            out.append({**r, "iv": None, "delta": None, "greeks_source": None, "underlying_price": None, "underlying_source": None})
        return out
    src = "put_call_parity" if estimated else "futcom_close"
    trade_d = opts[0]["trade_date"] if opts else None
    expiry_d = opts[0]["expiry_date"] if opts else None
    T = years_to_expiry(trade_d, expiry_d) if trade_d and expiry_d else None
    for r in opts:
        rec = {
            **r,
            "underlying_price": F,
            "underlying_source": src,
            "iv": None,
            "delta": None,
            "greeks_source": None,
        }
        if T is None:
            out.append(rec)
            continue
        if not r.get("traded"):
            out.append(rec)
            continue
        vol = int(r.get("volume_lots") or 0)
        oi = int(r.get("oi_lots") or 0)
        close = r.get("close")
        if vol < min_volume or oi <= 0 or close is None:
            out.append(rec)
            continue
        right = r["option_type"]
        if not _is_otm(right, float(r["strike"]), F):
            out.append(rec)
            continue
        iv = implied_vol_black76(float(close), F, float(r["strike"]), T, right)
        rec["iv"] = iv
        rec["delta"] = black76_delta(F, float(r["strike"]), T, iv, right) if iv else None
        rec["greeks_source"] = "black76_otm" if iv else None
        out.append(rec)
    return out


def persist_reconstruction(*, min_volume: int = MIN_VOLUME_DEFAULT) -> Dict[str, Any]:
    ensure_tarang_tables()
    run_id = str(uuid.uuid4())
    db = SessionLocal()
    updated = 0
    try:
        rows = db.execute(
            text(
                """
                SELECT id, symbol, expiry_date, option_type, strike, trade_date,
                       close, volume_lots, oi_lots, traded
                FROM tarang_hist_eod
                ORDER BY symbol, expiry_date, trade_date, option_type, strike
                """
            )
        ).mappings().all()
        groups: Dict[Tuple, List[Dict[str, Any]]] = defaultdict(list)
        for r in rows:
            d = dict(r)
            groups[(d["symbol"], d["expiry_date"], d["trade_date"])].append(d)
        for _k, slice_rows in groups.items():
            recs = reconstruct_slice(slice_rows, min_volume=min_volume)
            for rec in recs:
                if rec.get("id") is None:
                    continue
                db.execute(
                    text(
                        """
                        UPDATE tarang_hist_eod SET
                            iv = :iv, delta = :delta, greeks_source = :greeks_source,
                            underlying_price = :underlying_price,
                            underlying_source = :underlying_source,
                            reconstruction_run_id = :rid
                        WHERE id = :id
                        """
                    ),
                    {
                        "iv": rec.get("iv"),
                        "delta": rec.get("delta"),
                        "greeks_source": rec.get("greeks_source"),
                        "underlying_price": rec.get("underlying_price"),
                        "underlying_source": rec.get("underlying_source"),
                        "rid": run_id,
                        "id": rec["id"],
                    },
                )
                updated += 1
        db.commit()
        return {
            "reconstruction_run_id": run_id,
            "rows_updated": updated,
            "min_volume": min_volume,
            "expiry_day_policy": EXPIRY_DAY_POLICY,
            "tte_note": "T = 23:30 IST on trade_date to 23:30 IST on expiry_date / 365.25. Expiry-day options skipped (T would be a residual session stub).",
        }
    except Exception:
        db.rollback()
        raise
    finally:
        db.close()


def liquidity_report(*, d_min: float = 0.10, d_max: float = 0.16) -> Dict[str, Any]:
    """Per date+expiry+underlying: traded 10-16 |delta| strikes and whether a full IC with traded wings was possible."""
    ensure_tarang_tables()
    db = SessionLocal()
    try:
        rows = db.execute(
            text(
                """
                SELECT symbol, expiry_date, trade_date, option_type, strike, delta,
                       volume_lots, oi_lots, traded, close
                FROM tarang_hist_eod
                WHERE option_type IN ('CE','PE')
                ORDER BY symbol, expiry_date, trade_date
                """
            )
        ).mappings().all()
    finally:
        db.close()
    groups: Dict[Tuple, List[Dict[str, Any]]] = defaultdict(list)
    for r in rows:
        groups[(r["symbol"], str(r["expiry_date"]), str(r["trade_date"]))].append(dict(r))
    out_rows: List[Dict[str, Any]] = []
    for (sym, exp, td), qs in groups.items():
        traded = [q for q in qs if q.get("traded")]
        band = [
            q
            for q in traded
            if q.get("delta") is not None and d_min <= abs(float(q["delta"])) <= d_max
        ]
        n_band = len(band)
        short_pe = [q for q in band if q["option_type"] == "PE"]
        short_ce = [q for q in band if q["option_type"] == "CE"]
        ic = False
        if short_pe and short_ce:
            pe_k = max(q["strike"] for q in short_pe)
            ce_k = min(q["strike"] for q in short_ce)
            wing_pe = any(q["option_type"] == "PE" and q["strike"] < pe_k and q.get("traded") for q in qs)
            wing_ce = any(q["option_type"] == "CE" and q["strike"] > ce_k and q.get("traded") for q in qs)
            ic = wing_pe and wing_ce
        out_rows.append(
            {
                "symbol": sym,
                "expiry_date": exp[:10],
                "trade_date": td[:10],
                "traded_strikes_10_16d": n_band,
                "iron_condor_traded_wings": ic,
            }
        )
    return {
        "product": "Kosmic Tarang",
        "d_min": d_min,
        "d_max": d_max,
        "rows": out_rows,
        "dates": len({r["trade_date"] for r in out_rows}),
        "note": "EOD reconstructed |delta|; wings must have traded (volume>0, non-placeholder close).",
    }
