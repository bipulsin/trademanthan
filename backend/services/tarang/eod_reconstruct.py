"""EOD reconstruction: futures underlying, Black-76 IV/delta, liquidity bands.

Put-call parity (Black-76, DF=1, undiscounted — same as greeks.py default df=1.0):

    F ≈ K + (C − P)

at a common strike K. Median across strikes that have both a call and put close.
American/MCX early-exercise bias is ignored; this is the estimator used to
(1) fill F when no FUTCOM exists, and (2) pick the FUTCOM contract whose close
is closest to that parity-implied forward (not necessarily the option's own expiry).
"""
from __future__ import annotations

import uuid
from collections import defaultdict
from datetime import date, datetime, time
from typing import Any, Dict, List, Optional, Sequence, Tuple
from zoneinfo import ZoneInfo

from sqlalchemy import text

from backend.database import SessionLocal
from backend.services.tarang.calendar import MCX_CLOSE_MINUTES
from backend.services.tarang.greeks import black76_delta, implied_vol_black76
from backend.services.tarang.schema import ensure_tarang_tables

IST = ZoneInfo("Asia/Kolkata")
MIN_VOLUME_DEFAULT = 5
EXPIRY_DAY_POLICY = "skip_expiry_day_options"
YEARS = 365.25
PARITY_FORMULA = "F ≈ K + (C − P)  (Black-76 with DF=1, undiscounted)"


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
    """Median F = K + C − P over strikes with both closes. Prefer traded non-placeholder quotes."""
    def _index(rows: Sequence[Dict[str, Any]], *, traded_only: bool) -> Dict[float, float]:
        out: Dict[float, float] = {}
        for r in rows:
            if r.get("close") is None:
                continue
            if traded_only and not r.get("traded"):
                continue
            out[float(r["strike"])] = float(r["close"])
        return out

    for traded_only in (True, False):
        by_c = _index(calls, traded_only=traded_only)
        by_p = _index(puts, traded_only=traded_only)
        est = [k + by_c[k] - by_p[k] for k in by_c if k in by_p]
        if est:
            est.sort()
            return est[len(est) // 2]
    return None


def match_futures_underlying(
    parity_F: Optional[float],
    futures: Sequence[Dict[str, Any]],
    *,
    option_expiry: Optional[date] = None,
) -> Dict[str, Any]:
    """Pick FUTCOM whose close is closest to the parity-implied forward.

    Does not require the futures expiry to equal the option expiry. If no futures
    closes exist, fall back to parity and flag estimated=True.
    """
    cands = [f for f in futures if f.get("close") is not None and float(f["close"]) > 0]
    if not cands:
        return {
            "F": parity_F,
            "estimated": True,
            "source": "put_call_parity" if parity_F is not None else None,
            "fut_symbol": None,
            "fut_expiry": None,
            "parity_F": parity_F,
            "abs_diff": None,
        }
    if parity_F is None:
        if option_expiry is not None:
            same = [f for f in cands if _as_date(f.get("expiry_date")) == _as_date(option_expiry)]
            pick = same[0] if same else min(cands, key=lambda f: _as_date(f.get("expiry_date")) or date.max)
        else:
            pick = cands[0]
        return {
            "F": float(pick["close"]),
            "estimated": False,
            "source": "futcom_close",
            "fut_symbol": str(pick.get("symbol") or ""),
            "fut_expiry": _as_date(pick.get("expiry_date")),
            "parity_F": None,
            "abs_diff": None,
        }
    pick = min(cands, key=lambda f: abs(float(f["close"]) - float(parity_F)))
    return {
        "F": float(pick["close"]),
        "estimated": False,
        "source": "futcom_closest_parity",
        "fut_symbol": str(pick.get("symbol") or ""),
        "fut_expiry": _as_date(pick.get("expiry_date")),
        "parity_F": float(parity_F),
        "abs_diff": abs(float(pick["close"]) - float(parity_F)),
    }


def _as_date(v: Any) -> Optional[date]:
    if v is None:
        return None
    if isinstance(v, datetime):
        return v.date()
    if isinstance(v, date):
        return v
    try:
        return date.fromisoformat(str(v)[:10])
    except ValueError:
        return None


def _is_otm(right: str, strike: float, F: float) -> bool:
    if right == "CE":
        return strike > F
    if right == "PE":
        return strike < F
    return False


def reconstruct_slice(
    rows: Sequence[Dict[str, Any]],
    *,
    futures_for_date: Optional[Sequence[Dict[str, Any]]] = None,
    min_volume: int = MIN_VOLUME_DEFAULT,
) -> List[Dict[str, Any]]:
    """IV from OTM only; volume>=min and OI>0; skip untraded and placeholder closes."""
    opts = [r for r in rows if r.get("option_type") in ("CE", "PE")]
    futs = list(futures_for_date) if futures_for_date is not None else [r for r in rows if r.get("option_type") == "FUT"]
    calls = [r for r in opts if r.get("option_type") == "CE"]
    puts = [r for r in opts if r.get("option_type") == "PE"]
    expiry_d = _as_date(opts[0]["expiry_date"]) if opts else None
    trade_d = _as_date(opts[0]["trade_date"]) if opts else None
    parity_F = put_call_parity_F(calls, puts)
    matched = match_futures_underlying(parity_F, futs, option_expiry=expiry_d)
    F = matched.get("F")
    out: List[Dict[str, Any]] = []
    extra = {
        "underlying_fut_symbol": matched.get("fut_symbol"),
        "underlying_fut_expiry": matched.get("fut_expiry"),
        "underlying_estimated": bool(matched.get("estimated")),
        "parity_forward": matched.get("parity_F"),
        "parity_formula": PARITY_FORMULA,
    }
    if F is None:
        for r in opts:
            out.append(
                {
                    **r,
                    "iv": None,
                    "delta": None,
                    "greeks_source": None,
                    "underlying_price": None,
                    "underlying_source": None,
                    **extra,
                }
            )
        return out
    src = matched.get("source")
    T = years_to_expiry(trade_d, expiry_d) if trade_d and expiry_d else None
    for r in opts:
        rec = {
            **r,
            "underlying_price": F,
            "underlying_source": src,
            "iv": None,
            "delta": None,
            "greeks_source": None,
            **extra,
        }
        if T is None or not r.get("traded"):
            out.append(rec)
            continue
        vol = int(r.get("volume_lots") or 0)
        oi = int(r.get("oi_lots") or 0)
        close = r.get("close")
        if vol < min_volume or oi <= 0 or close is None:
            out.append(rec)
            continue
        right = r["option_type"]
        if not _is_otm(right, float(r["strike"]), float(F)):
            out.append(rec)
            continue
        iv = implied_vol_black76(float(close), float(F), float(r["strike"]), T, right)
        rec["iv"] = iv
        rec["delta"] = black76_delta(float(F), float(r["strike"]), T, iv, right) if iv else None
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
        futs_by: Dict[Tuple[str, date], List[Dict[str, Any]]] = defaultdict(list)
        opt_groups: Dict[Tuple, List[Dict[str, Any]]] = defaultdict(list)
        for r in rows:
            d = dict(r)
            td = _as_date(d["trade_date"])
            d["trade_date"] = td
            d["expiry_date"] = _as_date(d["expiry_date"])
            if d.get("option_type") == "FUT":
                futs_by[(str(d["symbol"]).upper(), td)].append(d)
            elif d.get("option_type") in ("CE", "PE"):
                opt_groups[(d["symbol"], d["expiry_date"], td)].append(d)
        for (sym, _exp, td), slice_rows in opt_groups.items():
            recs = reconstruct_slice(
                slice_rows,
                futures_for_date=futs_by.get((str(sym).upper(), td), []),
                min_volume=min_volume,
            )
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
                            underlying_fut_symbol = :underlying_fut_symbol,
                            underlying_fut_expiry = :underlying_fut_expiry,
                            underlying_estimated = :underlying_estimated,
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
                        "underlying_fut_symbol": rec.get("underlying_fut_symbol"),
                        "underlying_fut_expiry": rec.get("underlying_fut_expiry"),
                        "underlying_estimated": bool(rec.get("underlying_estimated")),
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
            "parity_formula": PARITY_FORMULA,
            "tte_note": "T = 23:30 IST on trade_date to 23:30 IST on expiry_date / 365.25. Expiry-day options skipped (T would be a residual session stub).",
            "underlying_match_note": "FUTCOM chosen by closest close to parity-implied F; not required to share the option expiry. No futures → parity F with estimated=true.",
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
