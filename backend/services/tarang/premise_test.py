"""Premise-test harness. Does not change live Screener defaults.

Structures, metric, and decision rule are locked in docs/tarang-premise-test.md
against SHA 36d7585. Extra runs are logged; structures are not added after results.
"""
from __future__ import annotations

import json
import math
import random
from collections import defaultdict
from datetime import date, datetime, timezone
from pathlib import Path
from typing import Any, Dict, List, Optional, Sequence, Tuple

from sqlalchemy import text

from backend.database import SessionLocal
from backend.services.tarang.backtest import (
    ALWAYS_EXCLUDE_EXPIRIES,
    LOT_SIZE,
    _as_date,
    _chain_for_day,
    _dte,
    _load_eod_rows,
    cycle_is_expired,
    eod_iv_gate,
    first_entry_reject,
    ist_today,
    mcx_entry_dte_ok,
    realized_vol_from_closes,
)
from backend.services.tarang.config import get_profiles, get_risk
from backend.services.tarang.eod_reconstruct import (
    FUT_RV_MIN_VOLUME_LOTS,
    SRC_FUTCOM,
    years_to_expiry,
)
from backend.services.tarang.fee_gate import gate_fee_and_limits, round_trip_fees_inr
from backend.services.tarang.schema import ensure_tarang_tables
from backend.services.tarang.structures import (
    build_structure,
    credit_spread_legs,
    floor_units,
    max_loss_per_unit_inr,
    pick_long_wing,
    pick_short_by_delta,
)

REGISTERED_SHA = "36d7585"
DOC_PATH = "docs/tarang-premise-test.md"
CACHE_PATH = Path(__file__).resolve().parents[3] / "data" / "tarang" / "premise_test_last.json"
HEADLINE_COST = 0.10
COST_GRID = (0.0, 0.05, 0.10, 0.20)
CAPS = (5000.0, 10000.0)
BOOTSTRAP_N = 1000

# Locked list — do not append after seeing P&L.
STRUCTURES: List[Dict[str, Any]] = [
    {"id": "A", "kind": "iron_condor", "label": "Condor 10-16d profile width", "short": (0.10, 0.16), "wing": "steps", "targets": (0.50, 0.25)},
    {"id": "B", "kind": "iron_condor", "label": "Condor ~20d / wings ~10d", "short": (0.18, 0.22), "wing": "delta", "wing_delta": 0.10, "targets": (0.50, 0.25)},
    {"id": "C", "kind": "iron_condor", "label": "Condor ~30d / wings ~15d", "short": (0.28, 0.32), "wing": "delta", "wing_delta": 0.15, "targets": (0.50, 0.25)},
    {"id": "D", "kind": "iron_butterfly", "label": "Iron fly ATM / wings ~15d", "short": (0.0, 0.0), "wing": "delta", "wing_delta": 0.15, "targets": (0.25, 0.50)},
    {"id": "E-A-PE", "kind": "put_credit_spread", "label": "Put credit of A", "short": (0.10, 0.16), "wing": "steps", "targets": (0.50, 0.25)},
    {"id": "E-A-CE", "kind": "call_credit_spread", "label": "Call credit of A", "short": (0.10, 0.16), "wing": "steps", "targets": (0.50, 0.25)},
    {"id": "E-C-PE", "kind": "put_credit_spread", "label": "Put credit of C", "short": (0.28, 0.32), "wing": "delta", "wing_delta": 0.15, "targets": (0.50, 0.25)},
    {"id": "E-C-CE", "kind": "call_credit_spread", "label": "Call credit of C", "short": (0.28, 0.32), "wing": "delta", "wing_delta": 0.15, "targets": (0.50, 0.25)},
]


def _fill(side: str, mid: Optional[float], cost: float) -> Optional[float]:
    if mid is None or float(mid) <= 0:
        return None
    m = float(mid)
    if str(side).upper() == "SELL":
        return m * (1.0 - cost)
    return m * (1.0 + cost)


def _credit(legs: Sequence[Dict[str, Any]], cost: float) -> Optional[float]:
    c = 0.0
    for lg in legs:
        px = _fill(lg.get("side"), lg.get("mid"), cost)
        if px is None:
            return None
        c += px if str(lg.get("side")).upper() == "SELL" else -px
    return c


def _debit_to_close(chain, legs: Sequence[Dict[str, Any]], cost: float) -> Optional[float]:
    d = 0.0
    for lg in legs:
        match = next(
            (q for q in chain.quotes if q.right == lg.get("right") and abs(float(q.strike) - float(lg["strike"])) < 1e-6),
            None,
        )
        if not match or match.mid is None:
            return None
        side_close = "BUY" if str(lg.get("side")).upper() == "SELL" else "SELL"
        px = _fill(side_close, match.mid, cost)
        if px is None:
            return None
        d += px if side_close == "BUY" else -px
    return d


def _pick_wing_delta(quotes, right: str, short, d_wing: float):
    if right == "PE":
        pool = [q for q in quotes if q.right == "PE" and q.strike < short.strike - 1e-9 and q.delta is not None]
    else:
        pool = [q for q in quotes if q.right == "CE" and q.strike > short.strike + 1e-9 and q.delta is not None]
    if not pool:
        return None
    return min(pool, key=lambda q: abs(abs(float(q.delta or 0)) - d_wing))


def _build_research(chain, spec: Dict[str, Any], width_steps: int) -> Dict[str, Any]:
    kind = spec["kind"]
    quotes = list(chain.quotes or [])
    F = chain.futures_or_spot
    step = float(chain.strike_step or 0)
    if step <= 0:
        return {"ok": False, "error": "strike_step_missing"}
    if kind != "iron_butterfly" and spec.get("wing") != "delta":
        return build_structure(chain, kind, spec["short"][0], spec["short"][1], width_steps)
    legs: List[Dict[str, Any]] = []
    credits: List[float] = []
    widths: List[float] = []
    shorts = []

    def one_side(right: str, dmin: float, dmax: float) -> Optional[str]:
        short = pick_short_by_delta(quotes, right, dmin, dmax, prefer_otm_of=F)
        if not short:
            return f"no_{right}_short"
        if spec.get("wing") == "delta":
            long = _pick_wing_delta(quotes, right, short, float(spec.get("wing_delta") or 0.15))
        else:
            long = pick_long_wing(quotes, right, short, width_steps, step)
        if not long:
            return f"no_{right}_long"
        sl, credit, width = credit_spread_legs(short, long)
        if not sl:
            return f"no_{right}_mids"
        legs.extend(sl)
        credits.append(credit)
        widths.append(width)
        shorts.append(short)
        return None

    if kind == "iron_butterfly":
        if F is None:
            return {"ok": False, "error": "missing_underlying"}
        atm = min({q.strike for q in quotes}, key=lambda k: abs(k - float(F)))
        pe_s = next((q for q in quotes if q.right == "PE" and abs(q.strike - atm) < 1e-9), None)
        ce_s = next((q for q in quotes if q.right == "CE" and abs(q.strike - atm) < 1e-9), None)
        if not pe_s or not ce_s:
            return {"ok": False, "error": "no_atm_short"}
        pe_l = _pick_wing_delta(quotes, "PE", pe_s, float(spec.get("wing_delta") or 0.15))
        ce_l = _pick_wing_delta(quotes, "CE", ce_s, float(spec.get("wing_delta") or 0.15))
        if not pe_l or not ce_l:
            return {"ok": False, "error": "no_wing"}
        for short, long in ((pe_s, pe_l), (ce_s, ce_l)):
            sl, credit, width = credit_spread_legs(short, long)
            if not sl:
                return {"ok": False, "error": "no_mids"}
            legs.extend(sl)
            credits.append(credit)
            widths.append(width)
            shorts.append(short)
    elif kind == "put_credit_spread":
        err = one_side("PE", spec["short"][0], spec["short"][1])
        if err:
            return {"ok": False, "error": err}
    elif kind == "call_credit_spread":
        err = one_side("CE", spec["short"][0], spec["short"][1])
        if err:
            return {"ok": False, "error": err}
    else:
        err = one_side("PE", spec["short"][0], spec["short"][1])
        if err:
            return {"ok": False, "error": err}
        err = one_side("CE", spec["short"][0], spec["short"][1])
        if err:
            return {"ok": False, "error": err}

    return {
        "ok": True,
        "structure": kind,
        "legs": legs,
        "net_credit": sum(credits),
        "width": max(widths) if widths else 0.0,
        "short_deltas": [s.delta for s in shorts],
        "lot_size": chain.lot_size,
        "expiry": chain.expiry,
    }


def _atm_traded_iv(rows: Sequence[Dict[str, Any]], F: Optional[float]) -> Optional[float]:
    traded = [r for r in rows if r.get("traded") and r.get("iv") is not None]
    if not traded:
        return None
    if F is None:
        ivs = sorted(float(r["iv"]) for r in traded)
        return ivs[len(ivs) // 2]
    rec = min(traded, key=lambda r: abs(float(r["strike"]) - float(F)))
    return float(rec["iv"]) if rec.get("iv") is not None else None


def _bootstrap(vals: List[float]) -> Optional[Dict[str, float]]:
    if len(vals) < 2:
        return None
    rng = random.Random(36)
    means = []
    n = len(vals)
    for _ in range(BOOTSTRAP_N):
        sample = [vals[rng.randrange(n)] for _ in range(n)]
        means.append(sum(sample) / n)
    means.sort()
    lo = means[int(0.025 * BOOTSTRAP_N)]
    hi = means[int(0.975 * BOOTSTRAP_N)]
    return {"lo": lo, "hi": hi, "n": n, "mean": sum(vals) / n}


def _fut_series(all_rows: List[Dict[str, Any]], und: str, fut_exp: Optional[date]) -> List[Tuple[date, float, int]]:
    if fut_exp is None:
        return []
    out = []
    for r in all_rows:
        if str(r.get("symbol") or "").upper() != und:
            continue
        if r.get("option_type") != "FUT":
            continue
        if _as_date(r.get("expiry_date")) != fut_exp:
            continue
        if r.get("close") is None:
            continue
        td = _as_date(r.get("trade_date"))
        if td:
            out.append((td, float(r["close"]), int(r.get("volume_lots") or 0)))
    out.sort()
    return out


def _rv_entry_to_expiry(series: List[Tuple[date, float, int]], entry: date, expiry: date) -> Optional[float]:
    xs = [c for d, c, v in series if entry <= d < expiry and v >= FUT_RV_MIN_VOLUME_LOTS]
    return realized_vol_from_closes(xs, min_returns=3, window=60)


def _delta_snapshot_cycles(today: date) -> Dict[str, Any]:
    try:
        ensure_tarang_tables()
        db = SessionLocal()
        try:
            rows = db.execute(
                text(
                    """
                    SELECT DISTINCT underlying_symbol, expiry_date
                    FROM tarang_chain_snapshots
                    WHERE venue = 'delta_india'
                    """
                )
            ).mappings().all()
        finally:
            db.close()
    except Exception as exc:
        return {"expired_cycles": 0, "open_cycles": 0, "error": str(exc), "note": "Delta snapshot query failed or empty."}
    expired = open_ = 0
    for r in rows:
        e = _as_date(r.get("expiry_date"))
        if e is None:
            continue
        if e < today:
            expired += 1
        else:
            open_ += 1
    return {
        "expired_cycles": expired,
        "open_cycles": open_,
        "note": "Weekly update. Few/zero expired snapshot cycles is expected this early.",
    }


def _spec_max_loss_lot(width: float, und: str) -> Optional[float]:
    return max_loss_per_unit_inr(width, 0.0, lot_size=LOT_SIZE.get(und, 10), venue="upstox_mcx")


def _avg(xs: List[float]) -> Optional[float]:
    return (sum(xs) / len(xs)) if xs else None


def run_premise_test(*, all_rows: Optional[List[Dict[str, Any]]] = None, today: Optional[date] = None) -> Dict[str, Any]:
    today = today or ist_today()
    if all_rows is None:
        try:
            ensure_tarang_tables()
            db = SessionLocal()
            try:
                all_rows = _load_eod_rows(db)
            finally:
                db.close()
        except Exception as exc:
            return {
                "registered_sha": REGISTERED_SHA,
                "doc": DOC_PATH,
                "status": "db_unavailable",
                "error": str(exc),
                "live_defaults_unchanged": True,
            }
    for r in all_rows:
        r["trade_date"] = _as_date(r.get("trade_date"))
        r["expiry_date"] = _as_date(r.get("expiry_date"))
        if r.get("mapped_fut_expiry"):
            r["mapped_fut_expiry"] = _as_date(r["mapped_fut_expiry"])

    profiles = get_profiles().get("profiles") or {}
    risk = get_risk()
    budget = float(((risk.get("buckets") or {}).get("ENERGY") or {}).get("per_trade_budget_inr") or 5000)
    hard = float(((risk.get("buckets") or {}).get("ENERGY") or {}).get("hard_cap_inr") or 10000)
    live_frac = float(risk.get("min_credit_fraction_of_width") or 0.20)
    cl = profiles.get("CL") or {}
    width_steps = int(cl.get("width_steps_min") or 5)
    min_dte = int(cl.get("expiry_min_dte") or 7)
    max_dte = int(cl.get("expiry_dte_max") or 35)
    pid = "CL"

    by_day_exp: Dict[Tuple[str, date, date], List[Dict[str, Any]]] = defaultdict(list)
    fut_index: Dict[Tuple[str, date], List[Tuple[date, float, int]]] = defaultdict(list)
    for r in all_rows:
        td, exp = r.get("trade_date"), r.get("expiry_date")
        if not td or not exp:
            continue
        und = str(r.get("symbol") or "").upper()
        by_day_exp[(und, exp, td)].append(r)
        if r.get("option_type") == "FUT" and r.get("close") is not None:
            fut_index[(und, exp)].append((td, float(r["close"]), int(r.get("volume_lots") or 0)))
    for k in fut_index:
        fut_index[k].sort()

    unds = sorted({k[0] for k in by_day_exp if any(r.get("option_type") in ("CE", "PE") for r in by_day_exp[k])})
    chains_traded: Dict[Tuple[str, date, date], Any] = {}
    chains_marks: Dict[Tuple[str, date, date], Any] = {}
    iv_median: Dict[Tuple[str, date], float] = {}
    iv_lists: Dict[Tuple[str, date], List[float]] = defaultdict(list)
    for (und, exp, td), sl in by_day_exp.items():
        if und not in LOT_SIZE:
            continue
        if not any(r.get("option_type") in ("CE", "PE") for r in sl):
            continue
        if exp in ALWAYS_EXCLUDE_EXPIRIES or not cycle_is_expired(exp, today):
            continue
        chains_traded[(und, exp, td)] = _chain_for_day(sl, profile_id=pid, require_traded=True)
        chains_marks[(und, exp, td)] = _chain_for_day(sl, profile_id=pid, require_traded=False)
        ch = chains_traded[(und, exp, td)]
        F = ch.futures_or_spot if ch else None
        atm = _atm_traded_iv(sl, F)
        if atm is not None:
            iv_lists[(und, td)].append(atm)
    iv_median = {k: sorted(v)[len(v) // 2] for k, v in iv_lists.items()}

    results: List[Dict[str, Any]] = []
    iv_rv_days: List[Dict[str, Any]] = []
    iv_pts: List[float] = []
    iv_var: List[float] = []

    def _empty_books() -> Dict[float, Dict[str, List[float]]]:
        return {c: {"hold": [], "rule": [], "gross": []} for c in COST_GRID}

    for spec in STRUCTURES:
        for und in unds:
            if und not in LOT_SIZE:
                continue
            lot = LOT_SIZE[und]
            expiries = sorted(
                {
                    k[1]
                    for k in by_day_exp
                    if k[0] == und and any(r.get("option_type") in ("CE", "PE") for r in by_day_exp[k])
                }
            )
            books = _empty_books()
            books_g = _empty_books()
            trades_h: Dict[float, int] = {c: 0 for c in COST_GRID}
            trades_g: Dict[float, int] = {c: 0 for c in COST_GRID}
            feasible_days = 0
            gated_days = 0
            credits_pct: List[float] = []
            rejected_cap = False
            narrowest = None
            for exp in expiries:
                if exp in ALWAYS_EXCLUDE_EXPIRIES or not cycle_is_expired(exp, today):
                    continue
                days = sorted({k[2] for k in by_day_exp if k[0] == und and k[1] == exp})
                cycle_hold = {c: 0.0 for c in COST_GRID}
                cycle_rule = {c: 0.0 for c in COST_GRID}
                cycle_gross = {c: 0.0 for c in COST_GRID}
                cycle_hold_g = {c: 0.0 for c in COST_GRID}
                cycle_rule_g = {c: 0.0 for c in COST_GRID}
                cycle_gross_g = {c: 0.0 for c in COST_GRID}
                n_tr = 0
                n_tr_g = 0
                for td in days:
                    dte = _dte(exp, td)
                    if not mcx_entry_dte_ok(dte, min_dte, max_dte):
                        continue
                    sl = by_day_exp.get((und, exp, td)) or []
                    chain = chains_traded.get((und, exp, td))
                    if not chain or not chain.futures_or_spot:
                        continue
                    built = _build_research(chain, spec, width_steps)
                    if not built.get("ok"):
                        continue
                    feasible_days += 1
                    width = float(built.get("width") or 0)
                    zc = _spec_max_loss_lot(width, und)
                    if zc is not None:
                        narrowest = zc if narrowest is None else min(narrowest, zc)
                        if zc > hard:
                            rejected_cap = True
                    F = chain.futures_or_spot
                    atm = _atm_traded_iv(sl, F)
                    mex = next(
                        (
                            _as_date(r.get("mapped_fut_expiry") or r.get("underlying_fut_expiry"))
                            for r in sl
                            if r.get("mapped_fut_expiry") or r.get("underlying_fut_expiry")
                        ),
                        None,
                    )
                    series = fut_index.get((und, mex), []) if mex else []
                    rv = _rv_entry_to_expiry(series, td, exp)
                    if atm is not None and rv is not None:
                        dlt = atm - rv
                        var_d = atm * atm - rv * rv
                        iv_pts.append(dlt)
                        iv_var.append(var_d)
                        if len(iv_rv_days) < 400:
                            iv_rv_days.append(
                                {
                                    "structure": spec["id"],
                                    "symbol": und,
                                    "expiry": exp.isoformat(),
                                    "trade_date": td.isoformat(),
                                    "atm_iv": atm,
                                    "rv_entry_to_expiry": rv,
                                    "iv_minus_rv_vol": dlt,
                                    "iv2_minus_rv2": var_d,
                                    "underlying_source": next((r.get("underlying_source") for r in sl if r.get("underlying_source")), SRC_FUTCOM),
                                }
                            )
                    mid_credit = built.get("net_credit")
                    if width > 0 and mid_credit is not None:
                        credits_pct.append(100.0 * float(mid_credit) / width)
                    later_chains = [(d, chains_marks[(und, exp, d)]) for d in days if d > td and chains_marks.get((und, exp, d))]
                    last_chain = later_chains[-1] if later_chains else None
                    entry_ds = [abs(float(dlt)) for dlt in (built.get("short_deltas") or []) if dlt is not None]
                    stop_d = min(2.0 * (sum(entry_ds) / len(entry_ds)), 0.45) if entry_ds else 0.45
                    t1 = spec["targets"][0]
                    day_hold = {c: None for c in COST_GRID}
                    day_rule = {c: None for c in COST_GRID}
                    day_gross = {c: None for c in COST_GRID}
                    units_10 = 0
                    credit_10 = None
                    fee_10 = 0.0
                    for cost in COST_GRID:
                        credit = _credit(built["legs"], cost)
                        if credit is None or credit <= 0 or width <= 0:
                            continue
                        ml = max_loss_per_unit_inr(width, credit, lot_size=lot, venue="upstox_mcx")
                        units = floor_units(budget, ml) if ml else 0
                        if units < 1:
                            continue
                        fees = round_trip_fees_inr(
                            venue="upstox_mcx",
                            legs=built["legs"],
                            units=units,
                            lot_size=lot,
                            contract_value=None,
                            underlying_price=F,
                        )
                        fee = float(fees.get("round_trip_inr") or 0)
                        if abs(cost - HEADLINE_COST) < 1e-12:
                            units_10 = units
                            credit_10 = credit
                            fee_10 = fee
                        if last_chain:
                            debit = _debit_to_close(last_chain[1], built["legs"], cost)
                            if debit is not None:
                                gross = (credit - debit) * lot * units
                                day_gross[cost] = gross
                                day_hold[cost] = gross - fee
                                cycle_hold[cost] += gross - fee
                                cycle_gross[cost] += gross
                                trades_h[cost] += 1
                                n_tr += 1
                        max_profit = credit * lot * units
                        exit_pnl = None
                        for _later, ch2 in later_chains:
                            debit = _debit_to_close(ch2, built["legs"], cost)
                            if debit is None:
                                continue
                            gross = (credit - debit) * lot * units
                            if spec["kind"] == "iron_butterfly" and F and ch2.futures_or_spot and atm:
                                T = years_to_expiry(td, exp) or (dte / 365.25)
                                sigma_move = float(F) * float(atm) * math.sqrt(max(T, 1e-6))
                                if abs(float(ch2.futures_or_spot) - float(F)) >= sigma_move:
                                    exit_pnl = gross - fee
                                    break
                                if ml and gross <= -0.5 * float(ml) * units:
                                    exit_pnl = gross - fee
                                    break
                            if max_profit > 0 and gross >= t1 * max_profit:
                                exit_pnl = gross - fee
                                break
                            cur_d = [abs(float(q.delta)) for q in ch2.quotes if q.delta is not None]
                            if cur_d and max(cur_d) >= stop_d:
                                exit_pnl = gross - fee
                                break
                        if exit_pnl is None and last_chain:
                            debit = _debit_to_close(last_chain[1], built["legs"], cost)
                            if debit is not None:
                                exit_pnl = (credit - debit) * lot * units - fee
                        if exit_pnl is not None:
                            day_rule[cost] = exit_pnl
                            cycle_rule[cost] += exit_pnl

                    iv_hist = [iv_median[(und, d)] for d in sorted({k[1] for k in iv_median if k[0] == und}) if d < td]
                    iv_res = eod_iv_gate(atm, rv, iv_hist)
                    fee_ok = None
                    if units_10 >= 1 and credit_10 and credit_10 > 0:
                        fg = gate_fee_and_limits(
                            venue="upstox_mcx",
                            legs=built["legs"],
                            net_credit_pts=credit_10,
                            units=units_10,
                            lot_size=lot,
                            contract_value=None,
                            underlying_price=F,
                        )
                        fee_ok = bool(fg["gate"].passed)
                    gate = first_entry_reject(
                        has_underlying=True,
                        n_quotes=sum(1 for r in sl if r.get("traded") and r.get("option_type") in ("CE", "PE")),
                        dte=dte,
                        min_dte=min_dte,
                        max_dte=max_dte,
                        iv_passed=iv_res.get("passed") if atm is not None else None,
                        credit=credit_10,
                        width=width,
                        min_credit_frac=live_frac,
                        units=units_10,
                        fee_ok=fee_ok,
                        n_fut=sum(1 for r in sl if r.get("option_type") == "FUT" and r.get("close")),
                        n_option_rows=sum(1 for r in sl if r.get("option_type") in ("CE", "PE")),
                    )
                    if gate is None:
                        gated_days += 1
                        for cost in COST_GRID:
                            if day_hold[cost] is not None:
                                cycle_hold_g[cost] += day_hold[cost]
                                cycle_gross_g[cost] += day_gross[cost] or 0.0
                                trades_g[cost] += 1
                                n_tr_g += 1
                            if day_rule[cost] is not None:
                                cycle_rule_g[cost] += day_rule[cost]
                if n_tr:
                    for cost in COST_GRID:
                        books[cost]["hold"].append(cycle_hold[cost])
                        books[cost]["rule"].append(cycle_rule[cost])
                        books[cost]["gross"].append(cycle_gross[cost])
                if n_tr_g:
                    for cost in COST_GRID:
                        books_g[cost]["hold"].append(cycle_hold_g[cost])
                        books_g[cost]["rule"].append(cycle_rule_g[cost])
                        books_g[cost]["gross"].append(cycle_gross_g[cost])

            headline = books[HEADLINE_COST]
            headline_g = books_g[HEADLINE_COST]
            n_tr_h = trades_h[HEADLINE_COST]
            rr = None
            if credits_pct:
                avg_c = sum(credits_pct) / len(credits_pct) / 100.0
                rr = (avg_c / (1.0 - avg_c)) if avg_c < 1 else None
            worst = min(headline["hold"]) if headline["hold"] else None
            wins = sum(1 for x in headline["hold"] if x > 0)
            row = {
                "id": spec["id"],
                "label": spec["label"],
                "underlying": und,
                "feasible_days_ungated": feasible_days,
                "gated_days_live_rules": gated_days,
                "trades_at_10pct": n_tr_h,
                "trades_at_10pct_gated": trades_g[HEADLINE_COST],
                "independent_cycles": len(headline["hold"]),
                "independent_cycles_gated": len(headline_g["hold"]),
                "mean_net_per_cycle_hold_10pct": _avg(headline["hold"]),
                "mean_net_per_cycle_rule_10pct": _avg(headline["rule"]),
                "mean_gross_per_cycle_hold_10pct": _avg(headline["gross"]),
                "mean_net_per_cycle_hold_10pct_gated": _avg(headline_g["hold"]),
                "mean_net_per_cycle_rule_10pct_gated": _avg(headline_g["rule"]),
                "win_rate_cycles_hold_10pct": (wins / len(headline["hold"])) if headline["hold"] else None,
                "worst_cycle_net_hold_10pct": worst,
                "worst_vs_caps": {
                    "cap_5000": None if worst is None else worst >= -5000,
                    "cap_10000": None if worst is None else worst >= -10000,
                },
                "credit_pct_width_mean": _avg(credits_pct),
                "reward_risk": rr,
                "narrowest_zero_credit_max_loss_per_lot": narrowest,
                "rejected_exceeds_caps": rejected_cap or (narrowest is not None and narrowest > hard),
                "vs_caps": {
                    "cap_5000": None if narrowest is None else narrowest <= 5000,
                    "cap_10000": None if narrowest is None else narrowest <= 10000,
                },
                "cost_grid": {
                    f"{int(c * 100)}pct": {
                        "trades": trades_h[c],
                        "mean_net_hold": _avg(books[c]["hold"]),
                        "mean_net_rule": _avg(books[c]["rule"]),
                        "gross_hold_sum": sum(books[c]["gross"]) if books[c]["gross"] else 0,
                        "net_hold_sum": sum(books[c]["hold"]) if books[c]["hold"] else 0,
                        "gated_mean_net_hold": _avg(books_g[c]["hold"]),
                    }
                    for c in COST_GRID
                },
                "bootstrap_hold_10pct": _bootstrap(headline["hold"]),
                "headline_pf_dd": "omitted_under_10_trades" if n_tr_h < 10 else "see_cost_grid",
            }
            results.append(row)

    if iv_pts:
        n = len(iv_pts)
        exceed = sum(1 for x in iv_pts if x > 0)
        iv_summary = {
            "days": n,
            "frac_iv_exceeds_rv": exceed / n,
            "mean_iv_minus_rv_vol": sum(iv_pts) / n,
            "mean_iv2_minus_rv2": sum(iv_var) / n,
            "sample_days_in_payload": len(iv_rv_days),
        }
    else:
        iv_summary = {"days": 0, "frac_iv_exceeds_rv": None, "mean_iv_minus_rv_vol": None, "mean_iv2_minus_rv2": None}

    supported = []
    for r in results:
        if r.get("rejected_exceeds_caps"):
            continue
        if (r.get("mean_net_per_cycle_hold_10pct") or 0) > 0 and (iv_summary.get("frac_iv_exceeds_rv") or 0) > 0.5:
            supported.append(r["id"])

    positive = [r["id"] for r in results if (r.get("mean_net_per_cycle_hold_10pct") or 0) > 0]
    iv_ok = (iv_summary.get("frac_iv_exceeds_rv") or 0) > 0.5
    if supported:
        plain = (
            f"IV exceeded mapped-contract RV on {100 * (iv_summary.get('frac_iv_exceeds_rv') or 0):.0f}% of entry days "
            f"(mean IV−RV {iv_summary.get('mean_iv_minus_rv_vol')}). "
            f"Mean net P&L per cycle at 10% per-leg cost is positive for {', '.join(supported)}. "
            "Live min-credit 0.20, ENERGY caps, DTE, time stop, and Phase 4 stay unchanged."
        )
    else:
        why = []
        if not iv_ok:
            why.append("ATM IV did not exceed RV on a majority of days")
        if not positive:
            why.append("no locked structure has positive mean net P&L per cycle at 10% per-leg cost")
        rejected = [r["id"] for r in results if r.get("rejected_exceeds_caps")]
        if rejected:
            why.append(f"structures {', '.join(rejected)} exceed ₹5k/₹10k zero-credit max loss")
        if not why:
            why.append("the locked decision rule is not met")
        plain = (
            "Premise is not supported for a live-rule change: "
            + "; ".join(why)
            + ". Options without changing live rules: change which research structure is studied next, defer MCX, or focus Delta."
        )

    ng_width = 3 * 1.0 * 250
    payload = {
        "product": "Kosmic Tarang",
        "registered_sha": REGISTERED_SHA,
        "doc": DOC_PATH,
        "metric": "mean net P&L per independent expired cycle at 10% per-leg cost",
        "plain_language": plain,
        "live_defaults_unchanged": {
            "min_credit_fraction_of_width": live_frac,
            "energy_budget": budget,
            "energy_hard_cap": hard,
            "phase4_approved": bool((risk.get("phase4_hold") or {}).get("approved")),
        },
        "iv_vs_rv": iv_summary,
        "iv_vs_rv_days": iv_rv_days,
        "structures": results,
        "supported_ids": supported,
        "max_loss_specs": {
            "CRUDEOILM": {"lot_size": 10, "note": "zero-credit max loss = width_pts × 10"},
            "NATGASMINI": {"lot_size": 250, "example_3step_1pt": ng_width, "note": "No NG bhavcopy in this run; spec only."},
        },
        "delta_premise": _delta_snapshot_cycles(today),
        "decision_inputs": {
            "iv_exceeds_rv_majority": iv_ok,
            "positive_mean_net_at_10pct": positive,
        },
        "asof_ist": today.isoformat(),
        "generated_at": datetime.now(timezone.utc).isoformat(),
    }
    try:
        CACHE_PATH.parent.mkdir(parents=True, exist_ok=True)
        CACHE_PATH.write_text(json.dumps(payload, indent=2, default=str), encoding="utf-8")
    except OSError:
        pass
    return payload


def load_cached_premise() -> Dict[str, Any]:
    if CACHE_PATH.is_file():
        return json.loads(CACHE_PATH.read_text(encoding="utf-8"))
    return {
        "registered_sha": REGISTERED_SHA,
        "doc": DOC_PATH,
        "status": "not_run",
        "note": "Run scripts/tarang_premise_test.py or POST /api/tarang/premise-test/run.",
    }
