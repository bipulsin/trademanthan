"""Follow-up premise analysis. Does not change live Screener / fee / cap defaults.

Registered in docs/tarang-premise-test.md against SHA 634d6171 before this P&L.
"""
from __future__ import annotations

import json
import math
from collections import defaultdict
from datetime import date, datetime, timezone
from pathlib import Path
from statistics import median
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
    ist_today,
    mcx_entry_dte_ok,
)
from backend.services.tarang.bhavcopy import scan_datewise_dir
from backend.services.tarang.chain_snapshots import unpack_chain_payload
from backend.services.tarang.config import get_profiles, get_risk
from backend.services.tarang.eod_reconstruct import years_to_expiry
from backend.services.tarang.fee_gate import round_trip_fees_inr
from backend.services.tarang.fees import delta_india_option_fee_inr
from backend.services.tarang.premise_test import (
    CACHE_PATH,
    COST_GRID,
    HEADLINE_COST,
    REGISTERED_SHA,
    STRUCTURES,
    _atm_traded_iv,
    _avg,
    _bootstrap,
    _build_research,
    _credit,
    _debit_to_close,
    _fill,
    _rv_entry_to_expiry,
    _spec_max_loss_lot,
)
from backend.services.tarang.schema import ensure_tarang_tables
from backend.services.tarang.structures import floor_units, max_loss_per_unit_inr

FOLLOWUP_SHA = "634d6171"
TARGET_DTE = 26
BUDGET = 5000.0
HARD = 10000.0
# Delta India live tickers 2026-09-20 ~22:00 IST (BTC+ETH, 822 two-sided). Half-spread / mid.
# MCX Monday session not yet open — used as EOD-mid proxy until Monday bid-ask exists.
CALIBRATED_HALF = {
    "ATM": 0.0078,
    "30": 0.0171,
    "15": 0.0266,
    "10": 0.0345,
    "5": 0.0769,
}
CALIBRATED_META = {
    "measured_at_ist": "2026-09-20 ~22:00",
    "source": "delta_india live tickers BTC+ETH",
    "n_two_sided": 822,
    "bins": CALIBRATED_HALF,
    "mcx_note": "Monday 21 Sep 2026 MCX session pending. Schedule applied to EOD mids as proxy. Not a live fill default.",
}

DELTA_EXTRA = [
    {"id": "Dlt-F", "kind": "iron_condor", "label": "Delta IC ~25d / wings ~10d", "short": (0.22, 0.28), "wing": "delta", "wing_delta": 0.10, "targets": (0.50, 0.25)},
    {"id": "Dlt-G", "kind": "iron_condor", "label": "Delta IC ~40d / wings ~15d", "short": (0.35, 0.45), "wing": "delta", "wing_delta": 0.15, "targets": (0.50, 0.25)},
    {"id": "Dlt-H", "kind": "iron_butterfly", "label": "Delta fly ATM / wings ~25d", "short": (0.0, 0.0), "wing": "delta", "wing_delta": 0.25, "targets": (0.25, 0.50)},
]

CACHE_FOLLOWUP = Path(__file__).resolve().parents[3] / "data" / "tarang" / "premise_followup_last.json"


def delta_bin(abs_delta: Optional[float]) -> str:
    ad = abs(float(abs_delta or 0.15))
    return min((("ATM", 0.50), ("30", 0.30), ("15", 0.15), ("10", 0.10), ("5", 0.05)), key=lambda x: abs(ad - x[1]))[0]


def _leg_cost(lg: Dict[str, Any]) -> float:
    return CALIBRATED_HALF[delta_bin(lg.get("delta"))]


def _credit_calibrated(legs: Sequence[Dict[str, Any]]) -> Optional[float]:
    c = 0.0
    for lg in legs:
        px = _fill(lg.get("side"), lg.get("mid"), _leg_cost(lg))
        if px is None:
            return None
        c += px if str(lg.get("side")).upper() == "SELL" else -px
    return c


def _debit_calibrated(chain, legs: Sequence[Dict[str, Any]]) -> Optional[float]:
    d = 0.0
    for lg in legs:
        match = next(
            (q for q in chain.quotes if q.right == lg.get("right") and abs(float(q.strike) - float(lg["strike"])) < 1e-6),
            None,
        )
        if not match or match.mid is None:
            return None
        side_close = "BUY" if str(lg.get("side")).upper() == "SELL" else "SELL"
        cost = CALIBRATED_HALF[delta_bin(match.delta if match.delta is not None else lg.get("delta"))]
        px = _fill(side_close, match.mid, cost)
        if px is None:
            return None
        d += px if side_close == "BUY" else -px
    return d


def _rule_exit_pnl(*, spec, later_chains, last_chain, legs, credit, cost_mode, lot, units, fee, F, atm, td, exp, dte, stop_d, t1, ml, width) -> Optional[float]:
    max_profit = credit * lot * units
    exit_pnl = None
    for _later, ch2 in later_chains:
        if cost_mode == "calibrated":
            debit = _debit_calibrated(ch2, legs)
        else:
            debit = _debit_to_close(ch2, legs, float(cost_mode))
        if debit is None:
            continue
        gross = (credit - debit) * lot * units
        if spec["kind"] == "iron_butterfly" and F and ch2.futures_or_spot and atm:
            T = years_to_expiry(td, exp) or (dte / 365.25)
            sigma_move = float(F) * float(atm) * math.sqrt(max(T, 1e-6))
            if abs(float(ch2.futures_or_spot) - float(F)) >= sigma_move:
                return gross - fee
            if ml and gross <= -0.5 * float(ml) * units:
                return gross - fee
        if max_profit > 0 and gross >= t1 * max_profit:
            return gross - fee
        cur_d = [abs(float(q.delta)) for q in ch2.quotes if q.delta is not None]
        if cur_d and max(cur_d) >= stop_d:
            return gross - fee
    if last_chain:
        if cost_mode == "calibrated":
            debit = _debit_calibrated(last_chain[1], legs)
        else:
            debit = _debit_to_close(last_chain[1], legs, float(cost_mode))
        if debit is not None:
            return (credit - debit) * lot * units - fee
    return exit_pnl


def _cycle_stats(xs: List[float]) -> Optional[Dict[str, Any]]:
    if not xs:
        return None
    ys = sorted(xs)
    return {
        "n": len(xs),
        "mean": sum(xs) / len(xs),
        "median": median(xs),
        "worst": min(xs),
        "best": max(xs),
        "bootstrap": _bootstrap(xs),
    }


def _pick_one_entry(cands: List[date], exp: date) -> Optional[date]:
    if not cands:
        return None
    return min(cands, key=lambda td: (abs(_dte(exp, td) - TARGET_DTE), td))


def run_mcx_followup(*, all_rows: Optional[List[Dict[str, Any]]] = None, today: Optional[date] = None) -> Dict[str, Any]:
    today = today or ist_today()
    if all_rows is None:
        ensure_tarang_tables()
        db = SessionLocal()
        try:
            all_rows = _load_eod_rows(db)
        finally:
            db.close()
    for r in all_rows:
        r["trade_date"] = _as_date(r.get("trade_date"))
        r["expiry_date"] = _as_date(r.get("expiry_date"))
        if r.get("mapped_fut_expiry"):
            r["mapped_fut_expiry"] = _as_date(r["mapped_fut_expiry"])

    profiles = get_profiles().get("profiles") or {}
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
    for (und, exp, td), sl in by_day_exp.items():
        if und not in LOT_SIZE:
            continue
        if not any(r.get("option_type") in ("CE", "PE") for r in sl):
            continue
        if exp in ALWAYS_EXCLUDE_EXPIRIES or not cycle_is_expired(exp, today):
            continue
        chains_traded[(und, exp, td)] = _chain_for_day(sl, profile_id=pid, require_traded=True)
        chains_marks[(und, exp, td)] = _chain_for_day(sl, profile_id=pid, require_traded=False)

    expired_by_sym: Dict[str, int] = defaultdict(int)
    seen_exp: set = set()
    for (und, exp, _td) in by_day_exp:
        if (und, exp) in seen_exp:
            continue
        if any(r.get("option_type") in ("CE", "PE") for r in by_day_exp[(und, exp, _td)]):
            if exp not in ALWAYS_EXCLUDE_EXPIRIES and cycle_is_expired(exp, today):
                expired_by_sym[und] += 1
                seen_exp.add((und, exp))

    # recount properly
    expired_by_sym = defaultdict(int)
    for und in unds:
        exps = {k[1] for k in by_day_exp if k[0] == und and any(r.get("option_type") in ("CE", "PE") for r in by_day_exp[k])}
        for exp in exps:
            if exp not in ALWAYS_EXCLUDE_EXPIRIES and cycle_is_expired(exp, today):
                expired_by_sym[und] += 1

    cost_modes: List[Any] = [0.0, 0.05, 0.10, 0.20, "calibrated"]
    out_rows: List[Dict[str, Any]] = []
    quartile_obs: List[Dict[str, Any]] = []
    cap_explain = {
        "why_prior_breached_caps": (
            "The first P&L run summed every qualifying day's P&L inside a cycle (often 10–20 entries), "
            "and used live floor_units(₹5,000, max_loss_per_lot) which can take 2 lots when per-lot max loss is ~₹2,500. "
            "Wings were required traded (require_traded=True on entry). Ungated did not skip days that passed floor_units. "
            "A −₹51k 'cycle' was the sum of many days, not a single 1-lot max-loss event."
        ),
        "this_run": "1 lot if credit-adjusted max loss per lot ≤ ₹5,000; skip otherwise. Also count skips vs ₹10,000 zero-credit. Cycle P&L is mean of days or one entry at ~26 DTE.",
    }

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
            day_books: Dict[str, Dict[Any, List[float]]] = {
                "hold": {m: [] for m in cost_modes},
                "rule": {m: [] for m in cost_modes},
                "gross": [],
            }
            one_books = {
                "hold": {m: [] for m in cost_modes},
                "rule": {m: [] for m in cost_modes},
                "gross": [],
            }
            skip_budget = 0
            skip_hard = 0
            skip_live_floor = 0
            taken = 0
            lots_used: List[int] = []
            ml_list: List[float] = []
            for exp in expiries:
                if exp in ALWAYS_EXCLUDE_EXPIRIES or not cycle_is_expired(exp, today):
                    continue
                days = sorted({k[2] for k in by_day_exp if k[0] == und and k[1] == exp})
                per_day: Dict[date, Dict[str, Any]] = {}
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
                    width = float(built.get("width") or 0)
                    zc = _spec_max_loss_lot(width, und)
                    if zc is not None and zc > HARD:
                        skip_hard += 1
                        continue
                    F = chain.futures_or_spot
                    later_chains = [(d, chains_marks[(und, exp, d)]) for d in days if d > td and chains_marks.get((und, exp, d))]
                    last_chain = later_chains[-1] if later_chains else None
                    if not last_chain:
                        continue
                    entry_ds = [abs(float(dlt)) for dlt in (built.get("short_deltas") or []) if dlt is not None]
                    stop_d = min(2.0 * (sum(entry_ds) / len(entry_ds)), 0.45) if entry_ds else 0.45
                    t1 = spec["targets"][0]
                    atm = _atm_traded_iv(sl, F)
                    mex = next(
                        (
                            _as_date(r.get("mapped_fut_expiry") or r.get("underlying_fut_expiry"))
                            for r in sl
                            if r.get("mapped_fut_expiry") or r.get("underlying_fut_expiry")
                        ),
                        None,
                    )
                    rv = _rv_entry_to_expiry(fut_index.get((und, mex), []) if mex else [], td, exp)
                    rec: Dict[str, Any] = {"dte": dte, "gross": None, "hold": {}, "rule": {}, "ml": None, "units": 0, "iv_rv": None}
                    if atm is not None and rv is not None:
                        rec["iv_rv"] = atm - rv
                    mid_credit = built.get("net_credit")
                    if mid_credit is None or width <= 0:
                        continue
                    # gross (zero cost), 1 lot if fits budget at mid credit
                    ml0 = max_loss_per_unit_inr(width, float(mid_credit), lot_size=lot, venue="upstox_mcx")
                    live_u = floor_units(BUDGET, ml0) if ml0 else 0
                    if live_u < 1:
                        skip_live_floor += 1
                    if ml0 is None or ml0 > BUDGET:
                        skip_budget += 1
                        continue
                    units = 1
                    taken += 1
                    lots_used.append(units)
                    ml_list.append(float(ml0))
                    rec["ml"] = float(ml0)
                    rec["units"] = units
                    fees0 = round_trip_fees_inr(
                        venue="upstox_mcx", legs=built["legs"], units=units, lot_size=lot,
                        contract_value=None, underlying_price=F,
                    )
                    # fees still apply on "gross"? User asked gross = zero cost. Gross = no spread, still show fees separately.
                    debit0 = _debit_to_close(last_chain[1], built["legs"], 0.0)
                    if debit0 is None:
                        continue
                    rec["gross"] = (float(mid_credit) - debit0) * lot * units
                    rec["gross_rule"] = _rule_exit_pnl(
                        spec=spec, later_chains=later_chains, last_chain=last_chain, legs=built["legs"],
                        credit=float(mid_credit), cost_mode=0.0, lot=lot, units=units, fee=0.0,
                        F=F, atm=atm, td=td, exp=exp, dte=dte, stop_d=stop_d, t1=t1, ml=ml0, width=width,
                    )
                    for mode in cost_modes:
                        if mode == "calibrated":
                            credit = _credit_calibrated(built["legs"])
                            debit_fn = lambda ch: _debit_calibrated(ch, built["legs"])
                            cmode: Any = "calibrated"
                        else:
                            credit = _credit(built["legs"], float(mode))
                            cmode = float(mode)
                        if credit is None or credit <= 0:
                            continue
                        ml = max_loss_per_unit_inr(width, credit, lot_size=lot, venue="upstox_mcx")
                        if ml is None or ml > BUDGET:
                            continue
                        fee = float(fees0.get("round_trip_inr") or 0)
                        if mode == "calibrated":
                            debit = _debit_calibrated(last_chain[1], built["legs"])
                        else:
                            debit = _debit_to_close(last_chain[1], built["legs"], float(mode))
                        if debit is None:
                            continue
                        rec["hold"][mode] = (credit - debit) * lot * units - fee
                        rec["rule"][mode] = _rule_exit_pnl(
                            spec=spec, later_chains=later_chains, last_chain=last_chain, legs=built["legs"],
                            credit=credit, cost_mode=cmode, lot=lot, units=units, fee=fee,
                            F=F, atm=atm, td=td, exp=exp, dte=dte, stop_d=stop_d, t1=t1, ml=ml, width=width,
                        )
                    per_day[td] = rec

                if not per_day:
                    continue
                # all-days: mean of per-day P&L
                day_books["gross"].append(_avg([per_day[d]["gross"] for d in per_day if per_day[d]["gross"] is not None]) or 0.0)
                for mode in cost_modes:
                    hs = [per_day[d]["hold"][mode] for d in per_day if mode in per_day[d]["hold"]]
                    rs = [per_day[d]["rule"][mode] for d in per_day if mode in per_day[d]["rule"] and per_day[d]["rule"][mode] is not None]
                    if hs:
                        day_books["hold"][mode].append(sum(hs) / len(hs))
                    if rs:
                        day_books["rule"][mode].append(sum(rs) / len(rs))
                one = _pick_one_entry(list(per_day), exp)
                if one:
                    rec = per_day[one]
                    if rec.get("gross") is not None:
                        one_books["gross"].append(rec["gross"])
                    for mode in cost_modes:
                        if mode in rec["hold"]:
                            one_books["hold"][mode].append(rec["hold"][mode])
                        if rec["rule"].get(mode) is not None:
                            one_books["rule"][mode].append(rec["rule"][mode])
                    if rec.get("iv_rv") is not None and HEADLINE_COST in rec["hold"]:
                        quartile_obs.append(
                            {
                                "structure": spec["id"],
                                "expiry": exp.isoformat(),
                                "trade_date": one.isoformat(),
                                "dte": rec["dte"],
                                "iv_minus_rv_vol": rec["iv_rv"],
                                "net_hold_10pct": rec["hold"].get(HEADLINE_COST),
                                "net_hold_calibrated": rec["hold"].get("calibrated"),
                                "gross": rec.get("gross"),
                            }
                        )

            def pack(books):
                return {
                    "gross": _cycle_stats(books["gross"]),
                    "uniform": {
                        f"{int(c * 100)}pct": {
                            "hold": _cycle_stats(books["hold"][c]),
                            "rule": _cycle_stats(books["rule"][c]),
                        }
                        for c in COST_GRID
                    },
                    "calibrated": {
                        "hold": _cycle_stats(books["hold"]["calibrated"]),
                        "rule": _cycle_stats(books["rule"]["calibrated"]),
                    },
                }

            out_rows.append(
                {
                    "id": spec["id"],
                    "label": spec["label"],
                    "underlying": und,
                    "aggregation_all_days": "mean of per-day P&L within cycle",
                    "one_entry": "day nearest 26 DTE, else earliest qualifying",
                    "sizing": "1 lot if max_loss_per_lot ≤ ₹5,000 else skip",
                    "lots_used_mean": _avg([float(x) for x in lots_used]),
                    "max_loss_per_lot_mean": _avg(ml_list),
                    "max_loss_per_lot_max": max(ml_list) if ml_list else None,
                    "taken_days": taken,
                    "skip_budget_5000": skip_budget,
                    "skip_hard_10000_zero_credit": skip_hard,
                    "skip_live_floor_units": skip_live_floor,
                    "all_days_mean_within_cycle": pack(day_books),
                    "one_entry_per_cycle": pack(one_books),
                    "headline_pf_dd": "omitted_under_10_trades" if taken < 10 else "not_headlined_followup",
                }
            )

    # exploratory quartiles on one-entry 10% net
    q_payload = {"label": "EXPLORATORY", "do_not_select_parameters": True, "by_structure": []}
    for sid in {o["structure"] for o in quartile_obs}:
        xs = [o for o in quartile_obs if o["structure"] == sid and o.get("iv_minus_rv_vol") is not None]
        if len(xs) < 4:
            q_payload["by_structure"].append({"id": sid, "n": len(xs), "note": "too few for quartiles"})
            continue
        feats = sorted(o["iv_minus_rv_vol"] for o in xs)
        n = len(feats)
        cuts = [feats[max(0, min(n - 1, int(n * p)))] for p in (0.25, 0.50, 0.75)]

        def qbin(v: float) -> int:
            if v <= cuts[0]:
                return 1
            if v <= cuts[1]:
                return 2
            if v <= cuts[2]:
                return 3
            return 4

        bins: Dict[int, List[float]] = {1: [], 2: [], 3: [], 4: []}
        bins_cal: Dict[int, List[float]] = {1: [], 2: [], 3: [], 4: []}
        for o in xs:
            b = qbin(o["iv_minus_rv_vol"])
            if o.get("net_hold_10pct") is not None:
                bins[b].append(o["net_hold_10pct"])
            if o.get("net_hold_calibrated") is not None:
                bins_cal[b].append(o["net_hold_calibrated"])
        q_payload["by_structure"].append(
            {
                "id": sid,
                "n_cycles": len(xs),
                "cuts_iv_minus_rv": cuts,
                "quartiles_net_hold_10pct": {f"Q{i}": _cycle_stats(bins[i]) for i in range(1, 5)},
                "quartiles_net_hold_calibrated": {f"Q{i}": _cycle_stats(bins_cal[i]) for i in range(1, 5)},
            }
        )

    return {
        "followup_registered_sha": FOLLOWUP_SHA,
        "locked_registered_sha": REGISTERED_SHA,
        "calibrated_schedule": CALIBRATED_META,
        "expired_cycles_per_commodity": dict(expired_by_sym),
        "cap_integrity": cap_explain,
        "structures": out_rows,
        "iv_rv_quartiles_exploratory": q_payload,
        "datewise": scan_datewise_dir(),
        "asof_ist": today.isoformat(),
        "generated_at": datetime.now(timezone.utc).isoformat(),
    }


def delta_snapshot_status(today: Optional[date] = None) -> Dict[str, Any]:
    today = today or ist_today()
    try:
        ensure_tarang_tables()
        db = SessionLocal()
        try:
            snaps = db.execute(
                text(
                    """
                    SELECT DISTINCT underlying_symbol, expiry_date
                    FROM tarang_chain_snapshots
                    WHERE venue = 'delta_india'
                    """
                )
            ).mappings().all()
            ft = db.execute(
                text(
                    """
                    SELECT meta, legs, record_type, venue, status, created_at
                    FROM tarang_trades
                    WHERE venue = 'delta_india'
                    """
                )
            ).mappings().all()
        finally:
            db.close()
    except Exception as exc:
        return {"error": str(exc), "expired_snapshot_cycles": 0, "expired_forward_test_cycles": 0}
    snap_exp = []
    for r in snaps:
        e = _as_date(r.get("expiry_date"))
        if e:
            snap_exp.append((str(r.get("underlying_symbol") or ""), e))
    expired_s = sorted({e for _u, e in snap_exp if e < today})
    open_s = sorted({e for _u, e in snap_exp if e >= today})
    ft_exp = []
    for r in ft:
        meta = r.get("meta") or {}
        if isinstance(meta, str):
            try:
                meta = json.loads(meta)
            except json.JSONDecodeError:
                meta = {}
        e = _as_date(meta.get("expiry") or meta.get("expiry_date"))
        if e:
            ft_exp.append(e)
    expired_ft = sorted({e for e in ft_exp if e < today})
    return {
        "expired_snapshot_cycles": len(expired_s),
        "open_snapshot_cycles": len(open_s),
        "first_expected_expiry": open_s[0].isoformat() if open_s else None,
        "open_expiries": [e.isoformat() for e in open_s],
        "expired_forward_test_cycles": len(expired_ft),
        "expired_forward_test_expiries": [e.isoformat() for e in expired_ft],
        "note": "Weekly. Zero expired snapshot/FT cycles is expected this early. Do not invent cycles.",
    }


def delta_fee_pct_from_latest_snapshots() -> Dict[str, Any]:
    """Maker (limit at mid) vs taker fee as % of credit from latest Delta snapshots. Research only."""
    try:
        ensure_tarang_tables()
        db = SessionLocal()
        try:
            rows = db.execute(
                text(
                    """
                    SELECT DISTINCT ON (underlying_symbol, expiry_date)
                           underlying_symbol, expiry_date, payload_gzip, captured_at
                    FROM tarang_chain_snapshots
                    WHERE venue = 'delta_india'
                    ORDER BY underlying_symbol, expiry_date, captured_at DESC
                    """
                )
            ).mappings().all()
        finally:
            db.close()
    except Exception as exc:
        return {"error": str(exc)}
    from backend.services.tarang.domain.types import OptionChain, OptionQuote

    specs = STRUCTURES + DELTA_EXTRA
    out: List[Dict[str, Any]] = []
    for r in rows[:24]:
        try:
            blob = r.get("payload_gzip") or r.get("payload")
            payload = unpack_chain_payload(blob) if isinstance(blob, (bytes, memoryview)) else blob
        except Exception:
            continue
        quotes_raw = (payload or {}).get("quotes") or []
        quotes = []
        for q in quotes_raw:
            bid, ask = q.get("bid"), q.get("ask")
            mid = q.get("mark") or q.get("mid")
            if mid is None and bid and ask:
                try:
                    mid = 0.5 * (float(bid) + float(ask))
                except (TypeError, ValueError):
                    mid = None
            quotes.append(
                OptionQuote(
                    instrument_key=str(q.get("instrument_key") or q.get("symbol") or ""),
                    symbol=str(q.get("symbol") or r["underlying_symbol"]),
                    underlying=str(r["underlying_symbol"]),
                    expiry=str(r["expiry_date"])[:10],
                    strike=float(q.get("strike") or 0),
                    right=str(q.get("right") or ""),
                    bid=float(bid) if bid else None,
                    ask=float(ask) if ask else None,
                    mid=float(mid) if mid else None,
                    iv=float(q["iv"]) if q.get("iv") is not None else None,
                    delta=float(q["delta"]) if q.get("delta") is not None else None,
                    oi=q.get("oi"),
                )
            )
        if not quotes:
            continue
        und = str(r["underlying_symbol"] or "").upper()
        cv = 0.001 if "BTC" in und else 0.01 if "ETH" in und else 0.001
        F = payload.get("underlying_price")
        step = payload.get("strike_step") or 0
        if not step:
            ks = sorted({q.strike for q in quotes})
            diffs = [ks[i + 1] - ks[i] for i in range(len(ks) - 1) if ks[i + 1] - ks[i] > 0]
            step = min(diffs) if diffs else 0
        chain = OptionChain(
            profile_id="BTCUSD" if "BTC" in und else "ETHUSD",
            venue="delta_india",
            underlying=und,
            expiry=str(r["expiry_date"])[:10],
            futures_or_spot=float(F) if F else None,
            lot_size=None,
            strike_step=float(step or 0),
            quotes=quotes,
            built_at=str(r.get("captured_at") or ""),
            meta={"contract_value": cv},
        )
        width_steps = 2
        for spec in specs:
            built = _build_research(chain, spec, width_steps)
            if not built.get("ok"):
                continue
            credit = built.get("net_credit")
            width = float(built.get("width") or 0)
            if not credit or credit <= 0:
                continue
            maker = taker = 0.0
            for lg in built["legs"]:
                px = float(lg.get("mid") or 0)
                maker += delta_india_option_fee_inr(premium_pts=px, contract_value=cv, qty=1, underlying_price=F, taker=False)["total_inr"]
                taker += delta_india_option_fee_inr(premium_pts=px, contract_value=cv, qty=1, underlying_price=F, taker=True)["total_inr"]
            # round trip ≈ 2×
            maker *= 2
            taker *= 2
            credit_inr = float(credit) * cv * 83.0
            if credit_inr <= 0:
                continue
            out.append(
                {
                    "structure": spec["id"],
                    "underlying": und,
                    "expiry": str(r["expiry_date"])[:10],
                    "width": width,
                    "credit_pts": credit,
                    "maker_fee_pct_credit": 100.0 * maker / credit_inr,
                    "taker_fee_pct_credit": 100.0 * taker / credit_inr,
                }
            )
    by_id: Dict[str, List[float]] = defaultdict(list)
    by_id_t: Dict[str, List[float]] = defaultdict(list)
    for row in out:
        by_id[row["structure"]].append(row["maker_fee_pct_credit"])
        by_id_t[row["structure"]].append(row["taker_fee_pct_credit"])
    return {
        "model": "limit at mid = maker 0.010% notional (3.5% premium cap + GST); taker 0.03% shown for contrast. Live paper still assume_taker. Not a live change.",
        "n_structure_snapshot_rows": len(out),
        "maker_pct_credit_mean_by_id": {k: (sum(v) / len(v) if v else None) for k, v in by_id.items()},
        "taker_pct_credit_mean_by_id": {k: (sum(v) / len(v) if v else None) for k, v in by_id_t.items()},
        "sample": out[:20],
        "extra_structures_registered": [s["id"] for s in DELTA_EXTRA],
        "expired_pnl_on_extra": "not_run_zero_expired_cycles_expected",
    }


def parity_diagnostics() -> Dict[str, Any]:
    ensure_tarang_tables()
    db = SessionLocal()
    try:
        by_month = db.execute(
            text(
                """
                SELECT date_trunc('month', trade_date)::date AS m, COUNT(*)
                FROM (
                  SELECT trade_date FROM tarang_hist_eod
                  WHERE symbol='CRUDEOILM' AND option_type IN ('CE','PE') AND parity_abs_pct_diff > 1
                  GROUP BY trade_date, expiry_date
                ) s GROUP BY 1 ORDER BY 1
                """
            )
        ).fetchall()
        signs = db.execute(
            text(
                """
                SELECT
                  SUM(CASE WHEN underlying_price - parity_forward > 0 THEN 1 ELSE 0 END) AS fut_above,
                  SUM(CASE WHEN underlying_price - parity_forward < 0 THEN 1 ELSE 0 END) AS fut_below,
                  SUM(CASE WHEN underlying_price - parity_forward = 0 THEN 1 ELSE 0 END) AS eq
                FROM (
                  SELECT trade_date, expiry_date, MAX(underlying_price) AS underlying_price, MAX(parity_forward) AS parity_forward
                  FROM tarang_hist_eod
                  WHERE symbol='CRUDEOILM' AND option_type IN ('CE','PE') AND parity_abs_pct_diff > 1
                  GROUP BY 1,2
                ) s
                """
            )
        ).fetchone()
        aug10 = db.execute(
            text(
                """
                SELECT expiry_date, MAX(underlying_source), MAX(underlying_fut_expiry), MAX(mapped_fut_expiry),
                       MAX(underlying_price), MAX(parity_forward), MAX(parity_abs_pct_diff)
                FROM tarang_hist_eod
                WHERE symbol='CRUDEOILM' AND option_type IN ('CE','PE') AND trade_date='2026-08-10'
                GROUP BY 1 ORDER BY 1
                """
            )
        ).fetchall()
        aug10_fut = db.execute(
            text(
                """
                SELECT expiry_date, close, volume_lots, open, high, low
                FROM tarang_hist_eod
                WHERE symbol='CRUDEOILM' AND option_type='FUT' AND trade_date='2026-08-10'
                ORDER BY expiry_date
                """
            )
        ).fetchall()
        flags = db.execute(
            text(
                """
                SELECT h.trade_date, h.expiry_date, h.mapped_fut_expiry,
                       MAX(h.underlying_price) AS ffut, MAX(h.parity_forward) AS fpar,
                       MAX(h.parity_abs_pct_diff) AS pct,
                       MAX(h.underlying_price) - MAX(h.parity_forward) AS signed
                FROM tarang_hist_eod h
                WHERE h.symbol='CRUDEOILM' AND h.option_type IN ('CE','PE') AND h.parity_abs_pct_diff > 1
                GROUP BY 1,2,3
                ORDER BY 1,2
                """
            )
        ).mappings().all()
        moves = db.execute(
            text(
                """
                SELECT trade_date, expiry_date, close, volume_lots
                FROM tarang_hist_eod
                WHERE symbol='CRUDEOILM' AND option_type='FUT'
                  AND expiry_date IN ('2026-03-19','2026-04-20','2026-05-18')
                ORDER BY expiry_date, trade_date
                """
            )
        ).mappings().all()
    finally:
        db.close()

    by_exp: Dict[date, List[Tuple[date, float]]] = defaultdict(list)
    for r in moves:
        if r["close"] is None:
            continue
        by_exp[_as_date(r["expiry_date"])].append((_as_date(r["trade_date"]), float(r["close"])))
    big = []
    for exp, xs in by_exp.items():
        xs = sorted(xs)
        for i in range(1, len(xs)):
            d0, c0 = xs[i - 1]
            d1, c1 = xs[i]
            if c0 <= 0:
                continue
            pct = abs(c1 - c0) / c0 * 100.0
            signed = (c1 - c0) / c0 * 100.0
            if date(2026, 3, 1) <= d1 <= date(2026, 4, 30):
                big.append({"trade_date": d1.isoformat(), "fut_expiry": exp.isoformat(), "abs_pct": pct, "signed_pct": signed, "close": c1, "prev_close": c0})
    big = sorted(big, key=lambda x: -x["abs_pct"])[:10]

    # stale-close: parity error vs |fut move| that day on mapped contract
    move_idx = {}
    for exp, xs in by_exp.items():
        xs = sorted(xs)
        for i in range(1, len(xs)):
            d1, c1 = xs[i]
            c0 = xs[i - 1][1]
            move_idx[(exp, d1)] = abs(c1 - c0)
    pairs = []
    for r in flags:
        me = _as_date(r["mapped_fut_expiry"])
        td = _as_date(r["trade_date"])
        mv = move_idx.get((me, td))
        ffut = r.get("ffut")
        fpar = r.get("fpar")
        if mv is None or ffut in (None, 0):
            continue
        err = abs(float(ffut) - float(fpar or 0))
        pairs.append((err, mv, float(r["pct"])))
    corr = None
    slope = None
    if len(pairs) >= 10:
        xs = [p[1] for p in pairs]
        ys = [p[0] for p in pairs]
        mx, my = sum(xs) / len(xs), sum(ys) / len(ys)
        varx = sum((x - mx) ** 2 for x in xs)
        cov = sum((x - mx) * (y - my) for x, y in zip(xs, ys))
        vary = sum((y - my) ** 2 for y in ys)
        slope = cov / varx if varx else None
        corr = cov / (varx * vary) ** 0.5 if varx and vary else None

    return {
        "count_pairs": len(flags),
        "by_month": [(str(m), int(c)) for m, c in by_month],
        "sign_Ffut_minus_Fparity": {"fut_above_parity": int(signs[0] or 0), "fut_below_parity": int(signs[1] or 0), "equal": int(signs[2] or 0)},
        "aug10_2026_option_expiries": [
            {"option_expiry": str(r[0]), "source": r[1], "fut_exp": str(r[2]), "mapped": str(r[3]), "F_fut": r[4], "F_parity": r[5], "abs_pct": r[6]}
            for r in aug10
        ],
        "aug10_2026_futures_rows": [
            {"fut_expiry": str(r[0]), "close": r[1], "volume_lots": r[2], "open": r[3], "high": r[4], "low": r[5]}
            for r in aug10_fut
        ],
        "timing_note": "Both option and futures closes are MCX EOD bhavcopy session closes for that trade_date. No separate timestamp in the file; stale-close is tested as |F_fut−F_parity| vs |same-day mapped futures move|.",
        "stale_close": {
            "n": len(pairs),
            "corr_abs_error_vs_abs_fut_move": corr,
            "regression_slope_error_on_move": slope,
            "read": "Positive correlation would support 'parity looks stale vs a large futures print that day'.",
        },
        "ten_largest_mapped_fut_moves_mar_apr_2026": big,
    }


def run_followup(*, all_rows: Optional[List[Dict[str, Any]]] = None, today: Optional[date] = None) -> Dict[str, Any]:
    mcx = run_mcx_followup(all_rows=all_rows, today=today)
    payload = {
        **mcx,
        "delta": {
            **delta_snapshot_status(today),
            "fees": delta_fee_pct_from_latest_snapshots(),
        },
        "parity": parity_diagnostics(),
        "ng_and_2025_futcom": {
            "NATGASMINI_OPTFUT": "not_found",
            "NATGASMINI_FUTCOM": "not_found",
            "CRUDEOILM_FUTCOM_SepDec2025": "not_found",
            "proceeded": "crude_only",
        },
        "kill_criteria": "docs/tarang-premise-test.md — PENDING OPERATOR APPROVAL",
        "live_defaults_unchanged": True,
    }
    try:
        CACHE_FOLLOWUP.parent.mkdir(parents=True, exist_ok=True)
        CACHE_FOLLOWUP.write_text(json.dumps(payload, indent=2, default=str), encoding="utf-8")
        # merge onto premise cache for existing Backtest card
        if CACHE_PATH.is_file():
            prem = json.loads(CACHE_PATH.read_text(encoding="utf-8"))
            prem["followup"] = {
                "registered_sha": FOLLOWUP_SHA,
                "plain_language_pending": True,
                "calibrated_schedule": CALIBRATED_META,
                "expired_cycles_per_commodity": payload.get("expired_cycles_per_commodity"),
                "structures_one_entry_gross_and_calibrated": [
                    {
                        "id": s["id"],
                        "one_entry_gross": (s.get("one_entry_per_cycle") or {}).get("gross"),
                        "one_entry_calibrated_hold": ((s.get("one_entry_per_cycle") or {}).get("calibrated") or {}).get("hold"),
                        "one_entry_10pct_hold": (((s.get("one_entry_per_cycle") or {}).get("uniform") or {}).get("10pct") or {}).get("hold"),
                        "skip_budget_5000": s.get("skip_budget_5000"),
                    }
                    for s in (payload.get("structures") or [])
                ],
            }
            CACHE_PATH.write_text(json.dumps(prem, indent=2, default=str), encoding="utf-8")
    except OSError:
        pass
    return payload
