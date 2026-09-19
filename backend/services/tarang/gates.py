"""Screener gates for Kosmic Tarang Phase 2 (pure / testable)."""
from __future__ import annotations

from dataclasses import asdict, dataclass, field
from datetime import date, datetime, timezone
from typing import Any, Dict, List, Optional, Sequence


@dataclass
class GateResult:
    name: str
    passed: bool
    actual: Any = None
    threshold: Any = None
    detail: str = ""
    status_hint: str = ""  # WATCHING | BLOCKED | WARMING_UP | ""

    def to_dict(self) -> Dict[str, Any]:
        return asdict(self)


def spread_pct_of_mid(bid: Optional[float], ask: Optional[float], mid: Optional[float] = None) -> Optional[float]:
    if bid is None or ask is None:
        return None
    if mid is None:
        mid = 0.5 * (bid + ask)
    if mid is None or mid <= 0:
        return None
    return 100.0 * (ask - bid) / mid


def gate_liquidity(
    legs: Sequence[Dict[str, Any]],
    max_spread_pct: float = 15.0,
    min_oi: float = 1.0,
) -> GateResult:
    """Every leg: bid-ask ≤ max_spread_pct of mid and non-trivial OI."""
    fails: List[str] = []
    for leg in legs:
        pct = spread_pct_of_mid(leg.get("bid"), leg.get("ask"), leg.get("mid"))
        oi = leg.get("oi")
        label = f"{leg.get('right')}@{leg.get('strike')}"
        if pct is None:
            fails.append(f"{label}: missing bid/ask")
        elif pct > max_spread_pct:
            fails.append(f"{label}: spread {pct:.1f}% > {max_spread_pct}%")
        if oi is None or float(oi) < min_oi:
            fails.append(f"{label}: OI={oi}")
    ok = not fails
    return GateResult(
        name="liquidity",
        passed=ok,
        actual={"fails": fails} if fails else "ok",
        threshold={"max_spread_pct_of_mid": max_spread_pct, "min_oi": min_oi},
        detail="; ".join(fails) if fails else "all legs liquid",
        status_hint="" if ok else "WATCHING",
    )


def gate_iv_percentile(
    percentile: Optional[float],
    snapshot_count: int,
    min_percentile: float = 50.0,
    min_snapshots: int = 60,
) -> GateResult:
    """IV percentile ≥ min; disabled (warm-up) until enough snapshots."""
    if snapshot_count < min_snapshots:
        return GateResult(
            name="iv_percentile",
            passed=True,
            actual={"percentile": percentile, "snapshots": snapshot_count},
            threshold={"min_percentile": min_percentile, "min_snapshots": min_snapshots},
            detail=f"warming up: {snapshot_count}/{min_snapshots} snapshots — gate disabled",
            status_hint="WARMING_UP",
        )
    if percentile is None:
        return GateResult(
            name="iv_percentile",
            passed=False,
            actual=None,
            threshold=min_percentile,
            detail="percentile unavailable",
            status_hint="WATCHING",
        )
    ok = float(percentile) >= float(min_percentile)
    return GateResult(
        name="iv_percentile",
        passed=ok,
        actual=percentile,
        threshold=min_percentile,
        detail=f"IV percentile {percentile:.1f} vs ≥{min_percentile}",
        status_hint="" if ok else "WATCHING",
    )


def gate_iv_vs_rv(
    atm_iv: Optional[float],
    rv_20d: Optional[float],
    relative_min: float = 0.10,
) -> GateResult:
    """ATM IV exceeds RV by at least relative_min (relative)."""
    if atm_iv is None or rv_20d is None or rv_20d <= 0:
        return GateResult(
            name="iv_vs_rv",
            passed=False,
            actual={"atm_iv": atm_iv, "rv_20d": rv_20d},
            threshold=relative_min,
            detail="ATM IV or RV missing",
            status_hint="WATCHING",
        )
    rel = (float(atm_iv) - float(rv_20d)) / float(rv_20d)
    ok = rel >= float(relative_min)
    return GateResult(
        name="iv_vs_rv",
        passed=ok,
        actual={"relative": round(rel, 4), "atm_iv": atm_iv, "rv_20d": rv_20d},
        threshold=relative_min,
        detail=f"IV vs RV relative {rel:.1%} (need ≥{relative_min:.0%})",
        status_hint="" if ok else "WATCHING",
    )


def gate_expiry_dte(
    expiry: Optional[str],
    dte: Optional[int],
    min_dte: Optional[int] = None,
    max_dte: Optional[int] = None,
    today: Optional[date] = None,
) -> GateResult:
    if dte is None and expiry:
        try:
            exp = date.fromisoformat(str(expiry)[:10])
            t = today or datetime.now(timezone.utc).date()
            dte = (exp - t).days
        except ValueError:
            dte = None
    if dte is None:
        return GateResult(
            name="expiry_dte",
            passed=False,
            actual=None,
            threshold={"min": min_dte, "max": max_dte},
            detail="DTE unknown",
            status_hint="WATCHING",
        )
    ok = True
    if min_dte is not None and dte < min_dte:
        ok = False
    if max_dte is not None and dte > max_dte:
        ok = False
    return GateResult(
        name="expiry_dte",
        passed=ok,
        actual=dte,
        threshold={"min": min_dte, "max": max_dte},
        detail=f"DTE={dte}",
        status_hint="" if ok else "WATCHING",
    )


def gate_event_blackout(
    profile_id: str,
    risk_bucket: str,
    events: Sequence[Dict[str, Any]],
    now: Optional[datetime] = None,
    default_hours_before: float = 2.5,
) -> GateResult:
    """No entry within blackout window before scheduled events."""
    now = now or datetime.now(timezone.utc)
    if now.tzinfo is None:
        now = now.replace(tzinfo=timezone.utc)
    blocking: List[str] = []
    for ev in events:
        applies = ev.get("applies_to") or []
        if profile_id not in applies and risk_bucket not in applies:
            continue
        starts = ev.get("starts_at")
        if not starts:
            continue
        if isinstance(starts, str):
            try:
                starts_dt = datetime.fromisoformat(starts.replace("Z", "+00:00"))
            except ValueError:
                continue
        elif isinstance(starts, datetime):
            starts_dt = starts
        else:
            continue
        if starts_dt.tzinfo is None:
            starts_dt = starts_dt.replace(tzinfo=timezone.utc)
        hours = float(ev.get("blackout_hours_before") or default_hours_before)
        delta_h = (starts_dt - now).total_seconds() / 3600.0
        if 0 <= delta_h <= hours:
            blocking.append(f"{ev.get('label') or ev.get('id')} in {delta_h:.1f}h")
    ok = not blocking
    return GateResult(
        name="event_blackout",
        passed=ok,
        actual=blocking or "clear",
        threshold={"hours_before": default_hours_before},
        detail="; ".join(blocking) if blocking else "no blackout",
        status_hint="" if ok else "BLOCKED",
    )


def gate_credit_fraction(
    net_credit: Optional[float],
    width: Optional[float],
    min_frac: float = 0.20,
) -> GateResult:
    if net_credit is None or width is None or width <= 0:
        return GateResult(
            name="credit_fraction",
            passed=False,
            actual=None,
            threshold=min_frac,
            detail="credit or width missing",
            status_hint="WATCHING",
        )
    frac = float(net_credit) / float(width)
    ok = frac >= float(min_frac)
    return GateResult(
        name="credit_fraction",
        passed=ok,
        actual=round(frac, 4),
        threshold=min_frac,
        detail=f"credit/width={frac:.1%} (need ≥{min_frac:.0%})",
        status_hint="" if ok else "WATCHING",
    )


def gate_sizing(
    max_loss_per_unit: Optional[float],
    budget_inr: float,
    units: int,
) -> GateResult:
    """Max loss must fit budget; never round up to one lot (units already floored)."""
    if max_loss_per_unit is None or max_loss_per_unit <= 0:
        return GateResult(
            name="sizing",
            passed=False,
            actual=None,
            threshold=budget_inr,
            detail="max loss unknown",
            status_hint="WATCHING",
        )
    if units < 1:
        return GateResult(
            name="sizing",
            passed=False,
            actual={"max_loss_per_unit": max_loss_per_unit, "units": 0},
            threshold=budget_inr,
            detail=f"max loss ₹{max_loss_per_unit:,.0f}/lot exceeds budget ₹{budget_inr:,.0f}",
            status_hint="WATCHING",
        )
    total = float(max_loss_per_unit) * int(units)
    ok = total <= float(budget_inr) + 1e-6
    return GateResult(
        name="sizing",
        passed=ok,
        actual={"max_loss_per_unit": max_loss_per_unit, "units": units, "total": total},
        threshold=budget_inr,
        detail=f"{units} lot(s) × ₹{max_loss_per_unit:,.0f} = ₹{total:,.0f} vs budget ₹{budget_inr:,.0f}",
        status_hint="" if ok else "WATCHING",
    )


def gate_portfolio_limit(
    open_risk_inr: float,
    candidate_risk_inr: float,
    portfolio_limit_inr: float,
) -> GateResult:
    projected = float(open_risk_inr) + float(candidate_risk_inr)
    ok = projected <= float(portfolio_limit_inr) + 1e-6
    return GateResult(
        name="portfolio_limit",
        passed=ok,
        actual={"open": open_risk_inr, "candidate": candidate_risk_inr, "projected": projected},
        threshold=portfolio_limit_inr,
        detail=f"projected open risk ₹{projected:,.0f} vs cap ₹{portfolio_limit_inr:,.0f}",
        status_hint="" if ok else "BLOCKED",
    )


def gate_stale_data(
    built_at: Optional[str],
    max_age_sec: float = 120.0,
    now: Optional[datetime] = None,
) -> GateResult:
    now = now or datetime.now(timezone.utc)
    if now.tzinfo is None:
        now = now.replace(tzinfo=timezone.utc)
    if not built_at:
        return GateResult(
            name="stale_data",
            passed=False,
            actual=None,
            threshold=max_age_sec,
            detail="no chain timestamp",
            status_hint="BLOCKED",
        )
    try:
        ts = datetime.fromisoformat(str(built_at).replace("Z", "+00:00"))
        if ts.tzinfo is None:
            ts = ts.replace(tzinfo=timezone.utc)
    except ValueError:
        return GateResult(
            name="stale_data",
            passed=False,
            actual=built_at,
            threshold=max_age_sec,
            detail="bad timestamp",
            status_hint="BLOCKED",
        )
    age = (now - ts).total_seconds()
    ok = age <= max_age_sec
    return GateResult(
        name="stale_data",
        passed=ok,
        actual=round(age, 1),
        threshold=max_age_sec,
        detail=f"chain age {age:.0f}s",
        status_hint="" if ok else "BLOCKED",
    )


def gate_short_delta(
    short_delta: Optional[float],
    d_min: float,
    d_max: float,
) -> GateResult:
    if short_delta is None:
        return GateResult(
            name="short_delta",
            passed=False,
            actual=None,
            threshold={"min": d_min, "max": d_max},
            detail="short delta missing",
            status_hint="WATCHING",
        )
    ad = abs(float(short_delta))
    ok = d_min <= ad <= d_max
    return GateResult(
        name="short_delta",
        passed=ok,
        actual=short_delta,
        threshold={"min": d_min, "max": d_max},
        detail=f"|δ|={ad:.3f} in [{d_min},{d_max}]",
        status_hint="" if ok else "WATCHING",
    )


def decide_structure_from_skew(
    put_iv_25d: Optional[float],
    call_iv_25d: Optional[float],
    skew_threshold_rel: float = 0.08,
) -> Dict[str, Any]:
    """
    Balanced → iron_condor.
    Heavy put skew (put IV >> call IV) → put_credit_spread.
    Heavy call skew → call_credit_spread.
    """
    if put_iv_25d is None or call_iv_25d is None or call_iv_25d <= 0 or put_iv_25d <= 0:
        return {
            "structure": "iron_condor",
            "reason": "skew unavailable — default condor",
            "skew_rel": None,
        }
    mid = 0.5 * (put_iv_25d + call_iv_25d)
    skew_rel = (put_iv_25d - call_iv_25d) / mid if mid else 0.0
    if skew_rel >= skew_threshold_rel:
        return {
            "structure": "put_credit_spread",
            "reason": f"put-heavy skew {skew_rel:.1%}",
            "skew_rel": skew_rel,
        }
    if skew_rel <= -skew_threshold_rel:
        return {
            "structure": "call_credit_spread",
            "reason": f"call-heavy skew {skew_rel:.1%}",
            "skew_rel": skew_rel,
        }
    return {
        "structure": "iron_condor",
        "reason": f"balanced skew {skew_rel:.1%}",
        "skew_rel": skew_rel,
    }


def aggregate_status(gates: Sequence[GateResult]) -> str:
    """QUALIFIED | WATCHING | BLOCKED | WARMING_UP."""
    if any(g.status_hint == "BLOCKED" and not g.passed for g in gates):
        return "BLOCKED"
    warming = any(g.status_hint == "WARMING_UP" for g in gates)
    failed = [g for g in gates if not g.passed]
    if not failed:
        return "WARMING_UP" if warming else "QUALIFIED"
    if warming and all(g.name != "iv_percentile" or g.passed for g in failed):
        # still have other failures
        pass
    return "WATCHING"


def compute_iv_percentile(history: Sequence[float], current: Optional[float]) -> Optional[float]:
    if current is None or not history:
        return None
    n = len(history)
    below = sum(1 for x in history if x is not None and float(x) <= float(current))
    return 100.0 * below / n
