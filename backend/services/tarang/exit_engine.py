"""ExitEngine — evaluate first-trigger-wins exit rules for Kosmic Tarang."""
from __future__ import annotations

from dataclasses import asdict, dataclass, field
from datetime import date, datetime, time, timezone
from typing import Any, Dict, List, Optional, Sequence
from zoneinfo import ZoneInfo

from backend.services.tarang.config import get_events, get_profiles, get_risk
from backend.services.tarang.gates import gate_event_blackout

IST = ZoneInfo("Asia/Kolkata")


@dataclass
class ExitTrigger:
    reason: str
    hit: bool
    distance_frac: float  # 0 = at trigger, 1 = far; lower = closer
    detail: str = ""
    value: Any = None
    threshold: Any = None

    def to_dict(self) -> Dict[str, Any]:
        return asdict(self)


@dataclass
class ExitEvaluation:
    should_exit: bool
    reason: Optional[str]
    triggers: List[ExitTrigger] = field(default_factory=list)
    closest: Optional[ExitTrigger] = None
    hard_exit_at_ist: Optional[str] = None
    seconds_to_hard_exit: Optional[float] = None

    def to_dict(self) -> Dict[str, Any]:
        return {
            "should_exit": self.should_exit,
            "reason": self.reason,
            "triggers": [t.to_dict() for t in self.triggers],
            "closest": self.closest.to_dict() if self.closest else None,
            "hard_exit_at_ist": self.hard_exit_at_ist,
            "seconds_to_hard_exit": self.seconds_to_hard_exit,
        }


def _parse_hhmm(s: str) -> time:
    parts = str(s or "00:00").split(":")
    return time(int(parts[0]), int(parts[1]) if len(parts) > 1 else 0)


def hard_exit_applies(
    venue: str,
    *,
    holding_mode: str = "INTRADAY",
    expiry: Optional[str] = None,
    now: Optional[datetime] = None,
) -> bool:
    """
    Delta 17:00 IST hard exit applies only to INTRADAY trades and to any
    position on its expiry day. POSITIONAL trades on other days are not flattened.
    MCX 23:15 IST hard exit applies to INTRADAY only (POSITIONAL holds overnight).
    """
    now_ist = (now or datetime.now(timezone.utc)).astimezone(IST)
    mode = str(holding_mode or "INTRADAY").upper()
    if venue == "delta_india":
        if mode == "INTRADAY":
            return True
        if expiry:
            try:
                return date.fromisoformat(str(expiry)[:10]) == now_ist.date()
            except ValueError:
                return False
        return False
    return mode == "INTRADAY"


def hard_exit_datetime(
    venue: str,
    *,
    holding_mode: str = "INTRADAY",
    expiry: Optional[str] = None,
    now: Optional[datetime] = None,
) -> Optional[datetime]:
    """Today's venue hard-exit instant in IST, or None if the rule does not apply."""
    if not hard_exit_applies(venue, holding_mode=holding_mode, expiry=expiry, now=now):
        return None
    risk = get_risk()
    he = risk.get("hard_exits") or {}
    now_ist = (now or datetime.now(timezone.utc)).astimezone(IST)
    if venue == "delta_india":
        hhmm = he.get("delta_flat_ist") or "17:00"
    else:
        hhmm = he.get("mcx_flat_ist") or "23:15"
    t = _parse_hhmm(hhmm)
    return now_ist.replace(hour=t.hour, minute=t.minute, second=0, microsecond=0)


def warning_datetime(venue: str, *, now: Optional[datetime] = None) -> datetime:
    risk = get_risk()
    he = risk.get("hard_exits") or {}
    now_ist = (now or datetime.now(timezone.utc)).astimezone(IST)
    key = "delta_warning_ist" if venue == "delta_india" else "mcx_warning_ist"
    default = "16:55" if venue == "delta_india" else "23:10"
    t = _parse_hhmm(he.get(key) or default)
    return now_ist.replace(hour=t.hour, minute=t.minute, second=0, microsecond=0)


def _clamp01(x: float) -> float:
    return max(0.0, min(1.0, x))


def evaluate_exits(
    *,
    entry_credit_pts: float,
    debit_to_close_pts: float,
    unrealized_pnl_inr: float,
    max_profit_inr: Optional[float],
    max_loss_inr: Optional[float],
    budget_inr: Optional[float],
    short_deltas: Sequence[Optional[float]],
    entry_atm_iv: Optional[float] = None,
    current_atm_iv: Optional[float] = None,
    venue: str = "upstox_mcx",
    profile_id: str = "",
    risk_bucket: str = "ENERGY",
    holding_mode: str = "INTRADAY",
    expiry: Optional[str] = None,
    exit_levels: Optional[Dict[str, Any]] = None,
    now: Optional[datetime] = None,
) -> ExitEvaluation:
    """First trigger that is hit wins; also compute distances for UI progress."""
    risk = get_risk()
    levels = dict(exit_levels or {})
    profit_frac = float(levels.get("profit_take_frac") or risk.get("profit_take_frac_of_credit") or 0.40)
    credit_mult = float(levels.get("credit_stop_multiple") or risk.get("credit_stop_multiple") or 2.0)
    d_min = float(levels.get("delta_stop_min") or risk.get("delta_stop_min") or 0.30)
    d_max = float(levels.get("delta_stop_max") or risk.get("delta_stop_max") or 0.35)
    iv_rise = float(risk.get("iv_stop_relative_rise") or 0.25)

    credit = max(float(entry_credit_pts or 0), 1e-9)
    debit = float(debit_to_close_pts or 0)
    # Captured fraction of credit: (credit - debit) / credit
    captured = (credit - debit) / credit
    triggers: List[ExitTrigger] = []

    # Profit target: captured >= profit_frac
    pt_hit = captured >= profit_frac
    pt_dist = _clamp01(1.0 - captured / max(profit_frac, 1e-9)) if not pt_hit else 0.0
    triggers.append(
        ExitTrigger(
            reason="PROFIT_TARGET",
            hit=pt_hit,
            distance_frac=pt_dist,
            detail=f"captured {captured:.1%} of credit (target {profit_frac:.0%})",
            value=captured,
            threshold=profit_frac,
        )
    )

    # Credit stop: debit >= credit_mult * credit
    cs_hit = debit >= credit_mult * credit
    cs_ratio = debit / credit
    cs_dist = _clamp01(1.0 - cs_ratio / credit_mult) if not cs_hit else 0.0
    triggers.append(
        ExitTrigger(
            reason="CREDIT_STOP",
            hit=cs_hit,
            distance_frac=cs_dist,
            detail=f"debit/credit={cs_ratio:.2f}x (stop {credit_mult:.1f}x)",
            value=cs_ratio,
            threshold=credit_mult,
        )
    )

    # Delta stop
    abs_deltas = [abs(float(d)) for d in short_deltas if d is not None]
    max_d = max(abs_deltas) if abs_deltas else 0.0
    d_hit = max_d >= d_min
    d_dist = _clamp01(1.0 - max_d / d_max) if not d_hit else 0.0
    triggers.append(
        ExitTrigger(
            reason="DELTA_STOP",
            hit=d_hit,
            distance_frac=d_dist,
            detail=f"max |short δ|={max_d:.3f} (band {d_min:.2f}–{d_max:.2f})",
            value=max_d,
            threshold={"min": d_min, "max": d_max},
        )
    )

    # Budget / max-loss stop
    cap = float(budget_inr if budget_inr is not None else (max_loss_inr or 0) or 0)
    loss = max(0.0, -float(unrealized_pnl_inr or 0))
    b_hit = cap > 0 and loss >= cap
    b_dist = _clamp01(1.0 - loss / cap) if cap > 0 and not b_hit else (0.0 if b_hit else 1.0)
    triggers.append(
        ExitTrigger(
            reason="BUDGET_STOP",
            hit=b_hit,
            distance_frac=b_dist,
            detail=f"loss ₹{loss:,.0f} / cap ₹{cap:,.0f}",
            value=loss,
            threshold=cap,
        )
    )

    # IV stop: IV up >= 25% relative while at a loss
    iv_hit = False
    iv_dist = 1.0
    iv_ratio = None
    if entry_atm_iv and current_atm_iv and float(entry_atm_iv) > 0:
        iv_ratio = float(current_atm_iv) / float(entry_atm_iv) - 1.0
        at_loss = float(unrealized_pnl_inr or 0) < 0
        iv_hit = at_loss and iv_ratio >= iv_rise
        iv_dist = 0.0 if iv_hit else _clamp01(1.0 - max(0.0, iv_ratio) / iv_rise)
    triggers.append(
        ExitTrigger(
            reason="IV_STOP",
            hit=iv_hit,
            distance_frac=iv_dist,
            detail=f"IV relative rise {iv_ratio if iv_ratio is not None else 'n/a'} (need ≥{iv_rise:.0%} while losing)",
            value=iv_ratio,
            threshold=iv_rise,
        )
    )

    # Time stop: DTE <= profile time_stop_dte
    ts_dte = None
    if expiry:
        try:
            exp_d = date.fromisoformat(str(expiry)[:10])
            now_ist = (now or datetime.now(timezone.utc)).astimezone(IST)
            ts_dte = (exp_d - now_ist.date()).days
        except ValueError:
            ts_dte = None
    profiles = get_profiles().get("profiles") or {}
    prof = profiles.get(profile_id) or profiles.get(str(profile_id).upper()) or {}
    default_ts = 5 if venue != "delta_india" else 1
    stop_at = levels.get("time_stop_dte")
    if stop_at is None:
        stop_at = prof.get("time_stop_dte")
    if stop_at is None:
        stop_at = default_ts
    stop_at_i = int(stop_at)
    ts_hit = ts_dte is not None and ts_dte <= stop_at_i
    ts_dist = 0.0 if ts_hit else (
        _clamp01((float(ts_dte) - stop_at_i) / max(float(ts_dte or 1), 1.0)) if ts_dte is not None else 1.0
    )
    triggers.append(
        ExitTrigger(
            reason="TIME_STOP",
            hit=ts_hit,
            distance_frac=ts_dist,
            detail=f"DTE={ts_dte} (time-stop ≤{stop_at_i})",
            value=ts_dte,
            threshold=stop_at_i,
        )
    )

    now_utc = now or datetime.now(timezone.utc)
    hard_at = hard_exit_datetime(venue, holding_mode=holding_mode, expiry=expiry, now=now_utc)
    secs = None
    he_hit = False
    if hard_at is not None:
        secs = (hard_at - now_utc.astimezone(IST)).total_seconds()
        he_hit = secs <= 0
    he_dist = 0.0 if he_hit else (_clamp01(secs / 7200.0) if secs is not None else 1.0)
    triggers.append(
        ExitTrigger(
            reason="HARD_EXIT",
            hit=he_hit,
            distance_frac=he_dist,
            detail=f"hard exit {hard_at.strftime('%H:%M') if hard_at else 'n/a'} IST ({secs if secs is not None else 'n/a'}s)",
            value=secs,
            threshold=hard_at.isoformat() if hard_at else None,
        )
    )

    # Event stop
    events_cfg = get_events()
    ev_list = events_cfg.get("events") if isinstance(events_cfg, dict) else events_cfg
    g = gate_event_blackout(profile_id, risk_bucket, ev_list or [], now=now_utc)
    ev_hit = not g.passed
    triggers.append(
        ExitTrigger(
            reason="EVENT_STOP",
            hit=ev_hit,
            distance_frac=0.0 if ev_hit else 1.0,
            detail=g.detail or "no blackout",
            value=g.actual,
            threshold=g.threshold,
        )
    )

    hit = [t for t in triggers if t.hit]
    # Priority order when multiple hit
    priority = [
        "HARD_EXIT",
        "BUDGET_STOP",
        "TIME_STOP",
        "CREDIT_STOP",
        "DELTA_STOP",
        "IV_STOP",
        "EVENT_STOP",
        "PROFIT_TARGET",
    ]
    reason = None
    if hit:
        hit_sorted = sorted(hit, key=lambda t: priority.index(t.reason) if t.reason in priority else 99)
        reason = hit_sorted[0].reason

    closest = min(triggers, key=lambda t: t.distance_frac) if triggers else None

    return ExitEvaluation(
        should_exit=bool(reason),
        reason=reason,
        triggers=triggers,
        closest=closest,
        hard_exit_at_ist=hard_at.strftime("%H:%M") if hard_at else None,
        seconds_to_hard_exit=secs,
    )


def net_greeks_from_legs(per_leg: Sequence[Dict[str, Any]], units: int = 1) -> Dict[str, Optional[float]]:
    """Approximate position Greeks: shorts negative delta contribution already in quote delta sign."""
    net_d = net_t = net_v = 0.0
    any_g = False
    for leg in per_leg:
        side = str(leg.get("side_open") or leg.get("side") or "").upper()
        sign = -1.0 if side == "SELL" else 1.0
        d = leg.get("delta")
        if d is not None:
            net_d += sign * float(d) * units
            any_g = True
        # theta/vega if present on mark
        th = leg.get("theta")
        if th is not None:
            net_t += sign * float(th) * units
        vg = leg.get("vega")
        if vg is not None:
            net_v += sign * float(vg) * units
    if not any_g:
        return {"delta": None, "theta": None, "vega": None}
    return {"delta": net_d, "theta": net_t or None, "vega": net_v or None}
