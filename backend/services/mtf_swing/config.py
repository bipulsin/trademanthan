"""MTF Swing defaults. See README.md for resolved ambiguities."""
from __future__ import annotations

from dataclasses import dataclass, replace
from datetime import date, timedelta
from typing import Dict, List, Optional, Sequence, Tuple


BACKTEST_START = date(2025, 11, 1)
BACKTEST_END = date(2026, 10, 31)
WARMUP_DAYS = 21
# Sessions after the cutoff used only to finish open trades (TIME / SL / T1).
TAIL_SESSIONS = 10
MAX_SESSIONS = 10
CLUSTER_PCT = 0.003  # 0.3% of the lower price
QTY_LOTS = 1

TIMEFRAMES: Tuple[str, ...] = ("60m", "15m")
INTERVAL = {"60m": "hours/1", "15m": "minutes/15"}

# Legacy pilot months (flat starts). Full-period continuous uses month_windows().
V2_MONTHS: Tuple[str, ...] = ("2026-06", "2026-10")
V2_FULL_START = "2025-11"
V2_FULL_END = "2026-10"

# Primary ablation matrix (spec names). Legacy short names still resolve via aliases.
ABLATION_VARIANTS: Tuple[str, ...] = (
    "v1_equivalent",
    "regime_only",
    "sl_only",
    "tp_only",
    "rr_only",
    "all",
)
LEAVE_ONE_OUT_VARIANTS: Tuple[str, ...] = (
    "all_except_regime",
    "all_except_sl",
    "all_except_tp",
    "all_except_rr",
)
VARIANT_ALIASES: Dict[str, str] = {
    "regime": "regime_only",
    "sl": "sl_only",
    "tp": "tp_only",
    "rr": "rr_only",
    "v1": "v1_equivalent",
}

TRADE_FIELDS = [
    "trade_id",
    "timeframe",
    "month",
    "symbol",
    "side",
    "screen_date",
    "entry_time",
    "entry_price",
    "entry_type",
    "lot_size",
    "qty_lots",
    "sl",
    "t1",
    "exit_time",
    "exit_price",
    "exit_reason",
    "sessions_held",
    "gross_pnl",
    "charges",
    "net_pnl",
    "r_multiple",
    "run_ts",
]

# Append-only v2 trades CSV. Extra columns never rewrite v1 trades.csv.
TRADE_FIELDS_V2 = TRADE_FIELDS + [
    "variant",
    "regime",
    "potential_rr",
    "sl_basis",
    "tp_r",
    "mtm_price",
    "mtm_pnl",
]

DISCARD_FIELDS = [
    "discard_id",
    "timeframe",
    "month",
    "symbol",
    "side",
    "screen_date",
    "reason",
    "detail",
    "event_time",
    "run_ts",
]

DISCARD_FIELDS_V2 = DISCARD_FIELDS + ["variant", "regime"]

REGIME_LOG_FIELDS = [
    "screen_date",
    "week",
    "symbol",
    "close",
    "ma",
    "prev_ma",
    "regime",
    "ma_type",
    "ma_length",
    "run_ts",
]

VARIANT_SUMMARY_FIELDS = [
    "variant",
    "timeframe",
    "month",
    "trades",
    "net_pnl",
    "win_rate",
    "profit_factor",
    "avg_pnl",
    "avg_r",
    "open_trades",
    "mtm_pnl_sum",
    "run_ts",
]

COVERAGE_LOG_FIELDS = [
    "coverage_id",
    "variant",
    "timeframe",
    "month",
    "symbol",
    "last_bar_ts",
    "bars",
    "run_ts",
]


def month_windows() -> List[Tuple[date, date, str]]:
    """Forward months Nov-2025 through Oct-2026 inclusive. Label is YYYY-MM."""
    out: List[Tuple[date, date, str]] = []
    y, m = 2025, 11
    for _ in range(12):
        start = date(y, m, 1)
        if m == 12:
            end = date(y, 12, 31)
            ny, nm = y + 1, 1
        else:
            end = date(y, m + 1, 1) - timedelta(days=1)
            ny, nm = y, m + 1
        out.append((start, end, f"{y:04d}-{m:02d}"))
        y, m = ny, nm
    return out


def month_window_for(label: str) -> Tuple[date, date, str]:
    for start, end, lab in month_windows():
        if lab == label:
            return start, end, lab
    raise ValueError(f"unknown month label {label!r}")


def months_between(start_label: str, end_label: str) -> List[str]:
    """Inclusive YYYY-MM labels from start through end within the backtest window."""
    month_window_for(start_label)
    month_window_for(end_label)
    if start_label > end_label:
        raise ValueError(f"start {start_label} is after end {end_label}")
    return [lab for _, _, lab in month_windows() if start_label <= lab <= end_label]


def normalize_variant(name: str) -> str:
    tag = (name or "").strip().lower()
    return VARIANT_ALIASES.get(tag, tag)


def known_variants(*, leave_one_out: bool = False) -> Tuple[str, ...]:
    if leave_one_out:
        return ABLATION_VARIANTS + LEAVE_ONE_OUT_VARIANTS
    return ABLATION_VARIANTS


@dataclass
class MtfSwingConfig:
    entry_timing: str = "next_open"  # "next_open" | "close"
    max_sessions: Optional[int] = MAX_SESSIONS  # None = no time exit (v2)
    cluster_pct: float = CLUSTER_PCT
    qty_lots: int = QTY_LOTS
    brokerage: float = 0.0
    charges: float = 0.0
    slippage_points: float = 0.0
    timeframe: str = "15m"
    # --- v2 experimental flags (defaults keep v1 behaviour) ---
    variant: str = "v1"
    regime_filter: bool = False
    regime_ma_type: str = "ema"
    regime_ma_length: int = 10
    regime_symbol: str = "NIFTY"
    neutral_action: str = "block"  # block | allow_both
    breadth_filter: bool = False
    sl_basis: str = "abs_extreme"  # abs_extreme | pivot_outside_prev_week
    tp_r: Optional[float] = None  # None → v1 opposite-extreme T1; else R-multiple TP
    min_potential_rr: Optional[float] = None  # None → v1 midpoint-limit path
    below_threshold_action: str = "skip"  # skip | limit_at_threshold
    ablation_tag: str = ""  # v1_equivalent | regime_only | ... | all | ""

    def charge_inr(self) -> float:
        return float(self.brokerage) + float(self.charges)

    @property
    def use_r_target(self) -> bool:
        return self.tp_r is not None and float(self.tp_r) > 0

    @property
    def target_exit_reason(self) -> str:
        return "TP" if self.use_r_target else "T1"

    @property
    def target_cancel_reason(self) -> str:
        return "TP_BEFORE_ENTRY" if self.use_r_target else "T1_BEFORE_ENTRY"


def v2_config(**overrides) -> MtfSwingConfig:
    """Full v2 experiment defaults (all four changes on)."""
    base = MtfSwingConfig(
        variant="v2",
        regime_filter=True,
        sl_basis="pivot_outside_prev_week",
        tp_r=1.5,
        max_sessions=None,
        min_potential_rr=1.3,
        below_threshold_action="skip",
        ablation_tag="all",
    )
    if not overrides:
        return base
    return replace(base, **overrides)


def _flags_for_variant(tag: str) -> Dict[str, object]:
    """Map a canonical variant name to engine flags layered on v1 defaults."""
    if tag == "v1_equivalent":
        return {
            "regime_filter": False,
            "sl_basis": "abs_extreme",
            "tp_r": None,
            "max_sessions": MAX_SESSIONS,
            "min_potential_rr": None,
        }
    if tag == "regime_only":
        return {"regime_filter": True}
    if tag == "sl_only":
        return {"sl_basis": "pivot_outside_prev_week"}
    if tag == "tp_only":
        return {"tp_r": 1.5, "max_sessions": None}
    if tag == "rr_only":
        return {"min_potential_rr": 1.3, "below_threshold_action": "skip"}
    if tag == "all":
        return {
            "regime_filter": True,
            "sl_basis": "pivot_outside_prev_week",
            "tp_r": 1.5,
            "max_sessions": None,
            "min_potential_rr": 1.3,
            "below_threshold_action": "skip",
        }
    if tag == "all_except_regime":
        return {
            "regime_filter": False,
            "sl_basis": "pivot_outside_prev_week",
            "tp_r": 1.5,
            "max_sessions": None,
            "min_potential_rr": 1.3,
            "below_threshold_action": "skip",
        }
    if tag == "all_except_sl":
        return {
            "regime_filter": True,
            "sl_basis": "abs_extreme",
            "tp_r": 1.5,
            "max_sessions": None,
            "min_potential_rr": 1.3,
            "below_threshold_action": "skip",
        }
    if tag == "all_except_tp":
        return {
            "regime_filter": True,
            "sl_basis": "pivot_outside_prev_week",
            "tp_r": None,
            "max_sessions": MAX_SESSIONS,
            "min_potential_rr": 1.3,
            "below_threshold_action": "skip",
        }
    if tag == "all_except_rr":
        return {
            "regime_filter": True,
            "sl_basis": "pivot_outside_prev_week",
            "tp_r": 1.5,
            "max_sessions": None,
            "min_potential_rr": None,
        }
    raise ValueError(f"unknown ablation variant {tag!r}")


def ablation_config(name: str, **overrides) -> MtfSwingConfig:
    """Build a named ablation variant on top of v1 defaults.

    Accepts both spec names (``regime_only``) and legacy short names (``regime``).
    """
    tag = normalize_variant(name)
    allowed = set(ABLATION_VARIANTS) | set(LEAVE_ONE_OUT_VARIANTS)
    if tag not in allowed:
        raise ValueError(f"ablation variant must be one of {sorted(allowed)}")
    flags = _flags_for_variant(tag)
    cfg = replace(MtfSwingConfig(variant="v2", ablation_tag=tag), **flags)  # type: ignore[arg-type]
    if overrides:
        cfg = replace(cfg, **overrides)
    return cfg


def resolve_variants(
    raw: Optional[Sequence[str]] = None,
    *,
    ablation: bool = False,
    leave_one_out: bool = False,
) -> List[str]:
    """Resolve CLI variant selectors.

    Bare ``all`` (or ``--ablation``) expands to the full primary matrix.
    A multi-item list may include the feature-combo variant named ``all``.
    """
    if ablation and not raw:
        return list(known_variants(leave_one_out=leave_one_out))
    if not raw:
        if leave_one_out:
            return list(known_variants(leave_one_out=True))
        return ["all"]
    parts: List[str] = []
    for item in raw:
        for part in str(item).split(","):
            p = part.strip()
            if p:
                parts.append(p)
    if not parts:
        return ["all"]
    # Bare --variants all → full primary matrix (optionally + leave-one-out).
    if len(parts) == 1 and parts[0].strip().lower() == "all":
        return list(known_variants(leave_one_out=leave_one_out))
    out: List[str] = []
    seen = set()
    allowed = set(known_variants(leave_one_out=True))
    for p in parts:
        tag = normalize_variant(p)
        if tag not in allowed:
            raise ValueError(f"unknown variant {p!r}; expected one of {sorted(allowed)}")
        if tag not in seen:
            seen.add(tag)
            out.append(tag)
    if leave_one_out:
        for tag in LEAVE_ONE_OUT_VARIANTS:
            if tag not in seen:
                out.append(tag)
                seen.add(tag)
    return out
