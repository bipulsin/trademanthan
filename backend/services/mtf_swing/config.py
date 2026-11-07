"""MTF Swing defaults. See README.md for resolved ambiguities."""
from __future__ import annotations

from dataclasses import dataclass, replace
from datetime import date, timedelta
from typing import List, Optional, Tuple


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

# v2 experiment months only (independent flat starts).
V2_MONTHS: Tuple[str, ...] = ("2026-06", "2026-10")

ABLATION_VARIANTS: Tuple[str, ...] = ("regime", "sl", "tp", "rr", "all")

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
    "open_trades",
    "mtm_pnl_sum",
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
    ablation_tag: str = ""  # regime | sl | tp | rr | all | ""

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


def ablation_config(name: str, **overrides) -> MtfSwingConfig:
    """Build a single-feature or full ablation variant on top of v1 defaults."""
    tag = (name or "all").strip().lower()
    if tag not in ABLATION_VARIANTS:
        raise ValueError(f"ablation variant must be one of {ABLATION_VARIANTS}")
    cfg = MtfSwingConfig(variant="v2", ablation_tag=tag)
    if tag in ("regime", "all"):
        cfg = replace(cfg, regime_filter=True)
    if tag in ("sl", "all"):
        cfg = replace(cfg, sl_basis="pivot_outside_prev_week")
    if tag in ("tp", "all"):
        cfg = replace(cfg, tp_r=1.5, max_sessions=None)
    if tag in ("rr", "all"):
        cfg = replace(cfg, min_potential_rr=1.3, below_threshold_action="skip")
    if overrides:
        cfg = replace(cfg, **overrides)
    return cfg
