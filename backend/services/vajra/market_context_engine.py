"""Session-level market bias for Vajra Execution Co-Pilot."""
from __future__ import annotations

from typing import Any, Dict, List, Optional

from backend.services.vajra.trade_state import (
    PHASE_BULL_EXPANSION,
    PHASE_BEAR_EXPANSION,
    PHASE_COMPRESSION,
    PHASE_EARLY_BULL,
    PHASE_EARLY_BEAR,
    PHASE_ROTATIONAL,
    PHASE_TREND_CONTINUATION,
    PHASE_WEAKENING,
    resolve_market_phase,
)


def _f(v: Any, default: float = 0.0) -> float:
    try:
        return float(v) if v is not None else default
    except (TypeError, ValueError):
        return default


def _index_bias(nifty_pct: float, bank_pct: float) -> str:
    if nifty_pct >= 0.35 and bank_pct >= 0.2:
        return "bullish"
    if nifty_pct <= -0.35 and bank_pct <= -0.2:
        return "bearish"
    if abs(nifty_pct) < 0.15 and abs(bank_pct) < 0.15:
        return "choppy"
    if nifty_pct > 0 and bank_pct < -0.1:
        return "rotational"
    if nifty_pct < 0 and bank_pct > 0.1:
        return "rotational"
    return "mean_reversion"


def build_session_market_context(
    rows: List[Dict[str, Any]],
    *,
    nifty_pct: Optional[float] = None,
    bank_pct: Optional[float] = None,
) -> Dict[str, Any]:
    """
    Classify market bias from index + screener row distribution.
    Display labels: Bullish, Bearish, Rotational, Mean-Reversion, Trend Expansion, Choppy.
    """
    if nifty_pct is None or bank_pct is None:
        try:
            from backend.services.vajra.discretionary import _market_index_pct

            idx = _market_index_pct()
            nifty_pct = _f(idx.get("nifty_pct"))
            bank_pct = _f(idx.get("bank_pct"))
        except Exception:
            nifty_pct = 0.0
            bank_pct = 0.0

    phase_counts: Dict[str, int] = {}
    exec_n = armed_n = 0
    for r in rows or []:
        ph = resolve_market_phase(r)
        phase_counts[ph] = phase_counts.get(ph, 0) + 1
        q = str(r.get("qualification_state") or r.get("qualification") or "").upper()
        if q == "EXECUTABLE":
            exec_n += 1
        elif q == "ARMED":
            armed_n += 1

    total = max(1, len(rows or []))
    expansion_n = sum(
        phase_counts.get(p, 0)
        for p in (
            PHASE_BULL_EXPANSION,
            PHASE_BEAR_EXPANSION,
            PHASE_EARLY_BULL,
            PHASE_EARLY_BEAR,
        )
    )
    rot_n = phase_counts.get(PHASE_ROTATIONAL, 0)
    weak_n = phase_counts.get(PHASE_WEAKENING, 0) + phase_counts.get(PHASE_COMPRESSION, 0)

    idx_bias = _index_bias(nifty_pct, bank_pct)
    label = "Choppy / Low Conviction"
    conviction = 45.0

    if expansion_n / total >= 0.35 and exec_n >= 2:
        label = "Trend Expansion"
        conviction = min(92.0, 60.0 + expansion_n / total * 40.0)
    elif idx_bias == "bullish" and expansion_n >= rot_n:
        label = "Bullish"
        conviction = min(88.0, 55.0 + nifty_pct * 8.0)
    elif idx_bias == "bearish" and expansion_n >= rot_n:
        label = "Bearish"
        conviction = min(88.0, 55.0 + abs(nifty_pct) * 8.0)
    elif rot_n / total >= 0.4 or idx_bias == "rotational":
        label = "Rotational"
        conviction = 52.0
    elif idx_bias == "mean_reversion":
        label = "Mean-Reversion"
        conviction = 48.0
    elif weak_n / total >= 0.3:
        label = "Choppy / Low Conviction"
        conviction = 38.0
    elif phase_counts.get(PHASE_TREND_CONTINUATION, 0) / total >= 0.25:
        label = "Trend Expansion"
        conviction = 72.0

    return {
        "market_bias": label,
        "market_bias_code": label.lower().replace(" ", "_").replace("/", "_"),
        "bias_conviction": round(conviction, 1),
        "nifty_pct": round(nifty_pct, 4),
        "banknifty_pct": round(bank_pct, 4),
        "executable_count": exec_n,
        "armed_count": armed_n,
        "phase_distribution": phase_counts,
        "guidance": (
            "Favor sector-aligned setups in stable Top 3. Use conditional plans — no blind entries."
            if exec_n or armed_n
            else "Discovery mode — wait for PREPARE/EXECUTABLE before sizing risk."
        ),
    }
