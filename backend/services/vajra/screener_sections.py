"""Screener sections — EXECUTABLE / ARMED / DISCOVERY without Top 8 padding."""
from __future__ import annotations

from typing import Any, Dict, List, Optional, Tuple

from backend.services.vajra.qualification_config import (
    DEFAULT_QUALIFICATION_CONFIG,
    STATE_ARMED,
    STATE_DISCOVERY,
    STATE_EXECUTABLE,
    STATE_REJECT,
)
from backend.services.vajra.trade_state import TOP8_EXCLUDED_PHASES, has_directional_conviction, resolve_market_phase


def _qual(row: Dict[str, Any]) -> str:
    return str(row.get("qualification_state") or row.get("qualification") or "").upper()


def _f(row: Dict[str, Any], key: str) -> float:
    v = row.get(key)
    try:
        return float(v) if v is not None else 0.0
    except (TypeError, ValueError):
        return 0.0


def _section_eligible(row: Dict[str, Any]) -> bool:
    if _qual(row) == STATE_REJECT:
        return False
    phase = str(row.get("market_phase") or row.get("market_context") or "")
    if phase in TOP8_EXCLUDED_PHASES:
        return False
    bias = str(row.get("execution_bias") or row.get("direction") or "").upper()
    if bias == "NEUTRAL":
        return False
    mp = resolve_market_phase(row)
    if row.get("directional_conviction") is False:
        return False
    if not row.get("directional_conviction"):
        if not has_directional_conviction(row, mp):
            return False
    return True


def rank_executable(row: Dict[str, Any]) -> tuple:
    return (
        -_f(row, "execution_score"),
        -_f(row, "conviction_score"),
        -_f(row, "risk_efficiency_score"),
        -_f(row, "volume_score"),
        str(row.get("stock") or row.get("security") or ""),
    )


def rank_armed(row: Dict[str, Any]) -> tuple:
    trigger = row.get("nearest_trigger") or {}
    prox = _f({"d": trigger.get("distance_pct")}, "d") if trigger.get("distance_pct") is not None else 99.0
    return (
        prox,
        -_f(row, "execution_score"),
        -_f(row, "structure_score"),
        -_f(row, "momentum_score"),
        str(row.get("stock") or row.get("security") or ""),
    )


def rank_discovery(row: Dict[str, Any]) -> tuple:
    return (
        -_f(row, "discovery_score"),
        -_f(row, "tps_score"),
        -_f(row, "volume_score"),
        str(row.get("stock") or row.get("security") or ""),
    )


def _sort_pool(rows: List[Dict[str, Any]], rank_fn) -> List[Dict[str, Any]]:
    return sorted(rows, key=rank_fn)


def _regime_banner(executable: List[Dict[str, Any]], armed: List[Dict[str, Any]], rows: List[Dict[str, Any]]) -> Optional[Dict[str, Any]]:
    if executable:
        return None
    rotational = sum(
        1
        for r in rows
        if "rotational" in str(r.get("market_phase") or "").lower()
        or "compression" in str(r.get("market_phase") or "").lower()
    )
    if rotational > len(rows) * 0.3 or (not armed and not executable):
        return {
            "type": "info",
            "message": (
                "No executable setups currently. "
                "Market is rotational/compression-dominant."
            ),
        }
    if not armed:
        return {
            "type": "info",
            "message": "No executable setups currently. Monitor Armed and Discovery sections.",
        }
    return {
        "type": "info",
        "message": "No executable setups currently. See Armed setups one trigger away.",
    }


def build_screener_sections(
    rows: List[Dict[str, Any]],
    *,
    limits: Optional[Dict[str, int]] = None,
) -> Dict[str, Any]:
    cfg = DEFAULT_QUALIFICATION_CONFIG
    lim = limits or cfg.section_limits
    eligible = [r for r in rows if _section_eligible(r)]

    executable = _sort_pool([r for r in eligible if _qual(r) == STATE_EXECUTABLE], rank_executable)
    armed = _sort_pool([r for r in eligible if _qual(r) == STATE_ARMED], rank_armed)
    discovery = _sort_pool([r for r in eligible if _qual(r) == STATE_DISCOVERY], rank_discovery)

    exec_rows = executable[: lim.get(STATE_EXECUTABLE, 8)]
    armed_rows = armed[: lim.get(STATE_ARMED, 8)]
    disc_rows = discovery[: lim.get(STATE_DISCOVERY, 8)]

    banner = _regime_banner(exec_rows, armed_rows, rows)

    sections = {
        STATE_EXECUTABLE: {
            "title": "Executable Now",
            "rows": exec_rows,
            "empty_message": None if exec_rows else "No immediate trade-ready setups.",
        },
        STATE_ARMED: {
            "title": "Armed — One Trigger Away",
            "rows": armed_rows,
            "empty_message": None if armed_rows else "No armed setups.",
        },
        STATE_DISCOVERY: {
            "title": "Discovery — Institutional Attention",
            "rows": disc_rows,
            "empty_message": None if disc_rows else "No discovery setups.",
        },
    }

    return {
        "sections": sections,
        "banner": banner,
        "top_picks": exec_rows,
        "top_sections": {
            STATE_EXECUTABLE: exec_rows,
            STATE_ARMED: armed_rows,
            STATE_DISCOVERY: disc_rows,
            "WATCHLIST": armed_rows + disc_rows,
        },
    }


def select_screener_sections(
    rows: List[Dict[str, Any]], n: int = 8
) -> Tuple[List[Dict[str, Any]], Dict[str, List[Dict[str, Any]]]]:
    """Backward-compatible wrapper — top_picks = EXECUTABLE only (no WATCH padding)."""
    out = build_screener_sections(rows, limits={STATE_EXECUTABLE: n, STATE_ARMED: n, STATE_DISCOVERY: n})
    return out["top_picks"], out["top_sections"]
