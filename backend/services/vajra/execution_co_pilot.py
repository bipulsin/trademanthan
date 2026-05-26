"""Vajra Execution Co-Pilot — context-aware discretionary assist layer."""
from __future__ import annotations

import logging
from datetime import date
from typing import Any, Dict, List, Optional, Set

from backend.services.vajra.execution_events import (
    aggregate_session_events,
    build_row_execution_events,
)
from backend.services.vajra.invalidation_monitor import collect_invalidation_signals
from backend.services.vajra.market_context_engine import build_session_market_context
from backend.services.vajra.setup_classifier import (
    quality_grade,
    resolve_execution_workflow_state,
)
from backend.services.vajra.trade_plan_generator import generate_conditional_trade_plan

logger = logging.getLogger(__name__)

# In-memory prior workflow state per session+stock (for transition alerts).
_prior_wf: Dict[str, Dict[str, str]] = {}


def _session_key(session_date: Optional[date]) -> str:
    from backend.services.smart_futures_session_date import effective_session_date_ist_for_trend

    sd = session_date or effective_session_date_ist_for_trend()
    return sd.isoformat()


def _load_active_trade_context(user_id: int) -> Dict[str, Dict[str, Any]]:
    try:
        from backend.services.vajra.trade_service import list_trades

        rows = list_trades(user_id, status="active", platform="vajra_futures")
        out: Dict[str, Dict[str, Any]] = {}
        for t in rows or []:
            sym = str(t.get("stock") or "").strip().upper()
            if sym:
                out[sym] = t
        return out
    except Exception as e:
        logger.debug("co_pilot active trades: %s", e)
        return {}


def enrich_row_co_pilot(
    row: Dict[str, Any],
    *,
    active_trades: Dict[str, Dict[str, Any]],
    prior_wf: Optional[str] = None,
) -> Dict[str, Any]:
    row = dict(row)
    stock = str(row.get("stock") or row.get("security") or "").strip().upper()
    trade = active_trades.get(stock) or {}
    active_stocks = set(active_trades.keys())

    if prior_wf:
        row["prior_execution_workflow_state"] = prior_wf

    row["invalidation_signals"] = collect_invalidation_signals(row)
    row["execution_workflow_state"] = resolve_execution_workflow_state(
        row,
        active_stocks=active_stocks,
        trade_health=(
            float(trade.get("trade_health") or trade.get("health_score"))
            if trade.get("trade_health") is not None or trade.get("health_score") is not None
            else None
        ),
        trade_lifecycle=str(trade.get("lifecycle_state") or ""),
    )
    from backend.services.vajra.setup_classifier import classify_setup_type

    row["setup_type"] = classify_setup_type(row)
    row["quality_grade"] = quality_grade(row)
    row["trade_plan"] = generate_conditional_trade_plan(row)
    row["execution_events"] = build_row_execution_events(row)
    # UI alias — map workflow to focus-mode display without removing qualification v2
    row["execution_state"] = row["execution_workflow_state"]
    return row


def apply_execution_co_pilot(
    rows: List[Dict[str, Any]],
    user_id: int,
    *,
    session_date: Optional[date] = None,
    narrative_symbols: Optional[Set[str]] = None,
) -> Dict[str, Any]:
    """Enrich all rating rows + session market context. Non-blocking overlay."""
    from backend.services.vajra.ltp_enrich import enrich_rows_with_ltp

    rows = enrich_rows_with_ltp(list(rows or []))
    sk = _session_key(session_date)
    bucket = _prior_wf.setdefault(sk, {})
    active_trades = _load_active_trade_context(user_id)

    enriched: List[Dict[str, Any]] = []
    for r in rows:
        stock = str(r.get("stock") or r.get("security") or "").strip().upper()
        prior = bucket.get(stock)
        row = enrich_row_co_pilot(r, active_trades=active_trades, prior_wf=prior)
        bucket[stock] = str(row.get("execution_workflow_state") or "")
        enriched.append(row)

    nifty = bank = None
    try:
        from backend.services.vajra.sector_intelligence import _session_snapshots

        b = _session_snapshots.get(sk) or {}
        nifty = b.get("nifty_pct")
    except Exception:
        pass

    market = build_session_market_context(enriched, nifty_pct=nifty, bank_pct=bank)
    events = aggregate_session_events(enriched)

    narr_syms = narrative_symbols or set()
    narr_count = 0
    if narr_syms:
        from backend.services.vajra.trade_plan_narrative import enrich_trade_plan_narrative

        bias_lbl = market.get("market_bias")
        for i, r in enumerate(enriched):
            sym = str(r.get("stock") or r.get("security") or "").strip().upper()
            if sym not in narr_syms or narr_count >= 3:
                continue
            plan = r.get("trade_plan")
            wf = str(r.get("execution_workflow_state") or "").upper()
            if not plan or wf not in ("PREPARE", "EXECUTABLE"):
                continue
            enriched[i] = dict(r)
            enriched[i]["trade_plan"] = enrich_trade_plan_narrative(
                plan, enriched[i], market_bias=bias_lbl
            )
            narr_count += 1

    return {
        "market_context": market,
        "execution_events": events,
        "rows": enriched,
    }
