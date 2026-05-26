"""Optional OpenAI narrative on top of rule-based Vajra trade plans."""
from __future__ import annotations

import json
import logging
from typing import Any, Dict, Optional

from backend.config import settings

logger = logging.getLogger(__name__)

MODEL = "gpt-4o-mini"


def enrich_trade_plan_narrative(
    trade_plan: Dict[str, Any],
    row: Dict[str, Any],
    *,
    market_bias: Optional[str] = None,
) -> Dict[str, Any]:
    """
    Adds trade_plan['narrative'] — discretionary coaching prose, not a blind signal.
    Returns trade_plan unchanged if OpenAI unavailable.
    """
    plan = dict(trade_plan)
    key = (settings.OPENAI_API_KEY or "").strip()
    if not key:
        plan["narrative"] = _fallback_narrative(plan, row, market_bias)
        return plan

    try:
        from openai import OpenAI
    except ImportError:
        plan["narrative"] = _fallback_narrative(plan, row, market_bias)
        return plan

    sym = plan.get("symbol") or row.get("stock")
    prompt = {
        "symbol": sym,
        "direction": plan.get("direction"),
        "setup_type": plan.get("setup_type"),
        "quality_grade": plan.get("quality_grade"),
        "confidence_pct": plan.get("confidence_pct"),
        "entry_condition": plan.get("entry_condition"),
        "stop_loss": plan.get("stop_loss"),
        "targets": plan.get("targets"),
        "market_context": plan.get("market_context"),
        "invalidation": plan.get("invalidation"),
        "session_market_bias": market_bias,
        "workflow_state": row.get("execution_workflow_state"),
    }

    system = (
        "You are a disciplined Indian F&O discretionary trading coach. "
        "Write 3–5 short sentences for the trader. "
        "NEVER say BUY NOW or SELL NOW. Use conditional language (if/then, only when). "
        "Emphasize chart confirmation, risk, and sector alignment. Plain text only."
    )
    user = "Trade plan JSON:\n" + json.dumps(prompt, default=str)[:4000]

    try:
        client = OpenAI(api_key=key, timeout=45.0, max_retries=1)
        resp = client.chat.completions.create(
            model=MODEL,
            messages=[
                {"role": "system", "content": system},
                {"role": "user", "content": user},
            ],
            temperature=0.35,
            max_tokens=280,
        )
        text = (resp.choices[0].message.content or "").strip()
        if text:
            plan["narrative"] = text
            return plan
    except Exception as e:
        logger.debug("trade_plan narrative openai: %s", e)

    plan["narrative"] = _fallback_narrative(plan, row, market_bias)
    return plan


def _fallback_narrative(
    plan: Dict[str, Any],
    row: Dict[str, Any],
    market_bias: Optional[str],
) -> str:
    sym = plan.get("symbol") or "Symbol"
    setup = plan.get("setup_type") or "setup"
    grade = plan.get("quality_grade") or "—"
    ec = plan.get("entry_condition") or "Confirm trigger on 5m chart before entry."
    parts = [
        f"{sym} shows a {setup} pattern (grade {grade}).",
        ec,
        "Size the position only if structure and sector remain aligned at trigger time.",
    ]
    if market_bias:
        parts.append(f"Session market bias is {market_bias} — adjust conviction accordingly.")
    inv = plan.get("invalidation") or []
    if inv:
        parts.append(str(inv[0]))
    return " ".join(parts)
