"""Public read-only Top-10 vs READY NOW diagnostics API (no auth)."""
from __future__ import annotations

from typing import Optional

from fastapi import APIRouter, Query

from backend.services import top10_vs_ready_now as svc

router = APIRouter(tags=["diagnostics-top10-vs-ready-now"])


@router.get("/scan/diagnostics/top10-vs-ready-now")
@router.get("/api/diagnostics/top10-vs-ready-now")
@router.get("/diagnostics/top10-vs-ready-now")
def get_top10_vs_ready_now(
    start: Optional[str] = Query(None, description="YYYY-MM-DD inclusive"),
    end: Optional[str] = Query(None, description="YYYY-MM-DD inclusive"),
    source: str = Query(
        "db",
        description="db (compute from consistency_log + RS snapshot, seed fallback) | seed",
    ),
    refresh: bool = Query(False, description="Bypass 5-minute response cache"),
):
    """Per (date, symbol) Top-10 vs READY NOW coverage. Research only — no gating."""
    prefer = "seed" if (source or "").strip().lower() == "seed" else "db"
    return svc.get_payload(start, end, prefer=prefer, refresh=refresh)
