"""Public Analysis snapshot API."""
from __future__ import annotations

from typing import Any, Dict

from fastapi import APIRouter

from backend.services.analysis_page.job import list_analysis_rows

router = APIRouter(tags=["analysis"])


@router.get("/api/analysis/symbols")
@router.get("/scan/analysis")
def get_analysis_symbols() -> Dict[str, Any]:
    rows, updated = list_analysis_rows()
    return {
        "ok": True,
        "count": len(rows),
        "updated_at": updated,
        "symbols": rows,
    }
