"""GET /api/ema-scan — the saved EMA 30 reclaim study."""
from __future__ import annotations

import json
import logging
from typing import Any, Dict

from fastapi import APIRouter, HTTPException

from backend.services.ema_scan.job import find_result

logger = logging.getLogger(__name__)

router = APIRouter(prefix="/api/ema-scan", tags=["ema-scan"])


@router.get("")
def get_ema_scan() -> Dict[str, Any]:
    path = find_result()
    if path is None:
        return {
            "ok": False,
            "signals": [],
            "skipped": [],
            "days": [],
            "detail": "EMA scan result is not on this server yet.",
        }
    try:
        doc = json.loads(path.read_text(encoding="utf-8"))
    except Exception as exc:
        logger.warning("ema_scan read %s: %s", path, exc)
        raise HTTPException(status_code=500, detail="Could not read the EMA scan result") from exc
    if isinstance(doc, dict):
        doc["ok"] = True
        doc["artifact_path"] = str(path)
    return doc
