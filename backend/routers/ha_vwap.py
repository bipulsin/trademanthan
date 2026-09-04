"""GET /api/ha-vwap/backtest — combined HA-VWAP artifact."""
from __future__ import annotations

import json
import logging
from pathlib import Path
from typing import Any, Dict, Optional

from fastapi import APIRouter, HTTPException

from backend.services.ha_vwap.config import ARTIFACT_COMBINED, PUBLIC_ARTIFACT

logger = logging.getLogger(__name__)

router = APIRouter(prefix="/api/ha-vwap", tags=["ha-vwap"])


def _candidate_paths() -> tuple:
    root = Path(__file__).resolve().parents[2]
    return (
        Path("/home/ubuntu/trademanthan/data/ha_vwap") / ARTIFACT_COMBINED,
        Path("/home/ubuntu/twcto/data/ha_vwap") / ARTIFACT_COMBINED,
        root / "data" / "ha_vwap" / ARTIFACT_COMBINED,
        root / "backend" / "data" / ARTIFACT_COMBINED,
        root / "frontend" / "public" / PUBLIC_ARTIFACT,
    )


def _find() -> Optional[Path]:
    for p in _candidate_paths():
        try:
            if p.is_file():
                return p
        except OSError:
            continue
    return None


@router.get("/backtest")
def get_ha_vwap_backtest() -> Dict[str, Any]:
    path = _find()
    if path is None:
        return {
            "ok": False,
            "summary": {"trades": 0, "win_pct": 0, "pnl": 0, "by_month": {}},
            "trades": [],
            "months_status": {},
            "detail": "No artifact yet. Run python3 scripts/run_ha_vwap_backtest.py",
        }
    try:
        doc = json.loads(path.read_text(encoding="utf-8"))
    except Exception as e:
        logger.warning("ha_vwap read %s: %s", path, e)
        raise HTTPException(status_code=500, detail=str(e))
    if isinstance(doc, dict):
        doc["artifact_path"] = str(path)
        doc.setdefault("ok", True)
    return doc
