"""Public read-only API for Open-Low 15m backtest artifact."""
from __future__ import annotations

import json
import logging
from pathlib import Path
from typing import Any, Dict, Optional

from fastapi import APIRouter, HTTPException

from backend.services.open_low_15m.config import ARTIFACT_NAME

logger = logging.getLogger(__name__)

router = APIRouter(prefix="/open-low-15m-backtest", tags=["open-low-15m-backtest"])


def _candidate_paths() -> tuple[Path, ...]:
    root = Path(__file__).resolve().parents[2]
    return (
        Path("/home/ubuntu/trademanthan/data") / ARTIFACT_NAME,
        root / "backend" / "data" / ARTIFACT_NAME,
        root / "data" / ARTIFACT_NAME,
    )


def _find_artifact() -> Optional[Path]:
    for p in _candidate_paths():
        try:
            if p.is_file():
                return p
        except OSError:
            continue
    return None


def _load_json(path: Path) -> Dict[str, Any]:
    try:
        with open(path, encoding="utf-8") as f:
            doc = json.load(f)
    except Exception as e:
        logger.warning("open_low_15m_backtest read %s: %s", path, e)
        raise HTTPException(status_code=500, detail=f"Could not read artifact: {e}")
    if isinstance(doc, dict):
        doc["artifact_path"] = str(path)
    return doc


@router.get("/data")
def get_open_low_backtest_data() -> Dict[str, Any]:
    path = _find_artifact()
    if path is None:
        raise HTTPException(
            status_code=503,
            detail=(
                "Open-Low 15m backtest artifact not found. Run "
                "`python3 scripts/run_open_low_15m_backtest.py` to generate it."
            ),
        )
    return _load_json(path)


@router.get("/health")
def backtest_health() -> Dict[str, str]:
    path = _find_artifact()
    return {"status": "ok" if path else "missing", "artifact": str(path or "")}
