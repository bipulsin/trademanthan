"""Read-only live Rocket/Crash REST replay summary."""
from __future__ import annotations

from typing import Any, Dict, Optional

from fastapi import APIRouter, Query

from backend.services.rocket_live_replay import fetch_summary

router = APIRouter(tags=["rocket-live-replay"])


@router.get("/rocket_live_replay_summary")
def rocket_live_replay_summary(
    score: Optional[int] = Query(None, description="Score bucket 2, 3, or 4"),
    side: Optional[str] = Query(None, description="long or short"),
    session_phase: Optional[str] = Query(None, description="early, mid, or late"),
    adx_bucket: Optional[str] = Query(None, description="lt20, 20to30, or gt30"),
) -> Dict[str, Any]:
    if score is not None and score not in (2, 3, 4):
        return {"error": "score must be 2, 3, or 4", "rows": [], "count": 0}
    if side and side.lower() not in ("long", "short"):
        return {"error": "side must be long or short", "rows": [], "count": 0}
    if session_phase and session_phase.lower() not in ("early", "mid", "late"):
        return {"error": "session_phase must be early, mid, or late", "rows": [], "count": 0}
    if adx_bucket and adx_bucket not in ("lt20", "20to30", "gt30"):
        return {"error": "adx_bucket must be lt20, 20to30, or gt30", "rows": [], "count": 0}
    rows = fetch_summary(
        score=score,
        side=side.lower() if side else None,
        session_phase=session_phase.lower() if session_phase else None,
        adx_bucket=adx_bucket,
    )
    for r in rows:
        for k, v in list(r.items()):
            if hasattr(v, "isoformat"):
                r[k] = v.isoformat()
            elif isinstance(v, float):
                r[k] = round(v, 8)
    return {"rows": rows, "count": len(rows)}
