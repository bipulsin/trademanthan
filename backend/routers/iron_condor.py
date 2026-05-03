"""
Iron Condor advisory API — analysis, saved positions, alerts. No order placement.
"""
from __future__ import annotations

from typing import Any, Dict, Optional

from fastapi import APIRouter, Depends, HTTPException
from pydantic import BaseModel, Field
from sqlalchemy.orm import Session

from backend.database import get_db
from backend.models.user import User
from backend.routers.auth import get_user_from_token, oauth2_scheme
from backend.services.iron_condor_universe import IRON_CONDOR_UNIVERSE
from backend.services import iron_condor_service as ic

router = APIRouter(prefix="/iron-condor", tags=["iron-condor"])


def _auth(token: str = Depends(oauth2_scheme), db: Session = Depends(get_db)) -> User:
    return get_user_from_token(token, db)


def _serialize_row(d: Dict[str, Any]) -> Dict[str, Any]:
    out = dict(d)
    for k in list(out.keys()):
        v = out[k]
        if hasattr(v, "isoformat") and not isinstance(v, str):
            try:
                out[k] = v.isoformat()
            except Exception:
                pass
    return out


class SettingsBody(BaseModel):
    trading_capital: Optional[float] = Field(None, ge=0)
    max_simultaneous_positions: Optional[int] = Field(None, ge=1, le=12)
    target_position_slots: Optional[int] = Field(None, ge=1, le=10)
    profit_target_pct_of_credit: Optional[float] = None
    stop_loss_pct_of_credit: Optional[float] = None


class AnalyzeBody(BaseModel):
    underlying: str = Field(..., min_length=1, max_length=32)


class SaveBody(BaseModel):
    analysis: Dict[str, Any]


@router.get("/universe")
def iron_condor_universe(_user: User = Depends(_auth)) -> Dict[str, Any]:
    return {"symbols": [{"symbol": k, "sector": v} for k, v in sorted(IRON_CONDOR_UNIVERSE.items())]}


@router.get("/workspace")
def workspace(
    db: Session = Depends(get_db),
    user: User = Depends(_auth),
) -> Dict[str, Any]:
    ic.ensure_iron_condor_tables()
    st = ic.get_or_create_settings(db, int(user.id))
    positions = [_serialize_row(r) for r in ic.list_positions(db, int(user.id))]
    alerts = [_serialize_row(r) for r in ic.recent_alerts(db, int(user.id), 40)]
    return {
        "settings": _serialize_row(dict(st)),
        "positions": positions,
        "alerts": alerts,
    }


@router.put("/settings")
def put_settings(
    body: SettingsBody,
    db: Session = Depends(get_db),
    user: User = Depends(_auth),
) -> Dict[str, Any]:
    patch = body.model_dump(exclude_unset=True)
    row = ic.update_settings(db, int(user.id), patch)
    return {"settings": _serialize_row(dict(row))}


@router.post("/analyze")
def analyze(
    body: AnalyzeBody,
    db: Session = Depends(get_db),
    user: User = Depends(_auth),
) -> Dict[str, Any]:
    try:
        out = ic.analyze_iron_condor(body.underlying.strip(), db, int(user.id))
        return {"success": True, "analysis": out}
    except ValueError as e:
        raise HTTPException(status_code=400, detail=str(e))
    except RuntimeError as e:
        raise HTTPException(status_code=502, detail=str(e))


@router.post("/positions")
def save_position(
    body: SaveBody,
    db: Session = Depends(get_db),
    user: User = Depends(_auth),
) -> Dict[str, Any]:
    try:
        pid = ic.persist_position_from_analysis(db, int(user.id), body.analysis)
        if not pid:
            raise HTTPException(status_code=500, detail="Failed to persist position")
        return {"success": True, "position_id": pid}
    except Exception as e:
        raise HTTPException(status_code=400, detail=str(e))


@router.post("/positions/{position_id}/close")
def close_position_route(
    position_id: int,
    db: Session = Depends(get_db),
    user: User = Depends(_auth),
) -> Dict[str, Any]:
    ok = ic.close_position(db, int(user.id), position_id)
    if not ok:
        raise HTTPException(status_code=404, detail="Open position not found")
    return {"success": True}


@router.post("/positions/{position_id}/evaluate-alerts")
def evaluate_route(
    position_id: int,
    db: Session = Depends(get_db),
    user: User = Depends(_auth),
) -> Dict[str, Any]:
    fires = ic.evaluate_position_alerts(db, int(user.id), position_id)
    return {"success": True, "new_alerts": fires}


@router.get("/health-public")
def health_public() -> Dict[str, str]:
    return {"module": "iron-condor", "mode": "advisory"}
