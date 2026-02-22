"""
CAR GPT Router - Cumulative Average Return analysis.
Accessible before/after login. No auth required.
"""
from fastapi import APIRouter, Depends, HTTPException
from sqlalchemy.orm import Session
from pydantic import BaseModel
from typing import List, Optional

from backend.database import get_db
from backend.models.car import CarStockList
from backend.config import settings
from backend.services.car_config_loader import get_number_of_weeks, set_number_of_weeks
from backend.services.car_service import (
    get_upstox_service,
    run_car_analysis_for_symbols,
    get_stock_name,
)

router = APIRouter(prefix="/cargpt", tags=["cargpt"])


class SaveStockListRequest(BaseModel):
    symbols: str  # Comma-separated
    number_of_weeks: Optional[int] = None


class SaveStockListResponse(BaseModel):
    success: bool
    message: str
    saved_count: int


class CarStockItem(BaseModel):
    id: int
    symbol: str
    stock_name: Optional[str] = None
    created_at: str


class CarAnalysisResult(BaseModel):
    symbol: str
    stock_name: str
    week_52_high_date: Optional[str]
    current_price: Optional[float]
    last_10_cumulative_avg: List[float]
    signal: str
    error: Optional[str] = None


@router.post("/save-stock-list", response_model=SaveStockListResponse)
async def save_stock_list(
    body: SaveStockListRequest,
    db: Session = Depends(get_db)
):
    """
    Save comma-separated stock symbols to carstocklist table.
    Optionally update number_of_weeks in config (stored in env/code, not DB).
    """
    symbols_raw = (body.symbols or "").strip()
    if not symbols_raw:
        return SaveStockListResponse(
            success=False,
            message="No symbols provided",
            saved_count=0
        )
    symbols = [s.strip().upper() for s in symbols_raw.split(",") if s.strip()]
    if not symbols:
        return SaveStockListResponse(
            success=False,
            message="No valid symbols",
            saved_count=0
        )
    saved = 0
    for sym in symbols:
        existing = db.query(CarStockList).filter(CarStockList.symbol == sym).first()
        if not existing:
            row = CarStockList(symbol=sym)
            db.add(row)
            saved += 1
    db.commit()
    if body.number_of_weeks is not None and body.number_of_weeks > 0:
        set_number_of_weeks(body.number_of_weeks)
        settings.CAR_NUMBER_OF_WEEKS = body.number_of_weeks
    return SaveStockListResponse(
        success=True,
        message=f"Saved {saved} new symbol(s)",
        saved_count=saved
    )


@router.get("/stock-list", response_model=List[CarStockItem])
async def get_stock_list(db: Session = Depends(get_db)):
    """Get all symbols from carstocklist with stock names from instruments, ordered by created_at desc."""
    rows = db.query(CarStockList).order_by(CarStockList.created_at.desc()).all()
    return [
        CarStockItem(
            id=r.id,
            symbol=r.symbol,
            stock_name=get_stock_name(r.symbol),
            created_at=r.created_at.isoformat() if r.created_at else ""
        )
        for r in rows
    ]


@router.get("/config")
async def get_config():
    """Get CAR config (number of weeks)."""
    return {"number_of_weeks": settings.CAR_NUMBER_OF_WEEKS}


@router.post("/config")
async def update_config(number_of_weeks: int):
    """Update number of weeks (in-memory for this process)."""
    if number_of_weeks < 1 or number_of_weeks > 260:
        raise HTTPException(400, "number_of_weeks must be between 1 and 260")
    settings.CAR_NUMBER_OF_WEEKS = number_of_weeks
    return {"number_of_weeks": settings.CAR_NUMBER_OF_WEEKS}


@router.get("/analyze", response_model=List[CarAnalysisResult])
async def run_car_analysis(db: Session = Depends(get_db)):
    """
    Run CAR analysis for all symbols in carstocklist.
    Returns analysis results for each symbol.
    """
    rows = db.query(CarStockList).order_by(CarStockList.created_at.desc()).all()
    symbols = [r.symbol for r in rows]
    if not symbols:
        return []
    upstox = get_upstox_service()
    weeks = get_number_of_weeks()
    settings.CAR_NUMBER_OF_WEEKS = weeks
    results = run_car_analysis_for_symbols(
        symbols=symbols,
        upstox_service=upstox,
        number_of_weeks=weeks
    )
    return [CarAnalysisResult(**r) for r in results]


class AnalyzeSymbolsRequest(BaseModel):
    symbols: List[str]


@router.post("/analyze-symbols", response_model=List[CarAnalysisResult])
async def run_car_analysis_for_input(body: AnalyzeSymbolsRequest):
    """
    Run CAR analysis for a given list of symbols (e.g. from setup form).
    Does not require symbols to be in DB.
    """
    symbols = [s.strip().upper() for s in (body.symbols or []) if (s or "").strip()]
    if not symbols:
        return []
    upstox = get_upstox_service()
    weeks = get_number_of_weeks()
    results = run_car_analysis_for_symbols(
        symbols=symbols,
        upstox_service=upstox,
        number_of_weeks=weeks
    )
    return [CarAnalysisResult(**r) for r in results]
