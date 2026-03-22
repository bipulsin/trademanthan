"""
CAR GPT Router - Cumulative Average Return analysis.
Accessible before/after login. No auth required.
"""
import logging
from decimal import Decimal, InvalidOperation

from fastapi import APIRouter, Depends, HTTPException
from sqlalchemy.orm import Session
from sqlalchemy import text
from sqlalchemy.exc import SQLAlchemyError, IntegrityError
from pydantic import BaseModel, Field
from typing import List, Optional

logger = logging.getLogger(__name__)

from backend.database import get_db, engine
from backend.models.car import CarStockList
from backend.config import settings
from backend.services.car_config_loader import get_number_of_weeks, set_number_of_weeks
from backend.services.car_service import (
    get_upstox_service,
    run_car_analysis_for_symbols,
    get_stock_name,
)
from backend.services.symbol_isin_mapping import get_stock_names_batch, get_instrument_key
from backend.services.car_nifty200_updater import (
    run_car_for_symbol_yahoo,
    run_car_for_symbol_upstox,
    _is_valid_car_result,
)

router = APIRouter(prefix="/cargpt", tags=["cargpt"])


class SaveStockListRequest(BaseModel):
    symbols: str  # Comma-separated
    number_of_weeks: Optional[int] = None
    user_id: Optional[int] = None
    buy_price: Optional[float] = 0.0


class SaveStockListResponse(BaseModel):
    success: bool
    message: str
    saved_count: int


class CarStockItem(BaseModel):
    id: int
    symbol: str
    userid: int
    buy_price: float
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


class AddCarStockRequest(BaseModel):
    symbol: str
    buy_price: float = Field(..., ge=0)
    user_id: int


class BulkCarStockRow(BaseModel):
    symbol: str
    buy_price: float = Field(..., ge=0)


class BulkCarStockUploadRequest(BaseModel):
    user_id: int
    rows: List[BulkCarStockRow]


def _normalize_symbol(raw_symbol: str) -> str:
    return (raw_symbol or "").strip().upper()


def _run_car_for_symbol_on_the_fly(symbol: str):
    """
    Run CAR for one symbol (Yahoo first, then Upstox if needed). Return dict with
    week_52_high_date, current_price, last_10_cumulative_avg (list), signal, or None.
    """
    symbol = _normalize_symbol(symbol)
    if not symbol:
        return None
    data = run_car_for_symbol_yahoo(symbol)
    if not data or not _is_valid_car_result(data):
        instrument_key = None
        with engine.begin() as conn:
            row = conn.execute(
                text("SELECT stock_instrument_key FROM arbitrage_master WHERE stock = :s"),
                {"s": symbol},
            ).mappings().first()
            if row and (row.get("stock_instrument_key") or "").strip():
                instrument_key = (row["stock_instrument_key"] or "").strip()
        if not instrument_key:
            instrument_key = get_instrument_key(symbol) or ""
            if instrument_key and "|" in instrument_key:
                pass
            else:
                instrument_key = ""
        if instrument_key:
            data = run_car_for_symbol_upstox(symbol, instrument_key)
    if not data or not _is_valid_car_result(data):
        return None
    d52 = data.get("date_52weekhigh")
    if d52 is not None and hasattr(d52, "strftime"):
        date_str = d52.strftime("%Y-%m-%d")
    elif isinstance(d52, str) and d52.strip():
        date_str = d52.strip()[:10]
    else:
        date_str = str(d52)[:10] if d52 else ""
    last10_raw = (data.get("last10daycummavg") or "").strip()
    last10_list = [x.strip() for x in last10_raw.split(",") if x.strip()] if last10_raw else []
    return {
        "week_52_high_date": date_str or None,
        "current_price": data.get("stock_ltp"),
        "last_10_cumulative_avg": last10_list,
        "signal": (data.get("signal") or "").strip() or None,
        "dma50": data.get("dma50"),
        "dma100": data.get("dma100"),
        "dma200": data.get("dma200"),
    }


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
    symbols = [_normalize_symbol(s) for s in symbols_raw.split(",") if s.strip()]
    if not symbols:
        return SaveStockListResponse(
            success=False,
            message="No valid symbols",
            saved_count=0
        )
    user_id = body.user_id or 4
    buy_price = float(body.buy_price or 0)
    saved = 0
    updated = 0
    for sym in symbols:
        existing = db.query(CarStockList).filter(
            CarStockList.symbol == sym,
            CarStockList.userid == user_id
        ).first()
        if not existing:
            row = CarStockList(symbol=sym, userid=user_id, buy_price=buy_price)
            db.add(row)
            saved += 1
        else:
            existing.buy_price = buy_price
            updated += 1
    db.commit()
    if body.number_of_weeks is not None and body.number_of_weeks > 0:
        set_number_of_weeks(body.number_of_weeks)
        settings.CAR_NUMBER_OF_WEEKS = body.number_of_weeks
    return SaveStockListResponse(
        success=True,
        message=f"Saved {saved} new symbol(s), updated {updated}",
        saved_count=saved
    )


@router.post("/add-stock", response_model=SaveStockListResponse)
async def add_single_stock(
    body: AddCarStockRequest,
    db: Session = Depends(get_db)
):
    symbol = _normalize_symbol(body.symbol)
    if not symbol:
        return SaveStockListResponse(success=False, message="Symbol is required", saved_count=0)

    existing = db.query(CarStockList).filter(
        CarStockList.symbol == symbol,
        CarStockList.userid == body.user_id
    ).first()

    if existing:
        existing.buy_price = float(body.buy_price)
        db.commit()
        return SaveStockListResponse(success=True, message="Stock updated successfully", saved_count=0)

    row = CarStockList(
        symbol=symbol,
        userid=body.user_id,
        buy_price=float(body.buy_price)
    )
    db.add(row)
    db.commit()
    return SaveStockListResponse(success=True, message="Stock added successfully", saved_count=1)


@router.post("/upload-stocks", response_model=SaveStockListResponse)
async def upload_stocks(
    body: BulkCarStockUploadRequest,
    db: Session = Depends(get_db)
):
    if not body.rows:
        return SaveStockListResponse(success=False, message="No rows provided", saved_count=0)

    if body.user_id is None or body.user_id < 1:
        raise HTTPException(status_code=400, detail="Valid user_id is required")

    saved = 0
    updated = 0
    try:
        for item in body.rows:
            symbol = _normalize_symbol(item.symbol)
            if not symbol:
                continue
            try:
                price_dec = Decimal(str(item.buy_price))
            except (InvalidOperation, ValueError, TypeError):
                raise HTTPException(
                    status_code=400,
                    detail=f"Invalid buy_price for symbol {symbol!r}",
                )
            if not price_dec.is_finite() or price_dec < 0:
                raise HTTPException(
                    status_code=400,
                    detail=f"Invalid buy_price for symbol {symbol!r}",
                )

            existing = db.query(CarStockList).filter(
                CarStockList.symbol == symbol,
                CarStockList.userid == body.user_id
            ).first()
            if existing:
                existing.buy_price = price_dec
                updated += 1
            else:
                db.add(
                    CarStockList(
                        symbol=symbol,
                        userid=body.user_id,
                        buy_price=price_dec,
                    )
                )
                saved += 1

        db.commit()
    except HTTPException:
        db.rollback()
        raise
    except IntegrityError as e:
        db.rollback()
        logger.exception("upload-stocks integrity error")
        raise HTTPException(
            status_code=409,
            detail="Database constraint conflict (duplicate symbol or invalid user). "
            "Ensure your CSV has unique symbols per user.",
        ) from e
    except SQLAlchemyError as e:
        db.rollback()
        logger.exception("upload-stocks database error")
        raise HTTPException(
            status_code=500,
            detail=f"Database error while saving CSV: {e!s}",
        ) from e
    except Exception as e:
        db.rollback()
        logger.exception("upload-stocks unexpected error")
        raise HTTPException(status_code=500, detail=str(e)) from e

    return SaveStockListResponse(
        success=True,
        message=f"Processed {saved + updated} rows (saved {saved}, updated {updated})",
        saved_count=saved,
    )


@router.get("/stock-list", response_model=List[CarStockItem])
async def get_stock_list(user_id: Optional[int] = None, db: Session = Depends(get_db)):
    """Get all symbols from carstocklist with stock names from instruments, ordered by created_at desc."""
    try:
        query = db.query(CarStockList)
        if user_id is not None:
            query = query.filter(CarStockList.userid == user_id)
        rows = query.order_by(CarStockList.created_at.desc()).all()
        symbols = [r.symbol for r in rows]
        names_map = get_stock_names_batch(symbols) if symbols else {}
        return [
            CarStockItem(
                id=r.id,
                symbol=r.symbol,
                userid=r.userid,
                buy_price=float(r.buy_price or 0),
                stock_name=names_map.get(r.symbol.upper(), r.symbol) if r.symbol else r.symbol,
                created_at=r.created_at.isoformat() if r.created_at else ""
            )
            for r in rows
        ]
    except Exception as e:
        import logging
        logging.getLogger(__name__).exception("stock-list error")
        return []


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


@router.get("/car-analysis-list")
async def get_car_analysis_list(user_id: int):
    """
    Return carstocklist rows for the given user_id only (no other users' data).
    Joins with car_nifty200 to show 52w high, ltp, last10 cumm avg, signal when available.
    For stocks not in car_nifty200 (blank data), runs CAR analysis on-the-fly (Yahoo then Upstox)
    so the CAR Analysis tab shows full data for all configured stocks.
    """
    if user_id <= 0:
        raise HTTPException(status_code=400, detail="Valid user_id is required")
    with engine.begin() as conn:
        rows = conn.execute(
            text(
                """
                SELECT c.symbol, c.userid,
                       n.date_52weekhigh, n.stock_ltp, n.last10daycummavg, n.signal, n.dma50, n.dma100, n.dma200
                FROM carstocklist c
                LEFT JOIN car_nifty200 n ON n.stock = c.symbol
                WHERE c.userid = :user_id
                ORDER BY c.created_at DESC
                """
            ),
            {"user_id": user_id},
        ).mappings().all()
    symbols = [(r["symbol"] or "").strip() for r in rows if (r.get("symbol") or "").strip()]
    names_map = get_stock_names_batch(symbols) if symbols else {}
    out = []
    for r in rows:
        symbol = (r.get("symbol") or "").strip()
        if not symbol:
            continue
        last10 = (r.get("last10daycummavg") or "").strip()
        last10_list = [x.strip() for x in last10.split(",") if x.strip()] if last10 else []
        d = r.get("date_52weekhigh")
        date_str = d.strftime("%Y-%m-%d") if d and hasattr(d, "strftime") else (str(d) if d else "")
        current_price = float(r["stock_ltp"]) if r.get("stock_ltp") is not None else None
        signal = (r.get("signal") or "").strip() or None
        dma50 = float(r["dma50"]) if r.get("dma50") is not None else None
        dma100 = float(r["dma100"]) if r.get("dma100") is not None else None
        dma200 = float(r["dma200"]) if r.get("dma200") is not None else None
        # If CAR data is missing (stock not in car_nifty200 or nulls), run analysis on-the-fly
        if (not date_str and not signal) or current_price is None:
            computed = _run_car_for_symbol_on_the_fly(symbol)
            if computed:
                date_str = date_str or computed.get("week_52_high_date") or ""
                current_price = current_price if current_price is not None else computed.get("current_price")
                last10_list = last10_list or computed.get("last_10_cumulative_avg") or []
                signal = signal or computed.get("signal")
                dma50 = dma50 if dma50 is not None else computed.get("dma50")
                dma100 = dma100 if dma100 is not None else computed.get("dma100")
                dma200 = dma200 if dma200 is not None else computed.get("dma200")
        out.append({
            "symbol": symbol,
            "stock_name": names_map.get(symbol.upper(), symbol),
            "week_52_high_date": date_str or None,
            "current_price": current_price,
            "last_10_cumulative_avg": last10_list,
            "signal": signal,
            "dma50": dma50,
            "dma100": dma100,
            "dma200": dma200,
        })
    return out


@router.get("/analyze", response_model=List[CarAnalysisResult])
async def run_car_analysis(user_id: int, db: Session = Depends(get_db)):
    """
    Run CAR analysis for all symbols in carstocklist (legacy; CAR Analysis tab now uses car-analysis-list).
    """
    if user_id <= 0:
        raise HTTPException(status_code=400, detail="Valid user_id is required")

    rows = db.query(CarStockList).filter(
        CarStockList.userid == user_id
    ).order_by(CarStockList.created_at.desc()).all()
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


@router.get("/nifty250-list", response_model=List[dict])
async def get_nifty250_list():
    """
    Return rows from car_nifty200 where signal is not null/blank.
    Sorted by signal in reverse order (e.g. BUY first). No CAR calculation - read-only from table.
    """
    with engine.begin() as conn:
        rows = conn.execute(
            text(
                """
                SELECT stock, stock_ltp, date_52weekhigh, last10daycummavg, signal, dma50, dma100, dma200
                FROM car_nifty200
                WHERE signal IS NOT NULL AND TRIM(signal) <> ''
                ORDER BY signal DESC
                """
            )
        ).mappings().all()
    out = []
    for r in rows:
        stock = (r.get("stock") or "").strip()
        if not stock:
            continue
        last10 = (r.get("last10daycummavg") or "").strip()
        last10_list = [x.strip() for x in last10.split(",") if x.strip()] if last10 else []
        d = r.get("date_52weekhigh")
        date_str = d.strftime("%Y-%m-%d") if d and hasattr(d, "strftime") else (str(d) if d else "")
        out.append({
            "symbol": stock,
            "stock_name": stock,
            "week_52_high_date": date_str,
            "current_price": float(r["stock_ltp"]) if r.get("stock_ltp") is not None else None,
            "last_10_cumulative_avg": last10_list,
            "signal": (r.get("signal") or "").strip(),
            "dma50": float(r["dma50"]) if r.get("dma50") is not None else None,
            "dma100": float(r["dma100"]) if r.get("dma100") is not None else None,
            "dma200": float(r["dma200"]) if r.get("dma200") is not None else None,
        })
    return out
