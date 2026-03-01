from fastapi import APIRouter, HTTPException
from sqlalchemy import text

from backend.database import engine

from backend.services.arbitrage_daily_setup_scheduler import (
    arbitrage_daily_setup_scheduler,
    run_arbitrage_daily_setup_now,
)

router = APIRouter(prefix="/scan/arbitrage", tags=["arbitrage"])


@router.post("/daily-setup/run")
async def run_daily_setup_now():
    """
    On-demand run of arbitrage_dailySetup job.
    """
    try:
        result = run_arbitrage_daily_setup_now()
        return result
    except Exception as exc:
        raise HTTPException(status_code=500, detail=f"Failed to run arbitrage_dailySetup: {exc}")


@router.get("/daily-setup/status")
async def get_daily_setup_status():
    """
    Get scheduler status for arbitrage_dailySetup job.
    """
    try:
        status = arbitrage_daily_setup_scheduler.get_status()
        status["job_name"] = "arbitrage_dailySetup"
        status["schedule"] = "09:16 Asia/Kolkata (daily)"
        return status
    except Exception as exc:
        raise HTTPException(status_code=500, detail=f"Failed to get scheduler status: {exc}")


@router.get("/selection")
async def get_arbitrage_selection():
    """
    Fetch arbitrage selection rows with filter:
    - currmth_future_ltp < stock_ltp
    - nextmth_future_ltp <= currmth_future_ltp
    - nextmth_future_ltp within 3 points below currmth_future_ltp
    """
    try:
        with engine.begin() as conn:
            rows = conn.execute(
                text(
                    """
                    SELECT
                        stock,
                        stock_ltp,
                        currmth_future_symbol,
                        currmth_future_ltp,
                        nextmth_future_symbol,
                        nextmth_future_ltp
                    FROM arbitrage_master
                    WHERE
                        stock_ltp IS NOT NULL
                        AND currmth_future_ltp IS NOT NULL
                        AND nextmth_future_ltp IS NOT NULL
                        AND currmth_future_ltp < stock_ltp
                        AND nextmth_future_ltp <= currmth_future_ltp
                        AND nextmth_future_ltp >= (currmth_future_ltp - 3)
                    ORDER BY stock ASC
                    """
                )
            ).mappings().all()

        return {
            "success": True,
            "count": len(rows),
            "rows": [dict(row) for row in rows],
        }
    except Exception as exc:
        raise HTTPException(status_code=500, detail=f"Failed to fetch arbitrage selection: {exc}")

