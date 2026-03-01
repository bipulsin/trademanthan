from fastapi import APIRouter, HTTPException

from backend.services.arbitrage_daily_setup_scheduler import (
    arbitrage_daily_setup_scheduler,
    run_arbitrage_daily_setup_now,
)

router = APIRouter(prefix="/arbitrage", tags=["arbitrage"])


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

