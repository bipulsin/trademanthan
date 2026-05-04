from fastapi import FastAPI, Depends, HTTPException, status
from fastapi.middleware.cors import CORSMiddleware
from fastapi.security import OAuth2PasswordBearer
from sqlalchemy.orm import sessionmaker
from contextlib import asynccontextmanager
from datetime import datetime, timezone
import os
import logging
import threading

import backend.env_bootstrap  # noqa: F401 — load `<project_root>/.env` before other backend imports

from backend.database import engine, SessionLocal, create_tables
import backend.models as models
import backend.routers.auth as auth
import backend.routers.dashboard as dashboard
import backend.routers.strategy as strategy
import backend.routers.broker as broker
import backend.routers.products as products
import backend.routers.algo as algo
import backend.routers.scan as scan
import backend.routers.cargpt as cargpt
import backend.routers.arbitrage as arbitrage
import backend.routers.smart_futures_stub as smart_futures
import backend.routers.smart_futures_backtest_router as smart_futures_backtest
import backend.routers.nks_intraday as nks_intraday
import backend.routers.fno_bullish as fno_bullish
import backend.routers.daily_futures as daily_futures
import backend.routers.futures_reports as futures_reports
import backend.routers.iron_condor as iron_condor
# OLD SCHEDULERS - DISABLED - Migrated to smart_future_algo
# from backend.services.master_stock_scheduler import start_scheduler, stop_scheduler
# from backend.services.instruments_downloader import start_instruments_scheduler, stop_instruments_scheduler
# from backend.services.health_monitor import start_health_monitor, stop_health_monitor
# from backend.services.vwap_updater import start_vwap_updater, stop_vwap_updater
# from backend.services.index_price_scheduler import start_index_price_scheduler, stop_index_price_scheduler

# NEW UNIFIED SCHEDULER - Smart Future Algo
from backend.services.smart_future_algo import start_smart_future_algo, stop_smart_future_algo
from backend.services.arbitrage_daily_setup_scheduler import (
    start_arbitrage_daily_setup_scheduler,
    stop_arbitrage_daily_setup_scheduler,
)
from backend.services.chartink_df_webhook_inbox_scheduler import (
    start_chartink_df_webhook_inbox_scheduler,
    stop_chartink_df_webhook_inbox_scheduler,
)
from backend.services.iron_condor_snapshot_scheduler import (
    start_iron_condor_snapshot_scheduler,
    stop_iron_condor_snapshot_scheduler,
)
# Configure logging with file handler - MUST be done before any loggers are created
log_dir = os.path.join(os.path.dirname(os.path.dirname(__file__)), 'logs')
os.makedirs(log_dir, exist_ok=True)

log_file = os.path.join(log_dir, 'trademanthan.log')

# Remove any existing handlers
root_logger = logging.getLogger()
for handler in root_logger.handlers[:]:
    root_logger.removeHandler(handler)

# Configure root logger
# CRITICAL: Write logs ONLY to the log file (trademanthan.log), not stdout/stderr
# This ensures all logs go to a single file regardless of how backend is started

# CRITICAL: Create a custom handler class that flushes after each emit
class FlushingFileHandler(logging.FileHandler):
    """FileHandler that flushes after each log entry to ensure immediate writes"""
    def emit(self, record):
        super().emit(record)
        self.flush()
        # Also force OS-level flush to ensure data is written to disk
        if hasattr(self.stream, 'fileno'):
            try:
                import os
                os.fsync(self.stream.fileno())
            except (OSError, AttributeError):
                pass  # Ignore if fsync fails

# Use FlushingFileHandler to ensure immediate log writes
file_handler = FlushingFileHandler(log_file, mode='a', encoding='utf-8')
file_handler.setLevel(logging.INFO)
file_handler.setFormatter(logging.Formatter('%(asctime)s - %(levelname)s - %(name)s - %(message)s', datefmt='%Y-%m-%d %H:%M:%S'))

logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(name)s - %(message)s',
    datefmt='%Y-%m-%d %H:%M:%S',
    handlers=[file_handler],
    force=True  # Override existing configuration
)

# Note: All child loggers will inherit from root logger and use the same handler

logger = logging.getLogger(__name__)
logger.info("🚀 TradeManthan backend starting...")

@asynccontextmanager
async def lifespan(app: FastAPI):
    """
    Lifespan context manager for startup and shutdown events
    This ensures schedulers start once and stay running
    """
    import asyncio
    import sys
    import traceback
    
    # Track if startup completed successfully
    startup_completed = False
    
    try:
        # STARTUP
        logger.info("=" * 60)
        logger.info("🚀 TRADE MANTHAN API STARTUP")
        logger.info("=" * 60)
        
        # OLD SCHEDULERS - DISABLED - All migrated to smart_future_algo
        # These are commented out to prevent them from starting
        # logger.info("⚠️ Old schedulers are disabled - using smart_future_algo instead")

        # Start unified Smart Future Algo Scheduler (consolidates all schedulers except Master Stock)
        try:
            logger.info("Starting Smart Future Algo Scheduler Controller...")
            try:
                start_smart_future_algo()
                logger.info("✅ Smart Future Algo Scheduler: STARTED")
                logger.info("   - Consolidates: Instruments, Health Monitor, VWAP Updater, Index Price, Entry slip monitor")
                logger.info("   - Master Stock download from Dhan removed")
                logger.info("   - All logs go to: logs/smart_future_algo.log")
            except ImportError as import_err:
                logger.error(f"❌ Smart Future Algo Scheduler: IMPORT ERROR - {import_err}", exc_info=True)
                logger.warning("⚠️ Continuing without scheduler - some scheduled jobs may not run")
            except Exception as scheduler_err:
                logger.error(f"❌ Smart Future Algo Scheduler: FAILED - {scheduler_err}", exc_info=True)
                logger.warning("⚠️ Continuing without scheduler - some scheduled jobs may not run")
        except Exception as e:
            logger.error(f"❌ Smart Future Algo Scheduler: CRITICAL ERROR - {e}", exc_info=True)
            logger.warning("⚠️ Backend will continue running but scheduled jobs may not work")

        # Start Arbitrage Daily Setup (09:10 primary, 09:20 backstop, weekdays / non-holiday)
        try:
            logger.info("Starting Arbitrage Daily Setup Scheduler...")
            start_arbitrage_daily_setup_scheduler()
            logger.info("✅ Arbitrage Daily Setup Scheduler: STARTED (09:10/09:20 Asia/Kolkata, Mon–Fri, non-holiday)")
        except Exception as e:
            logger.error(f"❌ Arbitrage Daily Setup Scheduler: FAILED - {e}", exc_info=True)
            logger.warning("⚠️ Continuing without arbitrage scheduler")

        try:
            logger.info("Starting ChartInk Daily Futures webhook inbox cleanup scheduler...")
            start_chartink_df_webhook_inbox_scheduler()
            logger.info(
                "✅ ChartInk DF inbox cleanup: STARTED (08:45 Asia/Kolkata daily; purge per CHARTINK_DF_INBOX_REFRESH_DAYS, default 5)"
            )
        except Exception as e:
            logger.error(f"❌ ChartInk DF inbox cleanup scheduler: FAILED - {e}", exc_info=True)
            logger.warning("⚠️ Continuing without ChartInk DF inbox cleanup scheduler")

        try:
            logger.info("Starting Iron Condor daily snapshot scheduler (pre-market VIX + ATR cache)...")
            start_iron_condor_snapshot_scheduler()
            logger.info("✅ Iron Condor snapshot scheduler: STARTED (08:33 IST weekdays)")
        except Exception as e:
            logger.error(f"❌ Iron Condor snapshot scheduler: FAILED - {e}", exc_info=True)
            logger.warning("⚠️ Continuing without Iron Condor snapshot scheduler")

        # Iron Condor: run DDL + instrument-key warm once per worker before traffic (avoids ~minute first picker load)
        try:
            from backend.services import iron_condor_service as _ic_warm

            logger.info("Warming Iron Condor tables + universe equity keys (runs once per worker)...")
            await asyncio.to_thread(_ic_warm.warm_iron_condor_startup)
            logger.info("✅ Iron Condor startup warm finished")
        except Exception as e:
            logger.warning("⚠️ Iron Condor startup warm skipped: %s", e)

        logger.info("=" * 60)
        logger.info("✅ STARTUP COMPLETE - All Services Active")
        logger.info("=" * 60)
        logger.info("✅ All services initialized and running")
        
        # Mark startup as completed
        startup_completed = True
        
        logger.info("✅ Lifespan startup completed successfully, entering yield phase...")
        
        try:
            yield  # Application runs here
            logger.info("✅ Lifespan yield completed normally")
        except GeneratorExit:
            logger.info("🛑 Lifespan generator exit - application shutting down")
            raise
        except Exception as e:
            # If there's an error during startup (like port binding), log it but don't run shutdown
            logger.error(f"❌ Error during application runtime: {e}")
            logger.error(f"❌ Traceback: {traceback.format_exc()}")
            if "address already in use" in str(e).lower() or "errno 98" in str(e).lower():
                logger.warning("Port 8000 is already in use. Another instance may be running. Skipping shutdown.")
                return
            raise
    except Exception as e:
        logger.error(f"❌ CRITICAL ERROR in lifespan startup: {e}")
        logger.error(f"❌ Traceback: {traceback.format_exc()}")
        startup_completed = False
        raise
    
    # SHUTDOWN - Only run if startup completed successfully
    if not startup_completed:
        logger.warning("Startup did not complete successfully. Skipping shutdown to avoid stopping other instance's schedulers.")
        return
        
    logger.info("🛑 Shutting down Trade Manthan API...")
    logger.info("🛑 Shutting down all services...")
    
    # Stop Smart Future Algo Scheduler (replaces all old schedulers)
    try:
        stop_smart_future_algo()
        logger.info("✅ Smart Future Algo Scheduler stopped")
    except Exception as e:
        logger.error(f"⚠️ Error stopping Smart Future Algo Scheduler: {e}", exc_info=True)

    try:
        stop_arbitrage_daily_setup_scheduler()
        logger.info("✅ Arbitrage Daily Setup Scheduler stopped")
    except Exception as e:
        logger.error(f"⚠️ Error stopping Arbitrage Daily Setup Scheduler: {e}", exc_info=True)

    try:
        stop_chartink_df_webhook_inbox_scheduler()
        logger.info("✅ ChartInk DF inbox cleanup scheduler stopped")
    except Exception as e:
        logger.error(f"⚠️ Error stopping ChartInk DF inbox cleanup scheduler: {e}", exc_info=True)

    try:
        stop_iron_condor_snapshot_scheduler()
        logger.info("✅ Iron Condor snapshot scheduler stopped")
    except Exception as e:
        logger.error(f"⚠️ Error stopping Iron Condor snapshot scheduler: {e}", exc_info=True)

    logger.info("✅ Shutdown complete")

app = FastAPI(
    title="Trade Manthan API",
    description="Professional Algo Trading Platform API",
    version="1.0.0",
    lifespan=lifespan
)

# Note: log_file_obj is managed by the FileHandler and remains open during app lifetime

# CORS middleware
app.add_middleware(
    CORSMiddleware,
    allow_origins=[
        "https://www.tradewithcto.com",
        "https://tradewithcto.com",
        "https://trademanthan.in",
        "https://www.trademanthan.in",
        "https://tradentical.com",
        "https://www.tradentical.com",
        "http://localhost:3000",
        "http://localhost:8000",
        "https://65.2.29.219",
    ],
    allow_credentials=True,
    allow_methods=["*"],
    allow_headers=["*"],
)

# Include routers (auth mounted twice so both /auth/... and /api/auth/... work behind any nginx proxy)
app.include_router(auth.router)
app.include_router(auth.router, prefix="/api")
app.include_router(dashboard.router)
app.include_router(strategy.router)
app.include_router(broker.router)
app.include_router(products.router)
app.include_router(algo.router)
app.include_router(scan.router)
app.include_router(cargpt.router)
app.include_router(arbitrage.router)
app.include_router(smart_futures.router, prefix="/api/smart-futures")
app.include_router(smart_futures.router, prefix="/smart-futures")
app.include_router(smart_futures_backtest.router, prefix="/api/smart-futures-backtest")
app.include_router(smart_futures_backtest.router, prefix="/smart-futures-backtest")
app.include_router(nks_intraday.router, prefix="/api/nks-intraday")
app.include_router(nks_intraday.router, prefix="/nks-intraday")
app.include_router(fno_bullish.router, prefix="/api/fno-bullish")
app.include_router(fno_bullish.router, prefix="/fno-bullish")
app.include_router(daily_futures.router, prefix="/api")
app.include_router(daily_futures.router, prefix="")
app.include_router(daily_futures.bearish_router, prefix="/api")
app.include_router(daily_futures.bearish_router, prefix="")
app.include_router(futures_reports.router, prefix="/api")
app.include_router(iron_condor.router, prefix="/api")
app.include_router(iron_condor.router, prefix="")

# Create/migrate tables in a daemon thread so import + uvicorn bind is not blocked by long DB locks
# (idle-in-transaction + migrations used to delay port 8000 for minutes → nginx 502).
def _create_tables_background() -> None:
    try:
        create_tables()
        logger.info("✅ Database tables created successfully (background)")
    except Exception as e:
        logger.warning(f"⚠️ Could not create database tables: {e}")
        logger.warning("Database will be initialized when first accessed")


threading.Thread(target=_create_tables_background, daemon=True, name="create_tables").start()


def get_database_info():
    """Get database connection information"""
    database_url = os.getenv("DATABASE_URL", "postgresql://trademanthan:trademanthan123@localhost/trademanthan")
    if "postgresql" in database_url:
        return "PostgreSQL connected", "production"
    else:
        return "Database connected", "development"

@app.get("/")
async def root():
    db_status, environment = get_database_info()
    return {
        "message": "Trade Manthan API is running!",
        "version": "1.0.0",
        "status": "active",
        "database": db_status,
        "environment": environment,
        "features": [
            "Algo Trading",
            "Portfolio Management", 
            "Real-time Data",
            "Google OAuth",
            "Broker Management",
            "Strategy Management",
            "Advanced Indicators",
            "Performance Analytics"
        ]
    }

def _health_payload() -> dict:
    """Shared JSON for /health and /api/health (nginx often proxies only /api/*)."""
    db_status, environment = get_database_info()
    return {
        "status": "healthy",
        "service": "Trade Manthan API",
        "database": db_status,
        "environment": environment,
        "timestamp": datetime.now(timezone.utc).isoformat().replace("+00:00", "Z"),
    }


@app.get("/health")
async def health_check():
    return _health_payload()


@app.get("/api/health")
async def health_check_api():
    return _health_payload()

@app.get("/api/status")
async def api_status():
    db_status, environment = get_database_info()
    return {
        "api": "Trade Manthan",
        "version": "1.0.0",
        "status": "operational",
        "environment": environment,
        "database": db_status.split()[0],  # Just the database type
        "features": [
            "Algo Trading",
            "Portfolio Management",
            "Real-time Data",
            "Google OAuth",
            "Broker Management",
            "Strategy Management",
            "Advanced Indicators",
            "Performance Analytics"
        ]
    }

if __name__ == "__main__":
    import uvicorn
    uvicorn.run(app, host="0.0.0.0", port=8000)
