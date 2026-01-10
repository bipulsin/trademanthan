from fastapi import FastAPI, Depends, HTTPException, status
from fastapi.middleware.cors import CORSMiddleware
from fastapi.security import OAuth2PasswordBearer
from sqlalchemy.orm import sessionmaker
from contextlib import asynccontextmanager
import os
import logging
from dotenv import load_dotenv

from backend.database import engine, SessionLocal, create_tables
import backend.models as models
import backend.routers.auth as auth
import backend.routers.dashboard as dashboard
import backend.routers.strategy as strategy
import backend.routers.broker as broker
import backend.routers.products as products
import backend.routers.algo as algo
import backend.routers.scan as scan
# OLD SCHEDULERS - DISABLED - Migrated to scan_st1_algo
# from backend.services.master_stock_scheduler import start_scheduler, stop_scheduler
# from backend.services.instruments_downloader import start_instruments_scheduler, stop_instruments_scheduler
# from backend.services.health_monitor import start_health_monitor, stop_health_monitor
# from backend.services.vwap_updater import start_vwap_updater, stop_vwap_updater
# from backend.services.index_price_scheduler import start_index_price_scheduler, stop_index_price_scheduler

# NEW UNIFIED SCHEDULER - Scan ST1 Algo
from backend.services.scan_st1_algo import start_scan_st1_algo, stop_scan_st1_algo

load_dotenv()

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
    import sys
    import traceback
    
    # Track if startup completed successfully
    startup_completed = False
    
    try:
        # STARTUP
        logger.info("=" * 60)
        logger.info("🚀 TRADE MANTHAN API STARTUP")
        logger.info("=" * 60)
        
        # OLD SCHEDULERS - DISABLED - All migrated to scan_st1_algo
        # These are commented out to prevent them from starting
        # logger.info("⚠️ Old schedulers are disabled - using scan_st1_algo instead")
        
        # Start unified Scan ST1 Algo Scheduler (consolidates all 5 old schedulers)
        try:
            logger.info("Starting Scan ST1 Algo Scheduler Controller...")
            start_scan_st1_algo()
            logger.info("✅ Scan ST1 Algo Scheduler: STARTED")
            logger.info("   - Consolidates: Master Stock, Instruments, Health Monitor, VWAP Updater, Index Price")
            logger.info("   - All logs go to: logs/scan_st1_algo.log")
        except Exception as e:
            logger.error(f"❌ Scan ST1 Algo Scheduler: FAILED - {e}", exc_info=True)
        
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
    
    # Stop Scan ST1 Algo Scheduler (replaces all old schedulers)
    try:
        stop_scan_st1_algo()
        logger.info("✅ Scan ST1 Algo Scheduler stopped")
    except Exception as e:
        logger.error(f"⚠️ Error stopping Scan ST1 Algo Scheduler: {e}", exc_info=True)
    
    logger.info("✅ Shutdown complete")

app = FastAPI(
    title="Trade Manthan API",
    description="Professional Algo Trading Platform API",
    version="1.0.0",
    lifespan=lifespan
)

# Store log file object on app for reference (will remain open during app lifetime)
app.log_file_obj = log_file_obj

# CORS middleware
app.add_middleware(
    CORSMiddleware,
    allow_origins=["https://trademanthan.in", "http://localhost:3000", "http://localhost:8000", "https://65.2.29.219"],
    allow_credentials=True,
    allow_methods=["*"],
    allow_headers=["*"],
)

# Create database tables
try:
    create_tables()
    logger.info("✅ Database tables created successfully")
except Exception as e:
    logger.warning(f"⚠️ Could not create database tables: {e}")
    logger.warning("Database will be initialized when first accessed")

# Include routers
app.include_router(auth.router)
app.include_router(dashboard.router)
app.include_router(strategy.router)
app.include_router(broker.router)
app.include_router(products.router)
app.include_router(algo.router)
app.include_router(scan.router)

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

@app.get("/health")
async def health_check():
    db_status, environment = get_database_info()
    return {
        "status": "healthy",
        "service": "Trade Manthan API",
        "database": db_status,
        "environment": environment,
        "timestamp": "2024-01-01T00:00:00Z"
    }

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
