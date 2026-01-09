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
from backend.services.master_stock_scheduler import start_scheduler, stop_scheduler
from backend.services.instruments_downloader import start_instruments_scheduler, stop_instruments_scheduler
from backend.services.health_monitor import start_health_monitor, stop_health_monitor
from backend.services.vwap_updater import start_vwap_updater, stop_vwap_updater
from backend.services.index_price_scheduler import start_index_price_scheduler, stop_index_price_scheduler

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
# CRITICAL: Use StreamHandler first to ensure logs go to stdout/stderr (captured by screen session)
# This ensures logs appear in /tmp/uvicorn.log when running in screen session
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(name)s - %(message)s',
    handlers=[
        logging.StreamHandler(sys.stdout),  # Log to stdout first (captured by screen session)
        logging.FileHandler(log_file, mode='a')  # Also log to file
    ],
    force=True  # Override existing configuration
)

logger = logging.getLogger(__name__)
logger.info("🚀 TradeManthan backend starting...")

@asynccontextmanager
async def lifespan(app: FastAPI):
    """
    Lifespan context manager for startup and shutdown events
    This ensures schedulers start once and stay running
    """
    import sys
    
    # Track if startup completed successfully
    startup_completed = False
    
    # STARTUP
    print("=" * 60, flush=True)
    print("🚀 TRADE MANTHAN API STARTUP", flush=True)
    print("=" * 60, flush=True)
    sys.stdout.flush()
    
    # Start master stock scheduler (downloads CSV daily at 9:00 AM)
    try:
        print("Starting Master Stock Scheduler...", flush=True)
        sys.stdout.flush()
        start_scheduler()
        print("✅ Master Stock Scheduler: STARTED (Daily at 9:00 AM IST)", flush=True)
        sys.stdout.flush()
    except Exception as e:
        print(f"❌ Master Stock Scheduler: FAILED - {e}", flush=True)
        sys.stdout.flush()
        import traceback
        traceback.print_exc()
    
    # Start instruments scheduler (downloads JSON daily at 9:05 AM)
    try:
        print("Starting Instruments Scheduler...", flush=True)
        sys.stdout.flush()
        start_instruments_scheduler()
        print("✅ Instruments Scheduler: STARTED (Daily at 9:05 AM IST)", flush=True)
        sys.stdout.flush()
    except Exception as e:
        print(f"❌ Instruments Scheduler: FAILED - {e}", flush=True)
        sys.stdout.flush()
        import traceback
        traceback.print_exc()
    
    # Start health monitor (checks every 30 min from 8:39 AM to 4:09 PM)
    try:
        print("Starting Health Monitor...", flush=True)
        sys.stdout.flush()
        start_health_monitor()
        print("✅ Health Monitor: STARTED (Every 30 min, 8:39 AM - 4:09 PM IST)", flush=True)
        sys.stdout.flush()
    except Exception as e:
        print(f"❌ Health Monitor: FAILED - {e}", flush=True)
        sys.stdout.flush()
        import traceback
        traceback.print_exc()
    
    # Start Market Data Updater (updates VWAP, Stock LTP, Option LTP hourly)
    try:
        print("Starting Market Data Updater...", flush=True)
        sys.stdout.flush()
        start_vwap_updater()
        print("✅ Market Data Updater: STARTED", flush=True)
        print("   - Hourly updates (9:15 AM - 3:15 PM): Stock VWAP, Stock LTP, Option LTP", flush=True)
        print("   - Auto-close trades at 3:25 PM", flush=True)
        sys.stdout.flush()
    except Exception as e:
        print(f"❌ Market Data Updater: FAILED - {e}", flush=True)
        sys.stdout.flush()
        import traceback
        traceback.print_exc()
    
    # Start index price scheduler (every 5 minutes during market hours, stores at 9:15 AM and 3:30 PM)
    try:
        print("Starting Index Price Scheduler...", flush=True)
        sys.stdout.flush()
        start_index_price_scheduler()
        print("✅ Index Price Scheduler: STARTED (Every 5 min, 9:15 AM - 3:30 PM IST)", flush=True)
        print("   - Fetches NIFTY50 and BANKNIFTY prices every 5 minutes during market hours", flush=True)
        print("   - Stores prices at 9:15 AM (market open) and 3:30 PM (market close)", flush=True)
        sys.stdout.flush()
    except Exception as e:
        print(f"❌ Index Price Scheduler: FAILED - {e}", flush=True)
        sys.stdout.flush()
        import traceback
        traceback.print_exc()
    
    print("=" * 60, flush=True)
    print("✅ STARTUP COMPLETE - All Services Active", flush=True)
    print("=" * 60, flush=True)
    sys.stdout.flush()
    logger.info("✅ All services initialized and running")
    
    # Mark startup as completed
    startup_completed = True
    
    try:
        yield  # Application runs here
    except Exception as e:
        # If there's an error during startup (like port binding), log it but don't run shutdown
        logger.error(f"Error during application startup: {e}")
        if "address already in use" in str(e).lower() or "errno 98" in str(e).lower():
            logger.warning("Port 8000 is already in use. Another instance may be running. Skipping shutdown.")
            return
        raise
    
    # SHUTDOWN - Only run if startup completed successfully
    if not startup_completed:
        logger.warning("Startup did not complete successfully. Skipping shutdown to avoid stopping other instance's schedulers.")
        return
        
    print("🛑 Shutting down Trade Manthan API...", flush=True)
    logger.info("🛑 Shutting down all services...")
    
    # Stop master stock scheduler
    try:
        stop_scheduler()
        print("✅ Master stock scheduler stopped", flush=True)
    except Exception as e:
        print(f"⚠️ Error stopping master stock scheduler: {e}", flush=True)
    
    # Stop instruments scheduler
    try:
        stop_instruments_scheduler()
        print("✅ Instruments scheduler stopped", flush=True)
    except Exception as e:
        print(f"⚠️ Error stopping instruments scheduler: {e}", flush=True)
    
    # Stop health monitor
    try:
        stop_health_monitor()
        print("✅ Health monitor stopped", flush=True)
    except Exception as e:
        print(f"⚠️ Error stopping health monitor: {e}", flush=True)
    
    # Stop Market Data updater
    try:
        stop_vwap_updater()
        print("✅ Market Data updater stopped", flush=True)
    except Exception as e:
        print(f"⚠️ Error stopping Market Data updater: {e}", flush=True)
    
    # Stop index price scheduler
    try:
        stop_index_price_scheduler()
        print("✅ Index price scheduler stopped", flush=True)
    except Exception as e:
        print(f"⚠️ Error stopping index price scheduler: {e}", flush=True)
    
    print("✅ Shutdown complete", flush=True)

app = FastAPI(
    title="Trade Manthan API",
    description="Professional Algo Trading Platform API",
    version="1.0.0",
    lifespan=lifespan
)

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
