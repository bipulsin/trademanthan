from fastapi import FastAPI, Depends, HTTPException, status
from fastapi.middleware.cors import CORSMiddleware
from fastapi.security import OAuth2PasswordBearer
from sqlalchemy.orm import sessionmaker
import os
import logging
from dotenv import load_dotenv

from database import engine, SessionLocal, create_tables
import models
import routers.auth as auth
import routers.dashboard as dashboard
import routers.strategy as strategy
import routers.broker as broker
import routers.products as products
import routers.algo as algo
import routers.scan as scan
from services.master_stock_scheduler import start_scheduler, stop_scheduler
from services.instruments_downloader import start_instruments_scheduler, stop_instruments_scheduler

load_dotenv()

# Configure logging
logger = logging.getLogger(__name__)

app = FastAPI(
    title="Trade Manthan API",
    description="Professional Algo Trading Platform API",
    version="1.0.0"
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

# Startup and shutdown events
@app.on_event("startup")
async def startup_event():
    """Initialize services on application startup"""
    import sys
    
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
    
    print("=" * 60, flush=True)
    print("✅ STARTUP COMPLETE - All Schedulers Active", flush=True)
    print("=" * 60, flush=True)
    sys.stdout.flush()

@app.on_event("shutdown")
async def shutdown_event():
    """Cleanup on application shutdown"""
    logger.info("🛑 Shutting down Trade Manthan API...")
    
    # Stop master stock scheduler
    try:
        stop_scheduler()
        logger.info("✅ Master stock scheduler stopped")
    except Exception as e:
        logger.warning(f"⚠️ Error stopping master stock scheduler: {e}")
    
    # Stop instruments scheduler
    try:
        stop_instruments_scheduler()
        logger.info("✅ Instruments scheduler stopped")
    except Exception as e:
        logger.warning(f"⚠️ Error stopping instruments scheduler: {e}")
    
    logger.info("✅ Shutdown complete")

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
