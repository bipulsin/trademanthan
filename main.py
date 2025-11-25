from fastapi import FastAPI, Depends, HTTPException, status
from fastapi.middleware.cors import CORSMiddleware
from fastapi.security import OAuth2PasswordBearer
from fastapi.staticfiles import StaticFiles
from sqlalchemy.orm import sessionmaker
import os
from dotenv import load_dotenv

from backend.database import engine, SessionLocal, create_tables
import backend.models as models
import backend.routers.auth as auth
import backend.routers.dashboard as dashboard
import backend.routers.strategy as strategy
import backend.routers.broker as broker
import backend.routers.products as products
import backend.routers.scan as scan

load_dotenv()

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
create_tables()

# Include routers
app.include_router(auth.router)
app.include_router(dashboard.router)
app.include_router(strategy.router)
app.include_router(broker.router)
app.include_router(products.router)
app.include_router(scan.router)

# Serve static files (frontend)
app.mount("/", StaticFiles(directory="frontend/public", html=True), name="static")

def get_database_info():
    """Get database connection information"""
    database_url = os.getenv("DATABASE_URL", "postgresql://trademanthan:trademanthan123@localhost/trademanthan")
    
    if "postgresql" in database_url:
        return "PostgreSQL connected", "production"
    else:
        return "Database connected", "production"

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
