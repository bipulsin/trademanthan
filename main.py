"""
Root main.py - Entry point for TradeManthan API
This imports the actual app from backend/main.py which includes the lifespan function and schedulers
"""
# Import the app from backend/main.py which has all the lifespan logic and schedulers
from backend.main import app

# Re-export app for uvicorn: uvicorn main:app
__all__ = ['app']
