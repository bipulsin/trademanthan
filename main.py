"""
Root main.py - Entry point for TradeManthan API
This imports the actual app from backend/main.py which includes the lifespan function and schedulers
"""
import sys
import os

# Add project root to Python path to ensure backend imports work
project_root = os.path.dirname(os.path.abspath(__file__))
if project_root not in sys.path:
    sys.path.insert(0, project_root)

# Import the app from backend/main.py which has all the lifespan logic and schedulers
try:
    from backend.main import app
except Exception as e:
    print(f"ERROR: Failed to import app from backend.main: {e}")
    import traceback
    traceback.print_exc()
    sys.exit(1)

# Re-export app for uvicorn: uvicorn main:app
__all__ = ['app']
