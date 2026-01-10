#!/bin/bash
# Diagnostic script to check why backend isn't starting
# Run this on EC2 server: bash backend/scripts/check_backend_startup.sh

echo "=== Backend Startup Diagnostics ==="
echo ""

# Check if port 8000 is in use
echo "1. Checking if port 8000 is in use:"
lsof -i :8000 2>/dev/null || echo "   Port 8000 is free"
echo ""

# Check if uvicorn process is running
echo "2. Checking for uvicorn processes:"
ps aux | grep -E "uvicorn.*main:app" | grep -v grep || echo "   No uvicorn processes found"
echo ""

# Check screen session
echo "3. Checking screen sessions:"
screen -ls 2>/dev/null || echo "   No screen sessions found"
echo ""

# Check recent logs
echo "4. Last 50 lines of trademanthan.log:"
if [ -f "/home/ubuntu/trademanthan/logs/trademanthan.log" ]; then
    tail -50 /home/ubuntu/trademanthan/logs/trademanthan.log
else
    echo "   Log file not found"
fi
echo ""

# Try to import the main module
echo "5. Testing Python imports:"
cd /home/ubuntu/trademanthan
source backend/venv/bin/activate
python3 -c "
import sys
sys.path.insert(0, '.')
try:
    from backend.main import app
    print('   ✅ Backend main module imported successfully')
except ImportError as e:
    print(f'   ❌ Import error: {e}')
    import traceback
    traceback.print_exc()
except Exception as e:
    print(f'   ❌ Error: {e}')
    import traceback
    traceback.print_exc()
" 2>&1 | head -30
echo ""

# Check if scan_st1_algo can be imported
echo "6. Testing scan_st1_algo import:"
python3 -c "
import sys
sys.path.insert(0, '.')
try:
    from backend.services.scan_st1_algo import scan_st1_algo_scheduler, start_scan_st1_algo
    print('   ✅ scan_st1_algo imported successfully')
    print(f'   Scheduler initialized: {scan_st1_algo_scheduler is not None}')
except ImportError as e:
    print(f'   ❌ Import error: {e}')
    import traceback
    traceback.print_exc()
except Exception as e:
    print(f'   ❌ Error: {e}')
    import traceback
    traceback.print_exc()
" 2>&1 | head -30
echo ""

echo "=== Diagnostics Complete ==="