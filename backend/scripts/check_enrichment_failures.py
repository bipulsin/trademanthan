#!/usr/bin/env python3
"""
Script to check today's enrichment failures and show full error messages
This helps diagnose why trades have "No Entry - Enrichment failed" status
"""

import sys
import os

# Add parent directory to path
script_dir = os.path.dirname(os.path.abspath(__file__))
parent_dir = os.path.dirname(os.path.dirname(script_dir))
sys.path.insert(0, parent_dir)

from backend.database import SessionLocal
from backend.models.trading import IntradayStockOption
from datetime import datetime, timedelta
import pytz
from collections import defaultdict

def main():
    db = SessionLocal()
    ist = pytz.timezone('Asia/Kolkata')
    now = datetime.now(ist)
    today = datetime.now(ist).replace(hour=0, minute=0, second=0, microsecond=0)
    
    # Find all trades with enrichment failures for today
    enrichment_failures = db.query(IntradayStockOption).filter(
        IntradayStockOption.trade_date >= today,
        IntradayStockOption.trade_date < today + timedelta(days=1),
        IntradayStockOption.no_entry_reason.like('Enrichment failed%')
    ).order_by(IntradayStockOption.alert_time.desc()).all()
    
    print('=' * 100)
    print(f'ENRICHMENT FAILURES CHECK - {today.strftime("%Y-%m-%d")}')
    print('=' * 100)
    print()
    
    if not enrichment_failures:
        print('✅ No enrichment failures found for today!')
        print()
        db.close()
        return
    
    print(f'Found {len(enrichment_failures)} trades with enrichment failures')
    print()
    
    # Group errors by error type
    error_groups = defaultdict(list)
    
    for trade in enrichment_failures:
        # Extract error message from no_entry_reason
        error_msg = trade.no_entry_reason
        if error_msg and error_msg.startswith('Enrichment failed:'):
            error_msg = error_msg.replace('Enrichment failed:', '').strip()
        else:
            error_msg = error_msg or 'Unknown error'
        
        # Categorize error
        error_category = categorize_error(error_msg)
        error_groups[error_category].append({
            'trade': trade,
            'error': error_msg
        })
    
    # Print summary by error category
    print('=' * 100)
    print('ERROR SUMMARY BY CATEGORY')
    print('=' * 100)
    print()
    
    for category, failures in sorted(error_groups.items(), key=lambda x: len(x[1]), reverse=True):
        print(f'{category}: {len(failures)} failure(s)')
    print()
    
    # Print detailed information for each failure
    print('=' * 100)
    print('DETAILED ERROR INFORMATION')
    print('=' * 100)
    print()
    
    for i, trade in enumerate(enrichment_failures, 1):
        print(f'[{i}/{len(enrichment_failures)}] {trade.stock_name}')
        print('-' * 100)
        print(f'  Alert Time:     {trade.alert_time.strftime("%Y-%m-%d %H:%M:%S IST") if trade.alert_time else "N/A"}')
        print(f'  Alert Type:     {trade.alert_type or "N/A"}')
        print(f'  Status:         {trade.status}')
        print(f'  Option Type:    {trade.option_type or "N/A"}')
        print(f'  Option Contract: {trade.option_contract or "N/A"}')
        print(f'  Instrument Key: {trade.instrument_key or "N/A"}')
        print(f'  Qty:            {trade.qty or 0}')
        print(f'  Buy Price:      ₹{trade.buy_price:.2f}' if trade.buy_price else '  Buy Price:      N/A')
        print(f'  Stock LTP:      ₹{trade.stock_ltp:.2f}' if trade.stock_ltp else '  Stock LTP:      N/A')
        print(f'  Stock VWAP:     ₹{trade.stock_vwap:.2f}' if trade.stock_vwap else '  Stock VWAP:     N/A')
        
        # Extract and show full error message
        error_msg = trade.no_entry_reason
        if error_msg and error_msg.startswith('Enrichment failed:'):
            error_msg = error_msg.replace('Enrichment failed:', '').strip()
        else:
            error_msg = error_msg or 'Unknown error'
        
        print(f'  Error Message:  {error_msg}')
        
        # Show error category
        error_category = categorize_error(error_msg)
        print(f'  Error Category: {error_category}')
        
        # Additional diagnostics
        print(f'  Diagnostics:')
        
        # Check if it's a token issue
        if 'token' in error_msg.lower() or '401' in error_msg or 'unauthorized' in error_msg.lower():
            print(f'    ⚠️  Likely Upstox API token expired or invalid')
            print(f'    💡  Solution: Refresh token via /scan/upstox/auth')
        
        # Check if it's a network issue
        if 'timeout' in error_msg.lower() or 'connection' in error_msg.lower() or 'network' in error_msg.lower():
            print(f'    ⚠️  Likely network/connectivity issue')
            print(f'    💡  Solution: Check network connectivity and Upstox API status')
        
        # Check if it's an instruments file issue
        if 'instruments' in error_msg.lower() or 'json' in error_msg.lower() or 'file' in error_msg.lower():
            print(f'    ⚠️  Likely instruments file issue')
            print(f'    💡  Solution: Check /home/ubuntu/trademanthan/data/instruments/nse_instruments.json')
        
        # Check if it's an option contract issue
        if 'option' in error_msg.lower() and 'contract' in error_msg.lower():
            print(f'    ⚠️  Likely option contract lookup issue')
            print(f'    💡  Solution: Check master_stock table and instruments file')
        
        # Check if it's a rate limit issue
        if '429' in error_msg or 'rate limit' in error_msg.lower():
            print(f'    ⚠️  Upstox API rate limit exceeded')
            print(f'    💡  Solution: Wait and retry, or reduce API call frequency')
        
        print()
    
    # Print recommendations
    print('=' * 100)
    print('RECOMMENDATIONS')
    print('=' * 100)
    print()
    
    if error_groups.get('Token/Authorization', []):
        print('🔑 TOKEN ISSUES DETECTED:')
        print('   1. Check token status: Visit https://trademanthan.in/scan/health')
        print('   2. Refresh token: Visit https://trademanthan.in/scan/upstox/auth')
        print('   3. Verify token is not expired')
        print()
    
    if error_groups.get('Network/Connectivity', []):
        print('🌐 NETWORK ISSUES DETECTED:')
        print('   1. Check server network connectivity')
        print('   2. Verify Upstox API is accessible')
        print('   3. Check firewall/proxy settings')
        print()
    
    if error_groups.get('Instruments File', []):
        print('📁 INSTRUMENTS FILE ISSUES DETECTED:')
        print('   1. Check if file exists: /home/ubuntu/trademanthan/data/instruments/nse_instruments.json')
        print('   2. Verify file is not corrupted')
        print('   3. Run instruments scheduler to regenerate if needed')
        print()
    
    if error_groups.get('Option Contract', []):
        print('📊 OPTION CONTRACT ISSUES DETECTED:')
        print('   1. Check master_stock table has entries for these stocks')
        print('   2. Verify instruments file contains option contracts')
        print('   3. Check option contract format matches expected pattern')
        print()
    
    if error_groups.get('Rate Limit', []):
        print('⏱️  RATE LIMIT ISSUES DETECTED:')
        print('   1. Reduce API call frequency')
        print('   2. Implement better retry logic with exponential backoff')
        print('   3. Consider caching API responses')
        print()
    
    if error_groups.get('Unknown', []):
        print('❓ UNKNOWN ERRORS DETECTED:')
        print('   1. Check backend logs: tail -500 /tmp/uvicorn.log | grep "Enrichment failed"')
        print('   2. Review full stack traces in logs')
        print('   3. Check for recent code changes that might have introduced bugs')
        print()
    
    print('=' * 100)
    print()
    
    db.close()

def categorize_error(error_msg):
    """Categorize error message into common types"""
    error_lower = error_msg.lower()
    
    if 'token' in error_lower or '401' in error_msg or 'unauthorized' in error_lower:
        return 'Token/Authorization'
    elif 'timeout' in error_lower or 'connection' in error_lower or 'network' in error_lower:
        return 'Network/Connectivity'
    elif 'instruments' in error_lower or ('json' in error_lower and 'file' in error_lower):
        return 'Instruments File'
    elif 'option' in error_lower and 'contract' in error_lower:
        return 'Option Contract'
    elif '429' in error_msg or 'rate limit' in error_lower:
        return 'Rate Limit'
    elif 'not found' in error_lower or '404' in error_msg:
        return 'Resource Not Found'
    elif 'bad request' in error_lower or '400' in error_msg:
        return 'Bad Request'
    else:
        return 'Unknown'

if __name__ == '__main__':
    main()

