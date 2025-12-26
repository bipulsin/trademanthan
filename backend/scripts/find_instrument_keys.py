#!/usr/bin/env python3
"""
Script to find instrument_key for stocks with enrichment failures
Uses the same process as the scan algorithm to lookup instrument_key from instruments.json
"""

import sys
import os
from pathlib import Path
import json
import re
from datetime import datetime
import pytz

# Add parent directory to path
script_dir = os.path.dirname(os.path.abspath(__file__))
parent_dir = os.path.dirname(os.path.dirname(script_dir))
sys.path.insert(0, parent_dir)

from backend.database import SessionLocal
from backend.models.trading import IntradayStockOption

def parse_option_contract(option_contract: str):
    """
    Parse option contract format: STOCK-MonthYYYY-STRIKE-CE/PE
    Example: DIXON-Jan2026-12000-PE
    
    Returns: (symbol, month, year, strike, opt_type) or None
    """
    if not option_contract:
        return None
    
    # Handle stocks with hyphens in symbol (e.g., BAJAJ-AUTO)
    match = re.match(r'^([A-Z-]+)-(\w{3})(\d{4})-(\d+\.?\d*?)-(CE|PE)$', option_contract)
    
    if not match:
        return None
    
    symbol, month, year, strike, opt_type = match.groups()
    strike_value = float(strike)
    
    # Parse month
    month_map = {
        'Jan': 1, 'Feb': 2, 'Mar': 3, 'Apr': 4,
        'May': 5, 'Jun': 6, 'Jul': 7, 'Aug': 8,
        'Sep': 9, 'Oct': 10, 'Nov': 11, 'Dec': 12
    }
    target_month = month_map.get(month[:3].capitalize(), None)
    target_year = int(year)
    
    if target_month is None:
        return None
    
    return {
        'symbol': symbol,
        'month': target_month,
        'year': target_year,
        'strike': strike_value,
        'opt_type': opt_type
    }

def find_instrument_key_from_instruments(option_contract: str, instruments_data: list):
    """
    Find instrument_key from instruments.json using the same logic as scan.py
    
    Args:
        option_contract: Option contract string (e.g., "DIXON-Jan2026-12000-PE")
        instruments_data: List of instrument dictionaries from instruments.json
    
    Returns:
        dict with instrument_key and matching details, or None if not found
    """
    # Parse option contract
    parsed = parse_option_contract(option_contract)
    if not parsed:
        return None
    
    symbol = parsed['symbol']
    target_month = parsed['month']
    target_year = parsed['year']
    strike_value = parsed['strike']
    opt_type = parsed['opt_type']
    
    # Search for matching instrument
    best_match = None
    best_match_score = float('inf')
    
    for inst in instruments_data:
        # Basic filters
        if (inst.get('underlying_symbol') == symbol and 
            inst.get('instrument_type') == opt_type and
            inst.get('segment') == 'NSE_FO'):
            
            # Check strike price - must match exactly (or very close for float precision)
            inst_strike = inst.get('strike_price', 0)
            strike_diff = abs(inst_strike - strike_value)
            
            # Check expiry date - must match exact date, not just month/year
            expiry_ms = inst.get('expiry', 0)
            if expiry_ms:
                # Handle both millisecond and second timestamps
                if expiry_ms > 1e12:
                    expiry_ms = expiry_ms / 1000
                inst_expiry = datetime.fromtimestamp(expiry_ms, tz=pytz.UTC)
                
                # Calculate match score (lower is better)
                # Priority: exact strike match > exact expiry date match
                score = strike_diff * 1000  # Strike difference weighted heavily
                
                # Check if expiry year and month match
                if inst_expiry.year == target_year and inst_expiry.month == target_month:
                    # Prefer exact strike match
                    if strike_diff < 0.01:  # Exact match (within 1 paise)
                        instrument_key = inst.get('instrument_key')
                        trading_symbol = inst.get('trading_symbol', 'Unknown')
                        return {
                            'instrument_key': instrument_key,
                            'trading_symbol': trading_symbol,
                            'strike': inst_strike,
                            'strike_diff': strike_diff,
                            'expiry': inst_expiry,
                            'match_type': 'exact'
                        }
                    else:
                        # Track best match if no exact match found yet
                        if best_match is None or score < best_match_score:
                            best_match = inst
                            best_match_score = score
    
    # If no exact match found, use best match (but log warning)
    if best_match:
        instrument_key = best_match.get('instrument_key')
        inst_strike = best_match.get('strike_price', 0)
        expiry_ms = best_match.get('expiry', 0)
        if expiry_ms > 1e12:
            expiry_ms = expiry_ms / 1000
        inst_expiry = datetime.fromtimestamp(expiry_ms, tz=pytz.UTC)
        trading_symbol = best_match.get('trading_symbol', 'Unknown')
        return {
            'instrument_key': instrument_key,
            'trading_symbol': trading_symbol,
            'strike': inst_strike,
            'strike_diff': abs(inst_strike - strike_value),
            'expiry': inst_expiry,
            'match_type': 'best_match'
        }
    
    return None

def find_instrument_key_for_stock(stock_name: str, option_type: str, instruments_data: list):
    """
    Try to find instrument_key directly from stock_name and option_type
    This is a fallback for stocks without option_contract (like HFCL)
    
    Args:
        stock_name: Stock symbol
        option_type: 'CE' or 'PE'
        instruments_data: List of instrument dictionaries
    
    Returns:
        dict with instrument_key and matching details, or None if not found
    """
    # Determine target expiry month (same logic as scan.py)
    ist = pytz.timezone('Asia/Kolkata')
    now = datetime.now(ist)
    
    if now.day > 17:
        if now.month == 12:
            target_month = 1
            target_year = now.year + 1
        else:
            target_month = now.month + 1
            target_year = now.year
    else:
        target_month = now.month
        target_year = now.year
    
    # Find all matching instruments
    matches = []
    for inst in instruments_data:
        if (inst.get('underlying_symbol') == stock_name and
            inst.get('instrument_type') == option_type and
            inst.get('segment') == 'NSE_FO'):
            
            expiry_ms = inst.get('expiry', 0)
            if expiry_ms:
                if expiry_ms > 1e12:
                    expiry_ms = expiry_ms / 1000
                inst_expiry = datetime.fromtimestamp(expiry_ms, tz=pytz.UTC)
                
                if inst_expiry.year == target_year and inst_expiry.month == target_month:
                    matches.append(inst)
    
    if not matches:
        return None
    
    # Return the first match (or could implement more sophisticated selection)
    inst = matches[0]
    expiry_ms = inst.get('expiry', 0)
    if expiry_ms > 1e12:
        expiry_ms = expiry_ms / 1000
    inst_expiry = datetime.fromtimestamp(expiry_ms, tz=pytz.UTC)
    
    return {
        'instrument_key': inst.get('instrument_key'),
        'trading_symbol': inst.get('trading_symbol', 'Unknown'),
        'strike': inst.get('strike_price', 0),
        'expiry': inst_expiry,
        'match_type': 'direct_search'
    }

def main():
    db = SessionLocal()
    ist = pytz.timezone('Asia/Kolkata')
    today = datetime.now(ist).replace(hour=0, minute=0, second=0, microsecond=0)
    
    # Target stocks
    target_stocks = ['HFCL', 'DIXON', 'MCX', 'APLAPOLLO', 'TITAN', 'COFORGE', 'NMDC']
    
    # Query database for these stocks from 2025-12-26
    query_date = datetime(2025, 12, 26, 0, 0, 0, tzinfo=ist)
    
    trades = db.query(IntradayStockOption).filter(
        IntradayStockOption.trade_date >= query_date,
        IntradayStockOption.trade_date < query_date.replace(day=27),
        IntradayStockOption.stock_name.in_(target_stocks),
        IntradayStockOption.no_entry_reason.like('Enrichment failed%')
    ).order_by(IntradayStockOption.stock_name).all()
    
    print('=' * 100)
    print(f'FINDING INSTRUMENT_KEY FOR ENRICHMENT FAILURES - 2025-12-26')
    print('=' * 100)
    print()
    
    if not trades:
        print('No trades found for the target stocks')
        db.close()
        return
    
    # Load instruments.json
    instruments_file = Path("/home/ubuntu/trademanthan/data/instruments/nse_instruments.json")
    if not instruments_file.exists():
        # Try local path for testing
        instruments_file = Path(__file__).parent.parent.parent / 'data' / 'instruments' / 'nse_instruments.json'
    
    if not instruments_file.exists():
        print(f'❌ ERROR: Instruments file not found at {instruments_file}')
        print('   Please ensure the file exists on the EC2 server')
        db.close()
        return
    
    print(f'✅ Loading instruments from: {instruments_file}')
    with open(instruments_file, 'r') as f:
        instruments_data = json.load(f)
    print(f'✅ Loaded {len(instruments_data)} instruments')
    print()
    
    results = []
    
    for trade in trades:
        stock_name = trade.stock_name
        option_contract = trade.option_contract
        option_type = trade.option_type
        alert_type = trade.alert_type
        
        print(f'[{stock_name}]')
        print('-' * 100)
        print(f'  Alert Type:     {alert_type}')
        print(f'  Option Type:    {option_type}')
        print(f'  Option Contract: {option_contract or "N/A"}')
        print(f'  Current Instrument Key: {trade.instrument_key or "N/A"}')
        
        result = {
            'stock_name': stock_name,
            'option_contract': option_contract,
            'option_type': option_type,
            'alert_type': alert_type,
            'current_instrument_key': trade.instrument_key,
            'found_instrument_key': None,
            'match_details': None,
            'status': 'not_found'
        }
        
        # Try to find instrument_key
        if option_contract and option_contract != 'N/A':
            # Use option_contract to find instrument_key
            print(f'  🔍 Searching for instrument_key using option_contract: {option_contract}')
            match_result = find_instrument_key_from_instruments(option_contract, instruments_data)
            
            if match_result:
                result['found_instrument_key'] = match_result['instrument_key']
                result['match_details'] = match_result
                result['status'] = 'found'
                
                print(f'  ✅ FOUND instrument_key: {match_result["instrument_key"]}')
                print(f'     Trading Symbol: {match_result["trading_symbol"]}')
                print(f'     Strike: {match_result["strike"]} (diff: {match_result["strike_diff"]:.4f})')
                print(f'     Expiry: {match_result["expiry"].strftime("%d %b %Y")}')
                print(f'     Match Type: {match_result["match_type"]}')
                
                if match_result['match_type'] == 'best_match':
                    print(f'     ⚠️  WARNING: Using best match (not exact)')
            else:
                print(f'  ❌ NOT FOUND: Could not find instrument_key for {option_contract}')
                print(f'     Searched for: symbol={parse_option_contract(option_contract)["symbol"] if parse_option_contract(option_contract) else "N/A"}, type={option_type}')
        else:
            # No option_contract - try direct search
            print(f'  🔍 No option_contract available, searching directly by stock_name and option_type')
            match_result = find_instrument_key_for_stock(stock_name, option_type, instruments_data)
            
            if match_result:
                result['found_instrument_key'] = match_result['instrument_key']
                result['match_details'] = match_result
                result['status'] = 'found'
                
                print(f'  ✅ FOUND instrument_key: {match_result["instrument_key"]}')
                print(f'     Trading Symbol: {match_result["trading_symbol"]}')
                print(f'     Strike: {match_result["strike"]}')
                print(f'     Expiry: {match_result["expiry"].strftime("%d %b %Y")}')
                print(f'     Match Type: {match_result["match_type"]}')
            else:
                print(f'  ❌ NOT FOUND: Could not find instrument_key for {stock_name} {option_type}')
        
        results.append(result)
        print()
    
    # Summary
    print('=' * 100)
    print('SUMMARY')
    print('=' * 100)
    print()
    
    found_count = sum(1 for r in results if r['status'] == 'found')
    not_found_count = len(results) - found_count
    
    print(f'Total stocks processed: {len(results)}')
    print(f'Found instrument_key: {found_count}')
    print(f'Not found: {not_found_count}')
    print()
    
    if found_count > 0:
        print('Stocks with instrument_key found:')
        for r in results:
            if r['status'] == 'found':
                print(f'  ✅ {r["stock_name"]}: {r["found_instrument_key"]}')
        print()
    
    if not_found_count > 0:
        print('Stocks without instrument_key:')
        for r in results:
            if r['status'] == 'not_found':
                print(f'  ❌ {r["stock_name"]}: {r["option_contract"] or "No option_contract"}')
        print()
    
    # Ask if user wants to update database
    print('=' * 100)
    print('DATABASE UPDATE')
    print('=' * 100)
    print()
    print('To update the database with found instrument_key values, run:')
    print('  python3 backend/scripts/find_instrument_keys.py --update')
    print()
    
    db.close()
    
    return results

if __name__ == '__main__':
    import argparse
    
    parser = argparse.ArgumentParser(description='Find instrument_key for enrichment failures')
    parser.add_argument('--update', action='store_true', help='Update database with found instrument_key values')
    
    args = parser.parse_args()
    
    results = main()
    
    if args.update and results:
        print('Updating database...')
        db = SessionLocal()
        ist = pytz.timezone('Asia/Kolkata')
        query_date = datetime(2025, 12, 26, 0, 0, 0, tzinfo=ist)
        
        updated_count = 0
        for result in results:
            if result['status'] == 'found' and result['found_instrument_key']:
                trade = db.query(IntradayStockOption).filter(
                    IntradayStockOption.trade_date >= query_date,
                    IntradayStockOption.trade_date < query_date.replace(day=27),
                    IntradayStockOption.stock_name == result['stock_name'],
                    IntradayStockOption.no_entry_reason.like('Enrichment failed%')
                ).first()
                
                if trade:
                    old_key = trade.instrument_key
                    trade.instrument_key = result['found_instrument_key']
                    db.commit()
                    updated_count += 1
                    print(f'  ✅ Updated {result["stock_name"]}: {old_key or "None"} -> {result["found_instrument_key"]}')
        
        db.close()
        print(f'\n✅ Updated {updated_count} records in database')

