#!/usr/bin/env python3
"""
One-time script to fix instrument_key for all trades entered on 18-Nov-2025
Uses the improved matching logic to find the correct instrument_key for each option contract
"""

import sys
import os
sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

from database import SessionLocal
from models.trading import IntradayStockOption
from datetime import datetime
import pytz
import json
import re

def find_instrument_key_for_contract(option_contract, instruments_data):
    """
    Find instrument_key for an option contract using improved matching logic
    """
    if not option_contract:
        return None
    
    # Parse option contract format: STOCK-MonthYear-STRIKE-CE/PE
    # Example: HEROMOTOCO-Dec2025-6000-CE
    match = re.match(r'^([A-Z]+)-([A-Za-z]{3})(\d{4})-(\d+\.?\d*)-(CE|PE)$', option_contract)
    if not match:
        print(f"  ⚠️  Could not parse option contract format: {option_contract}")
        return None
    
    symbol, month, year, strike, opt_type = match.groups()
    strike_value = float(strike)
    
    # Parse month and construct target expiry date
    month_map = {
        'Jan': 1, 'Feb': 2, 'Mar': 3, 'Apr': 4,
        'May': 5, 'Jun': 6, 'Jul': 7, 'Aug': 8,
        'Sep': 9, 'Oct': 10, 'Nov': 11, 'Dec': 12
    }
    target_month = month_map.get(month[:3].capitalize(), 11)
    target_year = int(year)
    
    # Search for matching option in NSE_FO segment
    # CRITICAL: Must match exactly on underlying_symbol, instrument_type, segment, strike, and expiry
    best_match = None
    best_match_score = 0
    instrument_key = None
    
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
                inst_expiry = datetime.fromtimestamp(expiry_ms)
                
                # Calculate match score (lower is better)
                # Priority: exact strike match > exact expiry date match
                score = strike_diff * 1000  # Strike difference weighted heavily
                
                # Check if expiry year and month match
                if inst_expiry.year == target_year and inst_expiry.month == target_month:
                    # Prefer exact strike match
                    if strike_diff < 0.01:  # Exact match (within 1 paise)
                        instrument_key = inst.get('instrument_key')
                        trading_symbol = inst.get('trading_symbol', 'Unknown')
                        print(f"  ✅ Found EXACT match:")
                        print(f"     Instrument Key: {instrument_key}")
                        print(f"     Trading Symbol: {trading_symbol}")
                        print(f"     Strike: {inst_strike} (requested: {strike_value}, diff: {strike_diff:.4f})")
                        print(f"     Expiry: {inst_expiry.strftime('%d %b %Y')}")
                        break  # Found exact match, exit loop
                    else:
                        # Track best match if no exact match found yet
                        if best_match is None or score < best_match_score:
                            best_match = inst
                            best_match_score = score
    
    # If no exact match found, use best match (but log warning)
    if not instrument_key and best_match:
        instrument_key = best_match.get('instrument_key')
        inst_strike = best_match.get('strike_price', 0)
        expiry_ms = best_match.get('expiry', 0)
        if expiry_ms > 1e12:
            expiry_ms = expiry_ms / 1000
        inst_expiry = datetime.fromtimestamp(expiry_ms)
        trading_symbol = best_match.get('trading_symbol', 'Unknown')
        print(f"  ⚠️  WARNING: Using BEST MATCH (not exact):")
        print(f"     Instrument Key: {instrument_key}")
        print(f"     Trading Symbol: {trading_symbol}")
        print(f"     Strike: {inst_strike} (requested: {strike_value}, diff: {abs(inst_strike - strike_value):.4f})")
        print(f"     Expiry: {inst_expiry.strftime('%d %b %Y')}")
        print(f"     ⚠️  This may not be the correct instrument!")
    
    if not instrument_key:
        print(f"  ❌ ERROR: Could not find instrument_key")
        print(f"     Searched for: symbol={symbol}, type={opt_type}, strike={strike_value}, expiry={target_month}/{target_year}")
    
    return instrument_key

def main():
    db = SessionLocal()
    ist = pytz.timezone('Asia/Kolkata')
    
    # Target date: 18-Nov-2025
    target_date = datetime(2025, 11, 18, 0, 0, 0).replace(tzinfo=ist)
    target_date_end = datetime(2025, 11, 19, 0, 0, 0).replace(tzinfo=ist)
    
    # Load instruments data
    instruments_file = os.path.join(os.path.dirname(os.path.dirname(os.path.abspath(__file__))), 'data', 'nse_instruments.json')
    
    if not os.path.exists(instruments_file):
        print(f"❌ ERROR: Instruments file not found: {instruments_file}")
        db.close()
        return
    
    print(f"Loading instruments data from {instruments_file}...")
    with open(instruments_file, 'r') as f:
        instruments_data = json.load(f)
    print(f"✅ Loaded {len(instruments_data)} instruments")
    
    # Get all trades from 18-Nov-2025
    trades = db.query(IntradayStockOption).filter(
        IntradayStockOption.trade_date >= target_date,
        IntradayStockOption.trade_date < target_date_end,
        IntradayStockOption.option_contract.isnot(None),
        IntradayStockOption.option_contract != ''
    ).all()
    
    print(f"\n{'='*80}")
    print(f"Found {len(trades)} trades on 18-Nov-2025")
    print(f"{'='*80}\n")
    
    updated_count = 0
    failed_count = 0
    
    for trade in trades:
        print(f"\n[{updated_count + failed_count + 1}/{len(trades)}] {trade.stock_name}: {trade.option_contract}")
        print(f"  Current Instrument Key: {trade.instrument_key}")
        
        # Find correct instrument_key
        new_instrument_key = find_instrument_key_for_contract(trade.option_contract, instruments_data)
        
        if new_instrument_key:
            if new_instrument_key != trade.instrument_key:
                trade.instrument_key = new_instrument_key
                updated_count += 1
                print(f"  ✅ Updated to: {new_instrument_key}")
            else:
                print(f"  ✓ Already correct: {new_instrument_key}")
        else:
            failed_count += 1
            print(f"  ❌ Failed to find instrument_key")
    
    # Commit changes
    if updated_count > 0:
        print(f"\n{'='*80}")
        print(f"Committing {updated_count} updates to database...")
        db.commit()
        print(f"✅ Successfully updated {updated_count} trades")
    else:
        print(f"\n{'='*80}")
        print(f"No updates needed - all instrument_keys are already correct")
    
    if failed_count > 0:
        print(f"⚠️  Failed to update {failed_count} trades")
    
    print(f"{'='*80}\n")
    
    db.close()

if __name__ == '__main__':
    main()

