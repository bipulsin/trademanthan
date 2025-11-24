#!/usr/bin/env python3
"""
One-time migration script to update instrument_key in intraday_stock_options table
Matches option_contract with instruments from upstox_instrument table or nse_instruments.json
"""

import sys
import os
from pathlib import Path
from datetime import datetime
import re
import json
import pytz

# Add parent directory to path
sys.path.append(str(Path(__file__).parent.parent))

from database import SessionLocal, engine
from models.trading import IntradayStockOption, UpstoxInstrument

def parse_option_contract(option_contract):
    """
    Parse option contract format: SYMBOL-MonthYYYY-STRIKE-CE/PE
    Example: MUTHOOTFIN-Nov2025-3800-CE
    Returns: (symbol, target_month, target_year, strike_value, opt_type) or None
    """
    if not option_contract:
        return None
    
    # Handle stocks with hyphens in symbol (e.g., BAJAJ-AUTO, IDFC-FIRSTB)
    match = re.match(r'^([A-Z-]+)-(\w{3})(\d{4})-(\d+\.?\d*?)-(CE|PE)$', option_contract)
    
    if not match:
        return None
    
    symbol, month, year, strike, opt_type = match.groups()
    strike_value = float(strike)
    
    # Parse expiry month and year
    month_map = {
        'Jan': 1, 'Feb': 2, 'Mar': 3, 'Apr': 4,
        'May': 5, 'Jun': 6, 'Jul': 7, 'Aug': 8,
        'Sep': 9, 'Oct': 10, 'Nov': 11, 'Dec': 12
    }
    target_month = month_map.get(month[:3].capitalize(), None)
    target_year = int(year)
    
    if target_month is None:
        return None
    
    return (symbol.upper(), target_month, target_year, strike_value, opt_type)

def find_instrument_key_from_db(db, symbol, target_month, target_year, strike_value, opt_type):
    """
    Find instrument_key from upstox_instrument table
    """
    try:
        # Query upstox_instrument table
        instruments = db.query(UpstoxInstrument).filter(
            UpstoxInstrument.underlying_symbol == symbol,
            UpstoxInstrument.segment == 'NSE_FO',
            UpstoxInstrument.instrument_type == opt_type,
            UpstoxInstrument.strike_price == strike_value
        ).all()
        
        if not instruments:
            return None
        
        # Filter by expiry month/year
        ist = pytz.timezone('Asia/Kolkata')
        for inst in instruments:
            if inst.expiry:
                # Ensure expiry is timezone-aware
                if inst.expiry.tzinfo is None:
                    expiry = ist.localize(inst.expiry)
                else:
                    expiry = inst.expiry.astimezone(ist)
                
                if expiry.year == target_year and expiry.month == target_month:
                    return inst.instrument_key
        
        return None
    except Exception as e:
        print(f"   ⚠️ Error querying upstox_instrument table: {e}")
        return None

def find_instrument_key_from_json(option_contract, symbol, target_month, target_year, strike_value, opt_type):
    """
    Fallback: Find instrument_key from nse_instruments.json file
    """
    try:
        instruments_file = Path("/home/ubuntu/trademanthan/data/instruments/nse_instruments.json")
        if not instruments_file.exists():
            instruments_file = Path(__file__).parent.parent.parent / 'data' / 'instruments' / 'nse_instruments.json'
        
        if not instruments_file.exists():
            return None
        
        with open(instruments_file, 'r') as f:
            instruments_data = json.load(f)
        
        # Search for matching instrument
        for instrument in instruments_data:
            if (instrument.get('underlying_symbol', '').upper() == symbol and
                instrument.get('segment') == 'NSE_FO' and
                instrument.get('instrument_type') == opt_type):
                
                inst_strike = float(instrument.get('strike_price', 0))
                if abs(inst_strike - strike_value) < 0.01:
                    # Check expiry month/year matches
                    expiry_timestamp = instrument.get('expiry', 0)
                    if expiry_timestamp:
                        try:
                            # Convert timestamp (milliseconds) to datetime
                            if expiry_timestamp > 1e12:
                                expiry_timestamp = expiry_timestamp / 1000
                            inst_expiry = datetime.fromtimestamp(expiry_timestamp, tz=pytz.UTC)
                            ist = pytz.timezone('Asia/Kolkata')
                            inst_expiry = inst_expiry.astimezone(ist)
                            
                            if inst_expiry.year == target_year and inst_expiry.month == target_month:
                                return instrument.get('instrument_key')
                        except (ValueError, TypeError) as exp_error:
                            continue
        
        return None
    except Exception as e:
        print(f"   ⚠️ Error reading nse_instruments.json: {e}")
        return None

def update_instrument_keys():
    """
    Main function to update instrument_key for all records missing it
    """
    db = SessionLocal()
    
    try:
        # Get all records with option_contract but no instrument_key
        records = db.query(IntradayStockOption).filter(
            IntradayStockOption.option_contract.isnot(None),
            IntradayStockOption.option_contract != '',
            (IntradayStockOption.instrument_key.is_(None) | 
             (IntradayStockOption.instrument_key == ''))
        ).all()
        
        total_count = len(records)
        print(f"📊 Found {total_count} records to update")
        print()
        
        if total_count == 0:
            print("✅ No records to update - all have instrument_key or no option_contract")
            return True
        
        updated_count = 0
        failed_count = 0
        
        for record in records:
            option_contract = record.option_contract
            stock_name = record.stock_name
            record_id = record.id
            
            print(f"Processing ID {record_id}: {stock_name} - {option_contract}")
            
            # Parse option contract
            parsed = parse_option_contract(option_contract)
            if not parsed:
                print(f"   ❌ Failed to parse option_contract: {option_contract}")
                failed_count += 1
                continue
            
            symbol, target_month, target_year, strike_value, opt_type = parsed
            print(f"   Looking for: {symbol}, {opt_type}, Strike: {strike_value}, Expiry: {target_month}/{target_year}")
            
            # Try to find instrument_key from database first
            instrument_key = find_instrument_key_from_db(
                db, symbol, target_month, target_year, strike_value, opt_type
            )
            
            # Fallback to JSON file if not found in database
            if not instrument_key:
                print(f"   Not found in database, trying JSON file...")
                instrument_key = find_instrument_key_from_json(
                    option_contract, symbol, target_month, target_year, strike_value, opt_type
                )
            
            if instrument_key:
                # Update record
                record.instrument_key = instrument_key
                updated_count += 1
                print(f"   ✅ Updated with instrument_key: {instrument_key}")
            else:
                print(f"   ❌ Could not find matching instrument_key")
                failed_count += 1
        
        # Commit all updates
        if updated_count > 0:
            db.commit()
            print()
            print(f"✅ Successfully updated {updated_count} records")
        
        if failed_count > 0:
            print(f"⚠️  Failed to update {failed_count} records")
        
        print()
        print(f"📊 Summary:")
        print(f"   Total records: {total_count}")
        print(f"   Updated: {updated_count}")
        print(f"   Failed: {failed_count}")
        
        return True
        
    except Exception as e:
        db.rollback()
        print(f"❌ Error updating instrument_keys: {e}")
        import traceback
        traceback.print_exc()
        return False
    finally:
        db.close()

if __name__ == "__main__":
    print("=" * 70)
    print("Instrument Key Migration Script")
    print("=" * 70)
    print()
    
    success = update_instrument_keys()
    
    if success:
        print()
        print("✅ Migration completed")
        sys.exit(0)
    else:
        print()
        print("❌ Migration failed")
        sys.exit(1)

