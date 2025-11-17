"""
One-time cleanup script to fix instrument_key for all trades entered on 17-Nov-2025
Uses the improved strict matching logic to find correct instrument_key for each option contract
"""

import sys
import os
from pathlib import Path
from datetime import datetime
import pytz
import json
import re

# Add parent directory to path for imports
sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

from database import SessionLocal
from models.trading import IntradayStockOption

def find_instrument_key_for_option(option_contract: str, instruments_data: list) -> tuple:
    """
    Find the correct instrument_key for an option contract using strict matching
    
    Returns:
        (instrument_key, trading_symbol, match_type) or (None, None, None) if not found
        match_type: 'exact' or 'best' or None
    """
    if not option_contract:
        return None, None, None
    
    # Parse option contract format: STOCK-Nov2025-STRIKE-CE/PE
    # Example: HEROMOTOCO-Nov2025-5800-CE
    match = re.match(r'^([A-Z-]+)-(\w{3})(\d{4})-(\d+\.?\d*?)-(CE|PE)$', option_contract)
    
    if not match:
        print(f"   ⚠️ Could not parse option contract format: {option_contract}")
        return None, None, None
    
    symbol, month, year, strike, opt_type = match.groups()
    strike_value = float(strike)
    
    # Parse month
    month_map = {
        'Jan': 1, 'Feb': 2, 'Mar': 3, 'Apr': 4,
        'May': 5, 'Jun': 6, 'Jul': 7, 'Aug': 8,
        'Sep': 9, 'Oct': 10, 'Nov': 11, 'Dec': 12
    }
    target_month = month_map.get(month[:3].capitalize(), 11)
    target_year = int(year)
    
    # Search for matching option with strict criteria
    best_match = None
    best_match_score = float('inf')
    
    for inst in instruments_data:
        # Basic filters
        if (inst.get('underlying_symbol') == symbol and 
            inst.get('instrument_type') == opt_type and
            inst.get('segment') == 'NSE_FO'):
            
            # Check strike price - must match exactly (within 1 paise)
            inst_strike = inst.get('strike_price', 0)
            strike_diff = abs(inst_strike - strike_value)
            
            # Check expiry date
            expiry_ms = inst.get('expiry', 0)
            if expiry_ms:
                # Handle both millisecond and second timestamps
                if expiry_ms > 1e12:
                    expiry_ms = expiry_ms / 1000
                inst_expiry = datetime.fromtimestamp(expiry_ms)
                
                # Check if expiry year and month match
                if inst_expiry.year == target_year and inst_expiry.month == target_month:
                    # Prefer exact strike match
                    if strike_diff < 0.01:  # Exact match (within 1 paise)
                        instrument_key = inst.get('instrument_key')
                        trading_symbol = inst.get('trading_symbol', 'Unknown')
                        return instrument_key, trading_symbol, 'exact'
                    else:
                        # Track best match if no exact match found yet
                        score = strike_diff * 1000
                        if score < best_match_score:
                            best_match = inst
                            best_match_score = score
    
    # If no exact match found, use best match (but log warning)
    if best_match:
        instrument_key = best_match.get('instrument_key')
        trading_symbol = best_match.get('trading_symbol', 'Unknown')
        inst_strike = best_match.get('strike_price', 0)
        print(f"   ⚠️ Using BEST MATCH (not exact) - Strike diff: {abs(inst_strike - strike_value):.4f}")
        return instrument_key, trading_symbol, 'best'
    
    return None, None, None


def fix_instrument_keys_for_17nov():
    """
    Fix instrument_key for all trades entered on 17-Nov-2025
    """
    print("=" * 80)
    print("FIXING INSTRUMENT_KEYS FOR TRADES ENTERED ON 17-NOV-2025")
    print("=" * 80)
    
    # Load instruments JSON file
    instruments_file = Path("/home/ubuntu/trademanthan/data/instruments/nse_instruments.json")
    
    if not instruments_file.exists():
        # Try local path for development
        instruments_file = Path(__file__).parent.parent.parent / "data" / "instruments" / "nse_instruments.json"
    
    if not instruments_file.exists():
        print(f"❌ ERROR: Instruments JSON file not found at {instruments_file}")
        return
    
    print(f"📂 Loading instruments from: {instruments_file}")
    with open(instruments_file, 'r') as f:
        instruments_data = json.load(f)
    
    print(f"✅ Loaded {len(instruments_data)} instruments")
    
    # Connect to database
    db = SessionLocal()
    try:
        ist = pytz.timezone('Asia/Kolkata')
        target_date = datetime(2025, 11, 17, 0, 0, 0).replace(tzinfo=ist)
        
        # Get all trades from 17-Nov-2025
        trades = db.query(IntradayStockOption).filter(
            IntradayStockOption.trade_date >= target_date,
            IntradayStockOption.trade_date < target_date.replace(day=18)
        ).all()
        
        print(f"\n📊 Found {len(trades)} trades from 17-Nov-2025")
        
        if not trades:
            print("✅ No trades found for 17-Nov-2025. Nothing to fix.")
            return
        
        # Process each trade
        fixed_count = 0
        unchanged_count = 0
        error_count = 0
        no_option_count = 0
        
        for trade in trades:
            try:
                option_contract = trade.option_contract
                current_instrument_key = trade.instrument_key
                
                if not option_contract:
                    print(f"\n⏭️  Skipping {trade.stock_name} - No option contract")
                    no_option_count += 1
                    continue
                
                print(f"\n🔍 Processing: {trade.stock_name} - {option_contract}")
                print(f"   Current instrument_key: {current_instrument_key}")
                
                # Find correct instrument_key
                new_instrument_key, trading_symbol, match_type = find_instrument_key_for_option(
                    option_contract, instruments_data
                )
                
                if new_instrument_key:
                    if new_instrument_key == current_instrument_key:
                        print(f"   ✅ Instrument key is already correct: {new_instrument_key}")
                        unchanged_count += 1
                    else:
                        print(f"   🔧 FIXING: {current_instrument_key} → {new_instrument_key}")
                        print(f"   Trading Symbol: {trading_symbol}")
                        print(f"   Match Type: {match_type}")
                        
                        trade.instrument_key = new_instrument_key
                        fixed_count += 1
                else:
                    print(f"   ❌ Could not find instrument_key for {option_contract}")
                    error_count += 1
                    
            except Exception as e:
                print(f"   ❌ ERROR processing {trade.stock_name}: {str(e)}")
                import traceback
                traceback.print_exc()
                error_count += 1
        
        # Commit all changes
        if fixed_count > 0:
            print(f"\n💾 Committing {fixed_count} fixes to database...")
            db.commit()
            print(f"✅ Successfully committed changes")
        else:
            print(f"\n✅ No changes needed - all instrument_keys are correct")
        
        # Print summary
        print("\n" + "=" * 80)
        print("SUMMARY")
        print("=" * 80)
        print(f"Total trades processed: {len(trades)}")
        print(f"✅ Fixed: {fixed_count}")
        print(f"✅ Already correct: {unchanged_count}")
        print(f"⏭️  No option contract: {no_option_count}")
        print(f"❌ Errors: {error_count}")
        print("=" * 80)
        
    except Exception as e:
        print(f"\n❌ FATAL ERROR: {str(e)}")
        import traceback
        traceback.print_exc()
        db.rollback()
    finally:
        db.close()


if __name__ == "__main__":
    fix_instrument_keys_for_17nov()

