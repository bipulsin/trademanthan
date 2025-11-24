#!/usr/bin/env python3
"""
One-time script to update sell_price for 14-Nov-2025 trades
Uses stored instrument_key to fetch current LTP from Upstox API
Only updates trades that are NOT 'no_entry' status
"""

import sys
import os
from pathlib import Path
from datetime import datetime
import pytz

# Add parent directory to path
sys.path.append(str(Path(__file__).parent.parent))

from database import SessionLocal
from models.trading import IntradayStockOption

def update_sell_prices_for_14nov():
    """
    Update sell_price for 14-Nov-2025 trades using stored instrument_key
    """
    db = SessionLocal()
    ist = pytz.timezone('Asia/Kolkata')
    
    try:
        # Target date: 2025-11-14
        target_date = datetime(2025, 11, 14, tzinfo=ist)
        
        # Import Upstox service (pre-initialized instance)
        try:
            from services.upstox_service import upstox_service as vwap_service
        except ImportError:
            print("❌ Could not import upstox_service")
            return False
        
        # Query all trades for 14-Nov-2025 that are NOT 'no_entry' and have instrument_key
        records = db.query(IntradayStockOption).filter(
            IntradayStockOption.trade_date >= target_date.replace(hour=0, minute=0, second=0),
            IntradayStockOption.trade_date < target_date.replace(hour=23, minute=59, second=59),
            IntradayStockOption.status != 'no_entry',
            IntradayStockOption.instrument_key.isnot(None),
            IntradayStockOption.instrument_key != ''
        ).all()
        
        total_count = len(records)
        print(f"📊 Found {total_count} records to update for 14-Nov-2025")
        print(f"   (Status != 'no_entry' and have instrument_key)")
        print()
        
        if total_count == 0:
            print("✅ No records to update")
            return True
        
        updated_count = 0
        failed_count = 0
        skipped_count = 0
        
        for record in records:
            stock_name = record.stock_name
            option_contract = record.option_contract
            instrument_key = record.instrument_key
            record_id = record.id
            current_sell_price = record.sell_price
            buy_price = record.buy_price
            status = record.status
            
            print(f"Processing ID {record_id}: {stock_name} - {option_contract}")
            print(f"   Status: {status}, Instrument Key: {instrument_key}")
            current_sell_display = current_sell_price if current_sell_price else 0.0
            buy_price_display = buy_price if buy_price else 0.0
            print(f"   Current Sell Price: ₹{current_sell_display:.2f}, Buy Price: ₹{buy_price_display:.2f}")
            
            try:
                # Fetch current LTP using instrument_key
                print(f"   Fetching LTP from Upstox API...")
                option_quote = vwap_service.get_market_quote_by_key(instrument_key)
                
                if not option_quote:
                    print(f"   ❌ No quote data returned from API")
                    failed_count += 1
                    continue
                
                if 'last_price' not in option_quote:
                    print(f"   ❌ No 'last_price' in quote data: {option_quote}")
                    failed_count += 1
                    continue
                
                new_ltp = option_quote['last_price']
                
                if not new_ltp or new_ltp <= 0:
                    print(f"   ⚠️  Invalid LTP: {new_ltp}")
                    failed_count += 1
                    continue
                
                print(f"   ✅ Fetched LTP: ₹{new_ltp:.2f}")
                
                # Sanity check: If LTP is >3x buy_price, it's likely wrong
                if buy_price and buy_price > 0:
                    ratio = new_ltp / buy_price
                    if ratio > 3.0:
                        print(f"   🚨 UNREALISTIC LTP DETECTED!")
                        print(f"      Buy Price: ₹{buy_price:.2f}")
                        print(f"      New LTP: ₹{new_ltp:.2f} ({ratio:.2f}x buy price)")
                        print(f"      ⚠️  Skipping update due to unrealistic value")
                        skipped_count += 1
                        continue
                
                # Update sell_price
                old_sell_price = current_sell_price or 0.0
                record.sell_price = new_ltp
                
                # Recalculate PnL if buy_price and qty are available
                if buy_price and record.qty:
                    record.pnl = (new_ltp - buy_price) * record.qty
                    print(f"   ✅ Updated: Sell Price: ₹{old_sell_price:.2f} → ₹{new_ltp:.2f}")
                    print(f"      PnL: ₹{record.pnl:.2f}")
                else:
                    print(f"   ✅ Updated: Sell Price: ₹{old_sell_price:.2f} → ₹{new_ltp:.2f}")
                    print(f"      (PnL not calculated - missing buy_price or qty)")
                
                updated_count += 1
                
            except Exception as e:
                print(f"   ❌ Error fetching/updating: {str(e)}")
                import traceback
                traceback.print_exc()
                failed_count += 1
            
            print()  # Empty line between records
        
        # Commit all updates
        if updated_count > 0:
            db.commit()
            print(f"✅ Successfully updated {updated_count} records")
        
        if skipped_count > 0:
            print(f"⚠️  Skipped {skipped_count} records (unrealistic LTP values)")
        
        if failed_count > 0:
            print(f"❌ Failed to update {failed_count} records")
        
        print()
        print(f"📊 Summary:")
        print(f"   Total records: {total_count}")
        print(f"   Updated: {updated_count}")
        print(f"   Skipped (unrealistic): {skipped_count}")
        print(f"   Failed: {failed_count}")
        
        return True
        
    except Exception as e:
        db.rollback()
        print(f"❌ Error updating sell prices: {e}")
        import traceback
        traceback.print_exc()
        return False
    finally:
        db.close()

if __name__ == "__main__":
    print("=" * 70)
    print("Update Sell Prices for 14-Nov-2025 Trades")
    print("=" * 70)
    print()
    
    success = update_sell_prices_for_14nov()
    
    if success:
        print()
        print("✅ Update completed")
        sys.exit(0)
    else:
        print()
        print("❌ Update failed")
        sys.exit(1)

