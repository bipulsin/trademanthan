"""
One-time script to update sell_price with current LTP using corrected instrument_key
and recalculate PnL for all trades from 17-Nov-2025
"""

import sys
import os
from datetime import datetime
import pytz

# Add parent directory to path for imports
sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

from database import SessionLocal
from models.trading import IntradayStockOption
from services.upstox_service import upstox_service

def update_sell_prices_with_current_ltp():
    """
    Update sell_price with current LTP using corrected instrument_key
    and recalculate PnL for all trades from 17-Nov-2025
    """
    print("=" * 80)
    print("UPDATING SELL_PRICE WITH CURRENT LTP FOR 17-NOV-2025 TRADES")
    print("=" * 80)
    
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
            print("✅ No trades found for 17-Nov-2025. Nothing to update.")
            return
        
        # Process each trade
        updated_count = 0
        skipped_count = 0
        error_count = 0
        no_instrument_key_count = 0
        
        for trade in trades:
            try:
                option_contract = trade.option_contract
                instrument_key = trade.instrument_key
                current_sell_price = trade.sell_price
                
                # Skip if no instrument_key
                if not instrument_key:
                    print(f"\n⏭️  Skipping {trade.stock_name} - No instrument_key")
                    no_instrument_key_count += 1
                    continue
                
                # Skip if no option contract
                if not option_contract:
                    print(f"\n⏭️  Skipping {trade.stock_name} - No option contract")
                    skipped_count += 1
                    continue
                
                print(f"\n🔍 Processing: {trade.stock_name} - {option_contract}")
                print(f"   Instrument Key: {instrument_key}")
                print(f"   Current sell_price: ₹{current_sell_price if current_sell_price else 0:.2f}")
                
                # Fetch current LTP using the corrected instrument_key
                try:
                    option_quote = upstox_service.get_market_quote_by_key(instrument_key)
                    
                    if not option_quote:
                        print(f"   ❌ No quote data returned from API")
                        error_count += 1
                        continue
                    
                    if 'last_price' not in option_quote:
                        print(f"   ❌ No 'last_price' in quote data: {option_quote}")
                        error_count += 1
                        continue
                    
                    new_ltp = option_quote['last_price']
                    
                    if not new_ltp or new_ltp <= 0:
                        print(f"   ⚠️  Invalid LTP: ₹{new_ltp}")
                        error_count += 1
                        continue
                    
                    print(f"   ✅ Fetched current LTP: ₹{new_ltp:.2f}")
                    
                    # Sanity check: If LTP is >3x buy_price, it's likely wrong
                    if trade.buy_price and trade.buy_price > 0:
                        ratio = new_ltp / trade.buy_price
                        if ratio > 3.0:
                            print(f"   🚨 UNREALISTIC LTP DETECTED!")
                            print(f"      Buy Price: ₹{trade.buy_price:.2f}")
                            print(f"      New LTP: ₹{new_ltp:.2f} ({ratio:.2f}x buy price)")
                            print(f"      ⚠️  Skipping update due to unrealistic value")
                            error_count += 1
                            continue
                    
                    # Update sell_price
                    old_sell_price = current_sell_price or 0.0
                    trade.sell_price = new_ltp
                    
                    # Recalculate PnL if buy_price and qty are available
                    if trade.buy_price and trade.qty:
                        old_pnl = trade.pnl or 0.0
                        new_pnl = (new_ltp - trade.buy_price) * trade.qty
                        trade.pnl = new_pnl
                        
                        print(f"   📝 Updated sell_price: ₹{old_sell_price:.2f} → ₹{new_ltp:.2f}")
                        print(f"   💰 Updated PnL: ₹{old_pnl:.2f} → ₹{new_pnl:.2f}")
                        print(f"      (Buy: ₹{trade.buy_price:.2f}, Qty: {trade.qty}, Sell: ₹{new_ltp:.2f})")
                    else:
                        print(f"   📝 Updated sell_price: ₹{old_sell_price:.2f} → ₹{new_ltp:.2f}")
                        print(f"   ⚠️  Could not recalculate PnL (buy_price or qty missing)")
                    
                    updated_count += 1
                    
                except Exception as e:
                    print(f"   ❌ ERROR fetching LTP: {str(e)}")
                    import traceback
                    traceback.print_exc()
                    error_count += 1
                    continue
                    
            except Exception as e:
                print(f"   ❌ ERROR processing {trade.stock_name}: {str(e)}")
                import traceback
                traceback.print_exc()
                error_count += 1
        
        # Commit all changes
        if updated_count > 0:
            print(f"\n💾 Committing {updated_count} updates to database...")
            db.commit()
            print(f"✅ Successfully committed changes")
        else:
            print(f"\n✅ No updates needed")
        
        # Print summary
        print("\n" + "=" * 80)
        print("SUMMARY")
        print("=" * 80)
        print(f"Total trades processed: {len(trades)}")
        print(f"✅ Updated: {updated_count}")
        print(f"⏭️  Skipped (no option contract): {skipped_count}")
        print(f"⏭️  Skipped (no instrument_key): {no_instrument_key_count}")
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
    update_sell_prices_with_current_ltp()

