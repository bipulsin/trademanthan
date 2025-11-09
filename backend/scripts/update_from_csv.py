"""
Script to update database records from CSV file
Updates Sell Time, Sell Price, Exit Reason, and PnL fields
"""

import sys
import os
import csv
from datetime import datetime
import pytz

# Add parent directory to path
sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

# Import after path is set
from sqlalchemy import create_engine
from sqlalchemy.orm import sessionmaker
from models.trading import IntradayStockOption

# Database configuration
DATABASE_URL = os.getenv("DATABASE_URL", "postgresql://trademanthan:trademanthan123@localhost/trademanthan")

def update_from_csv(csv_file_path):
    """
    Update database records from CSV file
    
    Args:
        csv_file_path: Path to CSV file with updated trade data
    """
    
    # Create database connection
    engine = create_engine(DATABASE_URL)
    SessionLocal = sessionmaker(autocommit=False, autoflush=False, bind=engine)
    db = SessionLocal()
    
    ist = pytz.timezone('Asia/Kolkata')
    
    try:
        print(f"\n{'='*80}")
        print(f"UPDATING DATABASE FROM CSV: {csv_file_path}")
        print(f"{'='*80}\n")
        
        # Read CSV file
        with open(csv_file_path, 'r') as file:
            csv_reader = csv.DictReader(file)
            rows = list(csv_reader)
        
        print(f"📄 Found {len(rows)} records in CSV file\n")
        
        updated_count = 0
        not_found_count = 0
        error_count = 0
        skipped_count = 0
        
        for row in rows:
            try:
                record_id = int(row['ID'])
                
                # Fetch record from database
                record = db.query(IntradayStockOption).filter(
                    IntradayStockOption.id == record_id
                ).first()
                
                if not record:
                    print(f"⚠️  Record ID {record_id} not found in database")
                    not_found_count += 1
                    continue
                
                # Check if any fields need updating
                needs_update = False
                updates = []
                
                # Parse and update Sell Time
                if row['Sell Time'] and row['Sell Time'].strip():
                    try:
                        # Parse datetime: "2025-11-07 11:15:00"
                        sell_time = datetime.strptime(row['Sell Time'].strip(), '%Y-%m-%d %H:%M:%S')
                        sell_time = ist.localize(sell_time)
                        
                        if record.sell_time != sell_time:
                            record.sell_time = sell_time
                            needs_update = True
                            updates.append(f"Sell Time={sell_time.strftime('%Y-%m-%d %H:%M:%S')}")
                    except ValueError as e:
                        print(f"⚠️  Invalid date format for ID {record_id}: {row['Sell Time']}")
                
                # Update Sell Price
                if row['Sell Price'] and row['Sell Price'].strip():
                    try:
                        sell_price = float(row['Sell Price'].strip())
                        if record.sell_price != sell_price:
                            record.sell_price = sell_price
                            needs_update = True
                            updates.append(f"Sell Price=₹{sell_price}")
                    except ValueError:
                        print(f"⚠️  Invalid sell price for ID {record_id}: {row['Sell Price']}")
                
                # Update Exit Reason
                if row['Exit Reason'] and row['Exit Reason'].strip():
                    exit_reason = row['Exit Reason'].strip()
                    if record.exit_reason != exit_reason:
                        record.exit_reason = exit_reason
                        needs_update = True
                        updates.append(f"Exit Reason={exit_reason}")
                
                # Update PnL
                if row['P&L'] and row['P&L'].strip():
                    try:
                        pnl = float(row['P&L'].strip())
                        if record.pnl != pnl:
                            record.pnl = pnl
                            needs_update = True
                            updates.append(f"PnL=₹{pnl}")
                    except ValueError:
                        print(f"⚠️  Invalid PnL for ID {record_id}: {row['P&L']}")
                
                # Update status to 'sold' if exit_reason is set
                if record.exit_reason and record.status != 'sold':
                    record.status = 'sold'
                    needs_update = True
                    updates.append("Status=sold")
                
                if needs_update:
                    record.updated_at = datetime.now(ist)
                    print(f"✅ ID {record_id} ({record.stock_name}): {', '.join(updates)}")
                    updated_count += 1
                else:
                    skipped_count += 1
                    
            except Exception as e:
                print(f"❌ Error processing ID {row.get('ID', 'unknown')}: {str(e)}")
                error_count += 1
                import traceback
                traceback.print_exc()
        
        # Commit all changes
        db.commit()
        
        print(f"\n{'='*80}")
        print("UPDATE COMPLETE")
        print(f"{'='*80}")
        print(f"✅ Successfully updated: {updated_count} records")
        print(f"⏭️  Skipped (no changes): {skipped_count} records")
        print(f"⚠️  Not found in database: {not_found_count} records")
        print(f"❌ Errors: {error_count} records")
        print(f"\nTotal processed: {len(rows)} records\n")
        
        return {
            "success": True,
            "total": len(rows),
            "updated": updated_count,
            "skipped": skipped_count,
            "not_found": not_found_count,
            "errors": error_count
        }
        
    except FileNotFoundError:
        print(f"❌ Error: CSV file not found: {csv_file_path}")
        return {"success": False, "error": "File not found"}
    except Exception as e:
        db.rollback()
        print(f"❌ Error updating database: {str(e)}")
        import traceback
        traceback.print_exc()
        return {"success": False, "error": str(e)}
    finally:
        db.close()


if __name__ == "__main__":
    # CSV file path
    csv_file = "/Users/bipulsahay/TradeManthan/trades_07_nov_2025_upd.csv"
    
    # Run the update
    result = update_from_csv(csv_file)
    
    # Exit with appropriate code
    sys.exit(0 if result.get("success") else 1)

