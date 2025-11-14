"""
Migration script to load Upstox instruments from JSON file to database
"""
import sys
import os
from pathlib import Path
from datetime import datetime
import json

# Add parent directory to path
sys.path.append(str(Path(__file__).parent.parent))

from database import SessionLocal, create_tables, Base
# Import all models to ensure they're registered with Base
from models import UpstoxInstrument  # This imports all models
from models.trading import UpstoxInstrument as UI
import pytz

def convert_expiry_timestamp(timestamp):
    """Convert Unix timestamp (milliseconds) to datetime"""
    if timestamp and isinstance(timestamp, (int, float)):
        # Handle both seconds and milliseconds
        if timestamp > 1e12:  # milliseconds
            timestamp = timestamp / 1000
        return datetime.fromtimestamp(timestamp, tz=pytz.UTC)
    return None

def migrate_instruments(json_file_path: str = None):
    """
    Migrate instruments from JSON file to database
    
    Args:
        json_file_path: Path to nse_instruments.json file
    """
    if json_file_path is None:
        # Default path
        json_file_path = Path(__file__).parent.parent.parent / 'data' / 'instruments' / 'nse_instruments.json'
    
    json_file_path = Path(json_file_path)
    
    if not json_file_path.exists():
        print(f"❌ JSON file not found: {json_file_path}")
        return False
    
    print(f"📂 Loading instruments from: {json_file_path}")
    
    # Create tables first - IMPORTANT: Import model before creating tables
    print("📋 Creating database tables...")
    # Import model to register it with Base
    from models.trading import UpstoxInstrument
    # Now create tables (this will create all registered models)
    create_tables()
    
    # Load JSON data
    print("📖 Reading JSON file...")
    with open(json_file_path, 'r') as f:
        instruments_data = json.load(f)
    
    print(f"✅ Loaded {len(instruments_data)} instruments from JSON")
    
    # Create database session
    db = SessionLocal()
    
    try:
        # Check if table already has data (handle case where table doesn't exist yet)
        try:
            existing_count = db.query(UpstoxInstrument).count()
        except Exception as e:
            # Table doesn't exist yet, will be created by create_tables()
            print(f"⚠️  Table check failed (this is OK if table is being created): {str(e)}")
            existing_count = 0
        
        if existing_count > 0:
            response = input(f"⚠️  Table already contains {existing_count} records. Delete all and reload? (yes/no): ")
            if response.lower() == 'yes':
                print(f"🗑️  Deleting {existing_count} existing records...")
                db.query(UpstoxInstrument).delete()
                db.commit()
                print("✅ Existing records deleted")
            else:
                print("❌ Migration cancelled")
                return False
        
        print(f"\n📝 Migrating {len(instruments_data)} instruments to database...")
        
        batch_size = 1000
        inserted = 0
        skipped = 0
        errors = 0
        
        for i, instrument in enumerate(instruments_data):
            try:
                # Skip if instrument_key is missing
                instrument_key = instrument.get('instrument_key')
                if not instrument_key:
                    skipped += 1
                    continue
                
                # Check if already exists (shouldn't happen if we deleted, but check anyway)
                existing = db.query(UpstoxInstrument).filter(
                    UpstoxInstrument.instrument_key == instrument_key
                ).first()
                
                if existing:
                    skipped += 1
                    continue
                
                # Convert expiry timestamp to datetime
                expiry = None
                expiry_timestamp = instrument.get('expiry')
                if expiry_timestamp:
                    expiry = convert_expiry_timestamp(expiry_timestamp)
                
                last_trading_date = None
                last_trading_timestamp = instrument.get('last_trading_date')
                if last_trading_timestamp:
                    last_trading_date = convert_expiry_timestamp(last_trading_timestamp)
                
                # Create UpstoxInstrument record
                upstox_instrument = UpstoxInstrument(
                    instrument_key=instrument_key,
                    name=instrument.get('name'),
                    trading_symbol=instrument.get('trading_symbol'),
                    exchange=instrument.get('exchange'),
                    segment=instrument.get('segment'),
                    instrument_type=instrument.get('instrument_type'),
                    exchange_token=str(instrument.get('exchange_token')) if instrument.get('exchange_token') else None,
                    isin=instrument.get('isin'),
                    asset_symbol=instrument.get('asset_symbol'),
                    asset_type=instrument.get('asset_type'),
                    underlying_symbol=instrument.get('underlying_symbol'),
                    underlying_type=instrument.get('underlying_type'),
                    underlying_key=instrument.get('underlying_key'),
                    asset_key=instrument.get('asset_key'),
                    strike_price=float(instrument.get('strike_price')) if instrument.get('strike_price') is not None else None,
                    expiry=expiry,
                    weekly=instrument.get('weekly', False),
                    last_trading_date=last_trading_date,
                    lot_size=int(instrument.get('lot_size')) if instrument.get('lot_size') is not None else None,
                    minimum_lot=int(instrument.get('minimum_lot')) if instrument.get('minimum_lot') is not None else None,
                    tick_size=float(instrument.get('tick_size')) if instrument.get('tick_size') is not None else None,
                    qty_multiplier=float(instrument.get('qty_multiplier')) if instrument.get('qty_multiplier') is not None else None,
                    freeze_quantity=float(instrument.get('freeze_quantity')) if instrument.get('freeze_quantity') is not None else None,
                    price_quote_unit=instrument.get('price_quote_unit'),
                    security_type=instrument.get('security_type'),
                    short_name=instrument.get('short_name')
                )
                
                db.add(upstox_instrument)
                inserted += 1
                
                # Commit in batches
                if inserted % batch_size == 0:
                    db.commit()
                    print(f"  ✅ Inserted {inserted} records... (skipped: {skipped}, errors: {errors})")
                    
            except Exception as e:
                errors += 1
                print(f"  ❌ Error processing instrument {i+1}: {str(e)}")
                if errors > 10:
                    print("  ⚠️  Too many errors, stopping...")
                    break
                continue
        
        # Final commit
        db.commit()
        
        print(f"\n✅ Migration complete!")
        print(f"   Inserted: {inserted}")
        print(f"   Skipped: {skipped}")
        print(f"   Errors: {errors}")
        
        # Verify
        total_count = db.query(UpstoxInstrument).count()
        print(f"   Total records in database: {total_count}")
        
        return True
        
    except Exception as e:
        print(f"❌ Migration failed: {str(e)}")
        import traceback
        traceback.print_exc()
        db.rollback()
        return False
    finally:
        db.close()

if __name__ == "__main__":
    import argparse
    
    parser = argparse.ArgumentParser(description='Migrate Upstox instruments from JSON to database')
    parser.add_argument('--json-file', type=str, help='Path to nse_instruments.json file')
    parser.add_argument('--force', action='store_true', help='Force migration (delete existing data without prompt)')
    
    args = parser.parse_args()
    
    if args.json_file:
        json_file_path = args.json_file
    else:
        json_file_path = None
    
    if args.force:
        # Force mode: delete existing data after ensuring table exists
        # Import model and create table first
        from models.trading import UpstoxInstrument
        create_tables()
        
        db = SessionLocal()
        try:
            try:
                existing_count = db.query(UpstoxInstrument).count()
                if existing_count > 0:
                    print(f"🗑️  Force mode: Deleting {existing_count} existing records...")
                    db.query(UpstoxInstrument).delete()
                    db.commit()
                    print("✅ Existing records deleted")
            except Exception:
                # Table doesn't exist yet, that's OK
                print("ℹ️  Table doesn't exist yet, will be created during migration")
        finally:
            db.close()
    
    success = migrate_instruments(json_file_path)
    sys.exit(0 if success else 1)

