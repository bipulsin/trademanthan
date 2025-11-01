from fastapi import APIRouter, Request, HTTPException, Depends
from fastapi.responses import JSONResponse, RedirectResponse
from datetime import datetime
from typing import List, Dict, Any, Optional
from pydantic import BaseModel
from sqlalchemy.orm import Session
from sqlalchemy import and_, func, desc
import json
import os
import sys
import requests
import secrets

# Add services to path
sys.path.append(os.path.dirname(os.path.dirname(__file__)))
from services.upstox_service import upstox_service as vwap_service
from database import get_db
from models.trading import IntradayStockOption, MasterStock
from config import settings

router = APIRouter(prefix="/scan", tags=["scan"])

# In-memory storage for Bullish and Bearish webhook data
# Structure: { "date": "YYYY-MM-DD", "alerts": [list of alerts with timestamps] }
bullish_data = {"date": None, "alerts": []}
bearish_data = {"date": None, "alerts": []}

class ChartinkWebhookData(BaseModel):
    """Model for Chartink webhook data"""
    stocks: Optional[List[Dict[str, Any]]] = None
    scan_name: Optional[str] = None
    alert_name: Optional[str] = None
    triggered_at: Optional[str] = None
    trigger_prices: Optional[Dict[str, float]] = None
    
class StockAlert(BaseModel):
    """Model for individual stock alert"""
    stock_name: str
    trigger_price: float
    alert_name: str
    scan_name: str
    triggered_at: str

# Helper function to find strike from option chain based on volume and OI
def find_strike_from_option_chain(vwap_service, stock_name: str, option_type: str, stock_ltp: float) -> Optional[Dict]:
    """
    Find the best option strike from Upstox option chain based on volume and OI
    
    Args:
        vwap_service: UpstoxService instance
        stock_name: Stock symbol
        option_type: 'CE' or 'PE'
        stock_ltp: Current stock LTP
        
    Returns:
        Dict with strike_price, volume, oi, or None
    """
    try:
        # Get option chain from Upstox API
        option_chain = vwap_service.get_option_chain(stock_name)
        
        if not option_chain:
            print(f"No option chain data available for {stock_name}")
            return None
        
        # Parse option chain data
        # Upstox returns a list of strikes with call and put options
        strikes = []
        
        if isinstance(option_chain, list):
            for strike_data in option_chain:
                strike_price = strike_data.get('strike_price', 0)
                
                # Get option data based on option type
                if option_type == 'CE':
                    option_data = strike_data.get('call_options', {}).get('market_data', {})
                else:  # PE
                    option_data = strike_data.get('put_options', {}).get('market_data', {})
                
                if option_data:
                    strikes.append({
                        'strike_price': float(strike_price),
                        'volume': float(option_data.get('volume', 0)),
                        'oi': float(option_data.get('oi', 0)),
                        'ltp': float(option_data.get('ltp', 0))
                    })
        
        if not strikes:
            print(f"No {option_type} options found in chain for {stock_name}")
            return None
        
        # For OTM options:
        # CE (Call): Strike > LTP
        # PE (Put): Strike < LTP
        otm_strikes = []
        for strike in strikes:
            if option_type == 'CE' and strike['strike_price'] > stock_ltp:
                otm_strikes.append(strike)
            elif option_type == 'PE' and strike['strike_price'] < stock_ltp:
                otm_strikes.append(strike)
        
        if not otm_strikes:
            print(f"No OTM {option_type} strikes found for {stock_name}")
            return None
        
        # Sort by volume * OI (combined metric for liquidity)
        otm_strikes.sort(key=lambda x: (x['volume'] * x['oi']), reverse=True)
        
        # Get top 3 most liquid strikes
        top_strikes = otm_strikes[:3]
        
        print(f"Top 3 most liquid {option_type} strikes for {stock_name}:")
        for i, strike in enumerate(top_strikes, 1):
            print(f"  {i}. Strike: {strike['strike_price']}, Vol: {strike['volume']}, OI: {strike['oi']}, Score: {strike['volume'] * strike['oi']}")
        
        # For OTM+2, we want the strike that's 2nd most liquid among OTM options
        # Index [1] gives us the 2nd item (0-based indexing: 0=most, 1=2nd, 2=3rd)
        selected = top_strikes[1] if len(top_strikes) > 1 else top_strikes[0]
        
        print(f"✅ Selected {option_type} strike: {selected['strike_price']} (Volume: {selected['volume']}, OI: {selected['oi']})")
        print(f"   This is the 2nd most liquid OTM strike with good trading activity")
        return selected
        
    except Exception as e:
        print(f"Error fetching option chain for {stock_name}: {str(e)}")
        return None

# Helper function to process webhook data
def find_option_contract_from_master_stock(db: Session, stock_name: str, option_type: str, stock_ltp: float, vwap_service=None) -> Optional[str]:
    """
    Find the correct option contract from master_stock table based on:
    - underlying_symbol matching stock_name
    - option_type matching (CE/PE)
    - Strike price from option chain API (volume/OI based) or calculated fallback
    
    Args:
        db: Database session
        stock_name: Stock symbol (e.g., 'RELIANCE')
        option_type: Option type ('CE' or 'PE')
        stock_ltp: Current stock LTP price
        vwap_service: UpstoxService instance for API calls
        
    Returns:
        symbol_name from master_stock table, or None if not found
    """
    try:
        # Try to get strike from option chain API first
        target_strike = None
        if vwap_service:
            strike_data = find_strike_from_option_chain(vwap_service, stock_name, option_type, stock_ltp)
            if strike_data:
                target_strike = strike_data['strike_price']
                print(f"Using option chain strike for {stock_name}: {target_strike} (Volume: {strike_data['volume']}, OI: {strike_data['oi']})")
        
        # Fallback to calculated strike if option chain not available
        if target_strike is None or target_strike == 0:
            print(f"Falling back to calculated strike for {stock_name}")
            # Calculate appropriate strike interval based on stock price
            if stock_ltp < 500:
                strike_interval = 10
            elif stock_ltp < 2000:
                strike_interval = 5
            else:
                strike_interval = 10
            
            # Calculate 2nd OTM strike
            if option_type == 'CE':
                target_strike = round(stock_ltp / strike_interval) * strike_interval + (2 * strike_interval)
            else:  # PE
                target_strike = round(stock_ltp / strike_interval) * strike_interval - (2 * strike_interval)
        
        print(f"Looking for {option_type} option with strike {target_strike} for {stock_name}")
        
        # Query master_stock table
        option_record = db.query(MasterStock).filter(
            and_(
                MasterStock.underlying_symbol == stock_name,
                MasterStock.option_type == option_type,
                MasterStock.strike_price == target_strike,
                MasterStock.expiry_flag == 'M'  # Monthly expiry
            )
        ).first()
        
        if option_record:
            print(f"Found exact option contract: {option_record.symbol_name}")
            return option_record.symbol_name
        
        # If exact strike not found, find the closest available strike
        print(f"Exact strike {target_strike} not found, looking for closest available strike")
        
        if option_type == 'CE':
            # For CE, find the closest strike >= target_strike
            closest_record = db.query(MasterStock).filter(
                and_(
                    MasterStock.underlying_symbol == stock_name,
                    MasterStock.option_type == option_type,
                    MasterStock.strike_price >= target_strike,
                    MasterStock.expiry_flag == 'M'
                )
            ).order_by(MasterStock.strike_price.asc()).first()
            
            # If no strike >= target found, get the highest available strike
            if not closest_record:
                print(f"No strike >= {target_strike} found, getting highest available strike")
                closest_record = db.query(MasterStock).filter(
                    and_(
                        MasterStock.underlying_symbol == stock_name,
                        MasterStock.option_type == option_type,
                        MasterStock.expiry_flag == 'M'
                    )
                ).order_by(MasterStock.strike_price.desc()).first()
        else:  # PE
            # For PE, find the closest strike <= target_strike
            closest_record = db.query(MasterStock).filter(
                and_(
                    MasterStock.underlying_symbol == stock_name,
                    MasterStock.option_type == option_type,
                    MasterStock.strike_price <= target_strike,
                    MasterStock.expiry_flag == 'M'
                )
            ).order_by(MasterStock.strike_price.desc()).first()
            
            # If no strike <= target found, get the lowest available strike
            if not closest_record:
                print(f"No strike <= {target_strike} found, getting lowest available strike")
                closest_record = db.query(MasterStock).filter(
                    and_(
                        MasterStock.underlying_symbol == stock_name,
                        MasterStock.option_type == option_type,
                        MasterStock.expiry_flag == 'M'
                    )
                ).order_by(MasterStock.strike_price.asc()).first()
        
        if closest_record:
            print(f"Found closest option contract: {closest_record.symbol_name} (strike: {closest_record.strike_price})")
            return closest_record.symbol_name
        else:
            print(f"No option contract found for {stock_name} {option_type} (target strike: {target_strike})")
            return None
            
    except Exception as e:
        print(f"Error finding option contract for {stock_name}: {str(e)}")
        return None


async def process_webhook_data(data: dict, db: Session, forced_type: str = None):
    """
    Process webhook data and store in database and in-memory cache
    
    Args:
        data: Raw webhook data from Chartink
        db: Database session
        forced_type: 'bullish' or 'bearish' to force the type, or None for auto-detection
    
    Returns:
        JSONResponse with status
    """
    global bullish_data, bearish_data
    
    try:
        print(f"Processing webhook data (forced_type={forced_type}): {json.dumps(data, indent=2)}")
        
        # Parse triggered_at time and combine with the last trading date
        import pytz
        from dateutil import parser
        
        ist = pytz.timezone('Asia/Kolkata')
        now = datetime.now(ist)
        triggered_at_raw = data.get("triggered_at", "")
        
        # For intraday alerts, use today if it's a trading day, otherwise get last trading date
        today = now.replace(hour=0, minute=0, second=0, microsecond=0)
        if vwap_service.is_trading_day(today):
            trading_date = today
        else:
            trading_date = vwap_service.get_last_trading_date(now)
        
        print(f"Current date: {now.strftime('%Y-%m-%d %A')}")
        print(f"Last trading date: {trading_date.strftime('%Y-%m-%d %A')}")
        
        # Try to parse the time and create a proper datetime
        try:
            if triggered_at_raw:
                # Parse the time from Chartink
                parsed_time = parser.parse(triggered_at_raw, fuzzy=True)
                
                # Map to correct Chartink schedule times based on the parsed time
                hour = parsed_time.hour
                minute = parsed_time.minute
                
                # Determine which Chartink schedule time this maps to
                if hour < 10 or (hour == 10 and minute <= 15):
                    corrected_time = "10:15 AM"
                    corrected_hour, corrected_minute = 10, 15
                elif hour < 11 or (hour == 11 and minute <= 15):
                    corrected_time = "11:15 AM"
                    corrected_hour, corrected_minute = 11, 15
                elif hour < 12 or (hour == 12 and minute <= 15):
                    corrected_time = "12:15 PM"
                    corrected_hour, corrected_minute = 12, 15
                elif hour < 13 or (hour == 13 and minute <= 15):
                    corrected_time = "1:15 PM"
                    corrected_hour, corrected_minute = 13, 15
                else:
                    corrected_time = "2:15 PM"
                    corrected_hour, corrected_minute = 14, 15
                
                # Create the corrected datetime directly
                triggered_datetime = trading_date.replace(
                    hour=corrected_hour,
                    minute=corrected_minute,
                    second=0,
                    microsecond=0
                )
                triggered_at_str = triggered_datetime.isoformat()
                triggered_at_display = corrected_time
                print(f"Original time: {triggered_at_raw} -> Corrected to: {corrected_time}")
                print(f"Triggered at: {triggered_datetime.strftime('%Y-%m-%d %H:%M:%S %A')}")
            else:
                # Default to first Chartink time if no time provided
                triggered_datetime = trading_date.replace(hour=10, minute=15, second=0, microsecond=0)
                triggered_at_str = triggered_datetime.isoformat()
                triggered_at_display = "10:15 AM"
        except Exception as e:
            print(f"Error parsing triggered_at '{triggered_at_raw}': {e}")
            # Default to first Chartink time on error
            triggered_datetime = trading_date.replace(hour=10, minute=15, second=0, microsecond=0)
            triggered_at_str = triggered_datetime.isoformat()
            triggered_at_display = "10:15 AM"
        
        # Process the data into a standardized format
        processed_data = {
            "scan_name": data.get("scan_name", "Unknown Scan"),
            "scan_url": data.get("scan_url", ""),
            "alert_name": data.get("alert_name", "Alert"),
            "triggered_at": triggered_at_str,
            "triggered_at_time": triggered_at_display,
            "received_at": datetime.now(ist).isoformat(),
            "stocks": []
        }
        
        # Handle different possible data formats
        stocks = data.get("stocks", "")
        trigger_prices = data.get("trigger_prices", "")
        
        # Chartink format: comma-separated strings
        if isinstance(stocks, str) and isinstance(trigger_prices, str):
            stock_list = [s.strip() for s in stocks.split(",") if s.strip()]
            price_list = [p.strip() for p in trigger_prices.split(",") if p.strip()]
            
            for i, stock_name in enumerate(stock_list):
                try:
                    price = float(price_list[i]) if i < len(price_list) else 0.0
                except (ValueError, IndexError):
                    price = 0.0
                
                stock_data = {
                    "stock_name": stock_name,
                    "trigger_price": price
                }
                processed_data["stocks"].append(stock_data)
        
        # Legacy format 1: stocks as list, trigger_prices as dict
        elif isinstance(stocks, list) and isinstance(trigger_prices, dict):
            for stock in stocks:
                if isinstance(stock, str):
                    stock_data = {
                        "stock_name": stock,
                        "trigger_price": trigger_prices.get(stock, 0.0)
                    }
                elif isinstance(stock, dict):
                    stock_data = {
                        "stock_name": stock.get("name", stock.get("stock_name", "Unknown")),
                        "trigger_price": stock.get("trigger_price", stock.get("price", 0.0))
                    }
                else:
                    continue
                processed_data["stocks"].append(stock_data)
        
        # Legacy format 2: stocks as dict
        elif isinstance(stocks, dict):
            for stock_name, price in stocks.items():
                stock_data = {
                    "stock_name": stock_name,
                    "trigger_price": float(price) if price else 0.0
                }
                processed_data["stocks"].append(stock_data)
        
        # Legacy format 3: only trigger_prices provided
        elif not stocks and isinstance(trigger_prices, dict):
            for stock_name, price in trigger_prices.items():
                stock_data = {
                    "stock_name": stock_name,
                    "trigger_price": float(price) if price else 0.0
                }
                processed_data["stocks"].append(stock_data)
        
        # Determine if this is Bullish or Bearish
        if forced_type:
            # Use forced type from endpoint
            is_bullish = (forced_type.lower() == 'bullish')
            is_bearish = (forced_type.lower() == 'bearish')
            print(f"Using forced type: {forced_type}")
        else:
            # Auto-detect from alert/scan name
            alert_name = processed_data.get("alert_name", "").lower()
            scan_name = processed_data.get("scan_name", "").lower()
            
            is_bullish = "bullish" in alert_name or "bullish" in scan_name
            is_bearish = "bearish" in alert_name or "bearish" in scan_name
            
            if not is_bullish and not is_bearish:
                # Default to bullish if not specified
                is_bullish = True
                print(f"Alert type not specified - defaulting to Bullish")
        
        # Force option type based on alert type
        forced_option_type = 'CE' if is_bullish else 'PE'
        print(f"Processing {len(processed_data['stocks'])} stocks with option type: {forced_option_type}")
        print(f"Alert name: {processed_data.get('alert_name', '')}")
        
        # Process each stock individually to fetch LTP and find option contract
        enriched_stocks = []
        for stock in processed_data["stocks"]:
            stock_name = stock.get("stock_name", "")
            trigger_price = stock.get("trigger_price", 0.0)
            
            print(f"Processing stock: {stock_name}")
            
            # Fetch LTP from Upstox API using market-quote/ltp endpoint
            stock_ltp = vwap_service.get_stock_ltp_from_market_quote(stock_name)
            
            if not stock_ltp or stock_ltp == 0:
                print(f"Could not fetch LTP for {stock_name}, using trigger price")
                stock_ltp = trigger_price
            
            print(f"Stock LTP for {stock_name}: ₹{stock_ltp}")
            
            # Fetch Stock VWAP using Upstox historical candles
            stock_vwap = 0.0
            try:
                stock_vwap = vwap_service.get_stock_vwap(stock_name)
                if stock_vwap > 0:
                    print(f"Stock VWAP for {stock_name}: ₹{stock_vwap}")
                else:
                    print(f"Could not fetch VWAP for {stock_name}")
            except Exception as e:
                print(f"Error fetching VWAP for {stock_name}: {str(e)}")
            
            # Find option contract from master_stock table
            option_contract = find_option_contract_from_master_stock(
                db, stock_name, forced_option_type, stock_ltp, vwap_service
            )
            
            # Extract option strike from option contract if available
            option_strike = 0.0
            qty = 0
            option_ltp = 0.0
            if option_contract:
                import re
                # Extract strike from format: STOCK-Nov2025-STRIKE-CE/PE
                match = re.search(r'-(\d+\.?\d*)-(?:CE|PE)$', option_contract)
                if match:
                    option_strike = float(match.group(1))
                    print(f"Extracted option strike: {option_strike} from {option_contract}")
                
                # Fetch lot_size and security_id from master_stock table
                try:
                    master_record = db.query(MasterStock).filter(
                        and_(
                            MasterStock.symbol_name == option_contract
                        )
                    ).first()
                    
                    if master_record:
                        if master_record.lot_size:
                            qty = int(master_record.lot_size)
                            print(f"Fetched lot_size for {option_contract}: {qty}")
                        
                        # Fetch option LTP using instruments JSON
                        try:
                            # Load instruments JSON
                            from pathlib import Path
                            import json as json_lib
                            
                            instruments_file = Path("/home/ubuntu/trademanthan/data/instruments/nse_instruments.json")
                            
                            if instruments_file.exists():
                                with open(instruments_file, 'r') as f:
                                    instruments_data = json_lib.load(f)
                                
                                # Find the option contract in instruments data
                                # Parse option contract format: STOCK-Nov2025-STRIKE-CE/PE
                                # Example: IDFCFIRSTB-Nov2025-85-CE
                                import re
                                from datetime import datetime
                                
                                # Handle stocks with hyphens in symbol (e.g., BAJAJ-AUTO)
                                match = re.match(r'^([A-Z-]+)-(\w{3})(\d{4})-(\d+\.?\d*?)-(CE|PE)$', option_contract)
                                
                                instrument_key = None
                                
                                if match:
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
                                    for inst in instruments_data:
                                        if (inst.get('underlying_symbol') == symbol and 
                                            inst.get('instrument_type') == opt_type and
                                            inst.get('segment') == 'NSE_FO'):
                                            # Check if strike matches
                                            inst_strike = inst.get('strike_price', 0)
                                            if abs(inst_strike - strike_value) < 1:  # Allow small float differences
                                                # Check expiry date
                                                expiry_ms = inst.get('expiry', 0)
                                                if expiry_ms:
                                                    inst_expiry = datetime.fromtimestamp(expiry_ms/1000)
                                                    if inst_expiry.year == target_year and inst_expiry.month == target_month:
                                                        instrument_key = inst.get('instrument_key')
                                                        print(f"Found matching option: {inst.get('trading_symbol')} (strike: {inst_strike}, expiry: {inst_expiry.strftime('%d %b %Y')})")
                                                        break
                                
                                if instrument_key:
                                    print(f"Found instrument key for {option_contract}: {instrument_key}")
                                    
                                    # Fetch market quote using the instrument key
                                    if vwap_service:
                                        quote_data = vwap_service.get_market_quote_by_key(instrument_key)
                                        if quote_data and quote_data.get('last_price'):
                                            option_ltp = float(quote_data.get('last_price', 0))
                                            print(f"✅ Fetched option LTP for {option_contract}: ₹{option_ltp}")
                                        else:
                                            print(f"Could not fetch option LTP for {option_contract} - no quote data")
                                    else:
                                        print(f"vwap_service not available")
                                else:
                                    print(f"Could not find instrument key for {option_contract} in instruments JSON")
                            else:
                                print(f"Instruments JSON file not found")
                        except Exception as ltp_error:
                            print(f"Error fetching option LTP from instruments JSON: {str(ltp_error)}")
                            import traceback
                            traceback.print_exc()
                    else:
                        print(f"Could not find master record for {option_contract}")
                except Exception as e:
                    print(f"Error fetching lot_size/option_ltp: {str(e)}")
            
            # Create enriched stock data
            enriched_stock = {
                "stock_name": stock_name,
                "trigger_price": trigger_price,
                "last_traded_price": stock_ltp,
                "stock_vwap": stock_vwap,  # Fetched from Yahoo VWAP service
                "option_type": forced_option_type,
                "option_contract": option_contract or "",
                "otm1_strike": option_strike,  # Extract strike from option contract
                "option_ltp": option_ltp,  # Fetched from Upstox API
                "option_vwap": 0.0,  # Not used in new approach
                "qty": qty  # Add qty from lot_size
            }
            
            enriched_stocks.append(enriched_stock)
            print(f"Enriched stock: {stock_name} - LTP: ₹{stock_ltp}, Option: {option_contract}")
        
        processed_data["stocks"] = enriched_stocks
        print(f"Successfully processed {len(enriched_stocks)} stocks")
        
        # Get current date for grouping
        current_date = trading_date.strftime('%Y-%m-%d')
        
        # Determine which data store to use
        target_data = bullish_data if is_bullish else bearish_data
        data_type = "Bullish" if is_bullish else "Bearish"
        
        # Check if this is a new date - if so, clear old data
        if target_data["date"] != current_date:
            print(f"New trading date detected for {data_type}: {current_date} (previous: {target_data['date']})")
            target_data["date"] = current_date
            target_data["alerts"] = []
        
        # Save each stock to database
        saved_count = 0
        for stock in processed_data.get("stocks", []):
            try:
                # Get option_ltp value
                option_ltp_value = stock.get("option_ltp", 0.0)
                
                db_record = IntradayStockOption(
                    alert_time=triggered_datetime,
                    alert_type=data_type,
                    scan_name=processed_data.get("scan_name", ""),
                    stock_name=stock.get("stock_name", ""),
                    stock_ltp=stock.get("last_traded_price") or stock.get("trigger_price"),
                    stock_vwap=stock.get("stock_vwap", 0.0),
                    option_contract=stock.get("option_contract", ""),
                    option_type=stock.get("option_type", ""),
                    option_strike=stock.get("otm1_strike"),
                    option_ltp=option_ltp_value,
                    option_vwap=stock.get("option_vwap"),
                    qty=stock.get("qty", 0),
                    trade_date=trading_date,
                    status='alert_received',
                    # Set buy_price and sell_price to option_ltp on first webhook
                    buy_price=option_ltp_value,
                    sell_price=option_ltp_value,
                    buy_time=triggered_datetime,  # Set buy_time to alert_time
                    # PnL is 0 initially (buy_price = sell_price)
                    pnl=0.0
                )
                db.add(db_record)
                saved_count += 1
            except Exception as db_error:
                print(f"Error saving stock {stock.get('stock_name')} to database: {str(db_error)}")
        
        # Commit all database records
        try:
            db.commit()
            print(f"Saved {saved_count} stocks to database for {data_type} alert")
        except Exception as commit_error:
            print(f"Error committing to database: {str(commit_error)}")
            db.rollback()
        
        # Add this alert to the beginning of the list (newest first) - in-memory cache
        target_data["alerts"].insert(0, processed_data)
        
        # Keep only last 50 alerts per section to prevent memory issues
        target_data["alerts"] = target_data["alerts"][:50]
        
        print(f"Stored {data_type} alert in memory. Total {data_type} alerts for {current_date}: {len(target_data['alerts'])}")
        
        # Save to file as backup
        data_dir = os.path.join(os.path.dirname(__file__), "..", "scan_data")
        os.makedirs(data_dir, exist_ok=True)
        
        # Save bullish and bearish data separately
        with open(os.path.join(data_dir, "bullish_data.json"), "w") as f:
            json.dump(bullish_data, f, indent=2)
        
        with open(os.path.join(data_dir, "bearish_data.json"), "w") as f:
            json.dump(bearish_data, f, indent=2)
        
        return JSONResponse(
            status_code=200,
            content={
                "status": "success",
                "message": f"{data_type} webhook data received and processed",
                "alert_type": data_type.lower(),
                "stocks_count": len(processed_data["stocks"]),
                "timestamp": processed_data["received_at"],
                "date": current_date
            }
        )
        
    except Exception as e:
        print(f"Error processing webhook: {str(e)}")
        import traceback
        traceback.print_exc()
        return JSONResponse(
            status_code=500,
            content={
                "status": "error",
                "message": f"Failed to process webhook: {str(e)}"
            }
        )

@router.post("/chartink-webhook-bullish")
async def receive_bullish_webhook(request: Request, db: Session = Depends(get_db)):
    """
    Dedicated endpoint for Bullish alerts from Chartink.com
    All alerts received here will be treated as BULLISH with CALL options.
    
    Expected JSON format from Chartink:
    {
        "stocks": "SEPOWER,ASTEC,EDUCOMP,KSERASERA,IOLCP,GUJAPOLLO,EMCO",
        "trigger_prices": "3.75,541.8,2.1,0.2,329.6,166.8,1.25",
        "triggered_at": "2:34 pm",
        "scan_name": "Bullish Breakout",
        "scan_url": "bullish-breakout",
        "alert_name": "Alert for Bullish Breakout"
    }
    """
    data = await request.json()
    return await process_webhook_data(data, db, forced_type='bullish')

@router.post("/chartink-webhook-bearish")
async def receive_bearish_webhook(request: Request, db: Session = Depends(get_db)):
    """
    Dedicated endpoint for Bearish alerts from Chartink.com
    All alerts received here will be treated as BEARISH with PUT options.
    
    Expected JSON format from Chartink:
    {
        "stocks": "SEPOWER,ASTEC,EDUCOMP,KSERASERA,IOLCP,GUJAPOLLO,EMCO",
        "trigger_prices": "3.75,541.8,2.1,0.2,329.6,166.8,1.25",
        "triggered_at": "2:34 pm",
        "scan_name": "Bearish Breakdown",
        "scan_url": "bearish-breakdown",
        "alert_name": "Alert for Bearish Breakdown"
    }
    """
    data = await request.json()
    return await process_webhook_data(data, db, forced_type='bearish')

@router.post("/chartink-webhook")
async def receive_chartink_webhook(request: Request, db: Session = Depends(get_db)):
    """
    Original webhook endpoint with auto-detection (for backward compatibility)
    
    Auto-detects if alert is Bullish or Bearish based on alert/scan name.
    Alert name should contain "Bullish" or "Bearish" to determine the section.
    If not specified, defaults to Bullish.
    
    For explicit control, use:
    - /scan/chartink-webhook-bullish for Bullish alerts
    - /scan/chartink-webhook-bearish for Bearish alerts
    
    Expected JSON format from Chartink:
    {
        "stocks": "SEPOWER,ASTEC,EDUCOMP,KSERASERA,IOLCP,GUJAPOLLO,EMCO",
        "trigger_prices": "3.75,541.8,2.1,0.2,329.6,166.8,1.25",
        "triggered_at": "2:34 pm",
        "scan_name": "Bullish Intraday Stock",
        "scan_url": "bullish-intraday-stock",
        "alert_name": "Alert for Bullish Intraday Stock"
    }
    """
    data = await request.json()
    return await process_webhook_data(data, db, forced_type=None)  # Auto-detect

@router.get("/latest")
async def get_latest_webhook_data(db: Session = Depends(get_db)):
    """
    Get the latest webhook data for both Bullish and Bearish sections from database
    Includes index trend check to determine if trading is allowed
    """
    import pytz
    
    try:
        ist = pytz.timezone('Asia/Kolkata')
        now = datetime.now(ist)
        
        # Calculate date filter based on current time
        # After 9:00 AM IST, only show today's data
        # Before 9:00 AM IST, show yesterday's data (yesterday's alerts are still relevant)
        current_hour = now.hour
        current_minute = now.minute
        
        # Calculate today's date
        today = now.replace(hour=0, minute=0, second=0, microsecond=0)
        
        # If it's after 9:00 AM IST, only show today's data
        # If it's before 9:00 AM IST, show yesterday's data
        if current_hour > 9 or (current_hour == 9 and current_minute >= 0):
            # After 9:00 AM - show only today's data
            filter_date = today
            print(f"Current time: {now.strftime('%Y-%m-%d %H:%M:%S')} IST - Showing TODAY's data (after 9:00 AM)")
        else:
            # Before 9:00 AM - show yesterday's data
            from datetime import timedelta
            filter_date = today - timedelta(days=1)
            print(f"Current time: {now.strftime('%Y-%m-%d %H:%M:%S')} IST - Showing YESTERDAY's data (before 9:00 AM)")
        
        # For intraday alerts, use today if it's a trading day, otherwise get last trading date
        if vwap_service.is_trading_day(today):
            trading_date = today
        else:
            trading_date = vwap_service.get_last_trading_date(now)
        current_date = trading_date.strftime('%Y-%m-%d')
        
        # Fetch Bullish alerts from database for the current trading day only
        bullish_records = db.query(IntradayStockOption).filter(
            IntradayStockOption.alert_type == 'Bullish',
            IntradayStockOption.trade_date == filter_date
        ).order_by(desc(IntradayStockOption.alert_time)).limit(200).all()
        
        # Fetch Bearish alerts from database for the current trading day only
        bearish_records = db.query(IntradayStockOption).filter(
            IntradayStockOption.alert_type == 'Bearish',
            IntradayStockOption.trade_date == filter_date
        ).order_by(desc(IntradayStockOption.alert_time)).limit(200).all()
        
        # Group records by alert_time for Bullish
        bullish_alerts = []
        if bullish_records:
            grouped_bullish = {}
            for record in bullish_records:
                # Database now stores IST timestamps directly, no conversion needed
                alert_time_ist = record.alert_time
                alert_key = alert_time_ist.isoformat()
                if alert_key not in grouped_bullish:
                    grouped_bullish[alert_key] = {
                        "scan_name": record.scan_name or "Unknown Scan",
                        "alert_name": f"Bullish Alert",
                        "triggered_at": alert_time_ist.isoformat(),
                        "received_at": record.created_date_time.isoformat(),
                        "stocks": []
                    }
                
                grouped_bullish[alert_key]["stocks"].append({
                    "stock_name": record.stock_name,
                    "trigger_price": record.stock_ltp or 0.0,
                    "last_traded_price": record.stock_ltp or 0.0,
                    "stock_vwap": record.stock_vwap or 0.0,
                    "option_contract": record.option_contract or "",
                    "option_type": record.option_type or "CE",
                    "otm1_strike": record.option_strike or 0.0,
                    "option_ltp": record.option_ltp or 0.0,
                    "option_vwap": record.option_vwap or 0.0,
                    "qty": record.qty or 0,
                    "buy_price": record.buy_price or 0.0,
                    "sell_price": record.sell_price or 0.0,
                    "pnl": record.pnl or 0.0
                })
            
            bullish_alerts = list(grouped_bullish.values())
        
        # Group records by alert_time for Bearish
        bearish_alerts = []
        if bearish_records:
            grouped_bearish = {}
            for record in bearish_records:
                # Database now stores IST timestamps directly, no conversion needed
                alert_time_ist = record.alert_time
                alert_key = alert_time_ist.isoformat()
                if alert_key not in grouped_bearish:
                    grouped_bearish[alert_key] = {
                        "scan_name": record.scan_name or "Unknown Scan",
                        "alert_name": f"Bearish Alert",
                        "triggered_at": alert_time_ist.isoformat(),
                        "received_at": record.created_date_time.isoformat(),
                        "stocks": []
                    }
                
                grouped_bearish[alert_key]["stocks"].append({
                    "stock_name": record.stock_name,
                    "trigger_price": record.stock_ltp or 0.0,
                    "last_traded_price": record.stock_ltp or 0.0,
                    "stock_vwap": record.stock_vwap or 0.0,
                    "option_contract": record.option_contract or "",
                    "option_type": record.option_type or "PE",
                    "otm1_strike": record.option_strike or 0.0,
                    "option_ltp": record.option_ltp or 0.0,
                    "option_vwap": record.option_vwap or 0.0,
                    "qty": record.qty or 0,
                    "buy_price": record.buy_price or 0.0,
                    "sell_price": record.sell_price or 0.0,
                    "pnl": record.pnl or 0.0
                })
            
            bearish_alerts = list(grouped_bearish.values())
        
        # Structure data in the expected format
        bullish_data = {
            "date": current_date,
            "alerts": bullish_alerts
        }
        
        bearish_data = {
            "date": current_date,
            "alerts": bearish_alerts
        }
        
        # Check index trends before returning data
        index_check = vwap_service.check_index_trends()
        
        return JSONResponse(
            status_code=200,
            content={
                "status": "success",
                "data": {
                    "bullish": bullish_data,
                    "bearish": bearish_data,
                    "index_check": index_check,
                    "allow_trading": index_check['allow_trading']
                }
            }
        )
        
    except Exception as e:
        print(f"Error fetching latest data from database: {str(e)}")
        import traceback
        traceback.print_exc()
        
        return JSONResponse(
            status_code=500,
            content={
                "status": "error",
                "message": f"Failed to fetch data: {str(e)}"
            }
        )

@router.post("/refresh-hourly")
async def refresh_hourly_prices(db: Session = Depends(get_db)):
    """
    Refresh option_ltp and sell_price hourly for existing records
    Only updates sell_price, buy_price remains unchanged (historical)
    """
    try:
        import pytz
        from datetime import datetime
        
        ist = pytz.timezone('Asia/Kolkata')
        now = datetime.now(ist)
        today = now.replace(hour=0, minute=0, second=0, microsecond=0)
        
        # Get all records for today
        records = db.query(IntradayStockOption).filter(
            IntradayStockOption.trade_date == today,
            IntradayStockOption.option_contract.isnot(None)
        ).all()
        
        updated_count = 0
        failed_count = 0
        
        print(f"Refreshing {len(records)} records...")
        
        for record in records:
            try:
                # Load instruments JSON if needed
                from pathlib import Path
                import json as json_lib
                
                instruments_file = Path("/home/ubuntu/trademanthan/data/instruments/nse_instruments.json")
                
                if not instruments_file.exists():
                    print(f"Instruments JSON not found")
                    continue
                
                with open(instruments_file, 'r') as f:
                    instruments_data = json_lib.load(f)
                
                # Find instrument_key for the option
                option_contract = record.option_contract
                if not option_contract:
                    continue
                
                import re
                from datetime import datetime
                
                # Parse option contract: STOCK-Nov2025-STRIKE-CE/PE
                match = re.match(r'^([A-Z-]+)-(\w{3})(\d{4})-(\d+\.?\d*?)-(CE|PE)$', option_contract)
                
                if match:
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
                    
                    # Find instrument_key
                    instrument_key = None
                    for inst in instruments_data:
                        if (inst.get('underlying_symbol') == symbol and 
                            inst.get('instrument_type') == opt_type and
                            inst.get('segment') == 'NSE_FO'):
                            inst_strike = inst.get('strike_price', 0)
                            if abs(inst_strike - strike_value) < 1:
                                expiry_ms = inst.get('expiry', 0)
                                if expiry_ms:
                                    inst_expiry = datetime.fromtimestamp(expiry_ms/1000)
                                    if inst_expiry.year == target_year and inst_expiry.month == target_month:
                                        instrument_key = inst.get('instrument_key')
                                        break
                    
                    # Fetch option LTP
                    if instrument_key:
                        quote_data = vwap_service.get_market_quote_by_key(instrument_key)
                        if quote_data and quote_data.get('last_price'):
                            new_option_ltp = float(quote_data.get('last_price', 0))
                            
                            # Update option_ltp and sell_price only
                            record.option_ltp = new_option_ltp
                            record.sell_price = new_option_ltp
                            record.sell_time = now
                            
                            # Calculate and update PnL
                            if record.buy_price and record.qty:
                                record.pnl = (new_option_ltp - record.buy_price) * record.qty
                            
                            updated_count += 1
                            print(f"✅ Updated {record.stock_name}: option_ltp=₹{new_option_ltp}, PnL=₹{record.pnl}")
                        else:
                            print(f"❌ Could not fetch LTP for {option_contract}")
                            failed_count += 1
                    else:
                        print(f"❌ Could not find instrument key for {option_contract}")
                        failed_count += 1
            except Exception as e:
                print(f"❌ Error processing {record.stock_name}: {str(e)}")
                failed_count += 1
        
        db.commit()
        
        return JSONResponse(
            status_code=200,
            content={
                "status": "success",
                "message": f"Hourly refresh completed",
                "updated": updated_count,
                "failed": failed_count
            }
        )
        
    except Exception as e:
        db.rollback()
        return JSONResponse(
            status_code=500,
            content={
                "status": "error",
                "message": f"Failed to refresh: {str(e)}"
            }
        )

@router.post("/refresh-current-vwap")
async def refresh_current_vwap():
    """
    Refresh LTP and OTM-1 strike for all stocks in both Bullish and Bearish data
    Called every 5 minutes to update prices and strikes
    Stock list remains unchanged
    """
    global bullish_data, bearish_data
    
    has_bullish = len(bullish_data.get("alerts", [])) > 0
    has_bearish = len(bearish_data.get("alerts", [])) > 0
    
    if not has_bullish and not has_bearish:
        return JSONResponse(
            status_code=404,
            content={
                "status": "no_data",
                "message": "No stocks data to refresh"
            }
        )
    
    try:
        import pytz
        
        # Function to refresh stocks in an alert
        def refresh_stocks_in_alert(alert, forced_option_type):
            stocks = alert.get('stocks', [])
            for stock in stocks:
                stock_name = stock.get('stock_name', '')
                
                # Get current LTP
                ltp = vwap_service.get_current_ltp(stock_name)
                
                if not ltp or ltp == 0:
                    print(f"Could not get LTP for {stock_name}, keeping old values")
                    continue
                
                # Get VWAP for comparison
                vwap = vwap_service.get_simple_vwap(stock_name)
                
                # Use forced option type for consistency
                option_type = forced_option_type
                
                # Get OTM-1 strike based on forced option type
                otm1_strike = vwap_service.get_otm1_strike(stock_name, option_type=option_type, spot_price=ltp)
                
                # Get monthly expiry and format option contract
                expiry_date = vwap_service.get_monthly_expiry()
                
                # Format option contract string and fetch option data
                option_contract = ""
                option_ltp = 0.0
                option_vwap_val = 0.0
                
                if otm1_strike and otm1_strike > 0:
                    strike_int = int(otm1_strike)
                    option_text = "CALL" if option_type == 'CE' else "PUT"
                    expiry_str = expiry_date.strftime('%d %b').upper()
                    option_contract = f"{stock_name} {expiry_str} {strike_int} {option_text}"
                    
                    # Fetch option LTP and VWAP
                    option_ltp = vwap_service.get_option_ltp(stock_name, expiry_date, otm1_strike, option_type)
                    option_vwap_val = vwap_service.get_option_vwap(stock_name, expiry_date, otm1_strike, option_type)
                
                # Update all fields
                stock['last_traded_price'] = ltp
                stock['otm1_strike'] = otm1_strike if otm1_strike else stock.get('otm1_strike', 0.0)
                stock['option_type'] = option_type
                stock['vwap'] = vwap if vwap else stock.get('vwap', 0.0)
                stock['option_contract'] = option_contract
                stock['option_ltp'] = option_ltp if option_ltp else stock.get('option_ltp', 0.0)
                stock['option_vwap'] = option_vwap_val if option_vwap_val else stock.get('option_vwap', 0.0)
            
            # Update timestamp
            alert['last_updated'] = datetime.now(pytz.timezone('Asia/Kolkata')).isoformat()
        
        # Refresh bullish data (always CE)
        bullish_count = 0
        if has_bullish:
            print(f"Refreshing Bullish alerts...")
            for alert in bullish_data['alerts']:
                refresh_stocks_in_alert(alert, 'CE')
                bullish_count += len(alert.get('stocks', []))
        
        # Refresh bearish data (always PE)
        bearish_count = 0
        if has_bearish:
            print(f"Refreshing Bearish alerts...")
            for alert in bearish_data['alerts']:
                refresh_stocks_in_alert(alert, 'PE')
                bearish_count += len(alert.get('stocks', []))
        
        # Save to files
        data_dir = os.path.join(os.path.dirname(__file__), "..", "scan_data")
        os.makedirs(data_dir, exist_ok=True)
        
        with open(os.path.join(data_dir, "bullish_data.json"), "w") as f:
            json.dump(bullish_data, f, indent=2)
        
        with open(os.path.join(data_dir, "bearish_data.json"), "w") as f:
            json.dump(bearish_data, f, indent=2)
        
        print(f"Successfully refreshed LTP and option strikes")
        
        return JSONResponse(
            status_code=200,
            content={
                "status": "success",
                "message": "LTP and option strikes refreshed",
                "bullish_stocks_count": bullish_count,
                "bearish_stocks_count": bearish_count,
                "timestamp": datetime.now().isoformat()
            }
        )
        
    except Exception as e:
        print(f"Error refreshing LTP and strikes: {str(e)}")
        return JSONResponse(
            status_code=500,
            content={
                "status": "error",
                "message": f"Failed to refresh VWAP: {str(e)}"
            }
        )

@router.get("/index-prices")
async def get_index_prices():
    """
    Get current NIFTY and BANKNIFTY prices with trends using real-time market quotes
    """
    try:
        # Get real-time market quotes for indices using correct instrument keys
        nifty_quote = vwap_service.get_market_quote_by_key(vwap_service.NIFTY50_KEY)
        banknifty_quote = vwap_service.get_market_quote_by_key(vwap_service.BANKNIFTY_KEY)
        
        # Process NIFTY data
        nifty_data = {}
        nifty_trend = 'unknown'
        if nifty_quote and nifty_quote.get('last_price', 0) > 0:
            ltp = float(nifty_quote['last_price'])
            day_open = float(nifty_quote.get('ohlc', {}).get('open', 0))
            close_price = float(nifty_quote.get('close_price', ltp))
            
            # Use close price as day open if ohlc open is not available
            if day_open == 0:
                day_open = close_price
            
            nifty_data = {
                'ltp': ltp,
                'day_open': day_open,
                'last_price': ltp
            }
            
            # Determine trend
            if ltp > day_open:
                nifty_trend = 'bullish'
            elif ltp < day_open:
                nifty_trend = 'bearish'
            else:
                nifty_trend = 'neutral'
        
        # Process BANKNIFTY data
        banknifty_data = {}
        banknifty_trend = 'unknown'
        if banknifty_quote and banknifty_quote.get('last_price', 0) > 0:
            ltp = float(banknifty_quote['last_price'])
            day_open = float(banknifty_quote.get('ohlc', {}).get('open', 0))
            close_price = float(banknifty_quote.get('close_price', ltp))
            
            # Use close price as day open if ohlc open is not available
            if day_open == 0:
                day_open = close_price
            
            banknifty_data = {
                'ltp': ltp,
                'day_open': day_open,
                'last_price': ltp
            }
            
            # Determine trend
            if ltp > day_open:
                banknifty_trend = 'bullish'
            elif ltp < day_open:
                banknifty_trend = 'bearish'
            else:
                banknifty_trend = 'neutral'
        
        # Check if we have valid data
        if nifty_data and banknifty_data and nifty_data.get('ltp', 0) > 0 and banknifty_data.get('ltp', 0) > 0:
            return JSONResponse(
                status_code=200,
                content={
                    "status": "success",
                    "data": {
                        "nifty": {
                            "name": "NIFTY 50",
                            "ltp": nifty_data.get('ltp', 0),
                            "close_price": nifty_data.get('close_price', nifty_data.get('ltp', 0)),
                            "day_open": nifty_data.get('day_open', 0),
                            "trend": nifty_trend,
                            "change": nifty_data.get('ltp', 0) - nifty_data.get('day_open', 0),
                            "change_percent": ((nifty_data.get('ltp', 0) - nifty_data.get('day_open', 0)) / nifty_data.get('day_open', 1)) * 100 if nifty_data.get('day_open', 0) > 0 else 0,
                            "market_status": "Live Data"
                        },
                        "banknifty": {
                            "name": "BANKNIFTY",
                            "ltp": banknifty_data.get('ltp', 0),
                            "close_price": banknifty_data.get('close_price', banknifty_data.get('ltp', 0)),
                            "day_open": banknifty_data.get('day_open', 0),
                            "trend": banknifty_trend,
                            "change": banknifty_data.get('ltp', 0) - banknifty_data.get('day_open', 0),
                            "change_percent": ((banknifty_data.get('ltp', 0) - banknifty_data.get('day_open', 0)) / banknifty_data.get('day_open', 1)) * 100 if banknifty_data.get('day_open', 0) > 0 else 0,
                            "market_status": "Live Data"
                        },
                        "timestamp": datetime.now().isoformat(),
                        "data_source": "realtime",
                        "market_status": "open"
                    }
                }
            )
        else:
            # Fallback to historical data if real-time data is not available
            print("Real-time data not available, falling back to historical data")
            index_check_result = vwap_service.check_index_trends()
            
            nifty_data = index_check_result.get('nifty_data', {})
            banknifty_data = index_check_result.get('banknifty_data', {})
            nifty_trend = index_check_result.get('nifty_trend', 'unknown')
            banknifty_trend = index_check_result.get('banknifty_trend', 'unknown')
            
            # Add market status information
            market_status = "Market Closed - Closing Price"
            
            if nifty_data and banknifty_data and nifty_data.get('ltp', 0) > 0 and banknifty_data.get('ltp', 0) > 0:
                # Use closing price instead of LTP when market is closed
                nifty_close_price = nifty_data.get('ltp', 0)  # LTP becomes closing price
                banknifty_close_price = banknifty_data.get('ltp', 0)  # LTP becomes closing price
                
                return JSONResponse(
                    status_code=200,
                    content={
                        "status": "success",
                        "data": {
                            "nifty": {
                                "name": "NIFTY 50",
                                "ltp": nifty_close_price,
                                "close_price": nifty_close_price,
                                "day_open": nifty_data.get('day_open', 0),
                                "trend": nifty_trend,
                                "change": nifty_close_price - nifty_data.get('day_open', 0),
                                "change_percent": ((nifty_close_price - nifty_data.get('day_open', 0)) / nifty_data.get('day_open', 1)) * 100 if nifty_data.get('day_open', 0) > 0 else 0,
                                "market_status": market_status
                            },
                            "banknifty": {
                                "name": "BANKNIFTY",
                                "ltp": banknifty_close_price,
                                "close_price": banknifty_close_price,
                                "day_open": banknifty_data.get('day_open', 0),
                                "trend": banknifty_trend,
                                "change": banknifty_close_price - banknifty_data.get('day_open', 0),
                                "change_percent": ((banknifty_close_price - banknifty_data.get('day_open', 0)) / banknifty_data.get('day_open', 1)) * 100 if banknifty_data.get('day_open', 0) > 0 else 0,
                                "market_status": market_status
                            },
                            "timestamp": datetime.now().isoformat(),
                            "data_source": "historical",
                            "market_status": "closed"
                        }
                    }
                )
            else:
                return JSONResponse(
                    status_code=401,
                    content={
                        "status": "error",
                        "message": "Token expired or invalid - API authentication failed. Please update your Upstox access token.",
                        "error_type": "token_expired"
                    }
                )
            
    except Exception as e:
        print(f"Error fetching index prices: {str(e)}")
        return JSONResponse(
            status_code=500,
            content={
                "status": "error",
                "message": f"Failed to fetch index prices: {str(e)}"
            }
        )


@router.get("/data-table")
async def get_intraday_stock_options_table(db: Session = Depends(get_db)):
    """
    Get intraday stock options data in tabular format for display
    """
    try:
        # Get all records from intraday_stock_options table
        records = db.query(IntradayStockOption).order_by(desc(IntradayStockOption.created_date_time)).limit(100).all()
        
        # Convert to tabular format
        table_data = []
        for record in records:
            table_data.append({
                "id": record.id,
                "alert_time": record.alert_time.strftime('%Y-%m-%d %H:%M:%S'),
                "alert_type": record.alert_type,
                "scan_name": record.scan_name,
                "stock_name": record.stock_name,
                "stock_ltp": record.stock_ltp,
                "stock_vwap": record.stock_vwap,
                "option_type": record.option_type,
                "option_contract": record.option_contract,
                "option_strike": record.option_strike,
                "option_ltp": record.option_ltp,
                "option_vwap": record.option_vwap,
                "qty": record.qty,
                "trade_date": record.trade_date.strftime('%Y-%m-%d'),
                "status": record.status,
                "created_date_time": record.created_date_time.strftime('%Y-%m-%d %H:%M:%S')
            })
        
        return JSONResponse(
            status_code=200,
            content={
                "status": "success",
                "data": table_data,
                "total_records": len(table_data)
            }
        )
        
    except Exception as e:
        return JSONResponse(
            status_code=500,
            content={
                "status": "error",
                "message": f"Error fetching data: {str(e)}"
            }
        )

@router.delete("/clear")
async def clear_webhook_data():
    """
    Clear both Bullish and Bearish webhook data (useful for testing)
    """
    global bullish_data, bearish_data
    bullish_data = {"date": None, "alerts": []}
    bearish_data = {"date": None, "alerts": []}
    
    return JSONResponse(
        status_code=200,
        content={
            "status": "success",
            "message": "All webhook data cleared"
        }
    )

@router.post("/update-upstox-token")
async def update_upstox_token(request: Request):
    """
    Update Upstox access token from frontend (LEGACY - use OAuth instead)
    """
    try:
        data = await request.json()
        new_token = data.get("access_token", "")
        
        if not new_token:
            return JSONResponse(
                status_code=400,
                content={"status": "error", "message": "Access token is required"}
            )
        
        # Update the token in upstox_service.py file
        import re
        from pathlib import Path
        
        service_file = Path(__file__).parent.parent / "services" / "upstox_service.py"
        
        with open(service_file, 'r') as f:
            content = f.read()
        
        # Replace the token
        pattern = r'(UPSTOX_ACCESS_TOKEN\s*=\s*")([^"]+)(")'
        new_content = re.sub(pattern, f'\\g<1>{new_token}\\g<3>', content)
        
        with open(service_file, 'w') as f:
            f.write(new_content)
        
        # Restart the backend service
        import subprocess
        subprocess.run(['sudo', 'systemctl', 'restart', 'trademanthan-backend'], 
                      capture_output=True)
        
        return JSONResponse(
            status_code=200,
            content={
                "status": "success",
                "message": "Upstox access token updated successfully"
            }
        )
        
    except Exception as e:
        return JSONResponse(
            status_code=500,
            content={
                "status": "error",
                "message": f"Failed to update token: {str(e)}"
        }
    )

# OAuth state storage (in production, use Redis or database)
oauth_states = {}

@router.get("/upstox/login")
async def upstox_oauth_login():
    """
    Initiate Upstox OAuth 2.0 login flow
    Redirects user to Upstox authorization page
    """
    try:
        # Generate a random state parameter for CSRF protection
        state = secrets.token_urlsafe(32)
        oauth_states[state] = {"timestamp": datetime.utcnow()}
        
        # Construct the authorization URL
        auth_url = (
            f"https://api.upstox.com/v2/login/authorization/dialog"
            f"?response_type=code"
            f"&client_id={settings.UPSTOX_API_KEY}"
            f"&redirect_uri={settings.UPSTOX_REDIRECT_URI}"
            f"&state={state}"
        )
        
        return JSONResponse(
            status_code=200,
            content={
                "status": "success",
                "auth_url": auth_url,
                "message": "Redirect user to this URL for authentication"
            }
        )
        
    except Exception as e:
        return JSONResponse(
            status_code=500,
            content={
                "status": "error",
                "message": f"Failed to generate auth URL: {str(e)}"
            }
        )

@router.get("/upstox/callback")
async def upstox_oauth_callback(code: str = None, state: str = None, error: str = None):
    """
    Handle OAuth callback from Upstox
    Exchange authorization code for access token
    """
    try:
        # Check for errors from Upstox
        if error:
            return JSONResponse(
                status_code=400,
                content={
                    "status": "error",
                    "message": f"Authorization failed: {error}"
                }
            )
        
        # Validate required parameters
        if not code:
            return JSONResponse(
                status_code=400,
                content={
                    "status": "error",
                    "message": "Authorization code missing"
                }
            )
        
        # Validate state parameter (CSRF protection)
        if state and state not in oauth_states:
            return JSONResponse(
                status_code=400,
                content={
                    "status": "error",
                    "message": "Invalid state parameter"
                }
            )
        
        # Clean up used state
        if state in oauth_states:
            del oauth_states[state]
        
        # Exchange authorization code for access token
        token_url = "https://api.upstox.com/v2/login/authorization/token"
        
        token_data = {
            "code": code,
            "client_id": settings.UPSTOX_API_KEY,
            "client_secret": settings.UPSTOX_API_SECRET,
            "redirect_uri": settings.UPSTOX_REDIRECT_URI,
            "grant_type": "authorization_code"
        }
        
        headers = {
            "Accept": "application/json",
            "Content-Type": "application/x-www-form-urlencoded"
        }
        
        response = requests.post(token_url, data=token_data, headers=headers, timeout=10)
        
        if response.status_code != 200:
            return JSONResponse(
                status_code=response.status_code,
                content={
                    "status": "error",
                    "message": f"Failed to get access token: {response.text}"
                }
            )
        
        token_response = response.json()
        access_token = token_response.get("access_token")
        
        if not access_token:
            return JSONResponse(
                status_code=500,
                content={
                    "status": "error",
                    "message": "No access token in response"
                }
            )
        
        # Update the token in upstox_service.py file
        import re
        from pathlib import Path
        
        service_file = Path(__file__).parent.parent / "services" / "upstox_service.py"
        
        with open(service_file, 'r') as f:
            content = f.read()
        
        # Replace the token
        pattern = r'(UPSTOX_ACCESS_TOKEN\s*=\s*")([^"]+)(")'
        new_content = re.sub(pattern, f'\\g<1>{access_token}\\g<3>', content)
        
        with open(service_file, 'w') as f:
            f.write(new_content)
        
        # Restart the backend service
        import subprocess
        subprocess.run(['sudo', 'systemctl', 'restart', 'trademanthan-backend'], 
                      capture_output=True)
        
        # Redirect to scan page with success message
        return RedirectResponse(
            url="/scan.html?auth=success",
            status_code=302
        )
        
    except Exception as e:
        return JSONResponse(
            status_code=500,
            content={
                "status": "error",
                "message": f"OAuth callback error: {str(e)}"
            }
        )

@router.get("/upstox/status")
async def upstox_oauth_status():
    """
    Check Upstox OAuth authentication status
    """
    try:
        # Check if upstox_service has a valid token
        if hasattr(vwap_service, 'access_token') and vwap_service.access_token:
            # Try to make a test API call to verify token validity
            test_url = "https://api.upstox.com/v2/user/profile"
            headers = {
                "Accept": "application/json",
                "Authorization": f"Bearer {vwap_service.access_token}"
            }
            
            response = requests.get(test_url, headers=headers, timeout=5)
            
            if response.status_code == 200:
                return JSONResponse(
                    status_code=200,
                    content={
                        "status": "success",
                        "authenticated": True,
                        "message": "Upstox token is valid"
                    }
                )
            else:
                return JSONResponse(
                    status_code=200,
                    content={
                        "status": "success",
                        "authenticated": False,
                        "message": "Upstox token is invalid or expired"
                    }
                )
        else:
            return JSONResponse(
                status_code=200,
                content={
                    "status": "success",
                    "authenticated": False,
                    "message": "No Upstox token configured"
                }
            )
            
    except Exception as e:
        return JSONResponse(
            status_code=500,
            content={
                "status": "error",
                "message": f"Failed to check status: {str(e)}"
            }
        )
