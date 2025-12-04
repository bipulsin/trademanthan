from fastapi import APIRouter, Request, HTTPException, Depends, Query, BackgroundTasks
from fastapi.responses import JSONResponse, RedirectResponse
from datetime import datetime
from typing import List, Dict, Any, Optional
from pydantic import BaseModel
from sqlalchemy.orm import Session
from sqlalchemy import and_, func, desc, text
from starlette.requests import ClientDisconnect
import json
import os
import sys
import requests
import secrets
import logging
import asyncio
from pathlib import Path

logger = logging.getLogger(__name__)

# Add services to path
sys.path.append(os.path.dirname(os.path.dirname(__file__)))

# Import health monitor for tracking webhook success/failure
try:
    from services.health_monitor import health_monitor
except ImportError:
    health_monitor = None  # Graceful degradation if not available
from backend.services.upstox_service import upstox_service as vwap_service
from backend.database import get_db
from backend.models.trading import IntradayStockOption, MasterStock
from backend.config import settings

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
    Find the best option strike from OTM-1 to OTM-5 based on highest volume/OI
    
    Logic:
    1. Get all OTM strikes (CE: strike > LTP, PE: strike < LTP)
    2. Sort by distance from LTP to identify OTM-1, OTM-2, ..., OTM-5
    3. Select the strike with HIGHEST volume × OI among OTM-1 to OTM-5
    
    Args:
        vwap_service: UpstoxService instance
        stock_name: Stock symbol
        option_type: 'CE' or 'PE'
        stock_ltp: Current stock LTP
        
    Returns:
        Dict with strike_price, volume, oi, ltp or None
    """
    try:
        # Get option chain from Upstox API
        option_chain = vwap_service.get_option_chain(stock_name)
        
        if not option_chain:
            logger.warning(f"No option chain data available for {stock_name}")
            print(f"No option chain data available for {stock_name}")
            return None
        
        # Parse option chain data
        # Upstox API returns a dictionary with 'strikes' key containing list of strike data
        strikes = []
        
        # Handle dictionary format (Upstox API v2 returns dict with 'strikes' key)
        strike_list = None
        if isinstance(option_chain, dict):
            logger.info(f"📊 Option chain for {stock_name} is a dictionary with keys: {list(option_chain.keys())}")
            print(f"📊 Option chain for {stock_name} is a dictionary with keys: {list(option_chain.keys())}")
            # Check if it has a 'strikes' key
            if 'strikes' in option_chain and isinstance(option_chain['strikes'], list):
                strike_list = option_chain['strikes']
                logger.info(f"✅ Found 'strikes' key with {len(strike_list)} strikes")
                print(f"✅ Found 'strikes' key with {len(strike_list)} strikes")
            elif 'data' in option_chain and isinstance(option_chain['data'], dict):
                # Nested data structure
                logger.info(f"📊 Found 'data' key with sub-keys: {list(option_chain['data'].keys())}")
                print(f"📊 Found 'data' key with sub-keys: {list(option_chain['data'].keys())}")
                if 'strikes' in option_chain['data'] and isinstance(option_chain['data']['strikes'], list):
                    strike_list = option_chain['data']['strikes']
                    logger.info(f"✅ Found 'strikes' in 'data' with {len(strike_list)} strikes")
                    print(f"✅ Found 'strikes' in 'data' with {len(strike_list)} strikes")
                else:
                    logger.warning(f"⚠️ Unexpected option chain structure for {stock_name}: {list(option_chain.get('data', {}).keys())}")
                    print(f"⚠️ Unexpected option chain structure for {stock_name}: {list(option_chain.get('data', {}).keys())}")
                    # Try to find any list in the data structure
                    for key, value in option_chain['data'].items():
                        if isinstance(value, list) and len(value) > 0:
                            logger.info(f"   Found list in '{key}' with {len(value)} items, first item type: {type(value[0])}")
                            print(f"   Found list in '{key}' with {len(value)} items, first item type: {type(value[0])}")
                            if isinstance(value[0], dict):
                                logger.info(f"   First item keys: {list(value[0].keys())}")
                                print(f"   First item keys: {list(value[0].keys())}")
                    return None
            else:
                logger.warning(f"⚠️ Unexpected option chain structure for {stock_name}: {list(option_chain.keys())}")
                print(f"⚠️ Unexpected option chain structure for {stock_name}: {list(option_chain.keys())}")
                # Try to find any list in the structure
                for key, value in option_chain.items():
                    if isinstance(value, list) and len(value) > 0:
                        logger.info(f"   Found list in '{key}' with {len(value)} items, first item type: {type(value[0])}")
                        print(f"   Found list in '{key}' with {len(value)} items, first item type: {type(value[0])}")
                        if isinstance(value[0], dict):
                            logger.info(f"   First item keys: {list(value[0].keys())}")
                            print(f"   First item keys: {list(value[0].keys())}")
                return None
        elif isinstance(option_chain, list):
            # Direct list format (legacy or different API version)
            strike_list = option_chain
            logger.info(f"✅ Option chain for {stock_name} is a direct list with {len(strike_list)} items")
            print(f"✅ Option chain for {stock_name} is a direct list with {len(strike_list)} items")
        else:
            logger.warning(f"⚠️ Unexpected option chain type for {stock_name}: {type(option_chain)}")
            print(f"⚠️ Unexpected option chain type for {stock_name}: {type(option_chain)}")
            return None
        
        # Parse strikes from the list
        logger.info(f"Parsing {len(strike_list)} strikes from option chain for {stock_name}")
        print(f"Parsing {len(strike_list)} strikes from option chain for {stock_name}")
        
        for strike_data in strike_list:
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
        
        logger.info(f"Found {len(strikes)} {option_type} options in chain for {stock_name}")
        print(f"Found {len(strikes)} {option_type} options in chain for {stock_name}")
        
        if not strikes:
            logger.warning(f"No {option_type} options found in chain for {stock_name}")
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
            logger.warning(f"No OTM {option_type} strikes found for {stock_name} (stock LTP: {stock_ltp})")
            print(f"No OTM {option_type} strikes found for {stock_name} (stock LTP: {stock_ltp})")
            return None
        
        # Sort by distance from LTP (closest first) to get OTM-1 to OTM-5
        otm_strikes.sort(key=lambda x: abs(x['strike_price'] - stock_ltp))
        
        # Get first 5 OTM strikes (OTM-1 to OTM-5)
        otm_1_to_5 = otm_strikes[:5]
        
        if not otm_1_to_5:
            print(f"Not enough OTM strikes for {stock_name}")
            return otm_strikes[0] if otm_strikes else None
        
        print(f"OTM-1 to OTM-5 strikes for {stock_name} {option_type}:")
        for i, strike in enumerate(otm_1_to_5, 1):
            liquidity_score = strike['volume'] * strike['oi']
            print(f"  OTM-{i}: Strike {strike['strike_price']}, Vol: {strike['volume']}, OI: {strike['oi']}, Score: {liquidity_score}")
        
        # Select strike with highest volume * OI among OTM-1 to OTM-5
        selected = max(otm_1_to_5, key=lambda x: x['volume'] * x['oi'])
        
        otm_position = otm_1_to_5.index(selected) + 1
        liquidity_score = selected['volume'] * selected['oi']
        print(f"✅ Selected OTM-{otm_position} strike: {selected['strike_price']} (Volume: {selected['volume']}, OI: {selected['oi']}, Score: {liquidity_score})")
        print(f"   Highest liquidity among OTM-1 to OTM-5")
        return selected
        
    except Exception as e:
        logger.error(f"Error fetching option chain for {stock_name}: {str(e)}", exc_info=True)
        print(f"Error fetching option chain for {stock_name}: {str(e)}")
        import traceback
        print(traceback.format_exc())
        return None

# Helper function to process webhook data
def find_option_contract_from_master_stock(db: Session, stock_name: str, option_type: str, stock_ltp: float, vwap_service=None) -> Optional[str]:
    """
    Find the correct option contract from master_stock table based on:
    - underlying_symbol matching stock_name
    - option_type matching (CE/PE)
    - Strike price from option chain API (volume/OI based) - REQUIRED, no fallback
    - Expiry month: If current date > 17th, use next month's expiry; otherwise use current month
    
    Args:
        db: Database session
        stock_name: Stock symbol (e.g., 'RELIANCE')
        option_type: Option type ('CE' or 'PE')
        stock_ltp: Current stock LTP price
        vwap_service: UpstoxService instance for API calls
        
    Returns:
        symbol_name from master_stock table, or None if option chain unavailable or contract not found
        (Trade will be marked as no_entry when None is returned)
    """
    try:
        import pytz
        ist = pytz.timezone('Asia/Kolkata')
        now = datetime.now(ist)
        
        # Determine target expiry month based on current date
        # If day > 17, use next month's expiry; otherwise use current month
        if now.day > 17:
            # Use next month's expiry
            if now.month == 12:
                target_expiry_month = 1
                target_expiry_year = now.year + 1
            else:
                target_expiry_month = now.month + 1
                target_expiry_year = now.year
        else:
            # Use current month's expiry
            target_expiry_month = now.month
            target_expiry_year = now.year
        
        print(f"Target expiry: {target_expiry_year}-{target_expiry_month:02d} (current date: {now.strftime('%Y-%m-%d')})")
        
        # Get strike from option chain API - REQUIRED, no fallback
        target_strike = None
        if vwap_service:
            strike_data = find_strike_from_option_chain(vwap_service, stock_name, option_type, stock_ltp)
            if strike_data:
                target_strike = strike_data['strike_price']
                print(f"Using option chain strike for {stock_name}: {target_strike} (Volume: {strike_data['volume']}, OI: {strike_data['oi']})")
        
        # If option chain not available, return None to mark trade as no_entry
        if target_strike is None or target_strike == 0:
            print(f"❌ Option chain not available for {stock_name} - Cannot determine strike. Trade will be marked as no_entry.")
            return None
        
        print(f"Looking for {option_type} option with strike {target_strike} for {stock_name}")
        
        # Query master_stock table with expiry month filter
        # Filter by expiry month/year to ensure we get the correct expiry
        option_record = db.query(MasterStock).filter(
            and_(
                MasterStock.underlying_symbol == stock_name,
                MasterStock.option_type == option_type,
                MasterStock.strike_price == target_strike,
                MasterStock.expiry_flag == 'M',  # Monthly expiry
                func.extract('year', MasterStock.sm_expiry_date) == target_expiry_year,
                func.extract('month', MasterStock.sm_expiry_date) == target_expiry_month
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
                    MasterStock.expiry_flag == 'M',
                    func.extract('year', MasterStock.sm_expiry_date) == target_expiry_year,
                    func.extract('month', MasterStock.sm_expiry_date) == target_expiry_month
                )
            ).order_by(MasterStock.strike_price.asc()).first()
            
            # If no strike >= target found, get the highest available strike for target expiry
            if not closest_record:
                print(f"No strike >= {target_strike} found, getting highest available strike")
                closest_record = db.query(MasterStock).filter(
                    and_(
                        MasterStock.underlying_symbol == stock_name,
                        MasterStock.option_type == option_type,
                        MasterStock.expiry_flag == 'M',
                        func.extract('year', MasterStock.sm_expiry_date) == target_expiry_year,
                        func.extract('month', MasterStock.sm_expiry_date) == target_expiry_month
                    )
                ).order_by(MasterStock.strike_price.desc()).first()
        else:  # PE
            # For PE, find the closest strike <= target_strike
            closest_record = db.query(MasterStock).filter(
                and_(
                    MasterStock.underlying_symbol == stock_name,
                    MasterStock.option_type == option_type,
                    MasterStock.strike_price <= target_strike,
                    MasterStock.expiry_flag == 'M',
                    func.extract('year', MasterStock.sm_expiry_date) == target_expiry_year,
                    func.extract('month', MasterStock.sm_expiry_date) == target_expiry_month
                )
            ).order_by(MasterStock.strike_price.desc()).first()
            
            # If no strike <= target found, get the lowest available strike for target expiry
            if not closest_record:
                print(f"No strike <= {target_strike} found, getting lowest available strike")
                closest_record = db.query(MasterStock).filter(
                    and_(
                        MasterStock.underlying_symbol == stock_name,
                        MasterStock.option_type == option_type,
                        MasterStock.expiry_flag == 'M',
                        func.extract('year', MasterStock.sm_expiry_date) == target_expiry_year,
                        func.extract('month', MasterStock.sm_expiry_date) == target_expiry_month
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
                elif hour < 14 or (hour == 14 and minute <= 15):
                    corrected_time = "2:15 PM"
                    corrected_hour, corrected_minute = 14, 15
                else:
                    # 3:15 PM is the last alert time
                    corrected_time = "3:15 PM"
                    corrected_hour, corrected_minute = 15, 15
                
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
        
        # Filter out index names (NIFTY, BANKNIFTY, etc.) - these are not tradable stocks
        INDEX_NAMES = ['NIFTY', 'NIFTY50', 'BANKNIFTY', 'FINNIFTY', 'MIDCPNIFTY', 'SENSEX', 'BANKEX']
        original_count = len(processed_data["stocks"])
        processed_data["stocks"] = [
            stock for stock in processed_data["stocks"]
            if stock.get("stock_name", "").strip().upper() not in INDEX_NAMES
        ]
        filtered_count = original_count - len(processed_data["stocks"])
        if filtered_count > 0:
            logger.info(f"🚫 Filtered out {filtered_count} index name(s) from stocks list")
            print(f"🚫 Filtered out {filtered_count} index name(s) from stocks list (original: {original_count}, remaining: {len(processed_data['stocks'])})")
        
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
        # IMPORTANT: Always save at minimum stock_name and alert_time, even if enrichment fails
        enriched_stocks = []
        for stock in processed_data["stocks"]:
            stock_name = stock.get("stock_name", "")
            trigger_price = stock.get("trigger_price", 0.0)
            
            print(f"Processing stock: {stock_name}")
            
            # Wrap entire enrichment in try-except to ensure stock is always added even if enrichment fails
            enrichment_successful = False
            try:
                # Initialize with defaults (will be saved even if API calls fail)
                stock_ltp = trigger_price
                stock_vwap = 0.0
                
                # Try to fetch both LTP and VWAP in a single call (optimized with fallback)
                try:
                    stock_data = vwap_service.get_stock_ltp_and_vwap(stock_name)
                    if stock_data:
                        if stock_data.get('ltp') and stock_data['ltp'] > 0:
                            stock_ltp = stock_data['ltp']
                            print(f"✅ Stock LTP for {stock_name}: ₹{stock_ltp:.2f}")
                        else:
                            print(f"⚠️ Could not fetch LTP for {stock_name}, using trigger price: ₹{trigger_price}")
                        
                        if stock_data.get('vwap') and stock_data['vwap'] > 0:
                            stock_vwap = stock_data['vwap']
                            print(f"✅ Stock VWAP for {stock_name}: ₹{stock_vwap:.2f}")
                        else:
                            print(f"⚠️ Could not fetch VWAP for {stock_name} - will retry via hourly updater")
                    else:
                        print(f"⚠️ Stock data fetch completely failed for {stock_name} - using defaults")
                        stock_ltp = trigger_price
                except Exception as e:
                    print(f"❌ Stock data fetch failed for {stock_name}: {str(e)} - Using trigger price")
                    import traceback
                    print(traceback.format_exc())
                    stock_ltp = trigger_price
                
                # Initialize option-related fields with defaults
                option_contract = None
                option_strike = 0.0
                qty = 0
                option_ltp = 0.0
                instrument_key = None  # Will store Upstox instrument key (e.g., NSE_FO|104500) for future LTP fetches
                
                # Try to find option contract (may fail if token expired)
                # Retry up to 3 times to ensure option contract is determined
                option_contract = None
                max_retries = 3
                for retry_attempt in range(1, max_retries + 1):
                    try:
                        option_contract = find_option_contract_from_master_stock(
                            db, stock_name, forced_option_type, stock_ltp, vwap_service
                        )
                        if option_contract:
                            print(f"✅ Option contract found for {stock_name} (attempt {retry_attempt}): {option_contract}")
                            break
                        else:
                            if retry_attempt < max_retries:
                                print(f"⚠️ No option contract found for {stock_name} (attempt {retry_attempt}/{max_retries}), retrying...")
                                import time
                                time.sleep(1)  # Brief delay before retry
                            else:
                                print(f"⚠️ No option contract found for {stock_name} after {max_retries} attempts")
                    except Exception as e:
                        if retry_attempt < max_retries:
                            print(f"⚠️ Option contract search failed for {stock_name} (attempt {retry_attempt}/{max_retries}): {str(e)}, retrying...")
                            import time
                            time.sleep(1)  # Brief delay before retry
                        else:
                            print(f"⚠️ Option contract search failed for {stock_name} after {max_retries} attempts: {str(e)}")
                            option_contract = None
                
                # Extract option strike and fetch option LTP if contract found
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
                                # datetime already imported at module level
                                
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
                                    # CRITICAL: Must match exactly on underlying_symbol, instrument_type, segment, strike, and expiry
                                    # This ensures each option contract gets its unique instrument_key
                                    best_match = None
                                    best_match_score = 0
                                    
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
                                                        print(f"✅ Found EXACT match for {option_contract}:")
                                                        print(f"   Instrument Key: {instrument_key}")
                                                        print(f"   Trading Symbol: {trading_symbol}")
                                                        print(f"   Strike: {inst_strike} (requested: {strike_value}, diff: {strike_diff:.4f})")
                                                        print(f"   Expiry: {inst_expiry.strftime('%d %b %Y')}")
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
                                        print(f"⚠️ WARNING: Using BEST MATCH (not exact) for {option_contract}:")
                                        print(f"   Instrument Key: {instrument_key}")
                                        print(f"   Trading Symbol: {trading_symbol}")
                                        print(f"   Strike: {inst_strike} (requested: {strike_value}, diff: {abs(inst_strike - strike_value):.4f})")
                                        print(f"   Expiry: {inst_expiry.strftime('%d %b %Y')}")
                                        print(f"   ⚠️ This may not be the correct instrument!")
                                    
                                    if not instrument_key:
                                        print(f"❌ ERROR: Could not find instrument_key for {option_contract}")
                                        print(f"   Searched for: symbol={symbol}, type={opt_type}, strike={strike_value}, expiry={target_month}/{target_year}")
                                
                                if instrument_key:
                                    print(f"Found instrument key for {option_contract}: {instrument_key}")
                                    
                                    # Fetch market quote using the instrument key
                                    if vwap_service:
                                        quote_data = vwap_service.get_market_quote_by_key(instrument_key)
                                        if quote_data and quote_data.get('last_price'):
                                            option_ltp = float(quote_data.get('last_price', 0))
                                            print(f"✅ Fetched option LTP for {option_contract}: ₹{option_ltp}")
                                            
                                            # ====================================================================
                                            # FETCH OPTION OHLC CANDLES (Current and Previous 1-hour)
                                            # ====================================================================
                                            try:
                                                option_candles = vwap_service.get_option_daily_candles_current_and_previous(instrument_key)
                                                if option_candles:
                                                    print(f"✅ Fetched option OHLC candles for {option_contract}")
                                                else:
                                                    print(f"⚠️ Could not fetch option OHLC candles for {option_contract}")
                                                    option_candles = None
                                            except Exception as candle_error:
                                                print(f"⚠️ Error fetching option OHLC candles: {str(candle_error)}")
                                                option_candles = None
                                        else:
                                            print(f"Could not fetch option LTP for {option_contract} - no quote data")
                                            option_candles = None
                                    else:
                                        print(f"vwap_service not available")
                                        option_candles = None
                                else:
                                    print(f"Could not find instrument key for {option_contract} in instruments JSON")
                                    option_candles = None
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
                
                # ====================================================================
                # FETCH PREVIOUS HOUR STOCK VWAP
                # ====================================================================
                stock_vwap_previous_hour = None
                stock_vwap_previous_hour_time = None
                if vwap_service and stock_name:
                    try:
                        prev_vwap_data = vwap_service.get_stock_vwap_for_previous_hour(stock_name)
                        if prev_vwap_data:
                            stock_vwap_previous_hour = prev_vwap_data.get('vwap')
                            stock_vwap_previous_hour_time = prev_vwap_data.get('time')
                            print(f"✅ Fetched previous hour VWAP for {stock_name}: ₹{stock_vwap_previous_hour:.2f} at {stock_vwap_previous_hour_time.strftime('%H:%M:%S')}")
                        else:
                            print(f"⚠️ Could not fetch previous hour VWAP for {stock_name}")
                    except Exception as prev_vwap_error:
                        print(f"⚠️ Error fetching previous hour VWAP: {str(prev_vwap_error)}")
                
                # Create enriched stock data
                # GUARANTEED FIELDS (always available from Chartink):
                # - stock_name, trigger_price, alert_time
                # OPTIONAL FIELDS (may be missing if Upstox token expired):
                # - stock_ltp, stock_vwap, option_contract, option_ltp, qty, instrument_key
                # NEW FIELDS:
                # - option_candles (current and previous OHLC)
                # - stock_vwap_previous_hour, stock_vwap_previous_hour_time
                enriched_stock = {
                "stock_name": stock_name,
                "trigger_price": trigger_price,
                "last_traded_price": stock_ltp,  # May be trigger_price if fetch failed
                "stock_vwap": stock_vwap,  # May be 0.0 if fetch failed
                "stock_vwap_previous_hour": stock_vwap_previous_hour,  # Previous hour VWAP
                "stock_vwap_previous_hour_time": stock_vwap_previous_hour_time,  # Previous hour VWAP time
                "option_type": forced_option_type,
                "option_contract": option_contract or "",  # May be empty if not found
                "otm1_strike": option_strike,  # May be 0.0 if not found
                "option_ltp": option_ltp,  # May be 0.0 if fetch failed
                "option_vwap": 0.0,  # Not used
                "qty": qty,  # May be 0 if not found
                "instrument_key": instrument_key,  # CRITICAL: Store instrument_key for each stock individually
                "option_candles": option_candles if 'option_candles' in locals() else None  # Current and previous OHLC candles
                }
            
                enriched_stocks.append(enriched_stock)
                enrichment_successful = True
                
                # Log what we got
                if option_contract:
                    print(f"✅ Enriched stock: {stock_name} - LTP: ₹{stock_ltp}, Option: {option_contract}, Qty: {qty}")
                else:
                    print(f"⚠️ Partial data for: {stock_name} - LTP: ₹{stock_ltp}, Option: N/A (token issue?)")
            
            except Exception as enrichment_error:
                # If enrichment fails completely, still create a minimal enriched_stock entry
                # This ensures the stock is saved to database even if enrichment fails
                print(f"❌ CRITICAL: Enrichment failed for {stock_name}: {str(enrichment_error)}")
                print(f"   Creating minimal enriched_stock entry to ensure stock is saved...")
                import traceback
                traceback.print_exc()
                
                # Create minimal enriched stock with defaults
                enriched_stock = {
                    "stock_name": stock_name,
                    "trigger_price": trigger_price,
                    "last_traded_price": trigger_price,  # Use trigger price as fallback
                    "stock_vwap": 0.0,
                    "stock_vwap_previous_hour": None,
                    "stock_vwap_previous_hour_time": None,
                    "option_type": forced_option_type,  # Ensure option_type is set
                    "option_contract": "",  # Will be retried later
                    "otm1_strike": 0.0,
                    "option_ltp": 0.0,
                    "option_vwap": 0.0,
                    "qty": 0,
                    "instrument_key": None,
                    "option_candles": None
                }
                enriched_stocks.append(enriched_stock)
                print(f"   ✅ Created minimal entry for {stock_name} - will be saved with status 'alert_received'")
        
        processed_data["stocks"] = enriched_stocks
        print(f"Successfully processed {len(enriched_stocks)} stocks")
        
        # ====================================================================
        # STOCK RANKING & SELECTION (If too many stocks)
        # ====================================================================
        MAX_STOCKS_PER_ALERT = 15  # Maximum stocks to enter per alert
        
        if len(enriched_stocks) > MAX_STOCKS_PER_ALERT:
            print(f"\n📊 TOO MANY STOCKS ({len(enriched_stocks)}) - Applying ranking to select best {MAX_STOCKS_PER_ALERT}")
            
            # Import ranker
            try:
                from services.stock_ranker import rank_and_select_stocks
                
                # Rank and select top stocks
                selected_stocks, summary = rank_and_select_stocks(
                    enriched_stocks, 
                    max_stocks=MAX_STOCKS_PER_ALERT,
                    alert_type=forced_option_type
                )
                
                print(f"✅ RANKING COMPLETE:")
                print(f"   • Total Available: {summary['total_available']}")
                print(f"   • Selected: {summary['total_selected']}")
                print(f"   • Rejected: {summary['total_rejected']}")
                print(f"   • Avg Score: {summary['avg_score']}")
                print(f"   • Score Range: {summary['min_score']}-{summary['max_score']}")
                
                # Replace stocks with selected ones
                enriched_stocks = selected_stocks
                processed_data["stocks"] = selected_stocks
                
            except ImportError as e:
                print(f"⚠️ Stock ranker not available, using all stocks: {str(e)}")
        else:
            print(f"✅ Stock count ({len(enriched_stocks)}) within limit ({MAX_STOCKS_PER_ALERT}), using all stocks")
        
        # Get current date for grouping
        current_date = trading_date.strftime('%Y-%m-%d')
        
        # Determine which data store to use
        target_data = bullish_data if is_bullish else bearish_data
        data_type = "Bullish" if is_bullish else "Bearish"
        
        # Check if this is a new date - if so, clear old data
        # IMPORTANT: Only clear if it's ACTUALLY a different day, not just different time
        if target_data["date"] != current_date:
            print(f"📅 New trading date detected for {data_type}: {current_date} (previous: {target_data['date']})")
            target_data["date"] = current_date
            target_data["alerts"] = []
            print(f"   Cleared old alerts from previous date")
        else:
            print(f"📅 Same trading date ({current_date}), appending to existing alerts")
        
        # Check index trends at the time of alert
        # Index trends determine trade entry, not alert display
        index_trends = vwap_service.check_index_trends()
        nifty_trend = index_trends.get("nifty_trend", "unknown")
        banknifty_trend = index_trends.get("banknifty_trend", "unknown")
        
        # Check if time is at or after 3:00 PM - NO NEW TRADES after this time
        alert_hour = triggered_datetime.hour
        alert_minute = triggered_datetime.minute
        is_after_3_00pm = (alert_hour > 15) or (alert_hour == 15 and alert_minute >= 0)
        
        # Special handling for 10:15 AM alerts (first alert of the day)
        # At 10:15 AM, market has only been open for 45 minutes, so:
        # - Previous hour VWAP may not be available (9:15 AM was before market open)
        # - Option candles may not be fully formed yet
        # For 10:15 AM alerts, skip VWAP slope and candle size filters
        is_10_15_alert = (alert_hour == 10 and alert_minute == 15)
        
        if is_after_3_00pm:
            print(f"🚫 ALERT TIME {triggered_at_display} is at or after 3:00 PM - NO NEW TRADES ALLOWED")
        
        # Determine if trade entry is allowed based on alert type and index trends
        # Rules:
        # 1. If both indices are Bullish → trade will be considered for both bullish & bearish alerts
        # 2. If both indices are Bearish → only Bearish scan alerts trade will be processed
        # 3. If indices are in opposite directions → no trade will be processed
        can_enter_trade_by_index = False
        
        # Check index trend alignment
        both_bullish = (nifty_trend == "bullish" and banknifty_trend == "bullish")
        both_bearish = (nifty_trend == "bearish" and banknifty_trend == "bearish")
        opposite_directions = not both_bullish and not both_bearish
        
        if is_bullish:
            # Bullish alert
            if both_bullish:
                # Both indices bullish → bullish alerts can enter
                can_enter_trade_by_index = True
                print(f"✅ BULLISH ALERT: Both indices bullish - Index check PASSED")
            elif both_bearish:
                # Both indices bearish → bullish alerts cannot enter
                can_enter_trade_by_index = False
                print(f"⚠️ BULLISH ALERT: Both indices bearish - Only bearish alerts allowed - NO TRADE")
            elif opposite_directions:
                # Indices in opposite directions → no trade
                can_enter_trade_by_index = False
                print(f"⚠️ BULLISH ALERT: Indices in opposite directions (NIFTY: {nifty_trend}, BANKNIFTY: {banknifty_trend}) - NO TRADE")
        elif is_bearish:
            # Bearish alert
            if both_bullish:
                # Both indices bullish → bearish alerts can enter
                can_enter_trade_by_index = True
                print(f"✅ BEARISH ALERT: Both indices bullish - Index check PASSED")
            elif both_bearish:
                # Both indices bearish → bearish alerts can enter
                can_enter_trade_by_index = True
                print(f"✅ BEARISH ALERT: Both indices bearish - Index check PASSED")
            elif opposite_directions:
                # Indices in opposite directions → no trade
                can_enter_trade_by_index = False
                print(f"⚠️ BEARISH ALERT: Indices in opposite directions (NIFTY: {nifty_trend}, BANKNIFTY: {banknifty_trend}) - NO TRADE")
        
        # Save each stock to database
        # CRITICAL: Always save at minimum stock_name and alert_time, even if enrichment failed
        saved_count = 0
        failed_count = 0
        SL_LOSS_TARGET = 3100.0  # Target loss for stop loss trigger
        
        stocks_to_save = processed_data.get('stocks', [])
        print(f"\n💾 Saving {len(stocks_to_save)} stocks to database...")
        
        if len(stocks_to_save) == 0:
            print("⚠️ WARNING: No stocks to save! Webhook payload may be empty or malformed.")
            print(f"   Original data keys: {list(data.keys())}")
            print(f"   Stocks field type: {type(data.get('stocks'))}")
            print(f"   Stocks field value: {data.get('stocks')}")
            logger.warning(f"No stocks found in webhook payload. Data: {json.dumps(data, indent=2)}")
        
        for stock in stocks_to_save:
            stock_name = stock.get("stock_name", "UNKNOWN")
            
            try:
                # Get option_ltp value and lot_size
                option_ltp_value = stock.get("option_ltp", 0.0)
                lot_size = stock.get("qty", 0)
                
                # ====================================================================
                # NEW ENTRY FILTERS: VWAP Slope + Option Candle Size
                # ====================================================================
                # Replaced momentum filter with:
                # 1. VWAP Slope Filter: Check if VWAP has 45-degree inclination
                # 2. Candle Size Filter: Check if current candle < 7-8x previous candle
                # ====================================================================
                
                stock_ltp = stock.get("last_traded_price", 0.0)
                stock_vwap = stock.get("stock_vwap", 0.0)
                stock_vwap_prev = stock.get("stock_vwap_previous_hour")
                stock_vwap_prev_time = stock.get("stock_vwap_previous_hour_time")
                option_type = stock.get("option_type", "")
                option_candles = stock.get("option_candles")
                
                # Initialize filter results
                vwap_slope_passed = False
                candle_size_passed = False
                vwap_slope_reason = ""
                candle_size_reason = ""
                
                # 1. VWAP SLOPE FILTER - SKIP INITIAL CALCULATION
                # VWAP slope will be calculated in cycle-based scheduler (10:30, 11:15, 12:15, 13:15, 14:15)
                # For webhook alerts, we only store the alert data, VWAP slope will be calculated later
                vwap_slope_reason = "VWAP slope will be calculated in cycle-based scheduler"
                
                # 2. CANDLE SIZE FILTER (Daily candles: current day vs previous day, up to current hour)
                # Candle size is calculated ONLY when stock is received from webhook alert
                # It will NOT be recalculated once status changes from No_Entry
                if option_candles:
                    try:
                        current_day_candle = option_candles.get('current_day_candle', {})
                        previous_day_candle = option_candles.get('previous_day_candle', {})
                        
                        if current_day_candle and previous_day_candle:
                            # Current day: High/Low from candles up to current time
                            current_high = current_day_candle.get('high', 0)
                            current_low = current_day_candle.get('low', 0)
                            current_size = abs(current_high - current_low)
                            
                            # Previous day: Complete day High/Low
                            previous_high = previous_day_candle.get('high', 0)
                            previous_low = previous_day_candle.get('low', 0)
                            previous_size = abs(previous_high - previous_low)
                            
                            if previous_size > 0:
                                size_ratio = current_size / previous_size
                                
                                # Check if current day candle (up to current time) is less than 7-8 times previous day candle (complete day)
                                # Using 7.5 as threshold (middle of 7-8 range)
                                if size_ratio < 7.5:
                                    candle_size_passed = True
                                    candle_size_reason = f"Daily candle size OK: Current Day (up to {triggered_datetime.hour}:{triggered_datetime.minute:02d}) High={current_high:.2f}, Low={current_low:.2f}, Size={current_size:.2f} < 7.5× Previous Day (complete) High={previous_high:.2f}, Low={previous_low:.2f}, Size={previous_size:.2f}, Ratio: {size_ratio:.2f}"
                                else:
                                    candle_size_reason = f"Daily candle size too large: Current Day (up to {triggered_datetime.hour}:{triggered_datetime.minute:02d}) High={current_high:.2f}, Low={current_low:.2f}, Size={current_size:.2f} >= 7.5× Previous Day (complete) High={previous_high:.2f}, Low={previous_low:.2f}, Size={previous_size:.2f}, Ratio: {size_ratio:.2f}"
                            else:
                                candle_size_reason = "Previous day candle size is zero (cannot calculate ratio)"
                        else:
                            candle_size_reason = "Missing daily candle data"
                    except Exception as candle_error:
                        candle_size_reason = f"Error calculating daily candle size: {str(candle_error)}"
                else:
                    candle_size_reason = "Option daily candles not available"
                
                # Determine trade entry based on:
                # 1. Time check (must be before 3:00 PM)
                # 2. Index trends (must be aligned)
                # 3. VWAP slope >= 45 degrees (calculated in cycle-based scheduler, not here)
                # 4. Current candle size < 7-8x previous candle (calculated here for webhook alerts)
                # 5. Valid option data (option_ltp > 0, lot_size > 0)
                # NOTE: VWAP slope is NOT calculated here - it will be calculated in cycle-based scheduler
                # For initial webhook processing, we only check candle size (if available)
                # For 10:15 AM alerts, skip filters due to insufficient historical data
                filters_passed = candle_size_passed if not is_10_15_alert else True
                
                if not is_after_3_00pm and can_enter_trade_by_index and filters_passed and option_ltp_value > 0 and lot_size > 0:
                    # Enter trade: Fetch current option LTP again, set buy_time to current system time, stop_loss from previous candle low
                    # IMPORTANT: sell_price remains NULL initially, will be populated by hourly updater
                    import pytz
                    ist = pytz.timezone('Asia/Kolkata')
                    current_time = datetime.now(ist)
                    
                    # Fetch current option LTP at entry moment (not from enrichment phase)
                    current_option_ltp = option_ltp_value  # Default to enrichment value
                    if stock.get('instrument_key'):
                        try:
                            option_quote = vwap_service.get_market_quote_by_key(stock.get('instrument_key'))
                            if option_quote and option_quote.get('last_price', 0) > 0:
                                current_option_ltp = float(option_quote.get('last_price', 0))
                                print(f"✅ Fetched fresh option LTP at entry: ₹{current_option_ltp:.2f}")
                            else:
                                print(f"⚠️ Could not fetch fresh option LTP, using enrichment value: ₹{current_option_ltp:.2f}")
                        except Exception as ltp_error:
                            print(f"⚠️ Error fetching fresh option LTP: {str(ltp_error)}, using enrichment value: ₹{current_option_ltp:.2f}")
                    
                    qty = lot_size
                    buy_price = current_option_ltp  # Use current LTP fetched at entry moment
                    buy_time = current_time  # Use current system time, not alert time
                    sell_price = None  # BLANK initially - will be updated hourly by market data updater
                    
                    # Stop Loss = Low price of previous day candle of the stock options
                    stop_loss_price = None
                    if option_candles and option_candles.get('previous_day_candle'):
                        previous_day_candle_low = option_candles.get('previous_day_candle', {}).get('low')
                        if previous_day_candle_low and previous_day_candle_low > 0:
                            stop_loss_price = float(previous_day_candle_low)
                            print(f"✅ Stop Loss set from previous day candle low: ₹{stop_loss_price:.2f}")
                        else:
                            print(f"⚠️ Previous day candle low not available, setting SL to 0.05")
                            stop_loss_price = 0.05
                    else:
                        print(f"⚠️ Previous day candle data not available, setting SL to 0.05")
                        stop_loss_price = 0.05
                    
                    status = 'bought'  # Trade entered
                    pnl = 0.0
                    entry_time_str = buy_time.strftime('%Y-%m-%d %H:%M:%S IST')
                    alert_time_str = triggered_datetime.strftime('%Y-%m-%d %H:%M:%S IST')
                    print(f"✅ TRADE ENTERED: {stock_name}")
                    print(f"   ⏰ Entry Time: {entry_time_str} (Alert Time: {alert_time_str})")
                    print(f"   📊 Entry Conditions:")
                    print(f"      - Time Check: ✅ Before 3:00 PM ({triggered_at_display})")
                    print(f"      - Index Trends: ✅ Aligned (NIFTY: {nifty_trend}, BANKNIFTY: {banknifty_trend})")
                    print(f"      - VWAP Slope: ✅ {vwap_slope_reason}")
                    print(f"      - Candle Size: ✅ {candle_size_reason}")
                    print(f"      - Option Data: ✅ Valid (LTP at entry: ₹{buy_price:.2f}, Qty: {lot_size})")
                    print(f"   💰 Trade Details:")
                    print(f"      - Buy Price: ₹{buy_price:.2f} (fetched at entry moment)")
                    print(f"      - Quantity: {qty}")
                    print(f"      - Stop Loss: ₹{stop_loss_price:.2f} (previous candle low)")
                    print(f"      - Stock LTP: ₹{stock_ltp:.2f}")
                    print(f"      - Stock VWAP: ₹{stock_vwap:.2f}")
                    print(f"      - Stock VWAP (Previous Hour): ₹{stock_vwap_prev:.2f if stock_vwap_prev else 'N/A'}")
                    print(f"      - Option Contract: {stock.get('option_contract', 'N/A')}")
                    filter_info = f"VWAP Slope: {'SKIPPED (10:15)' if is_10_15_alert else vwap_slope_reason} | Candle Size: {'SKIPPED (10:15)' if is_10_15_alert else candle_size_reason}"
                    logger.info(f"✅ ENTRY DECISION: {stock_name} | Entry Time: {entry_time_str} | Alert Time: {alert_time_str} | Price: ₹{buy_price:.2f} | SL: ₹{stop_loss_price:.2f} | {filter_info} | Indices: NIFTY={nifty_trend}, BANKNIFTY={banknifty_trend}")
                else:
                    # No entry: Store qty, buy_price, and SL for reference, but don't execute trade
                    # This helps track what trades would have been if conditions were favorable
                    import math
                    
                    qty = lot_size  # Store the quantity that would have been traded
                    buy_price = option_ltp_value  # Store the option price at alert time
                    buy_time = None  # Don't set buy time since trade wasn't executed
                    sell_price = None  # No sell since trade wasn't executed
                    
                    # Stop Loss = Low price of previous day candle (for analysis purposes)
                    stop_loss_price = None
                    if option_candles and option_candles.get('previous_day_candle'):
                        previous_day_candle_low = option_candles.get('previous_day_candle', {}).get('low')
                        if previous_day_candle_low and previous_day_candle_low > 0:
                            stop_loss_price = float(previous_day_candle_low)
                        else:
                            stop_loss_price = 0.05
                    else:
                        stop_loss_price = 0.05
                    
                    status = 'no_entry'  # Trade not entered
                    pnl = None  # No P&L since trade wasn't executed
                    
                    # Log reason for no entry with complete trade setup
                    no_entry_time_str = triggered_datetime.strftime('%Y-%m-%d %H:%M:%S IST')
                    if is_after_3_00pm:
                        print(f"🚫 NO ENTRY: {stock_name} - Alert time {triggered_at_display} is at or after 3:00 PM")
                        print(f"   ⏰ Decision Time: {no_entry_time_str}")
                        print(f"   📊 Entry Conditions:")
                        print(f"      - Time Check: ❌ At or after 3:00 PM ({triggered_at_display})")
                        print(f"      - Index Trends: {'✅' if can_enter_trade_by_index else '❌'} {'Aligned' if can_enter_trade_by_index else f'Not Aligned (NIFTY: {nifty_trend}, BANKNIFTY: {banknifty_trend})'}")
                        print(f"      - VWAP Slope: {'✅' if vwap_slope_passed else '❌'} {vwap_slope_reason}")
                        print(f"      - Candle Size: {'✅' if candle_size_passed else '❌'} {candle_size_reason}")
                        print(f"      - Option Data: {'✅' if option_ltp_value > 0 and lot_size > 0 else '❌'} {'Valid' if option_ltp_value > 0 and lot_size > 0 else f'Missing (LTP: {option_ltp_value}, Qty: {lot_size})'}")
                        print(f"   💰 Would have been: Buy ₹{buy_price}, Qty: {qty}, SL: ₹{stop_loss_price} (not executed)")
                        logger.info(f"🚫 NO ENTRY DECISION: {stock_name} | Time: {no_entry_time_str} | Reason: Time >= 3:00 PM")
                    elif not can_enter_trade_by_index:
                        print(f"⚠️ NO ENTRY: {stock_name} - Index trends not aligned (NIFTY: {nifty_trend}, BANKNIFTY: {banknifty_trend})")
                        print(f"   ⏰ Decision Time: {no_entry_time_str}")
                        print(f"   📊 Entry Conditions:")
                        print(f"      - Time Check: ✅ Before 3:00 PM ({triggered_at_display})")
                        print(f"      - Index Trends: ❌ Not Aligned (NIFTY: {nifty_trend}, BANKNIFTY: {banknifty_trend})")
                        print(f"      - VWAP Slope: {'✅' if vwap_slope_passed else '❌'} {vwap_slope_reason}")
                        print(f"      - Candle Size: {'✅' if candle_size_passed else '❌'} {candle_size_reason}")
                        print(f"      - Option Data: {'✅' if option_ltp_value > 0 and lot_size > 0 else '❌'} {'Valid' if option_ltp_value > 0 and lot_size > 0 else f'Missing (LTP: {option_ltp_value}, Qty: {lot_size})'}")
                        print(f"   💰 Would have been: Buy ₹{buy_price}, Qty: {qty}, SL: ₹{stop_loss_price} (not executed)")
                        logger.info(f"🚫 NO ENTRY DECISION: {stock_name} | Time: {no_entry_time_str} | Reason: Index trends not aligned (NIFTY={nifty_trend}, BANKNIFTY={banknifty_trend})")
                    elif not vwap_slope_passed and not is_10_15_alert:
                        print(f"🚫 NO ENTRY: {stock_name} - VWAP slope condition not met")
                        print(f"   ⏰ Decision Time: {no_entry_time_str}")
                        print(f"   📊 Entry Conditions:")
                        print(f"      - Time Check: ✅ Before 3:00 PM ({triggered_at_display})")
                        print(f"      - Index Trends: ✅ Aligned (NIFTY: {nifty_trend}, BANKNIFTY: {banknifty_trend})")
                        print(f"      - VWAP Slope: ❌ {vwap_slope_reason}")
                        print(f"      - Candle Size: {'✅' if candle_size_passed else '❌'} {candle_size_reason}")
                        print(f"      - Option Data: {'✅' if option_ltp_value > 0 and lot_size > 0 else '❌'} {'Valid' if option_ltp_value > 0 and lot_size > 0 else f'Missing (LTP: {option_ltp_value}, Qty: {lot_size})'}")
                        print(f"   💰 Would have been: Buy ₹{buy_price}, Qty: {qty}, SL: ₹{stop_loss_price} (not executed)")
                        logger.info(f"🚫 NO ENTRY DECISION: {stock_name} | Time: {no_entry_time_str} | Reason: {vwap_slope_reason}")
                    elif not candle_size_passed and not is_10_15_alert:
                        print(f"🚫 NO ENTRY: {stock_name} - Candle size condition not met")
                        print(f"   ⏰ Decision Time: {no_entry_time_str}")
                        print(f"   📊 Entry Conditions:")
                        print(f"      - Time Check: ✅ Before 3:00 PM ({triggered_at_display})")
                        print(f"      - Index Trends: ✅ Aligned (NIFTY: {nifty_trend}, BANKNIFTY: {banknifty_trend})")
                        print(f"      - VWAP Slope: ✅ {vwap_slope_reason}")
                        print(f"      - Candle Size: ❌ {candle_size_reason}")
                        print(f"      - Option Data: {'✅' if option_ltp_value > 0 and lot_size > 0 else '❌'} {'Valid' if option_ltp_value > 0 and lot_size > 0 else f'Missing (LTP: {option_ltp_value}, Qty: {lot_size})'}")
                        print(f"   💰 Would have been: Buy ₹{buy_price}, Qty: {qty}, SL: ₹{stop_loss_price} (not executed)")
                        logger.info(f"🚫 NO ENTRY DECISION: {stock_name} | Time: {no_entry_time_str} | Reason: {candle_size_reason}")
                    elif option_ltp_value <= 0 or lot_size <= 0:
                        print(f"⚠️ NO ENTRY: {stock_name} - Missing option data (option_ltp={option_ltp_value}, qty={lot_size})")
                        print(f"   ⏰ Decision Time: {no_entry_time_str}")
                        print(f"   📊 Entry Conditions:")
                        print(f"      - Time Check: ✅ Before 3:00 PM ({triggered_at_display})")
                        print(f"      - Index Trends: ✅ Aligned (NIFTY: {nifty_trend}, BANKNIFTY: {banknifty_trend})")
                        print(f"      - VWAP Slope: {'✅' if vwap_slope_passed else '❌'} {vwap_slope_reason}")
                        print(f"      - Candle Size: {'✅' if candle_size_passed else '❌'} {candle_size_reason}")
                        print(f"      - Option Data: ❌ Missing (LTP: {option_ltp_value}, Qty: {lot_size})")
                        print(f"   💰 Would have been: Buy ₹{buy_price}, Qty: {qty}, SL: ₹{stop_loss_price} (not executed)")
                        # For missing data, keep qty=0, buy_price=None, stop_loss=None
                        qty = 0
                        buy_price = None
                        stop_loss_price = None
                        logger.info(f"🚫 NO ENTRY DECISION: {stock_name} | Time: {no_entry_time_str} | Reason: Missing option data (LTP={option_ltp_value}, Qty={lot_size})")
                    else:
                        print(f"⚠️ NO ENTRY: {stock_name} - Unknown reason")
                        print(f"   ⏰ Decision Time: {no_entry_time_str}")
                        print(f"   📊 Entry Conditions:")
                        print(f"      - Time Check: ✅ Before 3:00 PM ({triggered_at_display})")
                        print(f"      - Index Trends: ✅ Aligned (NIFTY: {nifty_trend}, BANKNIFTY: {banknifty_trend})")
                        print(f"      - VWAP Slope: {'✅' if vwap_slope_passed else '❌'} {vwap_slope_reason}")
                        print(f"      - Candle Size: {'✅' if candle_size_passed else '❌'} {candle_size_reason}")
                        print(f"      - Option Data: ✅ Valid (LTP: ₹{option_ltp_value:.2f}, Qty: {lot_size})")
                        print(f"   💰 Would have been: Buy ₹{buy_price}, Qty: {qty}, SL: ₹{stop_loss_price} (not executed)")
                        logger.info(f"🚫 NO ENTRY DECISION: {stock_name} | Time: {no_entry_time_str} | Reason: Unknown")
                
                # ALWAYS create database record with whatever data we have
                # SAFEGUARD: If buy_price is set, ensure buy_time is set (for both entered and no_entry trades)
                # Use alert_time as fallback if buy_time is None
                # CRITICAL: This prevents data integrity issues where trades have buy_price but no buy_time
                # Note: Even no_entry trades should have buy_time set if buy_price is set, for display purposes
                if buy_price and buy_price > 0:
                    if buy_time is None:
                        buy_time = triggered_datetime  # Use alert_time as buy_time fallback
                        if status != 'no_entry':
                            print(f"⚠️  Setting buy_time to alert_time for {stock_name} (buy_price set but buy_time was None)")
                        else:
                            print(f"ℹ️  Setting buy_time to alert_time for {stock_name} (no_entry trade with buy_price)")
                    elif buy_time != triggered_datetime:
                        # If buy_time is set but different from alert_time, log for debugging
                        print(f"ℹ️  buy_time ({buy_time}) differs from alert_time ({triggered_datetime}) for {stock_name}")
                
                # CRITICAL: Get instrument_key from the stock dictionary, not from a variable
                # This ensures each stock gets its own unique instrument_key
                stock_instrument_key = stock.get("instrument_key")
                
                # Extract OHLC data from option_candles
                option_candles_data = stock.get("option_candles")
                current_day_candle = option_candles_data.get('current_day_candle', {}) if option_candles_data else {}
                previous_day_candle = option_candles_data.get('previous_day_candle', {}) if option_candles_data else {}
                
                # Calculate and save candle size ratio and status (use the same calculation as above)
                saved_candle_size_ratio = None
                saved_candle_size_status = None
                if option_candles_data and current_day_candle and previous_day_candle:
                    try:
                        current_high = current_day_candle.get('high', 0)
                        current_low = current_day_candle.get('low', 0)
                        previous_high = previous_day_candle.get('high', 0)
                        previous_low = previous_day_candle.get('low', 0)
                        
                        current_size = abs(current_high - current_low)
                        previous_size = abs(previous_high - previous_low)
                        
                        if previous_size > 0:
                            saved_candle_size_ratio = current_size / previous_size
                            saved_candle_size_status = "Pass" if saved_candle_size_ratio < 7.5 else "Fail"
                        else:
                            saved_candle_size_status = "Skipped"
                    except Exception as e:
                        logger.warning(f"Error calculating candle size for {stock_name}: {str(e)}")
                        saved_candle_size_status = None
                elif is_10_15_alert:
                    saved_candle_size_status = "Skipped"
                else:
                    # No candle data available
                    saved_candle_size_status = None
                
                # Ensure option_type is set correctly based on alert type if not already set
                option_type_from_stock = stock.get("option_type", "")
                if not option_type_from_stock:
                    # Set option_type based on alert_type (Bearish = PE, Bullish = CE)
                    option_type_from_stock = 'PE' if data_type == 'Bearish' else 'CE'
                    print(f"⚠️ Option type not found in stock data for {stock_name}, setting to {option_type_from_stock} based on alert type {data_type}")
                
                db_record = IntradayStockOption(
                    alert_time=triggered_datetime,
                    alert_type=data_type,
                    scan_name=processed_data.get("scan_name", ""),
                    stock_name=stock_name,
                    stock_ltp=stock.get("last_traded_price") or stock.get("trigger_price", 0.0),
                    stock_vwap=stock.get("stock_vwap", 0.0),
                    stock_vwap_previous_hour=stock.get("stock_vwap_previous_hour"),
                    stock_vwap_previous_hour_time=stock.get("stock_vwap_previous_hour_time"),
                    option_contract=stock.get("option_contract", ""),
                    option_type=option_type_from_stock,
                    option_strike=stock.get("otm1_strike", 0.0),
                    option_ltp=option_ltp_value,
                    option_vwap=stock.get("option_vwap", 0.0),
                    # Option daily OHLC candles (current day vs previous day)
                    option_current_candle_open=current_day_candle.get('open'),
                    option_current_candle_high=current_day_candle.get('high'),
                    option_current_candle_low=current_day_candle.get('low'),
                    option_current_candle_close=current_day_candle.get('close'),
                    option_current_candle_time=current_day_candle.get('time'),
                    option_previous_candle_open=previous_day_candle.get('open'),
                    option_previous_candle_high=previous_day_candle.get('high'),
                    option_previous_candle_low=previous_day_candle.get('low'),
                    option_previous_candle_close=previous_day_candle.get('close'),
                    option_previous_candle_time=previous_day_candle.get('time'),
                    # Candle size fields (calculated when stock is received from webhook)
                    candle_size_ratio=saved_candle_size_ratio,
                    candle_size_status=saved_candle_size_status,
                    qty=qty,
                    trade_date=trading_date,
                    status=status,
                    buy_price=buy_price,
                    instrument_key=stock_instrument_key,  # Get from stock dictionary, not from variable
                    stop_loss=stop_loss_price,
                    sell_price=sell_price,
                    buy_time=buy_time,  # Will be set to triggered_datetime if trade was entered
                    exit_reason=None,
                    pnl=pnl
                )
                db.add(db_record)
                saved_count += 1
                print(f"   💾 Saved {stock_name} to database (status: {status})")
                
            except Exception as db_error:
                failed_count += 1
                print(f"❌ Error saving stock {stock_name} to database: {str(db_error)}")
                import traceback
                traceback.print_exc()
                
                # Try to save with minimal data as last resort
                try:
                    print(f"   🔄 Attempting minimal save for {stock_name}...")
                    minimal_record = IntradayStockOption(
                        alert_time=triggered_datetime,
                        alert_type=data_type,
                        scan_name=processed_data.get("scan_name", "Unknown"),
                        stock_name=stock_name,
                        stock_ltp=stock.get("trigger_price", 0.0),
                        stock_vwap=0.0,
                        option_contract="",
                        option_type=forced_option_type,
                        option_strike=0.0,
                        option_ltp=0.0,
                        option_vwap=0.0,
                        qty=0,
                        trade_date=trading_date,
                        status='alert_received',  # Minimal status
                        buy_price=None,
                        stop_loss=None,
                        sell_price=None,
                        buy_time=None,
                        exit_reason=None,
                        pnl=None
                    )
                    db.add(minimal_record)
                    saved_count += 1
                    print(f"   ✅ Minimal save successful for {stock_name}")
                except Exception as minimal_error:
                    print(f"   ❌ Even minimal save failed for {stock_name}: {str(minimal_error)}")
        
        # Commit all database records
        try:
            db.commit()
            print(f"\n✅ DATABASE COMMIT SUCCESSFUL")
            print(f"   • Total Stocks Processed: {len(processed_data.get('stocks', []))}")
            print(f"   • Saved to DB: {saved_count} stocks")
            if failed_count > 0:
                print(f"   • Failed: {failed_count} stocks")
            print(f"   • Alert Type: {data_type}")
            print(f"   • Alert Time: {triggered_at_str}")
            print(f"\n📊 ENTRY FILTER SUMMARY:")
            print(f"   • VWAP Slope Filter: >= 45 degrees")
            print(f"   • Candle Size Filter: Current candle < 7.5× previous candle")
            print(f"   • Stocks that passed both filters: Check 'bought' status above")
            print(f"   • Stocks rejected: Check 'no_entry' with filter reasons")
        except Exception as commit_error:
            print(f"\n❌ DATABASE COMMIT FAILED: {str(commit_error)}")
            print(f"   • Attempted to save: {saved_count} stocks")
            print(f"   • Rolling back transaction...")
            db.rollback()
            
            # Log all stock names that were in this webhook for recovery
            print(f"\n⚠️ LOST ALERT - Stock names for manual recovery:")
            for stock in processed_data.get("stocks", []):
                print(f"   - {stock.get('stock_name', 'UNKNOWN')}: {stock.get('trigger_price', 0.0)}")
            
            raise HTTPException(
                status_code=500,
                detail=f"Database commit failed: {str(commit_error)}"
            )
        
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
        
        # Track webhook success
        if health_monitor:
            health_monitor.record_webhook_success()
        
        return JSONResponse(
            status_code=200,
            content={
                "status": "success",
                "message": f"{data_type} webhook data received and processed",
                "alert_type": data_type.lower(),
                "stocks_count": len(processed_data["stocks"]),
                "timestamp": processed_data["received_at"],
                "date": current_date,
                "saved_to_database": saved_count
            }
        )
        
    except Exception as e:
        print(f"❌ CRITICAL ERROR processing webhook: {str(e)}")
        import traceback
        traceback.print_exc()
        
        # Track webhook failure
        if health_monitor:
            health_monitor.record_webhook_failure()
        
        return JSONResponse(
            status_code=500,
            content={
                "status": "error",
                "message": f"Failed to process webhook: {str(e)}",
                "error_type": type(e).__name__
            }
        )

@router.post("/manual-start-schedulers")
async def manual_start_schedulers():
    """Manually start all schedulers if they're not running"""
    try:
        from services.health_monitor import start_health_monitor, health_monitor
        from services.vwap_updater import start_vwap_updater, vwap_updater
        from services.master_stock_scheduler import start_scheduler, master_stock_scheduler
        from services.instruments_downloader import start_instruments_scheduler, instruments_scheduler
        
        results = {}
        
        # Start Health Monitor
        if not health_monitor.is_running:
            start_health_monitor()
            results["health_monitor"] = "started"
            logger.info("✅ Health Monitor manually started")
        else:
            results["health_monitor"] = "already_running"
        
        # Start VWAP Updater
        if not vwap_updater.is_running:
            start_vwap_updater()
            results["vwap_updater"] = "started"
            logger.info("✅ VWAP Updater manually started")
        else:
            results["vwap_updater"] = "already_running"
        
        # Start Master Stock Scheduler
        if not master_stock_scheduler.is_running:
            start_scheduler()
            results["master_stock"] = "started"
            logger.info("✅ Master Stock Scheduler manually started")
        else:
            results["master_stock"] = "already_running"
        
        # Start Instruments Scheduler
        if not instruments_scheduler.is_running:
            start_instruments_scheduler()
            results["instruments"] = "started"
            logger.info("✅ Instruments Scheduler manually started")
        else:
            results["instruments"] = "already_running"
        
        return {
            "success": True,
            "message": "Schedulers checked and started if needed",
            "results": results,
            "timestamp": datetime.now().isoformat()
        }
        
    except Exception as e:
        logger.error(f"Error starting schedulers: {e}")
        import traceback
        traceback.print_exc()
        return {
            "success": False,
            "error": str(e),
            "timestamp": datetime.now().isoformat()
        }


@router.post("/manual-close-trades")
async def manual_close_trades(db: Session = Depends(get_db)):
    """Manually trigger close_all_open_trades - one-time emergency use"""
    try:
        from services.vwap_updater import close_all_open_trades
        
        logger.info("🔧 Manual trigger: Closing all open trades NOW")
        
        # Call the close function
        await close_all_open_trades()
        
        return {
            "success": True,
            "message": "All open trades closed successfully",
            "timestamp": datetime.now().isoformat()
        }
        
    except Exception as e:
        logger.error(f"Error in manual close trades: {e}")
        import traceback
        traceback.print_exc()
        return {
            "success": False,
            "error": str(e),
            "timestamp": datetime.now().isoformat()
        }


@router.post("/trigger-cycle-vwap-slope")
async def trigger_cycle_vwap_slope(cycle_number: int = Query(..., ge=1, le=5)):
    """
    Manually trigger VWAP slope calculation for a specific cycle
    Useful for testing or reprocessing missed cycles
    """
    try:
        from backend.services.vwap_updater import calculate_vwap_slope_for_cycle
        from datetime import datetime
        import pytz
        
        ist = pytz.timezone('Asia/Kolkata')
        now = datetime.now(ist)
        
        logger.info(f"🔧 Manual trigger: Cycle {cycle_number} VWAP slope calculation")
        
        # Run the cycle calculation
        await calculate_vwap_slope_for_cycle(cycle_number, now)
        
        return {
            "success": True,
            "message": f"Cycle {cycle_number} VWAP slope calculation completed",
            "cycle_number": cycle_number,
            "timestamp": now.isoformat()
        }
        
    except Exception as e:
        logger.error(f"Error triggering cycle {cycle_number}: {e}")
        import traceback
        traceback.print_exc()
        return {
            "success": False,
            "error": str(e),
            "cycle_number": cycle_number
        }

@router.post("/update-no-entry-trades")
async def manually_update_no_entry_trades(db: Session = Depends(get_db)):
    """
    Manually trigger update for all 'no_entry' trades from today
    This will:
    - Fetch and update VWAP slope data
    - Fetch and update candle size data  
    - Re-evaluate entry conditions
    - Update all hourly market data
    """
    try:
        from backend.services.vwap_updater import update_vwap_for_all_open_positions
        
        logger.info("🔄 Manual trigger: Updating all 'no_entry' trades from today")
        
        # Call the update function which handles no_entry trades
        await update_vwap_for_all_open_positions()
        
        # Count updated records
        import pytz
        ist = pytz.timezone('Asia/Kolkata')
        now = datetime.now(ist)
        today = now.replace(hour=0, minute=0, second=0, microsecond=0)
        
        no_entry_count = db.query(IntradayStockOption).filter(
            and_(
                IntradayStockOption.trade_date >= today,
                IntradayStockOption.status == 'no_entry',
                IntradayStockOption.exit_reason == None
            )
        ).count()
        
        return {
            "success": True,
            "message": f"Update completed for all 'no_entry' trades from today",
            "no_entry_trades_count": no_entry_count,
            "timestamp": datetime.now().isoformat()
        }
        
    except Exception as e:
        logger.error(f"Error in manual update no_entry trades: {e}")
        import traceback
        traceback.print_exc()
        return {
            "success": False,
            "error": str(e),
            "timestamp": datetime.now().isoformat()
        }

@router.get("/scheduler-status")
async def get_scheduler_status():
    """Get status of all schedulers - verifies they are running"""
    try:
        from backend.services.health_monitor import health_monitor
        from backend.services.vwap_updater import vwap_updater
        from backend.services.master_stock_scheduler import master_stock_scheduler
        from backend.services.instruments_downloader import instruments_scheduler
        
        status = {
            "health_monitor": {
                "running": health_monitor.is_running,
                "state": health_monitor.scheduler.state if health_monitor.scheduler and hasattr(health_monitor.scheduler, 'state') else None,
                "jobs_count": len(health_monitor.scheduler.get_jobs()) if health_monitor.scheduler else 0
            },
            "vwap_updater": {
                "running": vwap_updater.is_running,
                "state": vwap_updater.scheduler.state if vwap_updater.scheduler and hasattr(vwap_updater.scheduler, 'state') else None,
                "jobs_count": len(vwap_updater.scheduler.get_jobs()) if vwap_updater.scheduler else 0,
                "has_3_25pm_close_job": vwap_updater.scheduler.get_job('close_all_trades_eod') is not None if vwap_updater.scheduler else False
            },
            "master_stock": {
                "running": master_stock_scheduler.is_running
            },
            "instruments": {
                "running": instruments_scheduler.is_running
            }
        }
        
        # Get next few jobs for VWAP updater
        if vwap_updater.scheduler:
            jobs = vwap_updater.scheduler.get_jobs()
            status["vwap_updater"]["next_jobs"] = [
                {"name": job.name, "next_run": str(job.next_run_time)} 
                for job in sorted(jobs, key=lambda x: x.next_run_time if x.next_run_time else float('inf'))[:5]
            ]
        
        all_running = (
            health_monitor.is_running and 
            vwap_updater.is_running and
            master_stock_scheduler.is_running and
            instruments_scheduler.is_running
        )
        
        return {
            "success": True, 
            "all_schedulers_running": all_running,
            "schedulers": status
        }
        
    except Exception as e:
        logger.error(f"Error getting scheduler status: {e}")
        return {"success": False, "error": str(e)}


@router.post("/deploy-backend")
async def deploy_backend(background_tasks: BackgroundTasks):
    """
    Trigger backend deployment (git pull + restart)
    This runs in background and returns immediately
    """
    import subprocess
    
    def run_deployment():
        try:
            # Run deployment script in background (non-blocking)
            script_path = "/home/ubuntu/trademanthan/backend/scripts/deploy_backend.sh"
            subprocess.Popen(
                ["/bin/bash", script_path],
                stdout=subprocess.DEVNULL,
                stderr=subprocess.DEVNULL,
                start_new_session=True
            )
            logger.info("✅ Backend deployment initiated")
        except Exception as e:
            logger.error(f"Error starting deployment: {e}")
    
    # Start deployment in background
    background_tasks.add_task(run_deployment)
    
    return {
        "success": True,
        "message": "Deployment initiated. Check /tmp/deploy_backend.log for progress.",
        "status_endpoint": "/scan/deployment-status",
        "log_file": "/tmp/deploy_backend.log"
    }

@router.get("/deployment-status")
async def get_deployment_status():
    """Get the latest deployment status from log file"""
    try:
        log_file = "/tmp/deploy_backend.log"
        if os.path.exists(log_file):
            # Get last 20 lines
            with open(log_file, 'r') as f:
                lines = f.readlines()
                recent_lines = lines[-20:] if len(lines) > 20 else lines
                return {
                    "success": True,
                    "log": "".join(recent_lines),
                    "total_lines": len(lines)
                }
        else:
            return {
                "success": False,
                "message": "Deployment log not found"
            }
    except Exception as e:
        return {
            "success": False,
            "error": str(e)
        }

@router.get("/health")
async def health_check(db: Session = Depends(get_db)):
    """
    Health check endpoint for monitoring system status
    Returns status of all critical components
    """
    try:
        import pytz
        ist = pytz.timezone('Asia/Kolkata')
        now = datetime.now(ist)
        today = now.date()
        
        # Check database
        db_healthy = False
        try:
            db.execute(text("SELECT 1"))
            db_healthy = True
        except Exception as e:
            print(f"Database health check failed: {e}")
        
        # Check today's webhook activity
        today_alerts = 0
        try:
            today_alerts = db.query(IntradayStockOption).filter(
                IntradayStockOption.trade_date >= datetime.combine(today, datetime.min.time())
            ).count()
        except:
            pass
        
        # Check Upstox token (only during market hours to avoid unnecessary API calls)
        token_valid = False
        token_error = None
        try:
            # Only check during market hours (9 AM - 4 PM)
            current_hour = datetime.now().hour
            if 9 <= current_hour <= 16:
                result = vwap_service.check_index_trends()
                if result and result.get('nifty'):
                    token_valid = True
            else:
                # After market hours, assume token is valid (skip API call)
                token_valid = True
                token_error = "Market closed - token check skipped"
        except Exception as e:
            token_error = str(e)
        
        # Check instruments file
        instruments_exists = os.path.exists("/home/ubuntu/trademanthan/data/instruments/nse_instruments.json")
        
        # Overall health status
        is_healthy = db_healthy and (now.hour < 11 or today_alerts > 0 or now.weekday() >= 5)
        
        health_data = {
            "status": "healthy" if is_healthy else "degraded",
            "timestamp": now.isoformat(),
            "components": {
                "database": {
                    "status": "ok" if db_healthy else "error",
                    "healthy": db_healthy
                },
                "upstox_api": {
                    "status": "ok" if token_valid else "error",
                    "healthy": token_valid,
                    "error": token_error if not token_valid else None
                },
                "webhooks": {
                    "today_count": today_alerts,
                    "status": "ok" if (today_alerts > 0 or now.hour < 11 or now.weekday() >= 5) else "warning",
                    "message": f"{today_alerts} alerts today"
                },
                "instruments_file": {
                    "status": "ok" if instruments_exists else "error",
                    "exists": instruments_exists
                }
            },
            "metrics": {
                "consecutive_webhook_failures": health_monitor.webhook_failures if health_monitor else 0,
                "consecutive_token_failures": health_monitor.api_token_failures if health_monitor else 0,
                "consecutive_db_failures": health_monitor.database_failures if health_monitor else 0
            }
        }
        
        status_code = 200 if is_healthy else 503
        return JSONResponse(status_code=status_code, content=health_data)
        
    except Exception as e:
        print(f"Health check endpoint failed: {str(e)}")
        import traceback
        traceback.print_exc()
        return JSONResponse(
            status_code=500,
            content={
                "status": "error",
                "message": f"Health check failed: {str(e)}"
            }
        )

@router.post("/chartink-webhook-bullish")
async def receive_bullish_webhook(request: Request, background_tasks: BackgroundTasks, db: Session = Depends(get_db)):
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
    try:
        # Try to read JSON body with timeout protection
        data = await asyncio.wait_for(request.json(), timeout=2.0)
        
        # Enhanced logging: Log full payload for debugging
        stocks_count = len(data.get('stocks', '').split(',')) if isinstance(data.get('stocks'), str) else len(data.get('stocks', []))
        logger.info(f"📥 Received bullish webhook with {stocks_count} stocks")
        logger.info(f"📦 Full webhook payload: {json.dumps(data, indent=2)}")
        print(f"📥 Received bullish webhook at {datetime.now().isoformat()}")
        print(f"📦 Payload: {json.dumps(data, indent=2)}")
        
        # Respond immediately to prevent timeout
        response_data = {
            "status": "accepted",
            "message": "Bullish webhook received and queued for processing",
            "alert_type": "bullish",
            "timestamp": datetime.now().isoformat()
        }
        
        # Process in background to avoid blocking
        background_tasks.add_task(process_webhook_data, data, db, 'bullish')
        
        return JSONResponse(content=response_data, status_code=202)
        
    except ClientDisconnect:
        logger.error("⚠️ Client disconnected before webhook data could be read (bullish). Chartink timeout likely due to slow processing.")
        return JSONResponse(
            content={
                "status": "error",
                "message": "Client disconnected - webhook data lost. Please check Chartink timeout settings.",
                "suggestion": "Consider increasing Chartink webhook timeout or optimizing server processing speed"
            },
            status_code=499
        )
    except asyncio.TimeoutError:
        logger.error("⚠️ Timeout reading bullish webhook body")
        return JSONResponse(
            content={"status": "error", "message": "Timeout reading request body"},
            status_code=408
        )
    except Exception as e:
        logger.error(f"❌ Error processing bullish webhook: {str(e)}")
        return JSONResponse(
            content={"status": "error", "message": f"Error: {str(e)}"},
            status_code=500
        )

@router.post("/chartink-webhook-bearish")
async def receive_bearish_webhook(request: Request, background_tasks: BackgroundTasks, db: Session = Depends(get_db)):
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
    try:
        # Try to read JSON body with timeout protection
        data = await asyncio.wait_for(request.json(), timeout=2.0)
        
        # Enhanced logging: Log full payload for debugging
        stocks_count = len(data.get('stocks', '').split(',')) if isinstance(data.get('stocks'), str) else len(data.get('stocks', []))
        logger.info(f"📥 Received bearish webhook with {stocks_count} stocks")
        logger.info(f"📦 Full webhook payload: {json.dumps(data, indent=2)}")
        print(f"📥 Received bearish webhook at {datetime.now().isoformat()}")
        print(f"📦 Payload: {json.dumps(data, indent=2)}")
        
        # Respond immediately to prevent timeout
        response_data = {
            "status": "accepted",
            "message": "Bearish webhook received and queued for processing",
            "alert_type": "bearish",
            "timestamp": datetime.now().isoformat()
        }
        
        # Process in background to avoid blocking
        background_tasks.add_task(process_webhook_data, data, db, 'bearish')
        
        return JSONResponse(content=response_data, status_code=202)
        
    except ClientDisconnect:
        logger.error("⚠️ Client disconnected before webhook data could be read (bearish). Chartink timeout likely due to slow processing.")
        return JSONResponse(
            content={
                "status": "error",
                "message": "Client disconnected - webhook data lost. Please check Chartink timeout settings.",
                "suggestion": "Consider increasing Chartink webhook timeout or optimizing server processing speed"
            },
            status_code=499
        )
    except asyncio.TimeoutError:
        logger.error("⚠️ Timeout reading bearish webhook body")
        return JSONResponse(
            content={"status": "error", "message": "Timeout reading request body"},
            status_code=408
        )
    except Exception as e:
        logger.error(f"❌ Error processing bearish webhook: {str(e)}")
        return JSONResponse(
            content={"status": "error", "message": f"Error: {str(e)}"},
            status_code=500
        )

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
        from datetime import timedelta
        if current_hour > 9 or (current_hour == 9 and current_minute >= 0):
            # After 9:00 AM - show only today's data
            filter_date_start = today
            filter_date_end = today + timedelta(days=1)
            print(f"Current time: {now.strftime('%Y-%m-%d %H:%M:%S')} IST - Showing TODAY's data (after 9:00 AM)")
        else:
            # Before 9:00 AM - show yesterday's data
            filter_date_start = today - timedelta(days=1)
            filter_date_end = today
            print(f"Current time: {now.strftime('%Y-%m-%d %H:%M:%S')} IST - Showing YESTERDAY's data (before 9:00 AM)")
        
        # For intraday alerts, use today if it's a trading day, otherwise get last trading date
        if vwap_service.is_trading_day(today):
            trading_date = today
        else:
            trading_date = vwap_service.get_last_trading_date(now)
        current_date = trading_date.strftime('%Y-%m-%d')
        
        # Fetch Bullish alerts from database for the current trading day only
        # Use date range comparison instead of exact equality to handle timezone/time differences
        bullish_records = db.query(IntradayStockOption).filter(
            IntradayStockOption.alert_type == 'Bullish',
            IntradayStockOption.trade_date >= filter_date_start,
            IntradayStockOption.trade_date < filter_date_end
        ).order_by(desc(IntradayStockOption.alert_time)).limit(200).all()
        
        # Fetch Bearish alerts from database for the current trading day only
        # Use date range comparison instead of exact equality to handle timezone/time differences
        bearish_records = db.query(IntradayStockOption).filter(
            IntradayStockOption.alert_type == 'Bearish',
            IntradayStockOption.trade_date >= filter_date_start,
            IntradayStockOption.trade_date < filter_date_end
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
                
                # Calculate VWAP slope and candle size for display
                vwap_slope_status = None
                vwap_slope_angle = None
                vwap_slope_direction = None
                candle_size_ratio = None
                candle_size_status = None
                
                # Check if this is a 10:15 AM alert (filters are skipped)
                alert_hour = record.alert_time.hour if record.alert_time else None
                alert_minute = record.alert_time.minute if record.alert_time else None
                is_10_15_alert = (alert_hour == 10 and alert_minute == 15)
                
                # Use saved VWAP slope values if available (from cycle-based calculations)
                # Otherwise, calculate on-the-fly from stock_vwap and stock_vwap_previous_hour
                if hasattr(record, 'vwap_slope_angle') and record.vwap_slope_angle is not None:
                    # Use saved values from cycle-based calculation
                    vwap_slope_angle = record.vwap_slope_angle
                    vwap_slope_status = getattr(record, 'vwap_slope_status', None)
                    vwap_slope_direction = getattr(record, 'vwap_slope_direction', None)
                elif record.stock_vwap and record.stock_vwap > 0 and record.stock_vwap_previous_hour and record.stock_vwap_previous_hour > 0 and record.stock_vwap_previous_hour_time:
                    # Calculate VWAP slope on-the-fly if not saved yet
                    try:
                        slope_result = vwap_service.vwap_slope(
                            vwap1=record.stock_vwap_previous_hour,
                            time1=record.stock_vwap_previous_hour_time,
                            vwap2=record.stock_vwap,
                            time2=record.alert_time
                        )
                        # Handle new dictionary return format
                        if isinstance(slope_result, dict):
                            vwap_slope_status = slope_result.get("status", "No")
                            vwap_slope_angle = slope_result.get("angle", 0.0)
                            vwap_slope_direction = slope_result.get("direction", "flat")
                        else:
                            # Backward compatibility: handle old string return format
                            vwap_slope_status = slope_result
                    except:
                        pass
                elif is_10_15_alert:
                    # For 10:15 AM alerts, show "Skipped" since filters are bypassed
                    vwap_slope_status = "Skipped"
                
                # Calculate candle size if data is available
                if record.option_current_candle_high and record.option_current_candle_low and record.option_previous_candle_high and record.option_previous_candle_low:
                    try:
                        current_size = abs(record.option_current_candle_high - record.option_current_candle_low)
                        previous_size = abs(record.option_previous_candle_high - record.option_previous_candle_low)
                        if previous_size > 0:
                            candle_size_ratio = current_size / previous_size
                            candle_size_status = "Pass" if candle_size_ratio < 7.5 else "Fail"
                    except:
                        pass
                elif is_10_15_alert:
                    # For 10:15 AM alerts, show "Skipped" since filters are bypassed
                    candle_size_status = "Skipped"
                
                # Retry option contract determination if missing (only for recent records to avoid performance issues)
                option_contract = record.option_contract or ""
                # Only retry for records from today (not historical data)
                is_today = record.trade_date and record.trade_date.date() == today.date()
                if not option_contract and record.stock_ltp and record.stock_ltp > 0 and is_today:
                    try:
                        retry_contract = find_option_contract_from_master_stock(
                            db, record.stock_name, record.option_type or 'CE', record.stock_ltp, vwap_service
                        )
                        if retry_contract:
                            option_contract = retry_contract
                            # Update database record
                            record.option_contract = retry_contract
                            # Commit immediately to persist the change
                            try:
                                db.commit()
                                print(f"✅ Retried and found option contract for {record.stock_name}: {retry_contract}")
                            except Exception as commit_error:
                                db.rollback()
                                print(f"⚠️ Failed to commit option contract for {record.stock_name}: {str(commit_error)}")
                    except Exception as retry_error:
                        print(f"⚠️ Retry option contract determination failed for {record.stock_name}: {str(retry_error)}")
                
                grouped_bullish[alert_key]["stocks"].append({
                    "stock_name": record.stock_name,
                    "trigger_price": record.stock_ltp or 0.0,
                    "last_traded_price": record.stock_ltp or 0.0,
                    "stock_vwap": record.stock_vwap or 0.0,
                    "stock_vwap_previous_hour": record.stock_vwap_previous_hour,
                    "stock_vwap_previous_hour_time": record.stock_vwap_previous_hour_time.isoformat() if record.stock_vwap_previous_hour_time else None,
                    "option_contract": option_contract,
                    "option_type": record.option_type or "CE",
                    "otm1_strike": record.option_strike or 0.0,
                    "option_ltp": record.option_ltp or 0.0,
                    "option_vwap": record.option_vwap or 0.0,
                    # Option OHLC candles
                    "option_current_candle": {
                        "open": record.option_current_candle_open,
                        "high": record.option_current_candle_high,
                        "low": record.option_current_candle_low,
                        "close": record.option_current_candle_close,
                        "time": record.option_current_candle_time.isoformat() if record.option_current_candle_time else None
                    } if record.option_current_candle_open else None,
                    "option_previous_candle": {
                        "open": record.option_previous_candle_open,
                        "high": record.option_previous_candle_high,
                        "low": record.option_previous_candle_low,
                        "close": record.option_previous_candle_close,
                        "time": record.option_previous_candle_time.isoformat() if record.option_previous_candle_time else None
                    } if record.option_previous_candle_open else None,
                    # Entry filter status
                    "vwap_slope_status": vwap_slope_status,
                    "vwap_slope_angle": vwap_slope_angle,
                    "vwap_slope_direction": vwap_slope_direction,
                    "candle_size_ratio": candle_size_ratio,
                    "candle_size_status": candle_size_status,
                    "qty": record.qty or 0,
                    "buy_price": record.buy_price or 0.0,
                    "stop_loss": record.stop_loss or 0.0,
                    "sell_price": record.sell_price or 0.0,
                    "exit_reason": record.exit_reason or None,
                    "pnl": record.pnl or 0.0,
                    "status": record.status  # Include status to identify no_entry trades
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
                
                # Calculate VWAP slope and candle size for display
                vwap_slope_status = None
                vwap_slope_angle = None
                vwap_slope_direction = None
                candle_size_ratio = None
                candle_size_status = None
                
                # Check if this is a 10:15 AM alert (filters are skipped)
                alert_hour = record.alert_time.hour if record.alert_time else None
                alert_minute = record.alert_time.minute if record.alert_time else None
                is_10_15_alert = (alert_hour == 10 and alert_minute == 15)
                
                # Use saved VWAP slope values if available (from cycle-based calculations)
                # Otherwise, calculate on-the-fly from stock_vwap and stock_vwap_previous_hour
                if hasattr(record, 'vwap_slope_angle') and record.vwap_slope_angle is not None:
                    # Use saved values from cycle-based calculation
                    vwap_slope_angle = record.vwap_slope_angle
                    vwap_slope_status = getattr(record, 'vwap_slope_status', None)
                    vwap_slope_direction = getattr(record, 'vwap_slope_direction', None)
                elif record.stock_vwap and record.stock_vwap > 0 and record.stock_vwap_previous_hour and record.stock_vwap_previous_hour > 0 and record.stock_vwap_previous_hour_time:
                    # Calculate VWAP slope on-the-fly if not saved yet
                    try:
                        slope_result = vwap_service.vwap_slope(
                            vwap1=record.stock_vwap_previous_hour,
                            time1=record.stock_vwap_previous_hour_time,
                            vwap2=record.stock_vwap,
                            time2=record.alert_time
                        )
                        # Handle new dictionary return format
                        if isinstance(slope_result, dict):
                            vwap_slope_status = slope_result.get("status", "No")
                            vwap_slope_angle = slope_result.get("angle", 0.0)
                            vwap_slope_direction = slope_result.get("direction", "flat")
                        else:
                            # Backward compatibility: handle old string return format
                            vwap_slope_status = slope_result
                    except:
                        pass
                elif is_10_15_alert:
                    # For 10:15 AM alerts, show "Skipped" since filters are bypassed
                    vwap_slope_status = "Skipped"
                
                # Calculate candle size if data is available
                if record.option_current_candle_high and record.option_current_candle_low and record.option_previous_candle_high and record.option_previous_candle_low:
                    try:
                        current_size = abs(record.option_current_candle_high - record.option_current_candle_low)
                        previous_size = abs(record.option_previous_candle_high - record.option_previous_candle_low)
                        if previous_size > 0:
                            candle_size_ratio = current_size / previous_size
                            candle_size_status = "Pass" if candle_size_ratio < 7.5 else "Fail"
                    except:
                        pass
                elif is_10_15_alert:
                    # For 10:15 AM alerts, show "Skipped" since filters are bypassed
                    candle_size_status = "Skipped"
                
                # Retry option contract determination if missing (only for recent records to avoid performance issues)
                option_contract = record.option_contract or ""
                # Only retry for records from today (not historical data)
                is_today = record.trade_date and record.trade_date.date() == today.date()
                if not option_contract and record.stock_ltp and record.stock_ltp > 0 and is_today:
                    try:
                        retry_contract = find_option_contract_from_master_stock(
                            db, record.stock_name, record.option_type or 'PE', record.stock_ltp, vwap_service
                        )
                        if retry_contract:
                            option_contract = retry_contract
                            # Update database record
                            record.option_contract = retry_contract
                            # Commit immediately to persist the change
                            try:
                                db.commit()
                                print(f"✅ Retried and found option contract for {record.stock_name}: {retry_contract}")
                            except Exception as commit_error:
                                db.rollback()
                                print(f"⚠️ Failed to commit option contract for {record.stock_name}: {str(commit_error)}")
                    except Exception as retry_error:
                        print(f"⚠️ Retry option contract determination failed for {record.stock_name}: {str(retry_error)}")
                
                grouped_bearish[alert_key]["stocks"].append({
                    "stock_name": record.stock_name,
                    "trigger_price": record.stock_ltp or 0.0,
                    "last_traded_price": record.stock_ltp or 0.0,
                    "stock_vwap": record.stock_vwap or 0.0,
                    "stock_vwap_previous_hour": record.stock_vwap_previous_hour,
                    "stock_vwap_previous_hour_time": record.stock_vwap_previous_hour_time.isoformat() if record.stock_vwap_previous_hour_time else None,
                    "option_contract": option_contract,
                    "option_type": record.option_type or "PE",
                    "otm1_strike": record.option_strike or 0.0,
                    "option_ltp": record.option_ltp or 0.0,
                    "option_vwap": record.option_vwap or 0.0,
                    # Option OHLC candles
                    "option_current_candle": {
                        "open": record.option_current_candle_open,
                        "high": record.option_current_candle_high,
                        "low": record.option_current_candle_low,
                        "close": record.option_current_candle_close,
                        "time": record.option_current_candle_time.isoformat() if record.option_current_candle_time else None
                    } if record.option_current_candle_open else None,
                    "option_previous_candle": {
                        "open": record.option_previous_candle_open,
                        "high": record.option_previous_candle_high,
                        "low": record.option_previous_candle_low,
                        "close": record.option_previous_candle_close,
                        "time": record.option_previous_candle_time.isoformat() if record.option_previous_candle_time else None
                    } if record.option_previous_candle_open else None,
                    # Entry filter status
                    "vwap_slope_status": vwap_slope_status,
                    "vwap_slope_angle": vwap_slope_angle,
                    "vwap_slope_direction": vwap_slope_direction,
                    "candle_size_ratio": candle_size_ratio,
                    "candle_size_status": candle_size_status,
                    "qty": record.qty or 0,
                    "buy_price": record.buy_price or 0.0,
                    "stop_loss": record.stop_loss or 0.0,
                    "sell_price": record.sell_price or 0.0,
                    "exit_reason": record.exit_reason or None,
                    "pnl": record.pnl or 0.0,
                    "status": record.status  # Include status to identify no_entry trades
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
        
        # Check index trends before returning data (only during market hours to avoid unnecessary API calls)
        import pytz
        ist = pytz.timezone('Asia/Kolkata')
        current_time = datetime.now(ist)
        current_hour = current_time.hour
        
        # Only check index trends during market hours (9 AM - 4 PM)
        if 9 <= current_hour <= 16:
            index_check = vwap_service.check_index_trends()
        else:
            # After market hours, fetch closing prices to show last known state
            logger.info(f"⏰ After market hours ({current_hour}:00) - fetching closing prices for display")
            
            # Get OHLC data which contains closing prices
            nifty_quote = vwap_service.get_market_quote_by_key(vwap_service.NIFTY50_KEY)
            banknifty_quote = vwap_service.get_market_quote_by_key(vwap_service.BANKNIFTY_KEY)
            
            # Process NIFTY closing data
            nifty_data = {}
            nifty_trend = 'unknown'
            if nifty_quote:
                ohlc = nifty_quote.get('ohlc', {})
                nifty_close = float(ohlc.get('close', 0)) if ohlc.get('close') else float(nifty_quote.get('last_price', 0))
                nifty_open = float(ohlc.get('open', 0))
                if nifty_close > 0 and nifty_open > 0:
                    nifty_trend = 'bullish' if nifty_close > nifty_open else 'bearish' if nifty_close < nifty_open else 'neutral'
                    nifty_data = {
                        'ltp': nifty_close,
                        'day_open': nifty_open,
                        'close_price': nifty_close
                    }
            
            # Process BANKNIFTY closing data
            banknifty_data = {}
            banknifty_trend = 'unknown'
            if banknifty_quote:
                ohlc = banknifty_quote.get('ohlc', {})
                banknifty_close = float(ohlc.get('close', 0)) if ohlc.get('close') else float(banknifty_quote.get('last_price', 0))
                banknifty_open = float(ohlc.get('open', 0))
                if banknifty_close > 0 and banknifty_open > 0:
                    banknifty_trend = 'bullish' if banknifty_close > banknifty_open else 'bearish' if banknifty_close < banknifty_open else 'neutral'
                    banknifty_data = {
                        'ltp': banknifty_close,
                        'day_open': banknifty_open,
                        'close_price': banknifty_close
                    }
            
            index_check = {
                "nifty_trend": nifty_trend,
                "banknifty_trend": banknifty_trend,
                "allow_trading": False,
                "nifty_data": nifty_data,
                "banknifty_data": banknifty_data,
                "message": "Market closed - showing closing prices"
            }
        
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
        # datetime already imported at module level
        
        ist = pytz.timezone('Asia/Kolkata')
        now = datetime.now(ist)
        today = now.replace(hour=0, minute=0, second=0, microsecond=0)
        
        # Check if it's time-based exit time (3:25 PM IST)
        current_time = now.time()
        exit_time = datetime.strptime("15:25", "%H:%M").time()
        is_exit_time = current_time >= exit_time
        
        if is_exit_time:
            print(f"⏰ TIME-BASED EXIT: Current time {current_time.strftime('%H:%M')} >= 15:25 - Exiting all open trades")
        
        # Get all OPEN records for today (not exited, not no_entry)
        # Only update trades that are still open
        records = db.query(IntradayStockOption).filter(
            and_(
                IntradayStockOption.trade_date == today,
                IntradayStockOption.option_contract.isnot(None),
                IntradayStockOption.status != 'no_entry',  # Skip no_entry trades
                IntradayStockOption.exit_reason == None  # CRITICAL: Only update open trades, not exited ones
            )
        ).all()
        
        updated_count = 0
        failed_count = 0
        skipped_no_entry = db.query(IntradayStockOption).filter(
            IntradayStockOption.trade_date == today,
            IntradayStockOption.status == 'no_entry'
        ).count()
        
        print(f"Refreshing {len(records)} records (skipped {skipped_no_entry} 'no_entry' trades)...")
        
        for record in records:
            try:
                # SAFETY CHECK: Skip if trade already has exit_reason (should be filtered by query, but double-check)
                if record.exit_reason is not None:
                    print(f"⚠️ Skipping {record.stock_name} - already exited with reason: {record.exit_reason}")
                    continue
                
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
                # datetime already imported at module level
                
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
                    
                    # Find instrument_key with strict matching
                    # CRITICAL: Must match exactly to ensure each option gets its unique instrument_key
                    instrument_key = None
                    best_match = None
                    best_match_score = 0
                    
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
                                        print(f"✅ Found EXACT match for {option_contract}: {instrument_key} (strike: {inst_strike})")
                                        break  # Found exact match, exit loop
                                    else:
                                        # Track best match if no exact match found yet
                                        score = strike_diff * 1000
                                        if best_match is None or score < best_match_score:
                                            best_match = inst
                                            best_match_score = score
                    
                    # If no exact match found, use best match (but log warning)
                    if not instrument_key and best_match:
                        instrument_key = best_match.get('instrument_key')
                        inst_strike = best_match.get('strike_price', 0)
                        print(f"⚠️ WARNING: Using BEST MATCH (not exact) for {option_contract}: {instrument_key} (strike: {inst_strike}, requested: {strike_value})")
                    
                    if not instrument_key:
                        print(f"❌ ERROR: Could not find instrument_key for {option_contract}")
                    
                    # Fetch option LTP
                    if instrument_key:
                        quote_data = vwap_service.get_market_quote_by_key(instrument_key)
                        if quote_data and quote_data.get('last_price'):
                            new_option_ltp = float(quote_data.get('last_price', 0))
                            
                            # Always update option_ltp for current price tracking
                            record.option_ltp = new_option_ltp
                            
                            # Update stock_ltp and stock_vwap (fetch fresh values)
                            try:
                                # Fetch fresh stock LTP
                                stock_name = record.stock_name
                                stock_quote = vwap_service.get_stock_ltp_and_vwap(stock_name)
                                if stock_quote:
                                    if stock_quote.get('ltp'):
                                        record.stock_ltp = stock_quote.get('ltp')
                                    if stock_quote.get('vwap'):
                                        record.stock_vwap = stock_quote.get('vwap')
                            except Exception as e:
                                print(f"Could not update stock LTP/VWAP for {record.stock_name}: {str(e)}")
                            
                            # Only update sell_price if trade is not already closed
                            if not record.exit_reason:
                                # CHECK ALL EXIT CONDITIONS INDEPENDENTLY
                                # Then apply the highest priority exit
                                # Priority: Time > Stop Loss > VWAP Cross > Profit Target
                                
                                exit_conditions = {
                                    'time_based': False,
                                    'stop_loss': False,
                                    'vwap_cross': False,
                                    'profit_target': False
                                }
                                
                                # 1. CHECK TIME-BASED EXIT (3:25 PM) - HIGHEST PRIORITY
                                if is_exit_time:
                                    exit_conditions['time_based'] = True
                                    print(f"⏰ TIME EXIT CONDITION MET for {record.stock_name}: Current time >= 3:25 PM")
                                
                                # 2. CHECK STOP LOSS
                                if record.stop_loss and new_option_ltp <= record.stop_loss:
                                    exit_conditions['stop_loss'] = True
                                    print(f"🛑 STOP LOSS CONDITION MET for {record.stock_name}: LTP ₹{new_option_ltp} <= SL ₹{record.stop_loss}")
                                
                                # 3. CHECK VWAP CROSS (only after 11:15 AM)
                                vwap_check_time = datetime.strptime("11:15", "%H:%M").time()
                                current_time_check = now.time()
                                
                                if current_time_check >= vwap_check_time and record.stock_ltp and record.stock_vwap and record.option_type:
                                    # Enhanced logging for debugging
                                    print(f"📊 VWAP CHECK for {record.stock_name} ({record.option_type}): Stock LTP=₹{record.stock_ltp}, VWAP=₹{record.stock_vwap}, Time={current_time_check.strftime('%H:%M')}")
                                    
                                    if record.option_type == 'CE' and record.stock_ltp < record.stock_vwap:
                                        # Bullish trade: stock went below VWAP (bearish signal)
                                        exit_conditions['vwap_cross'] = True
                                        print(f"📉 VWAP CROSS CONDITION MET for {record.stock_name} (CE): Stock LTP ₹{record.stock_ltp} < VWAP ₹{record.stock_vwap}")
                                    elif record.option_type == 'PE' and record.stock_ltp > record.stock_vwap:
                                        # Bearish trade: stock went above VWAP (bullish signal)
                                        exit_conditions['vwap_cross'] = True
                                        print(f"📈 VWAP CROSS CONDITION MET for {record.stock_name} (PE): Stock LTP ₹{record.stock_ltp} > VWAP ₹{record.stock_vwap}")
                                    else:
                                        print(f"✅ VWAP OK for {record.stock_name} - Stock {record.stock_ltp} {'>' if record.option_type == 'CE' else '<'} VWAP {record.stock_vwap}")
                                elif current_time_check < vwap_check_time:
                                    print(f"⏰ VWAP check skipped for {record.stock_name} (time {current_time_check.strftime('%H:%M')} < 11:15 AM)")
                                
                                # 4. CHECK PROFIT TARGET (50% gain)
                                if record.buy_price and new_option_ltp >= (record.buy_price * 1.5):
                                    exit_conditions['profit_target'] = True
                                    print(f"🎯 PROFIT TARGET CONDITION MET for {record.stock_name}: LTP ₹{new_option_ltp} >= Target ₹{record.buy_price * 1.5}")
                                
                                # APPLY THE HIGHEST PRIORITY EXIT CONDITION
                                exit_applied = False
                                
                                if exit_conditions['time_based']:
                                    record.sell_price = new_option_ltp
                                    record.sell_time = now
                                    record.exit_reason = 'time_based'
                                    record.status = 'sold'
                                    if record.buy_price and record.qty:
                                        record.pnl = (new_option_ltp - record.buy_price) * record.qty
                                    print(f"✅ APPLIED: TIME EXIT for {record.stock_name}: PnL=₹{record.pnl}")
                                    exit_applied = True
                                
                                elif exit_conditions['stop_loss']:
                                    record.sell_price = new_option_ltp
                                    record.sell_time = now
                                    record.exit_reason = 'stop_loss'
                                    record.status = 'sold'
                                    if record.buy_price and record.qty:
                                        record.pnl = (new_option_ltp - record.buy_price) * record.qty
                                    print(f"✅ APPLIED: STOP LOSS EXIT for {record.stock_name}: PnL=₹{record.pnl}")
                                    exit_applied = True
                                
                                elif exit_conditions['vwap_cross']:
                                    record.sell_price = new_option_ltp
                                    record.sell_time = now
                                    record.exit_reason = 'stock_vwap_cross'
                                    record.status = 'sold'
                                    if record.buy_price and record.qty:
                                        record.pnl = (new_option_ltp - record.buy_price) * record.qty
                                    print(f"✅ APPLIED: VWAP CROSS EXIT for {record.stock_name}: PnL=₹{record.pnl}")
                                    exit_applied = True
                                
                                elif exit_conditions['profit_target']:
                                    record.sell_price = new_option_ltp
                                    record.sell_time = now
                                    record.exit_reason = 'profit_target'
                                    record.status = 'sold'
                                    if record.qty:
                                        record.pnl = (new_option_ltp - record.buy_price) * record.qty
                                    print(f"✅ APPLIED: PROFIT TARGET EXIT for {record.stock_name}: PnL=₹{record.pnl}")
                                    exit_applied = True
                                
                                # If no exit was applied, just update current price and PnL (trade still OPEN)
                                if not exit_applied:
                                    old_sell_price = record.sell_price or 0.0
                                    record.sell_price = new_option_ltp  # Update current Option LTP
                                    print(f"📝 PRICE UPDATE for {record.stock_name}: sell_price ₹{old_sell_price:.2f} → ₹{new_option_ltp:.2f} (OPEN trade)")
                                    
                                    # DO NOT update sell_time here - only set when trade exits
                                    if record.buy_price and record.qty:
                                        record.pnl = (new_option_ltp - record.buy_price) * record.qty  # Current unrealized P&L
                                        
                                    # Sanity check for unrealistic prices
                                    if record.buy_price and new_option_ltp > record.buy_price * 3:
                                        print(f"🚨 WARNING: Unrealistic option price for {record.stock_name}!")
                                        print(f"   Buy: ₹{record.buy_price:.2f}, Current: ₹{new_option_ltp:.2f} ({new_option_ltp/record.buy_price:.1f}x)")
                                        print(f"   Previous sell_price: ₹{old_sell_price:.2f}")
                                        print(f"   This may indicate data corruption!")
                            else:
                                # Trade already closed - this should NOT happen due to query filter
                                # But if it does, log it and skip
                                print(f"🚨 ERROR: {record.stock_name} already has exit_reason='{record.exit_reason}' but was still in query results!")
                                print(f"   This indicates query filter bug. Skipping update.")
                                continue
                            
                            updated_count += 1
                            print(f"✅ Updated {record.stock_name}: option_ltp=₹{new_option_ltp}, PnL=₹{record.pnl}, Exit={record.exit_reason or 'Open'}")
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
    Only fetches during market hours (9 AM - 4 PM) to avoid unnecessary API calls
    """
    try:
        # Check if during market hours (9 AM - 4 PM)
        import pytz
        ist = pytz.timezone('Asia/Kolkata')
        current_time = datetime.now(ist)
        current_hour = current_time.hour
        
        # If after market hours, fetch closing prices from OHLC (doesn't update live quotes)
        if current_hour < 9 or current_hour > 16:
            logger.info(f"⏰ After market hours ({current_hour}:00) - fetching closing prices")
            
            # Get OHLC data which contains closing prices
            nifty_quote = vwap_service.get_market_quote_by_key(vwap_service.NIFTY50_KEY)
            banknifty_quote = vwap_service.get_market_quote_by_key(vwap_service.BANKNIFTY_KEY)
            
            # Process NIFTY closing data
            nifty_close = 0
            nifty_open = 0
            nifty_trend = 'unknown'
            if nifty_quote:
                ohlc = nifty_quote.get('ohlc', {})
                nifty_close = float(ohlc.get('close', 0)) if ohlc.get('close') else float(nifty_quote.get('last_price', 0))
                nifty_open = float(ohlc.get('open', 0))
                if nifty_close > 0 and nifty_open > 0:
                    nifty_trend = 'bullish' if nifty_close > nifty_open else 'bearish' if nifty_close < nifty_open else 'neutral'
            
            # Process BANKNIFTY closing data
            banknifty_close = 0
            banknifty_open = 0
            banknifty_trend = 'unknown'
            if banknifty_quote:
                ohlc = banknifty_quote.get('ohlc', {})
                banknifty_close = float(ohlc.get('close', 0)) if ohlc.get('close') else float(banknifty_quote.get('last_price', 0))
                banknifty_open = float(ohlc.get('open', 0))
                if banknifty_close > 0 and banknifty_open > 0:
                    banknifty_trend = 'bullish' if banknifty_close > banknifty_open else 'bearish' if banknifty_close < banknifty_open else 'neutral'
            
            return JSONResponse(
                status_code=200,
                content={
                    "status": "success",
                    "data": {
                        "nifty": {
                            "name": "NIFTY 50",
                            "ltp": nifty_close,
                            "close_price": nifty_close,
                            "day_open": nifty_open,
                            "trend": nifty_trend,
                            "change": nifty_close - nifty_open if nifty_close > 0 and nifty_open > 0 else 0,
                            "change_percent": ((nifty_close - nifty_open) / nifty_open * 100) if nifty_open > 0 else 0,
                            "market_status": "Closed"
                        },
                        "banknifty": {
                            "name": "BANKNIFTY",
                            "ltp": banknifty_close,
                            "close_price": banknifty_close,
                            "day_open": banknifty_open,
                            "trend": banknifty_trend,
                            "change": banknifty_close - banknifty_open if banknifty_close > 0 and banknifty_open > 0 else 0,
                            "change_percent": ((banknifty_close - banknifty_open) / banknifty_open * 100) if banknifty_open > 0 else 0,
                            "market_status": "Closed"
                        },
                        "timestamp": datetime.now().isoformat(),
                        "data_source": "closing_prices",
                        "market_status": "closed",
                        "message": "Market closed - showing closing prices"
                    }
                }
            )
        
        # During market hours - fetch real-time data
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
            # Fallback to historical data if real-time data is not available (only during market hours)
            print("Real-time data not available, falling back to historical data")
            
            # Double-check we're still in market hours before fallback API call
            current_hour = datetime.now(ist).hour
            if 9 <= current_hour <= 16:
                index_check_result = vwap_service.check_index_trends()
            else:
                # Return default/empty data after hours
                index_check_result = {
                    "nifty_data": {"ltp": 0, "day_open": 0},
                    "banknifty_data": {"ltp": 0, "day_open": 0},
                    "nifty_trend": "unknown",
                    "banknifty_trend": "unknown"
                }
            
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
        
        # Redirect user to Upstox authorization page
        return RedirectResponse(url=auth_url, status_code=302)
        
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
        expires_in = token_response.get("expires_in")  # Seconds until expiration
        
        if not access_token:
            return JSONResponse(
                status_code=500,
                content={
                    "status": "error",
                    "message": "No access token in response"
                }
            )
        
        # Calculate expiration timestamp
        expires_at = None
        if expires_in:
            from datetime import datetime, timedelta
            expires_at = int((datetime.now() + timedelta(seconds=expires_in)).timestamp())
        else:
            # If expires_in not provided, decode from JWT token
            try:
                import base64
                parts = access_token.split('.')
                if len(parts) >= 2:
                    payload = parts[1]
                    # Add padding if needed
                    padding = len(payload) % 4
                    if padding:
                        payload += '=' * (4 - padding)
                    
                    decoded = base64.urlsafe_b64decode(payload)
                    jwt_data = json.loads(decoded)
                    expires_at = jwt_data.get('exp')
                    if expires_at:
                        logger.info(f"✅ Decoded expiration from JWT: {datetime.fromtimestamp(expires_at).strftime('%Y-%m-%d %H:%M:%S')}")
            except Exception as jwt_error:
                logger.warning(f"⚠️ Could not decode JWT expiration: {jwt_error}")
        
        # Save token using token manager (persistent storage)
        try:
            from services.token_manager import save_upstox_token
            if save_upstox_token(access_token, expires_at):
                logger.info("✅ Upstox token saved to token manager")
            else:
                logger.warning("⚠️ Failed to save token to token manager, trying fallback")
        except Exception as e:
            logger.error(f"❌ Token manager save failed: {str(e)}")
        
        # Also update the token in upstox_service.py file as backup
        try:
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
            
            logger.info("✅ Upstox token updated in service file")
        except Exception as e:
            logger.warning(f"⚠️ Could not update service file: {str(e)}")
        
        # Update the token in memory (so it works immediately without restart)
        if hasattr(vwap_service, 'access_token'):
            vwap_service.access_token = access_token
            logger.info("✅ Upstox token updated in memory")
        if hasattr(vwap_service, 'upstox'):
            vwap_service.upstox.set_access_token(access_token)
        
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
    Tests both user profile and market data endpoints to ensure token works for all operations
    """
    try:
        # Check if upstox_service has a valid token
        if hasattr(vwap_service, 'access_token') and vwap_service.access_token:
            # Test 1: User profile endpoint (basic auth check)
            test_url_profile = "https://api.upstox.com/v2/user/profile"
            headers = {
                "Accept": "application/json",
                "Authorization": f"Bearer {vwap_service.access_token}"
            }
            
            profile_response = requests.get(test_url_profile, headers=headers, timeout=5)
            
            # Test 2: Market data endpoint (critical for trading operations)
            # Use NIFTY 50 index quote as it's always available
            test_url_market = "https://api.upstox.com/v2/market-quote/quotes"
            market_params = {"instrument_key": "NSE_INDEX|Nifty 50"}
            market_response = requests.get(test_url_market, headers=headers, params=market_params, timeout=5)
            
            # Token is valid only if BOTH endpoints work
            # Market data endpoint is more critical - if it fails, token is effectively expired
            if profile_response.status_code == 200 and market_response.status_code == 200:
                return JSONResponse(
                    status_code=200,
                    content={
                        "status": "success",
                        "authenticated": True,
                        "message": "Upstox token is valid"
                    }
                )
            else:
                # Check if it's a 401 (Unauthorized) which indicates token expiration
                if market_response.status_code == 401 or profile_response.status_code == 401:
                    logger.warning("⚠️ Upstox token expired (401 Unauthorized)")
                return JSONResponse(
                    status_code=200,
                    content={
                        "status": "success",
                        "authenticated": False,
                        "message": "Upstox token is expired or invalid",
                        "error_type": "token_expired"
                    }
                )
        else:
            return JSONResponse(
                status_code=200,
                content={
                    "status": "success",
                    "authenticated": False,
                    "message": "No Upstox token configured",
                    "error_type": "token_expired"
                }
            )
            
    except Exception as e:
        logger.error(f"❌ Error checking Upstox token status: {str(e)}")
        return JSONResponse(
            status_code=500,
            content={
                "status": "error",
                "authenticated": False,
                "message": f"Failed to check status: {str(e)}",
                "error_type": "token_expired"
            }
        )


@router.post("/update-vwap")
async def manually_update_vwap(db: Session = Depends(get_db)):
    """
    Manually trigger market data update (VWAP, Stock LTP, Option LTP) for all open positions
    This is normally done automatically every hour during market hours
    
    Updates:
    - stock_vwap: Current VWAP of underlying stock
    - stock_ltp: Current Last Traded Price of stock  
    - sell_price: Current Last Traded Price of option contract
    
    These values are used for exit decisions (VWAP cross, stop loss, target, etc.)
    """
    try:
        import pytz
        from datetime import datetime
        
        ist = pytz.timezone('Asia/Kolkata')
        now = datetime.now(ist)
        today = now.replace(hour=0, minute=0, second=0, microsecond=0)
        
        logger.info(f"📊 Manual VWAP update triggered at {now.strftime('%Y-%m-%d %H:%M:%S IST')}")
        
        # Get all open positions from today (not sold/exited)
        open_positions = db.query(IntradayStockOption).filter(
            and_(
                IntradayStockOption.trade_date >= today,
                IntradayStockOption.status != 'sold',
                IntradayStockOption.exit_reason == None  # No exit reason means still open
            )
        ).all()
        
        if not open_positions:
            return JSONResponse(
                status_code=200,
                content={
                    "status": "success",
                    "message": "No open positions found to update",
                    "updated_count": 0,
                    "failed_count": 0
                }
            )
        
        logger.info(f"Found {len(open_positions)} open positions to update")
        
        # Update each position with Stock VWAP, Stock LTP, and Option LTP
        updated_count = 0
        failed_count = 0
        updates = []
        
        for position in open_positions:
            try:
                stock_name = position.stock_name
                option_contract = position.option_contract
                
                # 1. Fetch fresh Stock VWAP from API
                new_vwap = vwap_service.get_stock_vwap(stock_name)
                
                # 2. Fetch fresh Stock LTP (Last Traded Price)
                new_stock_ltp = vwap_service.get_stock_ltp_from_market_quote(stock_name)
                
                # 3. Fetch fresh Option LTP (if option contract exists)
                new_option_ltp = 0.0
                if option_contract:
                    try:
                        # Fetch option LTP using instruments JSON
                        from pathlib import Path
                        import json as json_lib
                        
                        instruments_file = Path("/home/ubuntu/trademanthan/data/instruments/nse_instruments.json")
                        
                        if instruments_file.exists():
                            with open(instruments_file, 'r') as f:
                                instruments_data = json_lib.load(f)
                            
                            # Find option contract in instruments data
                            import re
                            match = re.match(r'^([A-Z-]+)-(\w{3})(\d{4})-(\d+\.?\d*?)-(CE|PE)$', option_contract)
                            
                            if match:
                                symbol, month, year, strike, opt_type = match.groups()
                                strike_value = float(strike)
                                
                                # Search for matching instrument
                                for instrument_key, instrument_data in instruments_data.items():
                                    if (instrument_data.get('name', '').upper() == symbol.upper() and
                                        instrument_data.get('instrument_type') == 'OPTSTK' and
                                        instrument_data.get('option_type') == opt_type):
                                        
                                        # Check strike price match
                                        inst_strike = float(instrument_data.get('strike_price', 0))
                                        if abs(inst_strike - strike_value) < 0.01:
                                            # Found the option - fetch its LTP
                                            option_ltp_data = vwap_service.get_option_ltp(instrument_key)
                                            if option_ltp_data and option_ltp_data > 0:
                                                new_option_ltp = option_ltp_data
                                                break
                    except Exception as e:
                        logger.warning(f"Could not fetch option LTP for {option_contract}: {str(e)}")
                
                # Update position with new values
                update_info = {"stock": stock_name}
                updates_made = []
                
                if new_vwap and new_vwap > 0:
                    old_vwap = position.stock_vwap or 0.0
                    position.stock_vwap = new_vwap
                    update_info["vwap"] = {"old": round(old_vwap, 2), "new": round(new_vwap, 2)}
                    updates_made.append(f"VWAP: {old_vwap:.2f}→{new_vwap:.2f}")
                
                if new_stock_ltp and new_stock_ltp > 0:
                    old_stock_ltp = position.stock_ltp or 0.0
                    position.stock_ltp = new_stock_ltp
                    update_info["stock_ltp"] = {"old": round(old_stock_ltp, 2), "new": round(new_stock_ltp, 2)}
                    updates_made.append(f"Stock LTP: {old_stock_ltp:.2f}→{new_stock_ltp:.2f}")
                
                if new_option_ltp > 0:
                    old_option_ltp = position.sell_price or 0.0
                    position.sell_price = new_option_ltp
                    update_info["option_ltp"] = {"old": round(old_option_ltp, 2), "new": round(new_option_ltp, 2)}
                    updates_made.append(f"Option LTP: {old_option_ltp:.2f}→{new_option_ltp:.2f}")
                
                if updates_made:
                    position.updated_at = now
                    updates.append(update_info)
                    logger.info(f"✅ {stock_name}: {', '.join(updates_made)}")
                    updated_count += 1
                else:
                    logger.warning(f"⚠️ Could not fetch updated data for {stock_name}")
                    failed_count += 1
                    
            except Exception as e:
                logger.error(f"Error updating position for {position.stock_name}: {str(e)}")
                import traceback
                traceback.print_exc()
                failed_count += 1
        
        # Commit all updates
        db.commit()
        
        logger.info(f"📊 Manual Market Data Update Complete: {updated_count} updated, {failed_count} failed")
        
        return JSONResponse(
            status_code=200,
            content={
                "status": "success",
                "message": f"Market data update completed successfully (VWAP, Stock LTP, Option LTP)",
                "updated_count": updated_count,
                "failed_count": failed_count,
                "total_positions": len(open_positions),
                "updates": updates,
                "timestamp": now.strftime('%Y-%m-%d %H:%M:%S IST')
            }
        )
        
    except Exception as e:
        logger.error(f"Error in manual VWAP update: {str(e)}")
        import traceback
        traceback.print_exc()
        db.rollback()
        
        return JSONResponse(
            status_code=500,
            content={
                "status": "error",
                "message": f"Failed to update VWAP: {str(e)}"
            }
        )


@router.post("/backfill-vwap")
async def backfill_vwap_for_date(
    date_str: str = Query(..., description="Date in YYYY-MM-DD format (e.g., 2025-11-07)"),
    db: Session = Depends(get_db)
):
    """
    Backfill missing stock_vwap data for records from a specific date.
    This endpoint fixes records that have empty or zero VWAP values.
    """
    try:
        from datetime import datetime, timedelta
        import pytz
        
        ist = pytz.timezone('Asia/Kolkata')
        
        # Parse the date
        try:
            target_date = datetime.strptime(date_str, '%Y-%m-%d')
            target_date = ist.localize(target_date.replace(hour=0, minute=0, second=0, microsecond=0))
            next_date = target_date + timedelta(days=1)
        except ValueError:
            return {"success": False, "message": "Invalid date format. Use YYYY-MM-DD"}
        
        print(f"\n{'='*80}")
        print(f"VWAP BACKFILL FOR {date_str}")
        print(f"{'='*80}\n")
        
        # Get all records from the specified date
        records = db.query(IntradayStockOption).filter(
            IntradayStockOption.trade_date >= target_date,
            IntradayStockOption.trade_date < next_date
        ).all()
        
        total_records = len(records)
        print(f"📊 Found {total_records} total records from {date_str}")
        
        # Filter records with missing VWAP
        empty_vwap_records = [r for r in records if not r.stock_vwap or r.stock_vwap == 0.0]
        empty_count = len(empty_vwap_records)
        
        print(f"⚠️  Records with missing/zero stock_vwap: {empty_count}")
        
        if not empty_vwap_records:
            return {
                "success": True,
                "message": f"No records need VWAP backfill for {date_str}",
                "total_records": total_records,
                "empty_vwap": 0,
                "updated": 0,
                "failed": 0
            }
        
        # Use the imported vwap_service (which is upstox_service)
        # Already imported at top: from services.upstox_service import upstox_service as vwap_service
        
        # Group by unique stock names to avoid redundant API calls
        unique_stocks = {}
        for record in empty_vwap_records:
            stock_name = record.stock_name
            if stock_name not in unique_stocks:
                unique_stocks[stock_name] = []
            unique_stocks[stock_name].append(record)
        
        print(f"📈 Processing {len(unique_stocks)} unique stocks...\n")
        
        updated_count = 0
        failed_count = 0
        results = []
        
        for stock_name, stock_records in unique_stocks.items():
            try:
                print(f"Fetching VWAP for {stock_name} ({len(stock_records)} records)...")
                
                # Fetch VWAP from Upstox API
                vwap = vwap_service.get_stock_vwap(stock_name)
                
                if vwap and vwap > 0:
                    # Update all records for this stock
                    for record in stock_records:
                        record.stock_vwap = vwap
                        record.updated_at = datetime.now(ist)
                    
                    print(f"  ✅ Updated {len(stock_records)} records with VWAP = ₹{vwap:.2f}")
                    updated_count += len(stock_records)
                    results.append({
                        "stock": stock_name,
                        "status": "success",
                        "vwap": vwap,
                        "records_updated": len(stock_records)
                    })
                else:
                    print(f"  ⚠️  Could not fetch VWAP for {stock_name} (API returned 0 or failed)")
                    failed_count += len(stock_records)
                    results.append({
                        "stock": stock_name,
                        "status": "failed",
                        "reason": "API returned 0 or failed",
                        "records": len(stock_records)
                    })
                    
            except Exception as e:
                print(f"  ❌ Error processing {stock_name}: {str(e)}")
                failed_count += len(stock_records)
                results.append({
                    "stock": stock_name,
                    "status": "error",
                    "error": str(e),
                    "records": len(stock_records)
                })
        
        # Commit all changes
        db.commit()
        
        print(f"\n{'='*80}")
        print("BACKFILL COMPLETE")
        print(f"{'='*80}")
        print(f"✅ Successfully updated: {updated_count} records")
        print(f"❌ Failed: {failed_count} records\n")
        
        return {
            "success": True,
            "message": f"Backfill completed for {date_str}",
            "total_records": total_records,
            "empty_vwap": empty_count,
            "updated": updated_count,
            "failed": failed_count,
            "results": results
        }
        
    except Exception as e:
        db.rollback()
        print(f"❌ Error in backfill: {str(e)}")
        import traceback
        traceback.print_exc()
        return {
            "success": False,
            "message": f"Error: {str(e)}"
        }


@router.get("/trading-report")
async def get_trading_report(
    start_date: str = Query(None, description="Start date in YYYY-MM-DD format"),
    end_date: str = Query(None, description="End date in YYYY-MM-DD format"),
    alert_type: str = Query(None, description="Filter by Bullish or Bearish"),
    db: Session = Depends(get_db)
):
    """
    Get comprehensive trading report with daily statistics
    
    Returns daily summary with:
    - Total alerts, total trades
    - Bullish wins/losses, bearish wins/losses
    - Win rate, total P&L
    - Best and worst performers
    """
    try:
        from datetime import datetime
        import pytz
        
        ist = pytz.timezone('Asia/Kolkata')
        
        # Build query filters
        filters = []
        
        if start_date:
            try:
                start_dt = datetime.strptime(start_date, '%Y-%m-%d')
                start_dt = ist.localize(start_dt.replace(hour=0, minute=0, second=0))
                filters.append(IntradayStockOption.trade_date >= start_dt)
            except ValueError:
                return {"success": False, "message": "Invalid start_date format. Use YYYY-MM-DD"}
        
        if end_date:
            try:
                end_dt = datetime.strptime(end_date, '%Y-%m-%d')
                end_dt = ist.localize(end_dt.replace(hour=23, minute=59, second=59))
                filters.append(IntradayStockOption.trade_date <= end_dt)
            except ValueError:
                return {"success": False, "message": "Invalid end_date format. Use YYYY-MM-DD"}
        
        if alert_type:
            filters.append(IntradayStockOption.alert_type == alert_type)
        
        # Query base data
        if filters:
            query = db.query(IntradayStockOption).filter(and_(*filters))
        else:
            query = db.query(IntradayStockOption)
        
        # Get all records
        all_records = query.order_by(IntradayStockOption.trade_date.desc()).all()
        
        # Group by trading date
        from collections import defaultdict
        daily_data = defaultdict(lambda: {
            'date': None,
            'total_alerts': 0,
            'total_trades': 0,
            'bullish_alerts': 0,
            'bearish_alerts': 0,
            'bullish_wins': 0,
            'bullish_losses': 0,
            'bearish_wins': 0,
            'bearish_losses': 0,
            'total_pnl': 0.0,
            'best_trade': {'stock': None, 'pnl': 0.0},
            'worst_trade': {'stock': None, 'pnl': 0.0},
            'trades_detail': []
        })
        
        for record in all_records:
            date_key = record.trade_date.date().isoformat()
            day_data = daily_data[date_key]
            
            # Set date
            if day_data['date'] is None:
                day_data['date'] = date_key
            
            # Count alerts (all records)
            day_data['total_alerts'] += 1
            
            # Count alert types for market trend calculation
            if record.alert_type.lower() == 'bullish':
                day_data['bullish_alerts'] += 1
            elif record.alert_type.lower() == 'bearish':
                day_data['bearish_alerts'] += 1
            
            # Count trades (status = bought or sold)
            if record.status in ['bought', 'sold']:
                day_data['total_trades'] += 1
                
                # Track by alert type
                if record.pnl is not None:
                    if record.alert_type.lower() == 'bullish':
                        if record.pnl > 0:
                            day_data['bullish_wins'] += 1
                        else:
                            day_data['bullish_losses'] += 1
                    elif record.alert_type.lower() == 'bearish':
                        if record.pnl > 0:
                            day_data['bearish_wins'] += 1
                        else:
                            day_data['bearish_losses'] += 1
                    
                    # Total P&L
                    day_data['total_pnl'] += record.pnl
                    
                    # Best trade
                    if record.pnl > day_data['best_trade']['pnl']:
                        day_data['best_trade'] = {
                            'stock': record.stock_name,
                            'pnl': record.pnl,
                            'contract': record.option_contract
                        }
                    
                    # Worst trade
                    if record.pnl < day_data['worst_trade']['pnl']:
                        day_data['worst_trade'] = {
                            'stock': record.stock_name,
                            'pnl': record.pnl,
                            'contract': record.option_contract
                        }
        
        # Convert to list and calculate percentages
        report = []
        for date_key in sorted(daily_data.keys(), reverse=True):
            day = daily_data[date_key]
            
            total_closed = day['bullish_wins'] + day['bullish_losses'] + day['bearish_wins'] + day['bearish_losses']
            total_wins = day['bullish_wins'] + day['bearish_wins']
            win_rate = (total_wins / total_closed * 100) if total_closed > 0 else 0
            
            # Calculate market trend based on alert distribution
            # Since alerts are only generated when NIFTY & BANKNIFTY agree,
            # we can infer market trend from dominant alert type
            bullish_pct = (day['bullish_alerts'] / day['total_alerts'] * 100) if day['total_alerts'] > 0 else 0
            bearish_pct = (day['bearish_alerts'] / day['total_alerts'] * 100) if day['total_alerts'] > 0 else 0
            
            if bullish_pct >= 70:
                market_trend = 'bullish'  # Both indexes bullish
            elif bearish_pct >= 70:
                market_trend = 'bearish'  # Both indexes bearish
            else:
                market_trend = 'sideways'  # Mixed or opposite trends
            
            report.append({
                'date': day['date'],
                'market_trend': market_trend,
                'total_alerts': day['total_alerts'],
                'total_trades': day['total_trades'],
                'bullish_wins': day['bullish_wins'],
                'bullish_losses': day['bullish_losses'],
                'bearish_wins': day['bearish_wins'],
                'bearish_losses': day['bearish_losses'],
                'total_closed': total_closed,
                'win_rate': round(win_rate, 2),
                'total_pnl': round(day['total_pnl'], 2),
                'best_trade': day['best_trade'],
                'worst_trade': day['worst_trade']
            })
        
        return {
            "success": True,
            "data": report,
            "summary": {
                "total_days": len(report),
                "total_alerts": sum(d['total_alerts'] for d in report),
                "total_trades": sum(d['total_trades'] for d in report),
                "overall_pnl": round(sum(d['total_pnl'] for d in report), 2)
            }
        }
        
    except Exception as e:
        logger.error(f"Error generating trading report: {str(e)}")
        import traceback
        traceback.print_exc()
        return {
            "success": False,
            "message": f"Error: {str(e)}"
        }


@router.get("/daily-trades/{trade_date}")
async def get_daily_trades(
    trade_date: str,
    db: Session = Depends(get_db)
):
    """
    Get detailed trade information for a specific date
    Only returns trades with status != 'no_entry'
    
    Args:
        trade_date: Date in YYYY-MM-DD format
    
    Returns:
        List of trades with: stock_name, option_contract, qty, buy_price, sell_price, pnl, status
    """
    try:
        # Parse the date
        target_date = datetime.strptime(trade_date, "%Y-%m-%d").date()
        
        # Query trades for this date, excluding 'no_entry' status
        # Sort by Buy Time in ascending order (earliest trades first), then by stock_name ascending
        trades = db.query(IntradayStockOption).filter(
            func.date(IntradayStockOption.trade_date) == target_date,
            IntradayStockOption.status != 'no_entry'
        ).order_by(
            IntradayStockOption.buy_time.asc().nulls_last(),
            IntradayStockOption.stock_name.asc()
        ).all()
        
        # Format trade data
        trade_details = []
        for trade in trades:
            trade_details.append({
                "stock_name": trade.stock_name,
                "option_contract": trade.option_contract,
                "qty": trade.qty,
                "buy_price": float(trade.buy_price) if trade.buy_price else 0,
                "sell_price": float(trade.sell_price) if trade.sell_price else 0,
                "pnl": float(trade.pnl) if trade.pnl else 0,
                "status": trade.status,
                "exit_reason": trade.exit_reason,
                "alert_type": trade.alert_type,
                "alert_time": trade.alert_time.strftime("%H:%M") if trade.alert_time else "",
                "buy_time": trade.buy_time.strftime("%H:%M") if trade.buy_time else "",
                "sell_time": trade.sell_time.strftime("%H:%M") if trade.sell_time else ""
            })
        
        return {
            "success": True,
            "date": trade_date,
            "total_trades": len(trade_details),
            "trades": trade_details
        }
        
    except ValueError as e:
        return {
            "success": False,
            "message": f"Invalid date format. Use YYYY-MM-DD: {str(e)}",
            "trades": []
        }
    except Exception as e:
        logger.error(f"Error fetching daily trades for {trade_date}: {e}")
        return {
            "success": False,
            "message": f"Error: {str(e)}",
            "trades": []
        }


@router.get("/logs")
async def get_scan_logs(lines: int = Query(100, ge=1, le=1000)):
    """
    Get the last N lines from the application log file
    
    Args:
        lines: Number of lines to return (default 100, max 1000)
    
    Returns:
        JSON with log lines
    """
    try:
        # Determine log file path
        # Check if running on EC2 or locally
        if os.path.exists('/home/ubuntu/trademanthan/logs'):
            log_dir = Path('/home/ubuntu/trademanthan/logs')
        else:
            # Local environment
            log_dir = Path(__file__).parent.parent.parent / 'logs'
        
        # Find the most recent log file
        log_file = log_dir / 'trademanthan.log'
        
        # Alternative: try to find from logging configuration
        if not log_file.exists():
            # Try alternative locations
            alternative_paths = [
                Path('/var/log/trademanthan/trademanthan.log'),
                Path('/tmp/trademanthan.log'),
                Path.home() / 'trademanthan.log'
            ]
            
            for alt_path in alternative_paths:
                if alt_path.exists():
                    log_file = alt_path
                    break
        
        if not log_file.exists():
            return {
                "success": False,
                "message": f"Log file not found. Searched in: {log_dir}",
                "logs": []
            }
        
        # Read last N lines from log file
        with open(log_file, 'r', encoding='utf-8', errors='ignore') as f:
            # Read all lines
            all_lines = f.readlines()
            
            # Get last N lines
            log_lines = all_lines[-lines:] if len(all_lines) > lines else all_lines
            
            # Parse log lines into structured format
            parsed_logs = []
            for line in log_lines:
                line = line.strip()
                if not line:
                    continue
                
                # Try to parse structured log format
                # Typical format: 2025-11-10 14:15:30,123 - INFO - module - message
                parts = line.split(' - ', 3)
                
                if len(parts) >= 3:
                    parsed_logs.append({
                        'timestamp': parts[0].strip(),
                        'level': parts[1].strip() if len(parts) > 1 else 'INFO',
                        'module': parts[2].strip() if len(parts) > 2 else '',
                        'message': parts[3].strip() if len(parts) > 3 else line,
                        'raw': line
                    })
                else:
                    # Fallback for non-standard format
                    parsed_logs.append({
                        'timestamp': '',
                        'level': 'INFO',
                        'module': '',
                        'message': line,
                        'raw': line
                    })
            
            return {
                "success": True,
                "log_file": str(log_file),
                "total_lines": len(parsed_logs),
                "logs": parsed_logs
            }
    
    except FileNotFoundError:
        return {
            "success": False,
            "message": "Log file not found",
            "logs": []
        }
    except Exception as e:
        logger.error(f"Error reading logs: {str(e)}")
        return {
            "success": False,
            "message": f"Error reading logs: {str(e)}",
            "logs": []
        }
