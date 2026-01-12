from fastapi import APIRouter, Request, HTTPException, Depends, Query, BackgroundTasks
from fastapi.responses import JSONResponse, RedirectResponse
from datetime import datetime
from typing import List, Dict, Any, Optional
from pydantic import BaseModel
from sqlalchemy.orm import Session
from sqlalchemy import and_, func, desc, text, or_
from starlette.requests import ClientDisconnect
import json
import os
import sys
import requests
import secrets
import logging
import asyncio
from pathlib import Path

# Configure logger to write to scan_st1_algo.log instead of trademanthan.log
# This ensures all scan-related logs (webhooks, option contracts, trades) go to scan_st1_algo.log
# The scan_st1_algo service will also configure this logger, but we set it up here to ensure
# logs are written correctly even if scan_st1_algo hasn't started yet
log_dir = Path(__file__).parent.parent.parent / 'logs'
log_dir.mkdir(exist_ok=True)
log_file = log_dir / 'scan_st1_algo.log'

# Create file handler with immediate flushing
class FlushingFileHandler(logging.FileHandler):
    """FileHandler that flushes after each log entry to ensure immediate writes"""
    def emit(self, record):
        super().emit(record)
        self.flush()
        if hasattr(self.stream, 'fileno'):
            try:
                import os
                os.fsync(self.stream.fileno())
            except (OSError, AttributeError):
                pass

# Get the logger for backend.routers.scan (scan_st1_algo.py will also configure this)
logger = logging.getLogger(__name__)  # This will be 'backend.routers.scan'

# Check if handler already exists to avoid duplicates
handler_exists = False
for h in logger.handlers:
    if isinstance(h, logging.FileHandler):
        handler_path = getattr(h, 'baseFilename', None) or (getattr(h, 'stream', {}).name if hasattr(getattr(h, 'stream', None), 'name') else None)
        if handler_path and 'scan_st1_algo.log' in str(handler_path):
            handler_exists = True
            break

if not handler_exists:
    file_handler = FlushingFileHandler(log_file, mode='a', encoding='utf-8')
    file_handler.setLevel(logging.INFO)
    file_handler.setFormatter(logging.Formatter(
        '%(asctime)s - %(levelname)s - %(name)s - %(message)s',
        datefmt='%Y-%m-%d %H:%M:%S'
    ))
    logger.setLevel(logging.INFO)
    logger.addHandler(file_handler)
    logger.propagate = False  # Only log to scan_st1_algo.log, not to root logger

# Add services to path
sys.path.append(os.path.dirname(os.path.dirname(__file__)))

# Import health monitor for tracking webhook success/failure
try:
    from backend.services.health_monitor import health_monitor
except ImportError:
    try:
        from services.health_monitor import health_monitor
    except ImportError:
        health_monitor = None  # Graceful degradation if not available
from backend.services.upstox_service import upstox_service as vwap_service
from backend.database import get_db
from backend.models.trading import IntradayStockOption, MasterStock, HistoricalMarketData
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
    logger.info(f"🔍 find_strike_from_option_chain called for {stock_name} {option_type} (LTP: {stock_ltp})")
    try:
        # Get option chain from Upstox API
        logger.info(f"Calling vwap_service.get_option_chain({stock_name})")
        option_chain = vwap_service.get_option_chain(stock_name)
        logger.info(f"get_option_chain returned: type={type(option_chain)}, is None={option_chain is None}")
        
        if not option_chain:
            # Log as debug instead of warning - this is expected for some stocks that don't have options
            logger.debug(f"No option chain data available for {stock_name} (stock may not have options trading)")
            logger.info(f"No option chain data available for {stock_name}")
            return None
        
        # Debug: Log the structure of option_chain
        logger.info(f"Option chain structure for {stock_name}: {type(option_chain)}, keys: {list(option_chain.keys()) if isinstance(option_chain, dict) else 'not a dict'}")
        
        # Parse option chain data
        # Upstox API returns a dictionary with 'strikes' key containing list of strike data
        strikes = []
        
        # Handle dictionary format (Upstox API v2 returns dict with 'strikes' key)
        strike_list = None
        if isinstance(option_chain, dict):
            logger.info(f"📊 Option chain for {stock_name} is a dictionary with keys: {list(option_chain.keys())}")
            logger.info(f"📊 Option chain for {stock_name} is a dictionary with keys: {list(option_chain.keys())}")
            # Check if it has a 'strikes' key
            if 'strikes' in option_chain and isinstance(option_chain['strikes'], list):
                strike_list = option_chain['strikes']
                logger.info(f"✅ Found 'strikes' key with {len(strike_list)} strikes")
                logger.info(f"✅ Found 'strikes' key with {len(strike_list)} strikes")
            elif 'data' in option_chain and isinstance(option_chain['data'], dict):
                # Nested data structure
                logger.info(f"📊 Found 'data' key with sub-keys: {list(option_chain['data'].keys())}")
                logger.info(f"📊 Found 'data' key with sub-keys: {list(option_chain['data'].keys())}")
                if 'strikes' in option_chain['data'] and isinstance(option_chain['data']['strikes'], list):
                    strike_list = option_chain['data']['strikes']
                    logger.info(f"✅ Found 'strikes' in 'data' with {len(strike_list)} strikes")
                    logger.info(f"✅ Found 'strikes' in 'data' with {len(strike_list)} strikes")
                else:
                    logger.warning(f"⚠️ Unexpected option chain structure for {stock_name}: {list(option_chain.get('data', {}).keys())}")
                    logger.info(f"⚠️ Unexpected option chain structure for {stock_name}: {list(option_chain.get('data', {}).keys())}")
                    # Try to find any list in the data structure
                    for key, value in option_chain['data'].items():
                        if isinstance(value, list) and len(value) > 0:
                            logger.info(f"   Found list in '{key}' with {len(value)} items, first item type: {type(value[0])}")
                            logger.info(f"   Found list in '{key}' with {len(value)} items, first item type: {type(value[0])}")
                            if isinstance(value[0], dict):
                                logger.info(f"   First item keys: {list(value[0].keys())}")
                                logger.info(f"   First item keys: {list(value[0].keys())}")
                    return None
            else:
                logger.warning(f"⚠️ Unexpected option chain structure for {stock_name}: {list(option_chain.keys())}")
                logger.info(f"⚠️ Unexpected option chain structure for {stock_name}: {list(option_chain.keys())}")
                # Try to find any list in the structure
                for key, value in option_chain.items():
                    if isinstance(value, list) and len(value) > 0:
                        logger.info(f"   Found list in '{key}' with {len(value)} items, first item type: {type(value[0])}")
                        logger.info(f"   Found list in '{key}' with {len(value)} items, first item type: {type(value[0])}")
                        if isinstance(value[0], dict):
                            logger.info(f"   First item keys: {list(value[0].keys())}")
                            logger.info(f"   First item keys: {list(value[0].keys())}")
                return None
        elif isinstance(option_chain, list):
            # Direct list format (legacy or different API version)
            strike_list = option_chain
            logger.info(f"✅ Option chain for {stock_name} is a direct list with {len(strike_list)} items")
            logger.info(f"✅ Option chain for {stock_name} is a direct list with {len(strike_list)} items")
        else:
            logger.warning(f"⚠️ Unexpected option chain type for {stock_name}: {type(option_chain)}")
            logger.info(f"⚠️ Unexpected option chain type for {stock_name}: {type(option_chain)}")
            return None
        
        # Parse strikes from the list
        logger.info(f"Parsing {len(strike_list)} strikes from option chain for {stock_name}")
        logger.info(f"Parsing {len(strike_list)} strikes from option chain for {stock_name}")
        
        # Debug: Log first strike structure
        if strike_list and len(strike_list) > 0:
            first_strike = strike_list[0]
            logger.info(f"First strike structure for {stock_name}: type={type(first_strike)}, keys={list(first_strike.keys()) if isinstance(first_strike, dict) else 'not a dict'}")
            if isinstance(first_strike, dict):
                logger.info(f"First strike sample keys: {list(first_strike.keys())}")
                if 'call_options' in first_strike:
                    logger.info(f"call_options type: {type(first_strike['call_options'])}, keys: {list(first_strike['call_options'].keys()) if isinstance(first_strike['call_options'], dict) else 'not a dict'}")
                if 'put_options' in first_strike:
                    logger.info(f"put_options type: {type(first_strike['put_options'])}, keys: {list(first_strike['put_options'].keys()) if isinstance(first_strike['put_options'], dict) else 'not a dict'}")
        
        for strike_data in strike_list:
            if not isinstance(strike_data, dict):
                logger.warning(f"Skipping non-dict strike_data item: {type(strike_data)}")
                continue
                
            strike_price = strike_data.get('strike_price', 0)
            if not strike_price:
                logger.debug(f"Skipping strike_data with no strike_price: {list(strike_data.keys())}")
                continue
            
            # Get option data based on option type
            # Try multiple possible structures
            option_data = None
            if option_type == 'CE':
                if 'call_options' in strike_data:
                    call_opts = strike_data['call_options']
                    if isinstance(call_opts, dict):
                        option_data = call_opts.get('market_data', call_opts)  # Try market_data first, fallback to direct
                    elif isinstance(call_opts, list) and len(call_opts) > 0:
                        option_data = call_opts[0]  # If it's a list, take first item
            else:  # PE
                if 'put_options' in strike_data:
                    put_opts = strike_data['put_options']
                    if isinstance(put_opts, dict):
                        option_data = put_opts.get('market_data', put_opts)  # Try market_data first, fallback to direct
                    elif isinstance(put_opts, list) and len(put_opts) > 0:
                        option_data = put_opts[0]  # If it's a list, take first item
            
            if option_data and isinstance(option_data, dict):
                volume = option_data.get('volume', 0) or option_data.get('total_volume', 0)
                oi = option_data.get('oi', 0) or option_data.get('open_interest', 0)
                ltp = option_data.get('ltp', 0) or option_data.get('last_price', 0)
                
                # Always include the strike, even if volume/OI is 0 - we need it for OTM calculation
                strikes.append({
                    'strike_price': float(strike_price),
                    'volume': float(volume),
                    'oi': float(oi),
                    'ltp': float(ltp)
                })
                logger.debug(f"Added strike {strike_price} {option_type}: vol={volume}, oi={oi}, ltp={ltp}")
            else:
                logger.warning(f"No option_data found for strike {strike_price} {option_type} in {stock_name} - strike_data keys: {list(strike_data.keys()) if isinstance(strike_data, dict) else 'not a dict'}")
        
        logger.info(f"Found {len(strikes)} {option_type} options in chain for {stock_name}")
        logger.info(f"Found {len(strikes)} {option_type} options in chain for {stock_name}")
        
        if not strikes:
            logger.warning(f"No {option_type} options found in chain for {stock_name} - this will cause 'Missing option data' error")
            logger.info(f"No {option_type} options found in chain for {stock_name}")
            # Debug: Log first few strike_data items to understand structure
            if strike_list and len(strike_list) > 0:
                logger.debug(f"First strike_data item structure: {list(strike_list[0].keys()) if isinstance(strike_list[0], dict) else type(strike_list[0])}")
                if isinstance(strike_list[0], dict):
                    logger.debug(f"First strike_data sample: strike_price={strike_list[0].get('strike_price')}, call_options keys={list(strike_list[0].get('call_options', {}).keys())}, put_options keys={list(strike_list[0].get('put_options', {}).keys())}")
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
            logger.warning(f"No OTM {option_type} strikes found for {stock_name} (stock LTP: {stock_ltp}) - this will cause 'Missing option data' error")
            logger.info(f"No OTM {option_type} strikes found for {stock_name} (stock LTP: {stock_ltp})")
            # Debug: Log available strikes to understand why
            if strikes:
                sample_strikes = strikes[:5]
                logger.debug(f"Sample available strikes: {[s['strike_price'] for s in sample_strikes]}")
            return None
        
        # Sort by distance from LTP (closest first) to get OTM-1 to OTM-5
        otm_strikes.sort(key=lambda x: abs(x['strike_price'] - stock_ltp))
        
        # Get first 5 OTM strikes (OTM-1 to OTM-5)
        otm_1_to_5 = otm_strikes[:5]
        
        if not otm_1_to_5:
            logger.info(f"Not enough OTM strikes for {stock_name}")
            return otm_strikes[0] if otm_strikes else None
        
        logger.info(f"OTM-1 to OTM-5 strikes for {stock_name} {option_type}:")
        for i, strike in enumerate(otm_1_to_5, 1):
            liquidity_score = strike['volume'] * strike['oi']
            logger.info(f"  OTM-{i}: Strike {strike['strike_price']}, Vol: {strike['volume']}, OI: {strike['oi']}, Score: {liquidity_score}")
        
        # Select strike with highest volume * OI among OTM-1 to OTM-5
        selected = max(otm_1_to_5, key=lambda x: x['volume'] * x['oi'])
        
        otm_position = otm_1_to_5.index(selected) + 1
        liquidity_score = selected['volume'] * selected['oi']
        logger.info(f"✅ Selected OTM-{otm_position} strike: {selected['strike_price']} (Volume: {selected['volume']}, OI: {selected['oi']}, Score: {liquidity_score})")
        logger.info(f"   Highest liquidity among OTM-1 to OTM-5")
        return selected
        
    except Exception as e:
        logger.error(f"Error fetching option chain for {stock_name}: {str(e)}", exc_info=True)
        logger.info(f"Error fetching option chain for {stock_name}: {str(e)}")
        import traceback
        logger.info(traceback.format_exc())
        return None

# Helper function to process webhook data
def find_option_contract_from_instruments(stock_name: str, option_type: str, stock_ltp: float, vwap_service=None) -> Optional[str]:
    """
    Find the correct option contract from instruments.json based on:
    - underlying_symbol matching stock_name
    - option_type matching (CE/PE)
    - Strike price from option chain API (volume/OI based) - REQUIRED, no fallback
    - Expiry month: If current date > 17th, use next month's expiry; otherwise use current month
    
    Args:
        stock_name: Stock symbol (e.g., 'RELIANCE')
        option_type: Option type ('CE' or 'PE')
        stock_ltp: Current stock LTP price
        vwap_service: UpstoxService instance for API calls
        
    Returns:
        Option contract name (trading_symbol) from instruments.json
        or None if option chain unavailable or contract not found
        (Trade will be marked as no_entry when None is returned)
    """
    logger.info(f"🔍 find_option_contract_from_instruments called for {stock_name} {option_type} (LTP: {stock_ltp})")
    try:
        import pytz
        from pathlib import Path
        import json as json_lib
        from datetime import timedelta
        
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
        
        logger.info(f"Target expiry: {target_expiry_year}-{target_expiry_month:02d} (current date: {now.strftime('%Y-%m-%d')})")
        logger.info(f"Target expiry: {target_expiry_year}-{target_expiry_month:02d} (current date: {now.strftime('%Y-%m-%d')}) for {stock_name} {option_type}")
        
        # Get strike from option chain API - REQUIRED, no fallback
        target_strike = None
        if vwap_service:
            logger.info(f"Fetching strike from option chain for {stock_name} {option_type} (LTP: {stock_ltp})")
            try:
                strike_data = find_strike_from_option_chain(vwap_service, stock_name, option_type, stock_ltp)
                if strike_data:
                    target_strike = strike_data['strike_price']
                    logger.info(f"Using option chain strike for {stock_name}: {target_strike} (Volume: {strike_data['volume']}, OI: {strike_data['oi']})")
                    logger.info(f"Using option chain strike for {stock_name} {option_type}: {target_strike} (Volume: {strike_data['volume']}, OI: {strike_data['oi']})")
                else:
                    logger.warning(f"No strike data returned from option chain for {stock_name} {option_type} - this will cause 'Missing option data' error")
            except Exception as e:
                logger.error(f"Exception in find_strike_from_option_chain for {stock_name} {option_type}: {str(e)}", exc_info=True)
                strike_data = None
        
        # If option chain not available, return None to mark trade as no_entry
        if target_strike is None or target_strike == 0:
            logger.info(f"❌ Option chain not available for {stock_name} - Cannot determine strike. Trade will be marked as no_entry.")
            logger.warning(f"❌ Option chain not available for {stock_name} {option_type} - Cannot determine strike. Trade will be marked as no_entry.")
            return None
        
        logger.info(f"Looking for {option_type} option with strike {target_strike} for {stock_name}")
        logger.info(f"Looking for {option_type} option with strike {target_strike} for {stock_name}")
        
        # Load instruments.json
        instruments_file = Path("/home/ubuntu/trademanthan/data/instruments/nse_instruments.json")
        
        if not instruments_file.exists():
            logger.info(f"⚠️ Instruments JSON file not found: {instruments_file}")
            logger.error(f"Instruments JSON file not found: {instruments_file}")
            return None
        
        # Load instruments data with retry logic
        max_retries = 3
        instruments_data = None
        
        for retry in range(1, max_retries + 1):
            try:
                with open(instruments_file, 'r') as f:
                    instruments_data = json_lib.load(f)
                break  # Success - exit retry loop
            except (json_lib.JSONDecodeError, IOError, OSError) as file_error:
                if retry < max_retries:
                    import time
                    time.sleep(retry * 0.5)  # Exponential backoff
                else:
                    logger.info(f"❌ ERROR: Failed to read instruments file after {max_retries} attempts: {str(file_error)}")
                    logger.error(f"Failed to read instruments file for {stock_name} after {max_retries} attempts: {str(file_error)}")
                    return None
        
        if not instruments_data:
            logger.info(f"⚠️ Instruments data is empty")
            logger.error(f"Instruments data is empty for {stock_name}")
            return None
        
        # Validate that instruments_data is a list
        if not isinstance(instruments_data, list):
            error_msg = f"Instruments data is not a list (type: {type(instruments_data).__name__})"
            logger.info(f"❌ ERROR: {error_msg}")
            logger.error(f"{error_msg} for {stock_name}")
            return None
        
        # Calculate target expiry date range (±7 days tolerance)
        target_expiry_start = datetime(target_expiry_year, target_expiry_month, 1, tzinfo=ist)
        if target_expiry_month == 12:
            target_expiry_end = datetime(target_expiry_year + 1, 1, 1, tzinfo=ist) - timedelta(days=1)
        else:
            target_expiry_end = datetime(target_expiry_year, target_expiry_month + 1, 1, tzinfo=ist) - timedelta(days=1)
        
        expiry_tolerance_days = 7
        target_expiry_min = target_expiry_start - timedelta(days=expiry_tolerance_days)
        target_expiry_max = target_expiry_end + timedelta(days=expiry_tolerance_days)
        
        # Strike tolerance: ±1% or ±10, whichever is larger
        strike_tolerance = max(target_strike * 0.01, 10.0)
        
        # Search for exact match first
        best_match = None
        best_match_score = float('inf')
        
        for inst in instruments_data:
            # Skip non-dictionary entries
            if not isinstance(inst, dict):
                continue
            
            if (inst.get('underlying_symbol') == stock_name and 
                inst.get('instrument_type') == option_type and
                inst.get('segment') == 'NSE_FO'):
                
                inst_strike = inst.get('strike_price', 0)
                strike_diff = abs(inst_strike - target_strike)
                
                expiry_ms = inst.get('expiry', 0)
                if expiry_ms:
                    try:
                        if expiry_ms > 1e12:
                            expiry_ms = expiry_ms / 1000
                        inst_expiry = datetime.fromtimestamp(expiry_ms, tz=ist)
                    except (ValueError, OSError, OverflowError) as expiry_error:
                        # Skip instruments with invalid expiry timestamps
                        logger.debug(f"Skipping instrument with invalid expiry timestamp for {stock_name}: {expiry_error}")
                        continue
                    
                    # Check if expiry is within tolerance (same month/year or ±7 days)
                    expiry_in_range = (
                        (inst_expiry.year == target_expiry_year and inst_expiry.month == target_expiry_month) or
                        (target_expiry_min <= inst_expiry <= target_expiry_max)
                    )
                    
                    # Check if strike is within tolerance
                    strike_in_range = strike_diff <= strike_tolerance
                    
                    if expiry_in_range and strike_in_range:
                        # Score: prioritize exact expiry match, then strike difference
                        expiry_score = 0 if (inst_expiry.year == target_expiry_year and inst_expiry.month == target_expiry_month) else 1000
                        score = expiry_score + strike_diff
                        
                        # Exact match (same month/year and exact strike)
                        if strike_diff < 0.01 and expiry_in_range and (inst_expiry.year == target_expiry_year and inst_expiry.month == target_expiry_month):
                            # Fetch option contract name from instrument JSON
                            option_contract = inst.get('trading_symbol')
                            if not option_contract:
                                # Log warning but continue searching - there might be another exact match with trading_symbol
                                logger.warning(f"Exact match found for {stock_name} {option_type} strike {inst_strike} but trading_symbol is missing (instrument_key: {inst.get('instrument_key', 'N/A')})")
                                logger.info(f"⚠️ WARNING: Exact match found but trading_symbol not found in instrument JSON (strike: {inst_strike}, expiry: {inst_expiry.strftime('%d %b %Y')})")
                                continue
                            logger.info(f"✅ Found EXACT match for {stock_name} {option_type}: {option_contract}")
                            logger.info(f"   Strike: {inst_strike} (requested: {target_strike})")
                            logger.info(f"   Expiry: {inst_expiry.strftime('%d %b %Y')}")
                            logger.info(f"✅ Found EXACT match for {stock_name} {option_type}: {option_contract} (strike: {inst_strike}, expiry: {inst_expiry.strftime('%d %b %Y')})")
                            return option_contract
                        else:
                            # Track best match (within tolerance)
                            if best_match is None or score < best_match_score:
                                best_match = inst
                                best_match_score = score
        
        # If no exact match, use best match within tolerance
        if best_match:
            try:
                inst_strike = best_match.get('strike_price', 0)
                expiry_ms = best_match.get('expiry', 0)
                if expiry_ms:
                    if expiry_ms > 1e12:
                        expiry_ms = expiry_ms / 1000
                    inst_expiry = datetime.fromtimestamp(expiry_ms, tz=ist)
                else:
                    logger.warning(f"Best match for {stock_name} {option_type} has no expiry timestamp")
                    logger.info(f"⚠️ WARNING: Best match has no expiry timestamp, skipping")
                    return None
                
                # Fetch option contract name from instrument JSON
                option_contract = best_match.get('trading_symbol')
                if not option_contract:
                    logger.warning(f"Best match found for {stock_name} {option_type} but trading_symbol is missing (instrument_key: {best_match.get('instrument_key', 'N/A')}, strike: {inst_strike})")
                    logger.info(f"⚠️ WARNING: trading_symbol not found in best match instrument JSON (strike: {inst_strike}, expiry: {inst_expiry.strftime('%d %b %Y')})")
                    return None
                logger.info(f"⚠️ WARNING: Using BEST MATCH (within tolerance) for {stock_name} {option_type}: {option_contract}")
                logger.info(f"   Strike: {inst_strike} (requested: {target_strike}, diff: {abs(inst_strike - target_strike):.4f})")
                logger.info(f"   Expiry: {inst_expiry.strftime('%d %b %Y')} (requested: {target_expiry_month}/{target_expiry_year})")
                logger.info(f"⚠️ WARNING: Using BEST MATCH (within tolerance) for {stock_name} {option_type}: {option_contract} (strike: {inst_strike}, expiry: {inst_expiry.strftime('%d %b %Y')})")
                return option_contract
            except (ValueError, OSError, OverflowError, TypeError) as best_match_error:
                logger.error(f"Error processing best match for {stock_name} {option_type}: {best_match_error}")
                logger.info(f"❌ ERROR: Failed to process best match: {str(best_match_error)}")
                return None
        
        logger.info(f"No option contract found for {stock_name} {option_type} (target strike: {target_strike})")
        logger.warning(f"No option contract found for {stock_name} {option_type} (target strike: {target_strike})")
        return None
            
    except Exception as e:
        logger.info(f"Error finding option contract for {stock_name}: {str(e)}")
        logger.error(f"❌ EXCEPTION in find_option_contract_from_instruments for {stock_name} {option_type}: {str(e)}", exc_info=True)
        import traceback
        logger.error(f"Traceback: {traceback.format_exc()}")
        return None


def find_option_contract_from_master_stock(db: Session, stock_name: str, option_type: str, stock_ltp: float, vwap_service=None) -> Optional[str]:
    """
    DEPRECATED: This function now delegates to find_option_contract_from_instruments().
    Kept for backward compatibility during transition.
    """
    logger.info(f"🔍 find_option_contract_from_master_stock called for {stock_name} {option_type} (LTP: {stock_ltp})")
    try:
        result = find_option_contract_from_instruments(stock_name, option_type, stock_ltp, vwap_service)
        logger.info(f"find_option_contract_from_master_stock returning: {result}")
        return result
    except Exception as e:
        logger.error(f"❌ EXCEPTION in find_option_contract_from_master_stock for {stock_name} {option_type}: {str(e)}", exc_info=True)
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
    import json as json_module  # Use alias to avoid any potential shadowing issues
    
    try:
        logger.info(f"Processing webhook data (forced_type={forced_type}): {json_module.dumps(data, indent=2)}")
        
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
        
        logger.info(f"Current date: {now.strftime('%Y-%m-%d %A')}")
        logger.info(f"Last trading date: {trading_date.strftime('%Y-%m-%d %A')}")
        
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
                logger.info(f"Original time: {triggered_at_raw} -> Corrected to: {corrected_time}")
                logger.info(f"Triggered at: {triggered_datetime.strftime('%Y-%m-%d %H:%M:%S %A')}")
            else:
                # Default to first Chartink time if no time provided
                triggered_datetime = trading_date.replace(hour=10, minute=15, second=0, microsecond=0)
                triggered_at_str = triggered_datetime.isoformat()
                triggered_at_display = "10:15 AM"
        except Exception as e:
            logger.info(f"Error parsing triggered_at '{triggered_at_raw}': {e}")
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
        
        logger.info(f"🔍 Parsing stocks: type={type(stocks)}, value={repr(stocks)}")
        logger.info(f"🔍 Parsing trigger_prices: type={type(trigger_prices)}, value={repr(trigger_prices)}")
        
        # Chartink format: comma-separated strings
        if isinstance(stocks, str) and isinstance(trigger_prices, str):
            logger.info(f"✅ Using Chartink format (comma-separated strings)")
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
        logger.info(f"🔍 Before index filtering: {original_count} stocks")
        if original_count > 0:
            logger.info(f"🔍 Stock names before filtering: {[s.get('stock_name', 'UNKNOWN') for s in processed_data['stocks']]}")
        processed_data["stocks"] = [
            stock for stock in processed_data["stocks"]
            if stock.get("stock_name", "").strip().upper() not in INDEX_NAMES
        ]
        filtered_count = original_count - len(processed_data["stocks"])
        if filtered_count > 0:
            logger.info(f"🚫 Filtered out {filtered_count} index name(s) from stocks list")
            logger.info(f"🚫 Filtered out {filtered_count} index name(s) from stocks list (original: {original_count}, remaining: {len(processed_data['stocks'])})")
        else:
            logger.info(f"✅ No index names filtered - {len(processed_data['stocks'])} stocks remain")
        
        # Determine if this is Bullish or Bearish
        if forced_type:
            # Use forced type from endpoint
            is_bullish = (forced_type.lower() == 'bullish')
            is_bearish = (forced_type.lower() == 'bearish')
            logger.info(f"Using forced type: {forced_type}")
        else:
            # Auto-detect from alert/scan name
            alert_name = processed_data.get("alert_name", "").lower() if processed_data and isinstance(processed_data, dict) else ""
            scan_name = processed_data.get("scan_name", "").lower() if processed_data and isinstance(processed_data, dict) else ""
            scan_url = processed_data.get("scan_url", "").lower() if processed_data and isinstance(processed_data, dict) else ""
            
            # Check for bearish indicators first (more specific)
            is_bearish = ("bearish" in alert_name or "bearish" in scan_name or "bearish" in scan_url or
                         "put" in alert_name or "put" in scan_name or "put" in scan_url)
            
            # Check for bullish indicators
            is_bullish = ("bullish" in alert_name or "bullish" in scan_name or "bullish" in scan_url or
                         "call" in alert_name or "call" in scan_name or "call" in scan_url)
            
            if not is_bullish and not is_bearish:
                # Default to bullish if not specified (for backward compatibility)
                is_bullish = True
                logger.warning(f"⚠️ Alert type not specified in alert_name='{alert_name}', scan_name='{scan_name}', scan_url='{scan_url}' - defaulting to Bullish")
                logger.info(f"⚠️ Alert type not specified - defaulting to Bullish (alert_name='{alert_name}', scan_name='{scan_name}', scan_url='{scan_url}')")
            else:
                detected_type = "Bearish" if is_bearish else "Bullish"
                logger.info(f"✅ Auto-detected alert type: {detected_type} (alert_name='{alert_name}', scan_name='{scan_name}', scan_url='{scan_url}')")
                logger.info(f"✅ Auto-detected alert type: {detected_type}")
        
        # Force option type based on alert type
        forced_option_type = 'CE' if is_bullish else 'PE'
        stocks_count = len(processed_data['stocks']) if processed_data and isinstance(processed_data, dict) and 'stocks' in processed_data else 0
        logger.info(f"Processing {stocks_count} stocks with option type: {forced_option_type}")
        alert_name_display = processed_data.get('alert_name', '') if processed_data and isinstance(processed_data, dict) else ''
        logger.info(f"Alert name: {alert_name_display}")
        
        # Process each stock individually to fetch LTP and find option contract
        # IMPORTANT: Always save at minimum stock_name and alert_time, even if enrichment fails
        enriched_stocks = []
        stocks_to_enrich = processed_data.get("stocks", []) if processed_data and isinstance(processed_data, dict) else []
        for stock in stocks_to_enrich:
            stock_name = stock.get("stock_name", "")
            trigger_price = stock.get("trigger_price", 0.0)
            
            logger.info(f"Processing stock: {stock_name}")
            
            # Initialize all fields with defaults - each activity is independent
            stock_ltp = trigger_price
            stock_vwap = 0.0
            option_contract = None
            option_strike = 0.0
            qty = 0
            option_ltp = 0.0
            instrument_key = None
            option_candles = None
            stock_vwap_previous_hour = None
            stock_vwap_previous_hour_time = None
            
            # ====================================================================
            # ACTIVITY 1: Fetch Stock LTP and VWAP (Independent)
            # ====================================================================
            try:
                stock_data = vwap_service.get_stock_ltp_and_vwap(stock_name)
                if stock_data:
                    if stock_data.get('ltp') and stock_data['ltp'] > 0:
                        stock_ltp = stock_data['ltp']
                        logger.info(f"✅ Stock LTP for {stock_name}: ₹{stock_ltp:.2f}")
                    else:
                        logger.info(f"⚠️ Could not fetch LTP for {stock_name}, using trigger price: ₹{trigger_price}")
                    
                    if stock_data.get('vwap') and stock_data['vwap'] > 0:
                        stock_vwap = stock_data['vwap']
                        logger.info(f"✅ Stock VWAP for {stock_name}: ₹{stock_vwap:.2f}")
                    else:
                        logger.info(f"⚠️ Could not fetch VWAP for {stock_name} - will retry via hourly updater")
                else:
                    logger.info(f"⚠️ Stock data fetch completely failed for {stock_name} - using defaults")
            except Exception as e:
                logger.info(f"❌ Stock data fetch failed for {stock_name}: {str(e)} - Using trigger price")
                import traceback
                traceback.print_exc()
            
            # ====================================================================
            # ACTIVITY 2: Find Option Contract (Independent)
            # ====================================================================
            logger.info(f"🔍 ACTIVITY 2: Finding option contract for {stock_name} with forced_option_type={forced_option_type}, stock_ltp={stock_ltp}")
            max_retries = 3
            for retry_attempt in range(1, max_retries + 1):
                try:
                    logger.info(f"Calling find_option_contract_from_master_stock for {stock_name} (attempt {retry_attempt})")
                    option_contract = find_option_contract_from_master_stock(
                        db, stock_name, forced_option_type, stock_ltp, vwap_service
                    )
                    logger.info(f"find_option_contract_from_master_stock returned: {option_contract}")
                    if option_contract:
                        logger.info(f"✅ Option contract found for {stock_name} (attempt {retry_attempt}): {option_contract}")
                        logger.info(f"✅ Option contract found for {stock_name} (attempt {retry_attempt}): {option_contract}")
                        break
                    else:
                        if retry_attempt < max_retries:
                            logger.info(f"⚠️ No option contract found for {stock_name} (attempt {retry_attempt}/{max_retries}), retrying...")
                            logger.warning(f"⚠️ No option contract found for {stock_name} (attempt {retry_attempt}/{max_retries}), retrying...")
                            import time
                            time.sleep(1)  # Brief delay before retry
                        else:
                            logger.info(f"⚠️ No option contract found for {stock_name} after {max_retries} attempts")
                            logger.warning(f"⚠️ No option contract found for {stock_name} after {max_retries} attempts")
                except Exception as e:
                    if retry_attempt < max_retries:
                        logger.info(f"⚠️ Option contract search failed for {stock_name} (attempt {retry_attempt}/{max_retries}): {str(e)}, retrying...")
                        logger.warning(f"⚠️ Option contract search failed for {stock_name} (attempt {retry_attempt}/{max_retries}): {str(e)}, retrying...")
                        import time
                        time.sleep(1)  # Brief delay before retry
                    else:
                        logger.info(f"⚠️ Option contract search failed for {stock_name} after {max_retries} attempts: {str(e)}")
                        logger.error(f"⚠️ Option contract search failed for {stock_name} after {max_retries} attempts: {str(e)}")
                        option_contract = None
                
            # ====================================================================
            # ACTIVITY 3: Extract Option Strike (Independent - requires option_contract)
            # ====================================================================
            if option_contract:
                try:
                    import re
                    match = re.search(r'-(\d+\.?\d*)-(?:CE|PE)$', option_contract)
                    if match:
                        option_strike = float(match.group(1))
                        logger.info(f"✅ Extracted option strike: {option_strike} from {option_contract}")
                    else:
                        logger.info(f"⚠️ Could not extract option strike from {option_contract} - regex did not match")
                        logger.warning(f"Could not extract option strike from {option_contract} for {stock_name}")
                except Exception as e:
                    logger.info(f"❌ ERROR extracting option strike from {option_contract}: {str(e)}")
                    logger.error(f"Error extracting option strike from {option_contract} for {stock_name}: {str(e)}", exc_info=True)
            
            # ====================================================================
            # ACTIVITY 4: Fetch Lot Size from instruments.json (Independent - requires option_contract)
            # NOTE: Lot size is now fetched in ACTIVITY 5 when we find instrument_key
            # This activity is kept for backward compatibility but lot_size should be
            # fetched from instruments.json in Activity 5, not from master_stock table
            # ====================================================================
            # Lot size is now fetched from instruments.json in Activity 5 when instrument_key is found
            # This ensures we use the same data source and don't depend on master_stock table
            # If lot_size wasn't found in Activity 5, it will remain 0 (default)
            if option_contract and qty == 0:
                logger.info(f"⚠️ Lot size not found in instruments.json for {option_contract}, qty remains 0")
                logger.warning(f"Lot size not found in instruments.json for {option_contract} (stock: {stock_name})")
            
            # ====================================================================
            # ACTIVITY 5: Find Instrument Key from instruments.json (Independent - requires option_contract)
            # ====================================================================
            if option_contract:
                try:
                    from pathlib import Path
                    import json as json_lib
                    import re
                    
                    instruments_file = Path("/home/ubuntu/trademanthan/data/instruments/nse_instruments.json")
                    
                    if not instruments_file.exists():
                        logger.info(f"⚠️ Instruments JSON file not found: {instruments_file}")
                        logger.error(f"Instruments JSON file not found: {instruments_file}")
                    else:
                        # CRITICAL: Retry logic ensures we can read the file even if there are transient issues
                        # The file age doesn't matter - as long as instruments match, they will be found
                        max_retries = 3
                        instruments_data = None
                        
                        for retry in range(1, max_retries + 1):
                            try:
                                logger.info(f"📂 Loading instruments from: {instruments_file} (attempt {retry}/{max_retries})")
                                with open(instruments_file, 'r') as f:
                                    instruments_data = json_lib.load(f)
                                logger.info(f"✅ Loaded {len(instruments_data)} instruments from file")
                                break  # Success - exit retry loop
                            except (json_lib.JSONDecodeError, IOError, OSError) as file_error:
                                if retry < max_retries:
                                    import time
                                    wait_time = retry * 0.5  # Exponential backoff: 0.5s, 1s, 1.5s
                                    logger.info(f"⚠️ Failed to read instruments file (attempt {retry}/{max_retries}): {str(file_error)}")
                                    logger.info(f"   Retrying in {wait_time}s...")
                                    time.sleep(wait_time)
                                else:
                                    logger.info(f"❌ ERROR: Failed to read instruments file after {max_retries} attempts: {str(file_error)}")
                                    logger.error(f"Failed to read instruments file for {stock_name} after {max_retries} attempts: {str(file_error)}")
                                    import traceback
                                    traceback.print_exc()
                                    instruments_data = []  # Set to empty list to prevent further processing
                            except Exception as file_error:
                                # For other exceptions, don't retry
                                logger.info(f"❌ ERROR: Unexpected error reading instruments file: {str(file_error)}")
                                logger.error(f"Unexpected error reading instruments file for {stock_name}: {str(file_error)}")
                                import traceback
                                traceback.print_exc()
                                instruments_data = []
                                break
                        
                        if instruments_data is None:
                            instruments_data = []  # Ensure it's set to empty list if all retries failed
                        
                        if not instruments_data:
                            logger.info(f"⚠️ Instruments data is empty - cannot search for {option_contract}")
                            logger.warning(f"Instruments data is empty for {stock_name} - cannot find instrument_key")
                        else:
                            # FIRST: Try to find by trading_symbol directly (since find_option_contract_from_instruments now returns trading_symbol)
                            logger.info(f"🔍 Searching for instrument_key by trading_symbol: {option_contract}")
                            logger.info(f"🔍 Searching for instrument_key by trading_symbol: '{option_contract}' (stock: {stock_name})")
                            found_by_trading_symbol = False
                            match_count = 0
                            # Normalize option_contract for comparison (strip whitespace, handle case)
                            option_contract_normalized = option_contract.strip() if option_contract else ""
                            for inst in instruments_data:
                                if isinstance(inst, dict):
                                    inst_trading_symbol = inst.get('trading_symbol', '')
                                    if inst_trading_symbol:
                                        match_count += 1
                                        # Try exact match first
                                        inst_trading_symbol_normalized = inst_trading_symbol.strip()
                                        if inst_trading_symbol_normalized == option_contract_normalized:
                                            instrument_key = inst.get('instrument_key')
                                            if instrument_key:
                                                inst_lot_size = inst.get('lot_size')
                                                if inst_lot_size and inst_lot_size > 0:
                                                    qty = int(inst_lot_size)
                                                expiry_ms = inst.get('expiry', 0)
                                                if expiry_ms:
                                                    if expiry_ms > 1e12:
                                                        expiry_ms = expiry_ms / 1000
                                                    try:
                                                        inst_expiry = datetime.fromtimestamp(expiry_ms, tz=ist)
                                                        inst_strike = inst.get('strike_price', 0)
                                                        logger.info(f"✅ Found instrument by trading_symbol for {option_contract}:")
                                                        logger.info(f"   Instrument Key: {instrument_key}")
                                                        logger.info(f"   Strike: {inst_strike}")
                                                        logger.info(f"   Expiry: {inst_expiry.strftime('%d %b %Y')}")
                                                        logger.info(f"   Lot Size: {qty if inst_lot_size and inst_lot_size > 0 else 'Not available'}")
                                                        logger.info(f"✅ Found instrument by trading_symbol for {option_contract} (stock: {stock_name}): instrument_key={instrument_key}, strike={inst_strike}, expiry={inst_expiry.strftime('%d %b %Y')}, lot_size={qty if inst_lot_size and inst_lot_size > 0 else 'N/A'}")
                                                        found_by_trading_symbol = True
                                                        break
                                                    except (ValueError, OSError) as e:
                                                        logger.warning(f"Invalid expiry timestamp for {option_contract}: {expiry_ms}, error: {str(e)}")
                                                        continue
                                            else:
                                                logger.warning(f"Found trading_symbol match for {option_contract} but instrument_key is None")
                                                logger.info(f"⚠️ WARNING: Found trading_symbol match but instrument_key is None for {option_contract}")
                                                # Continue searching - maybe another instrument has the key
                            
                            if not found_by_trading_symbol:
                                logger.warning(f"⚠️ Could not find instrument by trading_symbol '{option_contract}' for {stock_name} (checked {match_count} instruments)")
                                logger.info(f"⚠️ Could not find instrument by trading_symbol '{option_contract}' for {stock_name}")
                            
                            # SECOND: If not found by trading_symbol, try parsing old format: STOCK-MonthYYYY-STRIKE-CE/PE
                            match = None
                            if not found_by_trading_symbol:
                                logger.info(f"⚠️ Not found by trading_symbol, trying old format parsing...")
                                match = re.match(r'^([A-Z-]+)-(\w{3})(\d{4})-(\d+\.?\d*?)-(CE|PE)$', option_contract)
                            
                            if not found_by_trading_symbol and match:
                                symbol, month, year, strike, opt_type = match.groups()
                                strike_value = float(strike)
                                
                                logger.info(f"🔍 Searching for instrument_key: symbol={symbol}, month={month}, year={year}, strike={strike_value}, type={opt_type}")
                                
                                # Parse month
                                month_map = {
                                    'Jan': 1, 'Feb': 2, 'Mar': 3, 'Apr': 4,
                                    'May': 5, 'Jun': 6, 'Jul': 7, 'Aug': 8,
                                    'Sep': 9, 'Oct': 10, 'Nov': 11, 'Dec': 12
                                }
                                target_month = month_map.get(month[:3].capitalize(), None)
                                target_year = int(year)
                                
                                if target_month:
                                    # Search for matching option in NSE_FO segment
                                    # Improved matching: Allow tolerance for expiry dates (±7 days) and strikes (±1%)
                                    best_match = None
                                    best_match_score = float('inf')
                                    match_count = 0
                                    from datetime import timedelta
                                    
                                    # Calculate target expiry date range (±7 days tolerance)
                                    target_expiry_start = datetime(target_year, target_month, 1, tzinfo=ist)
                                    # Get last day of target month
                                    if target_month == 12:
                                        target_expiry_end = datetime(target_year + 1, 1, 1, tzinfo=ist) - timedelta(days=1)
                                    else:
                                        target_expiry_end = datetime(target_year, target_month + 1, 1, tzinfo=ist) - timedelta(days=1)
                                    
                                    # Allow ±7 days tolerance for expiry matching
                                    expiry_tolerance_days = 7
                                    target_expiry_min = target_expiry_start - timedelta(days=expiry_tolerance_days)
                                    target_expiry_max = target_expiry_end + timedelta(days=expiry_tolerance_days)
                                    
                                    # Strike tolerance: ±1% or ±10, whichever is larger
                                    strike_tolerance = max(strike_value * 0.01, 10.0)
                                    
                                    for inst in instruments_data:
                                        if (inst.get('underlying_symbol') == symbol and 
                                            inst.get('instrument_type') == opt_type and
                                            inst.get('segment') == 'NSE_FO'):
                                            
                                            inst_strike = inst.get('strike_price', 0)
                                            strike_diff = abs(inst_strike - strike_value)
                                            
                                            expiry_ms = inst.get('expiry', 0)
                                            if expiry_ms:
                                                if expiry_ms > 1e12:
                                                    expiry_ms = expiry_ms / 1000
                                                inst_expiry = datetime.fromtimestamp(expiry_ms, tz=ist)
                                                
                                                # Check if expiry is within tolerance (same month/year or ±7 days)
                                                expiry_in_range = (
                                                    (inst_expiry.year == target_year and inst_expiry.month == target_month) or
                                                    (target_expiry_min <= inst_expiry <= target_expiry_max)
                                                )
                                                
                                                # Check if strike is within tolerance
                                                strike_in_range = strike_diff <= strike_tolerance
                                                
                                                if expiry_in_range and strike_in_range:
                                                    match_count += 1
                                                    # Score: prioritize exact expiry match, then strike difference
                                                    expiry_score = 0 if (inst_expiry.year == target_year and inst_expiry.month == target_month) else 1000
                                                    score = expiry_score + strike_diff
                                                    
                                                    if strike_diff < 0.01 and expiry_in_range and (inst_expiry.year == target_year and inst_expiry.month == target_month):  # Exact match
                                                        instrument_key = inst.get('instrument_key')
                                                        trading_symbol = inst.get('trading_symbol', 'Unknown')
                                                        # Also fetch lot_size from the same instrument record
                                                        inst_lot_size = inst.get('lot_size')
                                                        if inst_lot_size and inst_lot_size > 0:
                                                            qty = int(inst_lot_size)
                                                        logger.info(f"✅ Found EXACT match for {option_contract}:")
                                                        logger.info(f"   Instrument Key: {instrument_key}")
                                                        logger.info(f"   Trading Symbol: {trading_symbol}")
                                                        logger.info(f"   Strike: {inst_strike} (requested: {strike_value}, diff: {strike_diff:.4f})")
                                                        logger.info(f"   Expiry: {inst_expiry.strftime('%d %b %Y')}")
                                                        logger.info(f"   Lot Size: {qty if inst_lot_size and inst_lot_size > 0 else 'Not available'}")
                                                        break
                                                    else:
                                                        # Track best match (within tolerance)
                                                        if best_match is None or score < best_match_score:
                                                            best_match = inst
                                                            best_match_score = score
                                    
                                    logger.info(f"   📊 Found {match_count} instrument(s) matching symbol={symbol}, type={opt_type}, expiry={target_month}/{target_year} (with tolerance)")
                                    
                                    # Use best match if no exact match but we have matches within tolerance
                                    if not instrument_key and best_match:
                                        instrument_key = best_match.get('instrument_key')
                                        inst_strike = best_match.get('strike_price', 0)
                                        expiry_ms = best_match.get('expiry', 0)
                                        if expiry_ms > 1e12:
                                            expiry_ms = expiry_ms / 1000
                                        inst_expiry = datetime.fromtimestamp(expiry_ms, tz=ist)
                                        trading_symbol = best_match.get('trading_symbol', 'Unknown')
                                        # Also fetch lot_size from the best match
                                        inst_lot_size = best_match.get('lot_size')
                                        if inst_lot_size and inst_lot_size > 0:
                                            qty = int(inst_lot_size)
                                        logger.info(f"⚠️ WARNING: Using BEST MATCH (within tolerance) for {option_contract}:")
                                        logger.info(f"   Instrument Key: {instrument_key}")
                                        logger.info(f"   Trading Symbol: {trading_symbol}")
                                        logger.info(f"   Strike: {inst_strike} (requested: {strike_value}, diff: {abs(inst_strike - strike_value):.4f})")
                                        logger.info(f"   Expiry: {inst_expiry.strftime('%d %b %Y')} (requested: {target_month}/{target_year})")
                                        if inst_lot_size and inst_lot_size > 0:
                                            logger.info(f"   Lot Size: {qty}")
                                        else:
                                            logger.info(f"   ⚠️ Lot Size: Not available in instruments.json")
                                        logger.info(f"   ⚠️ This match is within tolerance but may not be exact!")
                                    
                                    # If instrument_key was found but lot_size is still 0, try to find lot_size from any instrument with same underlying_symbol
                                    if instrument_key and qty == 0:
                                        logger.info(f"⚠️ Instrument key found but lot_size not available, searching for lot_size from other {symbol} instruments...")
                                        for inst in instruments_data:
                                            if (inst.get('underlying_symbol') == symbol and 
                                                inst.get('segment') == 'NSE_FO' and
                                                inst.get('lot_size') and inst.get('lot_size') > 0):
                                                qty = int(inst.get('lot_size'))
                                                logger.info(f"✅ Found lot_size from another {symbol} instrument: {qty}")
                                                break
                                    
                                    if not instrument_key:
                                        logger.info(f"❌ ERROR: Could not find instrument_key for {option_contract}")
                                        logger.info(f"   Searched for: symbol={symbol}, type={opt_type}, strike={strike_value}, expiry={target_month}/{target_year}")
                                        logger.error(f"Could not find instrument_key for {stock_name} ({option_contract}): symbol={symbol}, type={opt_type}, strike={strike_value}, expiry={target_month}/{target_year}")
                                        
                                        # Debug: Count how many instruments match the symbol and type
                                        symbol_type_matches = [inst for inst in instruments_data 
                                                             if inst.get('underlying_symbol') == symbol 
                                                             and inst.get('instrument_type') == opt_type
                                                             and inst.get('segment') == 'NSE_FO']
                                        logger.info(f"   📊 Found {len(symbol_type_matches)} instruments with symbol={symbol} and type={opt_type}")
                                        
                                        # Show some examples
                                        if symbol_type_matches:
                                            logger.info(f"   Examples (first 5):")
                                            for inst in symbol_type_matches[:5]:
                                                expiry_ms = inst.get('expiry', 0)
                                                if expiry_ms > 1e12:
                                                    expiry_ms = expiry_ms / 1000
                                                expiry_dt = datetime.fromtimestamp(expiry_ms) if expiry_ms else None
                                                logger.info(f"      - Strike: {inst.get('strike_price', 0)}, Expiry: {expiry_dt.strftime('%d %b %Y') if expiry_dt else 'N/A'}, Key: {inst.get('instrument_key', 'N/A')}, Lot Size: {inst.get('lot_size', 'N/A')}")
                                else:
                                    logger.info(f"⚠️ Could not parse month from option contract: {option_contract} (month: {month})")
                                    logger.warning(f"Could not parse month from option contract for {stock_name}: {option_contract} (month: {month})")
                            else:
                                if not found_by_trading_symbol:
                                    logger.info(f"⚠️ Could not parse option contract format: {option_contract}")
                                    logger.warning(f"Could not parse option contract format for {stock_name}: {option_contract}")
                            
                            # Final check: If instrument_key is still None after all searching methods
                            if not instrument_key:
                                logger.info(f"❌ ERROR: Could not find instrument_key for {option_contract} after trying both trading_symbol lookup and format parsing")
                                logger.error(f"Could not find instrument_key for {stock_name} ({option_contract}) - tried trading_symbol lookup and format parsing")
                except Exception as e:
                    logger.info(f"❌ ERROR: Exception finding instrument_key for {option_contract}: {str(e)}")
                    logger.error(f"Exception finding instrument_key for {stock_name} ({option_contract}): {str(e)}", exc_info=True)
                    import traceback
                    traceback.print_exc()
            
            # ====================================================================
            # ACTIVITY 6: Fetch Option LTP (Independent - requires instrument_key)
            # ====================================================================
            # CRITICAL: Fetch option LTP even if candles fail - this is independent
            # CRITICAL: Also try to fetch if we have option_contract but no instrument_key yet
            if instrument_key and vwap_service:
                try:
                    logger.info(f"🔍 Fetching option LTP for {option_contract} using instrument_key: {instrument_key}")
                    quote_data = vwap_service.get_market_quote_by_key(instrument_key)
                    if quote_data and quote_data.get('last_price'):
                        option_ltp = float(quote_data.get('last_price', 0))
                        logger.info(f"✅ Fetched option LTP for {option_contract}: ₹{option_ltp}")
                    else:
                        logger.info(f"⚠️ Could not fetch option LTP for {option_contract} - no quote data returned")
                        logger.info(f"   Quote data: {quote_data}")
                        logger.warning(f"Could not fetch option LTP for {option_contract} (stock: {stock_name}, instrument_key: {instrument_key}) - quote_data: {quote_data}")
                        # Try fallback: use historical candles if available
                        try:
                            candles = vwap_service.get_historical_candles_by_instrument_key(instrument_key, interval="hours/1", days_back=1)
                            if candles and len(candles) > 0:
                                candles.sort(key=lambda x: x.get('timestamp', 0), reverse=True)
                                option_ltp = round(candles[0].get('close', 0), 2)
                                logger.info(f"✅ Fetched option LTP from historical candles: ₹{option_ltp}")
                        except Exception as fallback_error:
                            logger.info(f"⚠️ Fallback LTP fetch also failed: {str(fallback_error)}")
                except Exception as e:
                    logger.info(f"❌ ERROR fetching option LTP for {option_contract}: {str(e)}")
                    logger.error(f"Error fetching option LTP for {option_contract} (stock: {stock_name}, instrument_key: {instrument_key}): {str(e)}", exc_info=True)
            elif option_contract and vwap_service and not instrument_key:
                # FALLBACK: Try to find instrument_key again if we have option_contract but no instrument_key
                # This handles cases where Activity 5 failed due to nested conditions but option_contract exists
                logger.info(f"⚠️ instrument_key is None but option_contract exists: {option_contract}")
                logger.info(f"   Attempting fallback: Retry instrument_key lookup...")
                try:
                    # Try a simpler lookup - search by option contract string directly
                    from pathlib import Path
                    import json as json_lib
                    instruments_file = Path("/home/ubuntu/trademanthan/data/instruments/nse_instruments.json")
                    if instruments_file.exists():
                        with open(instruments_file, 'r') as f:
                            instruments_data = json_lib.load(f)
                        # Search for instrument by trading_symbol matching option_contract
                        for inst in instruments_data:
                            trading_symbol = inst.get('trading_symbol', '')
                            # Try to match option contract format in trading_symbol
                            if option_contract.replace('-', '') in trading_symbol.replace(' ', ''):
                                instrument_key = inst.get('instrument_key')
                                if instrument_key:
                                    logger.info(f"✅ Fallback: Found instrument_key: {instrument_key}")
                                    # Also get qty if available
                                    inst_lot_size = inst.get('lot_size')
                                    if inst_lot_size and inst_lot_size > 0 and qty == 0:
                                        qty = int(inst_lot_size)
                                        logger.info(f"✅ Fallback: Found lot_size: {qty}")
                                    # Now try to fetch option LTP
                                    try:
                                        quote_data = vwap_service.get_market_quote_by_key(instrument_key)
                                        if quote_data and quote_data.get('last_price'):
                                            option_ltp = float(quote_data.get('last_price', 0))
                                            logger.info(f"✅ Fallback: Fetched option LTP: ₹{option_ltp}")
                                    except Exception:
                                        pass
                                    break
                except Exception as fallback_error:
                    logger.info(f"⚠️ Fallback instrument_key lookup failed: {str(fallback_error)}")
                if not instrument_key:
                    logger.info(f"⚠️ Cannot fetch option LTP for {option_contract} - instrument_key is None (fallback also failed)")
                    logger.warning(f"Cannot fetch option LTP for {option_contract} (stock: {stock_name}) - instrument_key is None")
            elif not instrument_key:
                logger.info(f"⚠️ Cannot fetch option LTP for {option_contract if option_contract else 'N/A'} - instrument_key is None")
                if option_contract:
                    logger.warning(f"Cannot fetch option LTP for {option_contract} (stock: {stock_name}) - instrument_key is None")
            elif not vwap_service:
                logger.info(f"⚠️ Cannot fetch option LTP for {option_contract if option_contract else 'N/A'} - vwap_service is None")
                if option_contract:
                    logger.warning(f"Cannot fetch option LTP for {option_contract} (stock: {stock_name}) - vwap_service is None")
            
            # ====================================================================
            # ACTIVITY 7: Fetch Option Candles (Independent - requires instrument_key)
            # ====================================================================
            # NOTE: Candle fetch failure should NOT block option LTP or lot size
            # Candles are used for candle size filter, but option LTP and lot size are critical for trade entry
            if instrument_key and vwap_service:
                try:
                    logger.info(f"🔍 Fetching option candles for {option_contract} using instrument_key: {instrument_key}")
                    option_candles = vwap_service.get_option_daily_candles_current_and_previous(instrument_key)
                    if option_candles:
                        logger.info(f"✅ Fetched option OHLC candles for {option_contract}")
                        if option_candles.get('current_day_candle'):
                            logger.info(f"   Current day candle: {option_candles.get('current_day_candle')}")
                        if option_candles.get('previous_day_candle'):
                            logger.info(f"   Previous day candle: {option_candles.get('previous_day_candle')}")
                    else:
                        logger.info(f"⚠️ Could not fetch option OHLC candles for {option_contract} - returned None (this is OK, will continue with option LTP and lot size)")
                        logger.warning(f"Could not fetch option OHLC candles for {option_contract} (stock: {stock_name}, instrument_key: {instrument_key}) - returned None. Continuing with other data.")
                except Exception as e:
                    logger.info(f"❌ ERROR fetching option OHLC candles for {option_contract}: {str(e)} (this is OK, will continue with option LTP and lot size)")
                    logger.error(f"Error fetching option OHLC candles for {option_contract} (stock: {stock_name}, instrument_key: {instrument_key}): {str(e)}. Continuing with other data.", exc_info=True)
            elif not instrument_key:
                logger.info(f"⚠️ Cannot fetch option candles for {option_contract} - instrument_key is None")
                logger.warning(f"Cannot fetch option candles for {option_contract} (stock: {stock_name}) - instrument_key is None")
            elif not vwap_service:
                logger.info(f"⚠️ Cannot fetch option candles for {option_contract} - vwap_service is None")
                logger.warning(f"Cannot fetch option candles for {option_contract} (stock: {stock_name}) - vwap_service is None")
            
            # ====================================================================
            # ACTIVITY 8: Fetch Previous Hour VWAP (Independent)
            # ====================================================================
            if vwap_service and stock_name:
                try:
                    prev_vwap_data = vwap_service.get_stock_vwap_for_previous_hour(stock_name, reference_time=triggered_datetime)
                    if prev_vwap_data:
                        stock_vwap_previous_hour = prev_vwap_data.get('vwap')
                        stock_vwap_previous_hour_time = prev_vwap_data.get('time')
                        logger.info(f"✅ Fetched previous hour VWAP for {stock_name}: ₹{stock_vwap_previous_hour:.2f} at {stock_vwap_previous_hour_time.strftime('%H:%M:%S')}")
                    else:
                        logger.info(f"⚠️ Could not fetch previous hour VWAP for {stock_name}")
                except Exception as e:
                    logger.info(f"⚠️ Error fetching previous hour VWAP for {stock_name}: {str(e)}")
                
            # ====================================================================
            # CREATE ENRICHED STOCK DATA (Always created with whatever data we have)
            # ====================================================================
            # Check if enrichment is truly failed: option_contract found but instrument_key missing
            # This is critical because without instrument_key, we can't fetch option LTP or candles
            enrichment_failed = False
            enrichment_error_msg = None
            
            if option_contract and not instrument_key:
                enrichment_failed = True
                enrichment_error_msg = f"Could not find instrument_key for option contract {option_contract}"
                logger.info(f"❌ ENRICHMENT FAILED for {stock_name}: {enrichment_error_msg}")
            
            # ALWAYS create enriched_stock, regardless of enrichment success/failure
            # This ensures stocks are saved even if enrichment partially fails
            enriched_stock = {
                "stock_name": stock_name,
                "trigger_price": trigger_price,
                "last_traded_price": stock_ltp,
                "stock_vwap": stock_vwap,
                "stock_vwap_previous_hour": stock_vwap_previous_hour,
                "stock_vwap_previous_hour_time": stock_vwap_previous_hour_time,
                "option_type": forced_option_type,
                "option_contract": option_contract or "",
                "otm1_strike": option_strike,
                "option_ltp": option_ltp,
                "option_vwap": 0.0,  # Not used
                "qty": qty,
                "instrument_key": instrument_key,
                "option_candles": option_candles,
                "_enrichment_failed": enrichment_failed,
                "_enrichment_error": enrichment_error_msg
            }
            
            enriched_stocks.append(enriched_stock)
            enrichment_successful = True
            
            # Log what we got
            if option_contract:
                logger.info(f"✅ Enriched stock: {stock_name} - LTP: ₹{stock_ltp}, Option: {option_contract}, Qty: {qty}, Instrument Key: {instrument_key or 'N/A'}")
            else:
                logger.info(f"⚠️ Partial data for: {stock_name} - LTP: ₹{stock_ltp}, Option: N/A")
        
        processed_data["stocks"] = enriched_stocks
        logger.info(f"Successfully processed {len(enriched_stocks)} stocks")
        
        # ====================================================================
        # STOCK RANKING & SELECTION (If too many stocks)
        # ====================================================================
        MAX_STOCKS_PER_ALERT = 15  # Maximum stocks to enter per alert
        
        if len(enriched_stocks) > MAX_STOCKS_PER_ALERT:
            logger.info(f"\n📊 TOO MANY STOCKS ({len(enriched_stocks)}) - Applying ranking to select best {MAX_STOCKS_PER_ALERT}")
            
            # Import ranker
            try:
                from services.stock_ranker import rank_and_select_stocks
                
                # Rank and select top stocks
                selected_stocks, summary = rank_and_select_stocks(
                    enriched_stocks, 
                    max_stocks=MAX_STOCKS_PER_ALERT,
                    alert_type=forced_option_type
                )
                
                logger.info(f"✅ RANKING COMPLETE:")
                logger.info(f"   • Total Available: {summary['total_available']}")
                logger.info(f"   • Selected: {summary['total_selected']}")
                logger.info(f"   • Rejected: {summary['total_rejected']}")
                logger.info(f"   • Avg Score: {summary['avg_score']}")
                logger.info(f"   • Score Range: {summary['min_score']}-{summary['max_score']}")
                
                # Replace stocks with selected ones
                enriched_stocks = selected_stocks
                processed_data["stocks"] = selected_stocks
                
            except ImportError as e:
                logger.info(f"⚠️ Stock ranker not available, using all stocks: {str(e)}")
        else:
            logger.info(f"✅ Stock count ({len(enriched_stocks)}) within limit ({MAX_STOCKS_PER_ALERT}), using all stocks")
        
        # Get current date for grouping
        current_date = trading_date.strftime('%Y-%m-%d')
        
        # Determine which data store to use
        target_data = bullish_data if is_bullish else bearish_data
        data_type = "Bullish" if is_bullish else "Bearish"
        
        # Check if this is a new date - if so, clear old data
        # IMPORTANT: Only clear if it's ACTUALLY a different day, not just different time
        if target_data["date"] != current_date:
            logger.info(f"📅 New trading date detected for {data_type}: {current_date} (previous: {target_data['date']})")
            target_data["date"] = current_date
            target_data["alerts"] = []
            logger.info(f"   Cleared old alerts from previous date")
        else:
            logger.info(f"📅 Same trading date ({current_date}), appending to existing alerts")
        
        # Check index trends at the time of alert
        # Index trends determine trade entry, not alert display
        index_trends = vwap_service.check_index_trends()
        nifty_trend = index_trends.get("nifty_trend", "unknown")
        banknifty_trend = index_trends.get("banknifty_trend", "unknown")
        
        # Check if time is at or after 3:00 PM - NO NEW TRADES after this time
        alert_hour = triggered_datetime.hour
        alert_minute = triggered_datetime.minute
        is_after_3_00pm = (alert_hour > 15) or (alert_hour == 15 and alert_minute >= 0)
        
        # TEMPORARY EXCEPTION: Allow 3:15 PM alerts today only (January 12, 2026)
        # This is a one-time exception for today's 3:15 PM alerts
        import pytz
        ist = pytz.timezone('Asia/Kolkata')
        today = datetime.now(ist).date()
        is_3_15_alert = (alert_hour == 15 and alert_minute == 15)
        is_today = (triggered_datetime.date() == today)
        is_jan_12_2026 = (today == datetime(2026, 1, 12).date())
        
        # Override is_after_3_00pm for 3:15 PM alerts today only
        if is_3_15_alert and is_today and is_jan_12_2026:
            is_after_3_00pm = False
            logger.info(f"⚠️ TEMPORARY EXCEPTION: Allowing 3:15 PM alert today (January 12, 2026) - Time check bypassed")
        
        # Special handling flag for 10:15 AM alerts (first alert of the day)
        # NOTE: Historically we skipped VWAP slope and candle size filters for 10:15 alerts.
        # This behaviour has been CHANGED: 10:15 alerts will now go through the same
        # candle size filter as other alerts so they are not "skipped by design".
        # We still keep the flag for logging/analysis if needed.
        is_10_15_alert = (alert_hour == 10 and alert_minute == 15)
        
        if is_after_3_00pm:
            logger.info(f"🚫 ALERT TIME {triggered_at_display} is at or after 3:00 PM - NO NEW TRADES ALLOWED")
        
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
                logger.info(f"✅ BULLISH ALERT: Both indices bullish - Index check PASSED")
            elif both_bearish:
                # Both indices bearish → bullish alerts cannot enter
                can_enter_trade_by_index = False
                logger.info(f"⚠️ BULLISH ALERT: Both indices bearish - Only bearish alerts allowed - NO TRADE")
            elif opposite_directions:
                # Indices in opposite directions → no trade
                can_enter_trade_by_index = False
                logger.info(f"⚠️ BULLISH ALERT: Indices in opposite directions (NIFTY: {nifty_trend}, BANKNIFTY: {banknifty_trend}) - NO TRADE")
        elif is_bearish:
            # Bearish alert
            if both_bullish:
                # Both indices bullish → bearish alerts can enter
                can_enter_trade_by_index = True
                logger.info(f"✅ BEARISH ALERT: Both indices bullish - Index check PASSED")
            elif both_bearish:
                # Both indices bearish → bearish alerts can enter
                can_enter_trade_by_index = True
                logger.info(f"✅ BEARISH ALERT: Both indices bearish - Index check PASSED")
            elif opposite_directions:
                # Indices in opposite directions → no trade
                can_enter_trade_by_index = False
                logger.info(f"⚠️ BEARISH ALERT: Indices in opposite directions (NIFTY: {nifty_trend}, BANKNIFTY: {banknifty_trend}) - NO TRADE")
        
        # Save each stock to database
        # CRITICAL: Always save at minimum stock_name and alert_time, even if enrichment failed
        saved_count = 0
        failed_count = 0
        SL_LOSS_TARGET = 3100.0  # Target loss for stop loss trigger
        
        # Safely get stocks_to_save, handling case where processed_data might be None
        stocks_to_save = []
        if processed_data and isinstance(processed_data, dict):
            stocks_to_save = processed_data.get('stocks', [])
        elif enriched_stocks:
            # Fallback: Use enriched_stocks if processed_data is not available
            stocks_to_save = enriched_stocks
        
        # Ensure stocks_to_save is a list and filter out None values
        if not isinstance(stocks_to_save, list):
            stocks_to_save = []
        stocks_to_save = [s for s in stocks_to_save if s is not None and isinstance(s, dict)]
        
        logger.info(f"\n💾 Saving {len(stocks_to_save)} stocks to database...")
        
        if len(stocks_to_save) == 0:
            logger.info("⚠️ WARNING: No stocks to save! Webhook payload may be empty or malformed.")
            if data and isinstance(data, dict):
                logger.info(f"   Original data keys: {list(data.keys())}")
                logger.info(f"   Stocks field type: {type(data.get('stocks'))}")
                logger.info(f"   Stocks field value: {data.get('stocks')}")
            if processed_data and isinstance(processed_data, dict):
                logger.info(f"   Processed stocks count: {len(processed_data.get('stocks', []))}")
            logger.info(f"   Enriched stocks count: {len(enriched_stocks) if 'enriched_stocks' in locals() else 'N/A'}")
            if data and isinstance(data, dict):
                logger.warning(f"No stocks found in webhook payload. Data: {json_module.dumps(data, indent=2)}")
            processed_stocks_count = len(processed_data.get('stocks', [])) if processed_data and isinstance(processed_data, dict) else 0
            logger.warning(f"Processed stocks: {processed_stocks_count}, Enriched: {len(enriched_stocks) if 'enriched_stocks' in locals() else 'N/A'}")
        
        for stock in stocks_to_save:
            # Validate stock is a dict before accessing
            if not stock or not isinstance(stock, dict):
                logger.warning(f"Skipping invalid stock entry: {stock}")
                continue
                
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
                
                # Initialize saved_candle_size_status early to avoid UnboundLocalError
                # This will be calculated later, but we need it initialized here for the filter logic
                saved_candle_size_status = None
                saved_candle_size_ratio = None
                
                # 1. VWAP SLOPE FILTER - SKIP INITIAL CALCULATION
                # VWAP slope will be calculated in cycle-based scheduler (10:30, 11:15, 12:15, 13:15, 14:15)
                # For webhook alerts, we only store the alert data, VWAP slope will be calculated later
                vwap_slope_reason = "VWAP slope will be calculated in cycle-based scheduler"
                
                # 2. CANDLE SIZE FILTER (Daily candles: current day vs previous day, up to current hour)
                # Candle size is calculated ONLY when stock is received from webhook alert
                # It will NOT be recalculated once status changes from No_Entry
                # SPECIAL HANDLING FOR 10:15 AM ALERTS:
                # - Calculate candle size using previous day candle data (if current day data unavailable)
                # - Store the ratio and status for later cycles
                # - But DO NOT block trade entry at 10:15 AM based on candle size
                # - DO NOT set no_entry_reason = "Candle size" for 10:15 AM alerts
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
                        elif is_10_15_alert and previous_day_candle:
                            # For 10:15 AM alerts: If current day candle not available, use previous day candle data
                            # Compare previous day vs previous-previous day (if available) or use a default comparison
                            # This allows calculation at 10:15 AM even when current day data isn't ready
                            previous_high = previous_day_candle.get('high', 0)
                            previous_low = previous_day_candle.get('low', 0)
                            previous_size = abs(previous_high - previous_low)
                            
                            if previous_size > 0:
                                # At 10:15 AM, we use previous day candle as reference
                                # Since we don't have current day data yet, we'll calculate ratio later in cycles
                                # For now, just note that we have previous day data available
                                candle_size_reason = f"10:15 AM alert: Previous day candle available (High={previous_high:.2f}, Low={previous_low:.2f}, Size={previous_size:.2f}). Candle size will be calculated in next cycle."
                                # Don't set candle_size_passed here - it will be calculated in cycles
                            else:
                                candle_size_reason = "10:15 AM alert: Previous day candle size is zero (cannot calculate ratio)"
                        else:
                            candle_size_reason = "Missing daily candle data"
                    except Exception as candle_error:
                        candle_size_reason = f"Error calculating daily candle size: {str(candle_error)}"
                else:
                    if is_10_15_alert:
                        candle_size_reason = "10:15 AM alert: Option daily candles not available. Candle size will be calculated in next cycle."
                    else:
                        candle_size_reason = "Option daily candles not available"
                
                # Determine trade entry based on:
                # 1. Time check (must be before 3:00 PM)
                # 2. Index trends (must be aligned)
                # 3. VWAP slope >= 45 degrees (calculated in cycle-based scheduler, not here)
                # 4. Current candle size < 7-8x previous candle (calculated here for webhook alerts)
                # 5. Valid option data (option_ltp > 0, lot_size > 0)
                # NOTE: For 10:15 AM alerts, candle size is calculated but NOT used to block entry
                # Candle size will be recalculated and enforced in subsequent cycles (10:30 AM, 11:15 AM, etc.)
                # CRITICAL: Don't block entry if candle size is "Skipped" or not calculated
                if is_10_15_alert:
                    # For 10:15 AM alerts: Calculate candle size but don't block entry
                    # filters_passed = True (don't block based on candle size at 10:15 AM)
                    filters_passed = True
                elif saved_candle_size_status in [None, "Skipped", ""]:
                    # Candle size not calculated or skipped - don't block entry
                    filters_passed = True
                    logger.info(f"ℹ️ {stock_name}: Candle size status is '{saved_candle_size_status}' - Not blocking entry")
                else:
                    # For all other alerts: Apply candle size filter normally
                    filters_passed = candle_size_passed
                
                # Initialize no_entry_reason early (will be set if entry fails)
                no_entry_reason = None
                
                # Check if enrichment failed - this takes priority over other reasons
                if stock.get("_enrichment_failed"):
                    enrichment_error_msg = stock.get("_enrichment_error", "Unknown error")
                    # Store full error message (up to 255 chars to fit database field)
                    # Database field is String(255), so we can store up to 255 chars
                    # Reserve 20 chars for "Enrichment failed: " prefix
                    max_error_length = 255 - len("Enrichment failed: ")
                    if len(enrichment_error_msg) > max_error_length:
                        enrichment_error_msg = enrichment_error_msg[:max_error_length-3] + "..."
                    no_entry_reason = f"Enrichment failed: {enrichment_error_msg}"
                # Check entry conditions in priority order to set appropriate reason
                elif is_after_3_00pm:
                    no_entry_reason = "Time >= 3PM"
                elif not can_enter_trade_by_index:
                    no_entry_reason = "Index alignment"
                elif not filters_passed and not is_10_15_alert:
                    # For non-10:15 alerts: Block entry if candle size filter fails
                    # For 10:15 alerts: Don't set "Candle size" as no_entry_reason
                    no_entry_reason = "Candle size"
                # 10:15 AM alerts: Candle size is calculated but not used to block entry
                
                # CRITICAL: Retry option data fetch if missing but instrument_key exists
                # This handles cases where initial fetch failed but data is actually available
                if (option_ltp_value <= 0 or lot_size <= 0) and stock.get("instrument_key"):
                    logger.info(f"⚠️ Option data missing (LTP: {option_ltp_value}, Qty: {lot_size}), retrying fetch...")
                    try:
                        # Retry option LTP fetch
                        if option_ltp_value <= 0:
                            option_quote = vwap_service.get_market_quote_by_key(stock.get("instrument_key"))
                            if option_quote and option_quote.get('last_price', 0) > 0:
                                option_ltp_value = float(option_quote.get('last_price', 0))
                                logger.info(f"✅ Retry successful: Fetched option LTP: ₹{option_ltp_value}")
                                # Update stock dictionary for consistency
                                stock["option_ltp"] = option_ltp_value
                            else:
                                # Try fallback: use historical candles
                                try:
                                    candles = vwap_service.get_historical_candles_by_instrument_key(stock.get("instrument_key"), interval="hours/1", days_back=1)
                                    if candles and len(candles) > 0:
                                        candles.sort(key=lambda x: x.get('timestamp', 0), reverse=True)
                                        option_ltp_value = round(candles[0].get('close', 0), 2)
                                        logger.info(f"✅ Retry successful: Fetched option LTP from candles: ₹{option_ltp_value}")
                                        stock["option_ltp"] = option_ltp_value
                                except Exception:
                                    pass
                        
                        # Retry lot_size fetch from instruments.json
                        if lot_size <= 0 and stock.get("option_contract"):
                            try:
                                import json
                                instruments_file = "/home/ubuntu/trademanthan/data/instruments/nse_instruments.json"
                                if os.path.exists(instruments_file):
                                    with open(instruments_file, 'r') as f:
                                        instruments_data = json_module.load(f)
                                    
                                    # Parse option contract to get symbol and strike
                                    option_contract = stock.get("option_contract", "")
                                    if option_contract:
                                        # Extract symbol from option contract (e.g., "RELIANCE 28FEB2025 3000 CALL")
                                        parts = option_contract.split()
                                        if len(parts) >= 1:
                                            symbol = parts[0]
                                            # Search for any instrument with same underlying_symbol and get lot_size
                                            for inst in instruments_data:
                                                if (inst.get('underlying_symbol') == symbol and 
                                                    inst.get('segment') == 'NSE_FO' and
                                                    inst.get('lot_size') and inst.get('lot_size') > 0):
                                                    lot_size = int(inst.get('lot_size'))
                                                    logger.info(f"✅ Retry successful: Found lot_size: {lot_size}")
                                                    stock["qty"] = lot_size
                                                    break
                            except Exception as e:
                                logger.info(f"⚠️ Retry lot_size fetch failed: {str(e)}")
                    except Exception as retry_error:
                        logger.info(f"⚠️ Error retrying option data fetch: {str(retry_error)}")
                
                # Check for missing option data AFTER retry
                if option_ltp_value <= 0 or lot_size <= 0:
                    if not no_entry_reason:  # Only set if no other reason was set
                        no_entry_reason = "Missing option data"
                
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
                                logger.info(f"✅ Fetched fresh option LTP at entry: ₹{current_option_ltp:.2f}")
                            else:
                                logger.info(f"⚠️ Could not fetch fresh option LTP, using enrichment value: ₹{current_option_ltp:.2f}")
                        except Exception as ltp_error:
                            logger.info(f"⚠️ Error fetching fresh option LTP: {str(ltp_error)}, using enrichment value: ₹{current_option_ltp:.2f}")
                    
                    qty = lot_size
                    buy_price = current_option_ltp  # Use current LTP fetched at entry moment
                    buy_time = current_time  # Use current system time, not alert time
                    sell_price = None  # BLANK initially - will be updated hourly by market data updater
                    
                    # Stop Loss = 5% lower than the open price of the current candle
                    stop_loss_price = None
                    if option_candles and option_candles.get('current_day_candle'):
                        current_day_candle_open = option_candles.get('current_day_candle', {}).get('open')
                        if current_day_candle_open and current_day_candle_open > 0:
                            # Stop loss = 5% lower than candle open price
                            stop_loss_price = float(current_day_candle_open) * 0.95
                            logger.info(f"✅ Stop Loss set to 5% below current candle open: ₹{stop_loss_price:.2f} (candle open: ₹{current_day_candle_open:.2f})")
                        else:
                            # Fallback: 5% below buy price if candle open not available
                            stop_loss_price = buy_price * 0.95
                            logger.info(f"⚠️ Current day candle open not available, setting SL to 5% below buy price: ₹{stop_loss_price:.2f}")
                    else:
                        # Fallback: 5% below buy price if candles not available
                        stop_loss_price = buy_price * 0.95
                        logger.info(f"⚠️ Current day candle data not available, setting SL to 5% below buy price: ₹{stop_loss_price:.2f}")
                    
                    status = 'bought'  # Trade entered
                    pnl = 0.0
                    entry_time_str = buy_time.strftime('%Y-%m-%d %H:%M:%S IST')
                    alert_time_str = triggered_datetime.strftime('%Y-%m-%d %H:%M:%S IST')
                    logger.info(f"✅ TRADE ENTERED: {stock_name}")
                    logger.info(f"   ⏰ Entry Time: {entry_time_str} (Alert Time: {alert_time_str})")
                    logger.info(f"   📊 Entry Conditions:")
                    logger.info(f"      - Time Check: ✅ Before 3:00 PM ({triggered_at_display})")
                    logger.info(f"      - Index Trends: ✅ Aligned (NIFTY: {nifty_trend}, BANKNIFTY: {banknifty_trend})")
                    logger.info(f"      - VWAP Slope: ✅ {vwap_slope_reason}")
                    logger.info(f"      - Candle Size: ✅ {candle_size_reason}")
                    logger.info(f"      - Option Data: ✅ Valid (LTP at entry: ₹{buy_price:.2f}, Qty: {lot_size})")
                    logger.info(f"   💰 Trade Details:")
                    logger.info(f"      - Buy Price: ₹{buy_price:.2f} (fetched at entry moment)")
                    logger.info(f"      - Quantity: {qty}")
                    logger.info(f"      - Stop Loss: ₹{stop_loss_price:.2f} (previous candle low)")
                    logger.info(f"      - Stock LTP: ₹{stock_ltp:.2f}")
                    logger.info(f"      - Stock VWAP: ₹{stock_vwap:.2f}")
                    prev_vwap_str = f"₹{stock_vwap_prev:.2f}" if stock_vwap_prev else "N/A"
                    logger.info(f"      - Stock VWAP (Previous Hour): {prev_vwap_str}")
                    logger.info(f"      - Option Contract: {stock.get('option_contract', 'N/A')}")
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
                            # Fallback: 5% below buy price if candle data not available
                            stop_loss_price = buy_price * 0.95 if buy_price else 0.05
                    else:
                        # Fallback: 5% below buy price if candle data not available
                        stop_loss_price = buy_price * 0.95 if buy_price else 0.05
                    
                    status = 'no_entry'  # Trade not entered
                    pnl = None  # No P&L since trade wasn't executed
                    # no_entry_reason already set above based on which condition failed
                    
                    # Log reason for no entry with complete trade setup
                    no_entry_time_str = triggered_datetime.strftime('%Y-%m-%d %H:%M:%S IST')
                    if is_after_3_00pm:
                        # Already set above, but ensure it's set
                        if not no_entry_reason:
                            no_entry_reason = "Time >= 3PM"
                        logger.info(f"🚫 NO ENTRY: {stock_name} - Alert time {triggered_at_display} is at or after 3:00 PM")
                        logger.info(f"   ⏰ Decision Time: {no_entry_time_str}")
                        logger.info(f"   📊 Entry Conditions:")
                        logger.info(f"      - Time Check: ❌ At or after 3:00 PM ({triggered_at_display})")
                        logger.info(f"      - Index Trends: {'✅' if can_enter_trade_by_index else '❌'} {'Aligned' if can_enter_trade_by_index else f'Not Aligned (NIFTY: {nifty_trend}, BANKNIFTY: {banknifty_trend})'}")
                        logger.info(f"      - VWAP Slope: {'✅' if vwap_slope_passed else '❌'} {vwap_slope_reason}")
                        logger.info(f"      - Candle Size: {'✅' if candle_size_passed else '❌'} {candle_size_reason}")
                        logger.info(f"      - Option Data: {'✅' if option_ltp_value > 0 and lot_size > 0 else '❌'} {'Valid' if option_ltp_value > 0 and lot_size > 0 else f'Missing (LTP: {option_ltp_value}, Qty: {lot_size})'}")
                        logger.info(f"   💰 Would have been: Buy ₹{buy_price}, Qty: {qty}, SL: ₹{stop_loss_price} (not executed)")
                        logger.info(f"🚫 NO ENTRY DECISION: {stock_name} | Time: {no_entry_time_str} | Reason: Time >= 3:00 PM")
                    elif not can_enter_trade_by_index:
                        # Already set above, but ensure it's set
                        if not no_entry_reason:
                            no_entry_reason = "Index alignment"
                        logger.info(f"⚠️ NO ENTRY: {stock_name} - Index trends not aligned (NIFTY: {nifty_trend}, BANKNIFTY: {banknifty_trend})")
                        logger.info(f"   ⏰ Decision Time: {no_entry_time_str}")
                        logger.info(f"   📊 Entry Conditions:")
                        logger.info(f"      - Time Check: ✅ Before 3:00 PM ({triggered_at_display})")
                        logger.info(f"      - Index Trends: ❌ Not Aligned (NIFTY: {nifty_trend}, BANKNIFTY: {banknifty_trend})")
                        logger.info(f"      - VWAP Slope: {'✅' if vwap_slope_passed else '❌'} {vwap_slope_reason}")
                        logger.info(f"      - Candle Size: {'✅' if candle_size_passed else '❌'} {candle_size_reason}")
                        logger.info(f"      - Option Data: {'✅' if option_ltp_value > 0 and lot_size > 0 else '❌'} {'Valid' if option_ltp_value > 0 and lot_size > 0 else f'Missing (LTP: {option_ltp_value}, Qty: {lot_size})'}")
                        logger.info(f"   💰 Would have been: Buy ₹{buy_price}, Qty: {qty}, SL: ₹{stop_loss_price} (not executed)")
                        logger.info(f"🚫 NO ENTRY DECISION: {stock_name} | Time: {no_entry_time_str} | Reason: Index trends not aligned (NIFTY={nifty_trend}, BANKNIFTY={banknifty_trend})")
                    elif not filters_passed and not is_10_15_alert:
                        # filters_passed = candle_size_passed for non-10:15 alerts
                        # Already set above, but ensure it's set
                        if not no_entry_reason:
                            no_entry_reason = "Candle size"
                        logger.info(f"🚫 NO ENTRY: {stock_name} - Candle size condition not met")
                        logger.info(f"   ⏰ Decision Time: {no_entry_time_str}")
                        logger.info(f"   📊 Entry Conditions:")
                        logger.info(f"      - Time Check: ✅ Before 3:00 PM ({triggered_at_display})")
                        logger.info(f"      - Index Trends: ✅ Aligned (NIFTY: {nifty_trend}, BANKNIFTY: {banknifty_trend})")
                        logger.info(f"      - VWAP Slope: ✅ {vwap_slope_reason}")
                        logger.info(f"      - Candle Size: ❌ {candle_size_reason}")
                        logger.info(f"      - Option Data: {'✅' if option_ltp_value > 0 and lot_size > 0 else '❌'} {'Valid' if option_ltp_value > 0 and lot_size > 0 else f'Missing (LTP: {option_ltp_value}, Qty: {lot_size})'}")
                        logger.info(f"   💰 Would have been: Buy ₹{buy_price}, Qty: {qty}, SL: ₹{stop_loss_price} (not executed)")
                        logger.info(f"🚫 NO ENTRY DECISION: {stock_name} | Time: {no_entry_time_str} | Reason: {candle_size_reason}")
                    elif option_ltp_value <= 0 or lot_size <= 0:
                        # Already set above, but ensure it's set
                        if not no_entry_reason:
                            no_entry_reason = "Missing option data"
                        logger.info(f"⚠️ NO ENTRY: {stock_name} - Missing option data (option_ltp={option_ltp_value}, qty={lot_size})")
                        logger.info(f"   ⏰ Decision Time: {no_entry_time_str}")
                        logger.info(f"   📊 Entry Conditions:")
                        logger.info(f"      - Time Check: ✅ Before 3:00 PM ({triggered_at_display})")
                        logger.info(f"      - Index Trends: ✅ Aligned (NIFTY: {nifty_trend}, BANKNIFTY: {banknifty_trend})")
                        logger.info(f"      - VWAP Slope: {'✅' if vwap_slope_passed else '❌'} {vwap_slope_reason}")
                        logger.info(f"      - Candle Size: {'✅' if candle_size_passed else '❌'} {candle_size_reason}")
                        logger.info(f"      - Option Data: ❌ Missing (LTP: {option_ltp_value}, Qty: {lot_size})")
                        logger.info(f"   💰 Would have been: Buy ₹{buy_price}, Qty: {qty}, SL: ₹{stop_loss_price} (not executed)")
                        # For missing data, keep qty=0, buy_price=None, stop_loss=None
                        qty = 0
                        buy_price = None
                        stop_loss_price = None
                        logger.info(f"🚫 NO ENTRY DECISION: {stock_name} | Time: {no_entry_time_str} | Reason: Missing option data (LTP={option_ltp_value}, Qty={lot_size})")
                    else:
                        # If we reach here, all conditions passed but entry still failed - set unknown reason
                        if not no_entry_reason:
                            no_entry_reason = "Unknown"
                        logger.info(f"⚠️ NO ENTRY: {stock_name} - Unknown reason")
                        logger.info(f"   ⏰ Decision Time: {no_entry_time_str}")
                        logger.info(f"   📊 Entry Conditions:")
                        logger.info(f"      - Time Check: ✅ Before 3:00 PM ({triggered_at_display})")
                        logger.info(f"      - Index Trends: ✅ Aligned (NIFTY: {nifty_trend}, BANKNIFTY: {banknifty_trend})")
                        logger.info(f"      - VWAP Slope: {'✅' if vwap_slope_passed else '❌'} {vwap_slope_reason}")
                        logger.info(f"      - Candle Size: {'✅' if candle_size_passed else '❌'} {candle_size_reason}")
                        logger.info(f"      - Option Data: ✅ Valid (LTP: ₹{option_ltp_value:.2f}, Qty: {lot_size})")
                        logger.info(f"   💰 Would have been: Buy ₹{buy_price}, Qty: {qty}, SL: ₹{stop_loss_price} (not executed)")
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
                            logger.info(f"⚠️  Setting buy_time to alert_time for {stock_name} (buy_price set but buy_time was None)")
                        else:
                            logger.info(f"ℹ️  Setting buy_time to alert_time for {stock_name} (no_entry trade with buy_price)")
                    elif buy_time != triggered_datetime:
                        # If buy_time is set but different from alert_time, log for debugging
                        logger.info(f"ℹ️  buy_time ({buy_time}) differs from alert_time ({triggered_datetime}) for {stock_name}")
                
                # CRITICAL: Get instrument_key from the stock dictionary, not from a variable
                # This ensures each stock gets its own unique instrument_key
                # IMPORTANT: instrument_key MUST be saved even if subsequent enrichment steps fail
                stock_instrument_key = stock.get("instrument_key")
                if not stock_instrument_key and option_contract:
                    # Fallback: Try to find instrument_key from option_contract if it wasn't set
                    # This ensures we preserve instrument_key even if lookup failed earlier
                    logger.info(f"⚠️ instrument_key not found in stock data for {stock_name}, attempting fallback lookup...")
                    # Note: This is a safety check - instrument_key should already be set in Activity 5
                
                # Extract OHLC data from option_candles
                option_candles_data = stock.get("option_candles") if stock and isinstance(stock, dict) else None
                current_day_candle = option_candles_data.get('current_day_candle') if option_candles_data and isinstance(option_candles_data, dict) else None
                previous_day_candle = option_candles_data.get('previous_day_candle') if option_candles_data and isinstance(option_candles_data, dict) else None
                # Ensure they are dicts, not None
                if current_day_candle is None or not isinstance(current_day_candle, dict):
                    current_day_candle = {}
                if previous_day_candle is None or not isinstance(previous_day_candle, dict):
                    previous_day_candle = {}
                
                # Calculate and save candle size ratio and status (use the same calculation as above)
                # For 10:15 AM alerts: Use previous day candle data if current day data isn't available
                saved_candle_size_ratio = None
                saved_candle_size_status = None
                if option_candles_data and current_day_candle and previous_day_candle and isinstance(current_day_candle, dict) and isinstance(previous_day_candle, dict):
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
                elif is_10_15_alert and previous_day_candle and isinstance(previous_day_candle, dict):
                    # For 10:15 AM alerts: Use previous day candle data
                    # Store previous day candle data for later cycle calculations
                    # At 10:15 AM, we have previous day data but current day data may not be ready yet
                    # Candle size will be recalculated in next cycle (10:30 AM) with current day data
                    try:
                        previous_high = previous_day_candle.get('high', 0)
                        previous_low = previous_day_candle.get('low', 0)
                        previous_size = abs(previous_high - previous_low)
                        
                        if previous_size > 0:
                            # Store that we have previous day data available
                            # Ratio will be calculated in next cycle when current day data is available
                            saved_candle_size_status = "Pending"  # Will be recalculated in next cycle
                            logger.info(f"10:15 AM alert for {stock_name}: Previous day candle data available (Size={previous_size:.2f}). Candle size will be calculated in next cycle.")
                        else:
                            saved_candle_size_status = "Skipped"
                    except Exception as e:
                        logger.warning(f"Error processing previous day candle for 10:15 alert {stock_name}: {str(e)}")
                        saved_candle_size_status = "Pending"
                elif is_10_15_alert:
                    # 10:15 AM alert but no candle data available at all
                    saved_candle_size_status = "Pending"  # Will try to calculate in next cycle
                else:
                    # No candle data available (non-10:15 alerts)
                    saved_candle_size_status = None
                
                # Ensure option_type is set correctly based on alert type if not already set
                option_type_from_stock = stock.get("option_type", "") if stock and isinstance(stock, dict) else ""
                if not option_type_from_stock:
                    # Set option_type based on alert_type (Bearish = PE, Bullish = CE)
                    option_type_from_stock = 'PE' if data_type == 'Bearish' else 'CE'
                    logger.info(f"⚠️ Option type not found in stock data for {stock_name}, setting to {option_type_from_stock} based on alert type {data_type}")
                
                # Safely get scan_name from processed_data (handle case where processed_data might be None)
                scan_name_for_record = ""
                if processed_data and isinstance(processed_data, dict):
                    scan_name_for_record = processed_data.get("scan_name", "")
                elif data and isinstance(data, dict):
                    # Fallback to original data if processed_data is not available
                    scan_name_for_record = data.get("scan_name", "")
                
                # Safely get values from stock dict, handling None case
                if stock and isinstance(stock, dict):
                    stock_ltp_val = stock.get("last_traded_price") or stock.get("trigger_price", 0.0)
                    stock_vwap_val = stock.get("stock_vwap", 0.0)
                    stock_vwap_prev = stock.get("stock_vwap_previous_hour")
                    stock_vwap_prev_time = stock.get("stock_vwap_previous_hour_time")
                    option_contract_val = stock.get("option_contract", "")
                    option_strike_val = stock.get("otm1_strike", 0.0)
                    option_vwap_val = stock.get("option_vwap", 0.0)
                else:
                    stock_ltp_val = 0.0
                    stock_vwap_val = 0.0
                    stock_vwap_prev = None
                    stock_vwap_prev_time = None
                    option_contract_val = ""
                    option_strike_val = 0.0
                    option_vwap_val = 0.0
                
                db_record = IntradayStockOption(
                    alert_time=triggered_datetime,
                    alert_type=data_type,
                    scan_name=scan_name_for_record,
                    stock_name=stock_name,
                    stock_ltp=stock_ltp_val,
                    stock_vwap=stock_vwap_val,
                    stock_vwap_previous_hour=stock_vwap_prev,
                    stock_vwap_previous_hour_time=stock_vwap_prev_time,
                    option_contract=option_contract_val,
                    option_type=option_type_from_stock,
                    option_strike=option_strike_val,
                    option_ltp=option_ltp_value,
                    option_vwap=option_vwap_val,
                    # Option daily OHLC candles (current day vs previous day)
                    # Safely extract candle data, handling None and empty dict cases
                    option_current_candle_open=current_day_candle.get('open') if (current_day_candle and isinstance(current_day_candle, dict)) else None,
                    option_current_candle_high=current_day_candle.get('high') if (current_day_candle and isinstance(current_day_candle, dict)) else None,
                    option_current_candle_low=current_day_candle.get('low') if (current_day_candle and isinstance(current_day_candle, dict)) else None,
                    option_current_candle_close=current_day_candle.get('close') if (current_day_candle and isinstance(current_day_candle, dict)) else None,
                    option_current_candle_time=current_day_candle.get('time') if (current_day_candle and isinstance(current_day_candle, dict)) else None,
                    option_previous_candle_open=previous_day_candle.get('open') if (previous_day_candle and isinstance(previous_day_candle, dict)) else None,
                    option_previous_candle_high=previous_day_candle.get('high') if (previous_day_candle and isinstance(previous_day_candle, dict)) else None,
                    option_previous_candle_low=previous_day_candle.get('low') if (previous_day_candle and isinstance(previous_day_candle, dict)) else None,
                    option_previous_candle_close=previous_day_candle.get('close') if (previous_day_candle and isinstance(previous_day_candle, dict)) else None,
                    option_previous_candle_time=previous_day_candle.get('time') if (previous_day_candle and isinstance(previous_day_candle, dict)) else None,
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
                    pnl=pnl,
                    no_entry_reason=no_entry_reason if status == 'no_entry' else None
                )
                db.add(db_record)
                saved_count += 1
                logger.info(f"   💾 Saved {stock_name} to database (status: {status})")
                
                # ═══════════════════════════════════════════════════════════════
                # CALCULATE VWAP SLOPE AND CANDLE SIZE IMMEDIATELY AFTER SAVE
                # ═══════════════════════════════════════════════════════════════
                # Calculate VWAP slope and candle size right after trade is saved
                # This ensures calculations happen immediately, not waiting for cycle scheduler
                try:
                    from backend.services.vwap_updater import calculate_vwap_slope_for_trade, recalculate_candle_size_for_trade
                    
                    # Flush to ensure trade is in database with ID
                    db.flush()
                    
                    # Calculate VWAP slope (vwap_service is already imported at module level)
                    vwap_slope_calculated = calculate_vwap_slope_for_trade(db_record, db, vwap_service)
                    if vwap_slope_calculated:
                        logger.info(f"   ✅ VWAP slope calculated for {stock_name}")
                    else:
                        logger.info(f"   ⚠️ VWAP slope calculation skipped for {stock_name} (will be calculated in cycle)")
                    
                    # Recalculate candle size if instrument_key is available
                    if db_record.instrument_key:
                        candle_size_calculated = recalculate_candle_size_for_trade(db_record, db, vwap_service)
                        if candle_size_calculated:
                            logger.info(f"   ✅ Candle size recalculated for {stock_name}")
                except Exception as calc_error:
                    logger.warning(f"⚠️ Error calculating VWAP slope/candle size for {stock_name}: {str(calc_error)}")
                    # Don't fail the entire save if calculation fails
                
                # ═══════════════════════════════════════════════════════════════
                # SAVE HISTORICAL MARKET DATA AT WEBHOOK TIME (10:15 AM, etc.)
                # ═══════════════════════════════════════════════════════════════
                # Save historical snapshot when webhook is received
                # This captures the initial market state at alert time
                try:
                    from backend.services.vwap_updater import historical_data_exists
                    
                    # Use triggered_datetime as scan_date (the actual alert time)
                    scan_datetime = triggered_datetime
                    
                    # Check if historical data already exists for this stock at this time
                    if not historical_data_exists(db, stock_name, scan_datetime):
                        # VWAP slope not calculated yet at webhook time (will be calculated in cycle scheduler)
                        # Get all available data from webhook enrichment
                        # Safely get values from stock dict, handling None case
                        if stock and isinstance(stock, dict):
                            stock_vwap_prev = stock.get("stock_vwap_previous_hour")
                            stock_vwap_prev_time = stock.get("stock_vwap_previous_hour_time")
                            option_vwap_val = stock.get("option_vwap", 0.0)
                            option_vwap_value = option_vwap_val if option_vwap_val and option_vwap_val > 0 else None
                            stock_vwap_val = stock.get("stock_vwap", 0.0)
                            stock_vwap_save = stock_vwap_val if stock_vwap_val and stock_vwap_val > 0 else None
                            stock_ltp_val = stock.get("last_traded_price") or stock.get("trigger_price", 0.0)
                            stock_ltp_save = stock_ltp_val if stock_ltp_val and stock_ltp_val > 0 else None
                            option_contract_val = stock.get("option_contract", "")
                        else:
                            stock_vwap_prev = None
                            stock_vwap_prev_time = None
                            option_vwap_value = None
                            stock_vwap_save = None
                            stock_ltp_save = None
                            option_contract_val = ""
                        
                        historical_record = HistoricalMarketData(
                            stock_name=stock_name,
                            stock_vwap=stock_vwap_save,
                            stock_ltp=stock_ltp_save,
                            stock_vwap_previous_hour=stock_vwap_prev if stock_vwap_prev and stock_vwap_prev > 0 else None,
                            stock_vwap_previous_hour_time=stock_vwap_prev_time,
                            vwap_slope_angle=None,
                            vwap_slope_status=None,
                            vwap_slope_direction=None,
                            vwap_slope_time=None,
                            option_contract=option_contract_val,
                            option_instrument_key=stock_instrument_key,
                            option_ltp=option_ltp_value if option_ltp_value > 0 else None,
                            option_vwap=option_vwap_value,
                            scan_date=scan_datetime,
                            scan_time=scan_datetime.strftime('%I:%M %p').lower()
                        )
                        db.add(historical_record)
                        logger.debug(f"📊 Saved historical data for {stock_name} at webhook time {scan_datetime.strftime('%H:%M:%S')}")
                    else:
                        logger.debug(f"⏭️ Skipping duplicate historical data for {stock_name} at {scan_datetime.strftime('%H:%M:%S')} (already exists)")
                except Exception as hist_error:
                    logger.warning(f"⚠️ Failed to save historical data for {stock_name} at webhook time: {str(hist_error)}")
                    # Don't fail webhook processing if historical save fails
                
            except Exception as db_error:
                failed_count += 1
                error_msg = str(db_error)
                logger.info(f"❌ Error saving stock {stock_name} to database: {error_msg}")
                logger.error(f"Database save error for {stock_name}: {error_msg}", exc_info=True)
                import traceback
                traceback.print_exc()
                
                # Try to save with minimal data as last resort
                # Preserve any data that was successfully fetched before the database error
                try:
                    logger.info(f"   🔄 Attempting minimal save for {stock_name}...")
                    # Preserve option_contract and instrument_key if they were found
                    preserved_option_contract = stock.get("option_contract", "") if stock else ""
                    preserved_instrument_key = stock.get("instrument_key") if stock else None
                    preserved_stock_ltp = stock.get("last_traded_price") or stock.get("trigger_price", 0.0) if stock else 0.0
                    preserved_stock_vwap = stock.get("stock_vwap", 0.0) if stock else 0.0
                    preserved_stock_vwap_previous_hour = stock.get("stock_vwap_previous_hour") if stock else None
                    preserved_stock_vwap_previous_hour_time = stock.get("stock_vwap_previous_hour_time") if stock else None
                    
                    # Create a more descriptive error message
                    # Truncate error message to fit in database field (255 chars)
                    max_error_length = 255 - len("Database save failed: ")
                    if len(error_msg) > max_error_length:
                        error_msg = error_msg[:max_error_length-3] + "..."
                    db_error_reason = f"Database save failed: {error_msg}"
                    
                    # Safely get scan_name from processed_data (handle case where processed_data might be None)
                    scan_name_value = "Unknown"
                    if processed_data and isinstance(processed_data, dict):
                        scan_name_value = processed_data.get("scan_name", "Unknown")
                    elif data and isinstance(data, dict):
                        # Fallback to original data if processed_data is not available
                        scan_name_value = data.get("scan_name", "Unknown")
                    
                    minimal_record = IntradayStockOption(
                        alert_time=triggered_datetime,
                        alert_type=data_type,
                        scan_name=scan_name_value,
                        stock_name=stock_name,
                        stock_ltp=preserved_stock_ltp,
                        stock_vwap=preserved_stock_vwap,
                        stock_vwap_previous_hour=preserved_stock_vwap_previous_hour,
                        stock_vwap_previous_hour_time=preserved_stock_vwap_previous_hour_time,
                        option_contract=preserved_option_contract,
                        option_type=forced_option_type,
                        option_strike=stock.get("otm1_strike", 0.0) if stock else 0.0,
                        option_ltp=stock.get("option_ltp", 0.0) if stock else 0.0,
                        option_vwap=0.0,
                        qty=stock.get("qty", 0) if stock else 0,
                        trade_date=trading_date,
                        status='alert_received',  # Minimal status - saved when database save fails
                        buy_price=None,
                        stop_loss=None,
                        sell_price=None,
                        buy_time=None,
                        exit_reason=None,
                        pnl=None,
                        instrument_key=preserved_instrument_key,
                        no_entry_reason=db_error_reason  # More descriptive reason: database save error
                    )
                    db.add(minimal_record)
                    saved_count += 1
                    logger.info(f"   ✅ Minimal save successful for {stock_name}")
                    
                    # ═══════════════════════════════════════════════════════════════
                    # CALCULATE VWAP SLOPE AND CANDLE SIZE FOR MINIMAL RECORD
                    # ═══════════════════════════════════════════════════════════════
                    # Even for minimal records, try to calculate VWAP slope and candle size
                    # This ensures these values are populated even if the initial save failed
                    try:
                        from backend.services.vwap_updater import calculate_vwap_slope_for_trade, recalculate_candle_size_for_trade
                        
                        # Flush to ensure minimal_record is in database with ID
                        db.flush()
                        
                        # Calculate VWAP slope for minimal record
                        vwap_slope_calculated = calculate_vwap_slope_for_trade(minimal_record, db, vwap_service)
                        if vwap_slope_calculated:
                            logger.info(f"   ✅ VWAP slope calculated for minimal record {stock_name}")
                        else:
                            logger.info(f"   ⚠️ VWAP slope calculation skipped for minimal record {stock_name} (will be calculated in cycle)")
                        
                        # Recalculate candle size if instrument_key is available
                        if minimal_record.instrument_key:
                            candle_size_calculated = recalculate_candle_size_for_trade(minimal_record, db, vwap_service)
                            if candle_size_calculated:
                                logger.info(f"   ✅ Candle size recalculated for minimal record {stock_name}")
                    except Exception as calc_error:
                        logger.warning(f"⚠️ Error calculating VWAP slope/candle size for minimal record {stock_name}: {str(calc_error)}")
                        # Don't fail the minimal save if calculation fails
                    
                    # Save historical data even for minimal records
                    try:
                        from backend.services.vwap_updater import historical_data_exists
                        scan_datetime = triggered_datetime
                        if not historical_data_exists(db, stock_name, scan_datetime):
                            # Minimal record - VWAP slope not available, but try to get any available data
                            if stock and isinstance(stock, dict):
                                stock_vwap_prev = stock.get("stock_vwap_previous_hour")
                                stock_vwap_prev_time = stock.get("stock_vwap_previous_hour_time")
                                option_vwap_val = stock.get("option_vwap", 0.0)
                                option_vwap_value = option_vwap_val if option_vwap_val and option_vwap_val > 0 else None
                                stock_ltp_val = stock.get("trigger_price", 0.0)
                                stock_ltp_save = stock_ltp_val if stock_ltp_val and stock_ltp_val > 0 else None
                            else:
                                stock_vwap_prev = None
                                stock_vwap_prev_time = None
                                option_vwap_value = None
                                stock_ltp_save = None
                            
                            historical_record = HistoricalMarketData(
                                stock_name=stock_name,
                                stock_vwap=None,
                                stock_ltp=stock_ltp_save,
                                stock_vwap_previous_hour=stock_vwap_prev if stock_vwap_prev and stock_vwap_prev > 0 else None,
                                stock_vwap_previous_hour_time=stock_vwap_prev_time,
                                vwap_slope_angle=None,
                                vwap_slope_status=None,
                                vwap_slope_direction=None,
                                vwap_slope_time=None,
                                option_contract="",
                                option_instrument_key=None,
                                option_ltp=None,
                                option_vwap=option_vwap_value,
                                scan_date=scan_datetime,
                                scan_time=scan_datetime.strftime('%I:%M %p').lower()
                            )
                            db.add(historical_record)
                            logger.debug(f"📊 Saved minimal historical data for {stock_name} at webhook time {scan_datetime.strftime('%H:%M:%S')}")
                    except Exception as hist_error:
                        logger.warning(f"⚠️ Failed to save minimal historical data for {stock_name}: {str(hist_error)}")
                except Exception as minimal_error:
                    logger.info(f"   ❌ Even minimal save failed for {stock_name}: {str(minimal_error)}")
        
        # Commit all database records
        try:
            db.commit()
            logger.info(f"\n✅ DATABASE COMMIT SUCCESSFUL")
            logger.info(f"   • Total Stocks Processed: {len(processed_data.get('stocks', []))}")
            logger.info(f"   • Saved to DB: {saved_count} stocks")
            if failed_count > 0:
                logger.info(f"   • Failed: {failed_count} stocks")
            logger.info(f"   • Alert Type: {data_type}")
            logger.info(f"   • Alert Time: {triggered_at_str}")
            logger.info(f"\n📊 ENTRY FILTER SUMMARY:")
            logger.info(f"   • VWAP Slope Filter: >= 45 degrees")
            logger.info(f"   • Candle Size Filter: Current candle < 7.5× previous candle")
            logger.info(f"   • Stocks that passed both filters: Check 'bought' status above")
            logger.info(f"   • Stocks rejected: Check 'no_entry' with filter reasons")
        except Exception as commit_error:
            logger.info(f"\n❌ DATABASE COMMIT FAILED: {str(commit_error)}")
            logger.info(f"   • Attempted to save: {saved_count} stocks")
            logger.info(f"   • Rolling back transaction...")
            db.rollback()
            
            # Log all stock names that were in this webhook for recovery
            logger.info(f"\n⚠️ LOST ALERT - Stock names for manual recovery:")
            for stock in processed_data.get("stocks", []):
                logger.info(f"   - {stock.get('stock_name', 'UNKNOWN')}: {stock.get('trigger_price', 0.0)}")
            
            raise HTTPException(
                status_code=500,
                detail=f"Database commit failed: {str(commit_error)}"
            )
        
        # Add this alert to the beginning of the list (newest first) - in-memory cache
        target_data["alerts"].insert(0, processed_data)
        
        # Keep only last 50 alerts per section to prevent memory issues
        target_data["alerts"] = target_data["alerts"][:50]
        
        logger.info(f"Stored {data_type} alert in memory. Total {data_type} alerts for {current_date}: {len(target_data['alerts'])}")
        
        # Save to file as backup
        data_dir = os.path.join(os.path.dirname(__file__), "..", "scan_data")
        os.makedirs(data_dir, exist_ok=True)
        
        # Helper function to serialize datetime objects
        def json_serializer(obj):
            if isinstance(obj, datetime):
                return obj.isoformat()
            raise TypeError(f"Type {type(obj)} not serializable")
        
        # Save bullish and bearish data separately
        try:
            with open(os.path.join(data_dir, "bullish_data.json"), "w") as f:
                json_module.dump(bullish_data, f, indent=2, default=json_serializer)
        except Exception as save_error:
            logger.warning(f"Failed to save bullish_data.json: {str(save_error)}")
        
        try:
            with open(os.path.join(data_dir, "bearish_data.json"), "w") as f:
                json_module.dump(bearish_data, f, indent=2, default=json_serializer)
        except Exception as save_error:
            logger.warning(f"Failed to save bearish_data.json: {str(save_error)}")
        
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
        logger.info(f"❌ CRITICAL ERROR processing webhook: {str(e)}")
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
    """Manually start Scan ST1 Algo Scheduler (replaces all old schedulers)"""
    try:
        from backend.services.scan_st1_algo import start_scan_st1_algo, scan_st1_algo_scheduler
        
        # Check if scan_st1_algo is already running
        if scan_st1_algo_scheduler and scan_st1_algo_scheduler.is_running and scan_st1_algo_scheduler.scheduler.running:
            return {
                "success": True,
                "message": "Scan ST1 Algo Scheduler is already running",
                "jobs_count": len(scan_st1_algo_scheduler.scheduler.get_jobs()),
                "timestamp": datetime.now().isoformat()
            }
        
        # Stop first if it exists but is in a bad state
        if scan_st1_algo_scheduler and scan_st1_algo_scheduler.scheduler:
            try:
                if scan_st1_algo_scheduler.scheduler.running:
                    scan_st1_algo_scheduler.stop()
            except:
                pass
        
        # Start the unified scheduler
        start_scan_st1_algo()
        jobs_count = len(scan_st1_algo_scheduler.scheduler.get_jobs())
        
        logger.info(f"✅ Scan ST1 Algo Scheduler manually started with {jobs_count} jobs")
        
        return {
            "success": True,
            "message": "Scan ST1 Algo Scheduler started successfully",
            "jobs_count": jobs_count,
            "timestamp": datetime.now().isoformat()
        }
        
    except Exception as e:
        logger.error(f"Error starting Scan ST1 Algo Scheduler: {e}")
        import traceback
        traceback.print_exc()
        return {
            "success": False,
            "error": str(e),
            "timestamp": datetime.now().isoformat()
        }


@router.post("/manual-health-check")
async def manual_health_check(db: Session = Depends(get_db)):
    """Manually trigger health check job - runs immediately"""
    try:
        from backend.services.health_monitor import health_monitor
        
        logger.info("🔧 Manual trigger: Running health check NOW")
        logger.info("Health monitor available: %s", health_monitor is not None)
        
        # Force flush immediately after logging
        for handler in logging.getLogger().handlers:
            if hasattr(handler, 'flush'):
                handler.flush()
        
        # Call the health check function directly
        if health_monitor and hasattr(health_monitor, 'perform_health_check'):
            logger.info("Executing health check...")
            # Force flush
            for handler in logging.getLogger().handlers:
                if hasattr(handler, 'flush'):
                    handler.flush()
            health_monitor.perform_health_check()
            logger.info("Health check execution completed")
            # Force flush after completion
            for handler in logging.getLogger().handlers:
                if hasattr(handler, 'flush'):
                    handler.flush()
            
            # Get failure counts
            result = {
                "success": True,
                "message": "Health check completed successfully",
                "webhook_failures": health_monitor.webhook_failures,
                "api_token_failures": health_monitor.api_token_failures,
                "database_failures": health_monitor.database_failures,
                "timestamp": datetime.now().isoformat()
            }
            
            # Check if any issues were found
            if (health_monitor.webhook_failures > 0 or 
                health_monitor.api_token_failures > 0 or 
                health_monitor.database_failures > 0):
                result["has_issues"] = True
                result["message"] = "Health check completed with issues detected"
            else:
                result["has_issues"] = False
            
            return result
        else:
            return {
                "success": False,
                "message": "Health monitor not available",
                "timestamp": datetime.now().isoformat()
            }
        
    except Exception as e:
        logger.error(f"Error in manual health check: {e}")
        import traceback
        traceback.print_exc()
        return {
            "success": False,
            "error": str(e),
            "timestamp": datetime.now().isoformat()
        }

@router.post("/test-scan-st1-algo-log")
async def test_scan_st1_algo_log():
    """Test endpoint to write a log entry to scan_st1_algo.log"""
    try:
        from backend.services.scan_st1_algo import logger as scan_st1_logger
        import datetime as dt
        
        test_message = f"🧪 TEST LOG ENTRY - {dt.datetime.now().strftime('%Y-%m-%d %H:%M:%S')} - Scan ST1 Algo Logger Test"
        scan_st1_logger.info(test_message)
        
        # Force flush
        for handler in scan_st1_logger.handlers:
            if hasattr(handler, 'flush'):
                handler.flush()
                if hasattr(handler, 'stream') and hasattr(handler.stream, 'fileno'):
                    try:
                        import os
                        os.fsync(handler.stream.fileno())
                    except (OSError, AttributeError):
                        pass
        
        return {
            "success": True,
            "message": "Test log entry written to scan_st1_algo.log",
            "test_message": test_message,
            "timestamp": dt.datetime.now().isoformat()
        }
    except Exception as e:
        import traceback
        return {
            "success": False,
            "error": str(e),
            "traceback": traceback.format_exc(),
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

@router.post("/recalculate-vwap-slope-today")
async def recalculate_vwap_slope_today(db: Session = Depends(get_db)):
    """
    Recalculate VWAP slope for ALL today's trades that are missing it
    This is a backfill endpoint to fix missing VWAP slope calculations
    Useful when cycles didn't run or calculations failed
    """
    try:
        from backend.services.vwap_updater import calculate_vwap_slope_for_trade
        from backend.services.upstox_service import upstox_service
        import pytz
        from datetime import timedelta
        
        ist = pytz.timezone('Asia/Kolkata')
        now = datetime.now(ist)
        today = now.replace(hour=0, minute=0, second=0, microsecond=0)
        
        logger.info(f"🔄 Backfill: Recalculating VWAP slope for today's trades")
        
        # Get all trades for today that are missing VWAP slope
        trades_to_fix = db.query(IntradayStockOption).filter(
            IntradayStockOption.trade_date >= today,
            IntradayStockOption.trade_date < today + timedelta(days=1),
            or_(
                IntradayStockOption.vwap_slope_angle.is_(None),
                IntradayStockOption.vwap_slope_status.is_(None)
            )
        ).all()
        
        if not trades_to_fix:
            return {
                "success": True,
                "message": "All trades already have VWAP slope calculated",
                "processed_count": 0,
                "timestamp": now.isoformat()
            }
        
        processed_count = 0
        success_count = 0
        failed_count = 0
        
        for trade in trades_to_fix:
            try:
                stock_name = trade.stock_name
                logger.info(f"🔄 Calculating VWAP slope for {stock_name} (alert_time: {trade.alert_time})")
                
                # Use the calculate_vwap_slope_for_trade function
                result = calculate_vwap_slope_for_trade(trade, db, upstox_service)
                
                if result:
                    db.commit()
                    success_count += 1
                    logger.info(f"✅ Successfully calculated VWAP slope for {stock_name}")
                else:
                    failed_count += 1
                    logger.warning(f"⚠️ Failed to calculate VWAP slope for {stock_name}")
                
                processed_count += 1
            except Exception as e:
                failed_count += 1
                logger.error(f"❌ Error calculating VWAP slope for {trade.stock_name}: {str(e)}")
                import traceback
                logger.error(traceback.format_exc())
                db.rollback()
        
        return {
            "success": True,
            "message": f"VWAP slope recalculation completed",
            "total_trades": len(trades_to_fix),
            "processed_count": processed_count,
            "success_count": success_count,
            "failed_count": failed_count,
            "timestamp": now.isoformat()
        }
        
    except Exception as e:
        logger.error(f"Error in VWAP slope backfill: {e}")
        import traceback
        traceback.print_exc()
        return {
            "success": False,
            "error": str(e),
            "timestamp": datetime.now().isoformat()
        }

@router.post("/recalculate-all-today")
async def recalculate_all_today_trades(db: Session = Depends(get_db)):
    """
    Recalculate VWAP slope and candle size for ALL today's trades
    This processes trades regardless of status or alert_time
    Useful for fixing missing VWAP slope/candle size data
    """
    try:
        from backend.services.vwap_updater import calculate_vwap_slope_for_cycle
        from backend.services.upstox_service import upstox_service
        import pytz
        from datetime import timedelta
        
        ist = pytz.timezone('Asia/Kolkata')
        now = datetime.now(ist)
        today = now.replace(hour=0, minute=0, second=0, microsecond=0)
        
        logger.info(f"🔄 Manual trigger: Recalculating VWAP slope and candle size for ALL today's trades")
        
        # Get all trades for today
        all_trades = db.query(IntradayStockOption).filter(
            IntradayStockOption.trade_date >= today,
            IntradayStockOption.trade_date < today + timedelta(days=1)
        ).all()
        
        if not all_trades:
            return {
                "success": True,
                "message": "No trades found for today",
                "processed_count": 0,
                "timestamp": now.isoformat()
            }
        
        processed_count = 0
        vwap_slope_updated = 0
        candle_size_updated = 0
        
        for trade in all_trades:
            try:
                stock_name = trade.stock_name
                updated = False
                
                # 0. Try to get instrument_key first if missing but option_contract exists
                if not trade.instrument_key and trade.option_contract:
                    try:
                        from pathlib import Path
                        import json as json_lib
                        
                        instruments_path = Path(__file__).parent.parent.parent / "instruments.json"
                        if instruments_path.exists():
                            with open(instruments_path, 'r') as f:
                                instruments_data = json_lib.load(f)
                                
                            for instrument in instruments_data:
                                if instrument.get('symbol') == trade.option_contract:
                                    trade.instrument_key = instrument.get('instrument_key')
                                    logger.info(f"✅ Found instrument_key for {stock_name}: {trade.instrument_key}")
                                    updated = True
                                    break
                    except Exception as inst_error:
                        logger.warning(f"Error fetching instrument_key for {stock_name}: {str(inst_error)}")
                
                # 1. Calculate VWAP slope if missing
                if trade.vwap_slope_angle is None or trade.vwap_slope_status is None:
                    # Get current stock VWAP
                    stock_data = upstox_service.get_stock_ltp_and_vwap(stock_name)
                    if stock_data:
                        current_vwap = stock_data.get('vwap', 0)
                        
                        # Get previous hour VWAP
                        # Pass alert_time as reference_time so previous hour is calculated correctly
                        prev_vwap_data = upstox_service.get_stock_vwap_for_previous_hour(stock_name, reference_time=trade.alert_time)
                        if prev_vwap_data and current_vwap > 0:
                            prev_vwap = prev_vwap_data.get('vwap', 0)
                            prev_vwap_time = prev_vwap_data.get('time')
                            
                            if prev_vwap > 0 and prev_vwap_time:
                                # Calculate VWAP slope
                                slope_result = upstox_service.vwap_slope(
                                    vwap1=prev_vwap,
                                    time1=prev_vwap_time,
                                    vwap2=current_vwap,
                                    time2=now
                                )
                                
                                if isinstance(slope_result, dict):
                                    trade.vwap_slope_status = slope_result.get("status", "No")
                                    trade.vwap_slope_angle = slope_result.get("angle", 0.0)
                                    trade.vwap_slope_direction = slope_result.get("direction", "flat")
                                else:
                                    trade.vwap_slope_status = slope_result if isinstance(slope_result, str) else "No"
                                    trade.vwap_slope_angle = 0.0
                                    trade.vwap_slope_direction = "flat"
                                
                                trade.stock_vwap = current_vwap
                                trade.stock_vwap_previous_hour = prev_vwap
                                trade.stock_vwap_previous_hour_time = prev_vwap_time
                                trade.vwap_slope_time = now
                                vwap_slope_updated += 1
                                updated = True
                                logger.info(f"✅ Updated VWAP slope for {stock_name}: {trade.vwap_slope_angle:.2f}° ({trade.vwap_slope_status})")
                            else:
                                logger.warning(f"⚠️ Missing VWAP data for {stock_name}: prev_vwap={prev_vwap}, prev_time={prev_vwap_time}, current_vwap={current_vwap}")
                        else:
                            logger.warning(f"⚠️ Could not fetch previous hour VWAP for {stock_name}")
                    else:
                        logger.warning(f"⚠️ Could not fetch stock data for {stock_name}")
                
                # 2. Calculate candle size if missing and instrument_key exists
                if (trade.candle_size_ratio is None or trade.candle_size_status is None) and trade.instrument_key:
                    try:
                        option_candles = upstox_service.get_option_daily_candles_current_and_previous(trade.instrument_key)
                        if option_candles:
                            current_day_candle = option_candles.get('current_day_candle', {})
                            previous_day_candle = option_candles.get('previous_day_candle', {})
                            
                            if current_day_candle and previous_day_candle:
                                current_size = abs(current_day_candle.get('high', 0) - current_day_candle.get('low', 0))
                                previous_size = abs(previous_day_candle.get('high', 0) - previous_day_candle.get('low', 0))
                                
                                if previous_size > 0:
                                    candle_size_ratio = current_size / previous_size
                                    trade.candle_size_ratio = candle_size_ratio
                                    trade.candle_size_status = "Pass" if candle_size_ratio < 7.5 else "Fail"
                                    
                                    # Update OHLC data
                                    trade.option_current_candle_open = current_day_candle.get('open')
                                    trade.option_current_candle_high = current_day_candle.get('high')
                                    trade.option_current_candle_low = current_day_candle.get('low')
                                    trade.option_current_candle_close = current_day_candle.get('close')
                                    trade.option_current_candle_time = current_day_candle.get('time')
                                    trade.option_previous_candle_open = previous_day_candle.get('open')
                                    trade.option_previous_candle_high = previous_day_candle.get('high')
                                    trade.option_previous_candle_low = previous_day_candle.get('low')
                                    trade.option_previous_candle_close = previous_day_candle.get('close')
                                    trade.option_previous_candle_time = previous_day_candle.get('time')
                                    
                                    candle_size_updated += 1
                                    updated = True
                                    logger.info(f"✅ Updated candle size for {stock_name}: {candle_size_ratio:.2f}x ({trade.candle_size_status})")
                    except Exception as candle_error:
                        logger.warning(f"Error calculating candle size for {stock_name}: {str(candle_error)}")
                elif trade.candle_size_ratio is None or trade.candle_size_status is None:
                    logger.warning(f"⚠️ Cannot calculate candle size for {stock_name}: instrument_key={trade.instrument_key}, option_contract={trade.option_contract}")
                
                if updated:
                    db.commit()
                    processed_count += 1
                    
            except Exception as trade_error:
                logger.error(f"Error processing trade {trade.stock_name}: {str(trade_error)}")
                db.rollback()
                continue
        
        return {
            "success": True,
            "message": f"Recalculation completed for today's trades",
            "total_trades": len(all_trades),
            "processed_count": processed_count,
            "vwap_slope_updated": vwap_slope_updated,
            "candle_size_updated": candle_size_updated,
            "timestamp": now.isoformat()
        }
        
    except Exception as e:
        logger.error(f"Error in recalculate all today trades: {e}")
        import traceback
        traceback.print_exc()
        return {
            "success": False,
            "error": str(e),
            "timestamp": datetime.now().isoformat()
        }

@router.post("/process-all-today-stocks")
async def process_all_today_stocks(db: Session = Depends(get_db)):
    """
    One-time process to update all stocks for today:
    - Set buy_price to current LTP
    - Set qty to option lot_size
    - Update status to 'bought'
    - Set buy_time to current time
    - Set stop_loss to 5% below buy_price
    """
    try:
        import pytz
        from sqlalchemy import and_
        from backend.services.upstox_service import upstox_service as vwap_service
        
        ist = pytz.timezone('Asia/Kolkata')
        now = datetime.now(ist)
        today = now.replace(hour=0, minute=0, second=0, microsecond=0)
        tomorrow = today.replace(day=today.day + 1) if today.day < 28 else today.replace(month=today.month + 1, day=1)
        
        # Get all trades for today that have instrument_key but no buy_price
        all_trades = db.query(IntradayStockOption).filter(
            and_(
                IntradayStockOption.trade_date >= today,
                IntradayStockOption.trade_date < tomorrow,
                IntradayStockOption.instrument_key.isnot(None),
                IntradayStockOption.instrument_key != ""
            )
        ).all()
        
        logger.info(f"📊 Processing {len(all_trades)} stocks for today...")
        
        processed_count = 0
        updated_count = 0
        error_count = 0
        
        # Load instruments.json to get lot_size
        instruments_file = Path("/home/ubuntu/trademanthan/data/instruments/nse_instruments.json")
        instruments_data = []
        if instruments_file.exists():
            try:
                with open(instruments_file, 'r') as f:
                    instruments_data = json.load(f)
                logger.info(f"✅ Loaded {len(instruments_data)} instruments from JSON")
            except Exception as e:
                logger.warning(f"⚠️ Could not load instruments.json: {str(e)}")
        
        for trade in all_trades:
            try:
                stock_name = trade.stock_name
                instrument_key = trade.instrument_key
                
                # Skip if already has buy_price and status is 'bought'
                if trade.buy_price and trade.status == 'bought':
                    logger.info(f"⏭️ Skipping {stock_name} - already processed (buy_price: ₹{trade.buy_price:.2f}, status: {trade.status})")
                    continue
                
                # Get current LTP
                current_ltp = None
                try:
                    if vwap_service:
                        option_quote = vwap_service.get_market_quote_by_key(instrument_key)
                        if option_quote and option_quote.get('last_price', 0) > 0:
                            current_ltp = float(option_quote.get('last_price', 0))
                            logger.info(f"✅ Fetched current LTP for {stock_name}: ₹{current_ltp:.2f}")
                        else:
                            logger.warning(f"⚠️ Could not fetch LTP for {stock_name} (instrument_key: {instrument_key})")
                    else:
                        logger.warning(f"⚠️ vwap_service not available for {stock_name}")
                except Exception as ltp_error:
                    logger.warning(f"⚠️ Error fetching LTP for {stock_name}: {str(ltp_error)}")
                
                if not current_ltp or current_ltp <= 0:
                    logger.warning(f"⚠️ Skipping {stock_name} - invalid LTP: {current_ltp}")
                    error_count += 1
                    continue
                
                # Get lot_size from instruments.json
                lot_size = None
                if instruments_data:
                    for inst in instruments_data:
                        if isinstance(inst, dict) and inst.get('instrument_key') == instrument_key:
                            lot_size = inst.get('lot_size')
                            if lot_size and lot_size > 0:
                                logger.info(f"✅ Found lot_size for {stock_name}: {lot_size}")
                                break
                
                if not lot_size or lot_size <= 0:
                    # Try to get from trade.qty if available
                    if trade.qty and trade.qty > 0:
                        lot_size = trade.qty
                        logger.info(f"✅ Using existing qty as lot_size for {stock_name}: {lot_size}")
                    else:
                        logger.warning(f"⚠️ Could not find lot_size for {stock_name} (instrument_key: {instrument_key})")
                        error_count += 1
                        continue
                
                # Update trade
                trade.buy_price = current_ltp
                trade.qty = lot_size
                trade.status = 'bought'
                trade.buy_time = now
                trade.stop_loss = current_ltp * 0.95  # 5% below buy_price
                trade.no_entry_reason = None  # Clear no_entry_reason since we're entering
                
                updated_count += 1
                processed_count += 1
                
                logger.info(f"✅ Updated {stock_name}: buy_price=₹{current_ltp:.2f}, qty={lot_size}, stop_loss=₹{trade.stop_loss:.2f}")
                
            except Exception as trade_error:
                logger.error(f"❌ Error processing {trade.stock_name}: {str(trade_error)}")
                import traceback
                logger.error(traceback.format_exc())
                error_count += 1
                continue
        
        # Commit all changes
        try:
            db.commit()
            logger.info(f"✅ Successfully committed {updated_count} updates to database")
        except Exception as commit_error:
            db.rollback()
            logger.error(f"❌ Database commit failed: {str(commit_error)}")
            raise
        
        return {
            "success": True,
            "message": f"Processed all stocks for today",
            "total_trades": len(all_trades),
            "processed_count": processed_count,
            "updated_count": updated_count,
            "error_count": error_count,
            "timestamp": now.isoformat()
        }
        
    except Exception as e:
        db.rollback()
        logger.error(f"Error in process all today stocks: {e}")
        import traceback
        traceback.print_exc()
        return {
            "success": False,
            "error": str(e),
            "timestamp": datetime.now().isoformat()
        }

@router.get("/scheduler-status")
async def get_scheduler_status():
    """Get status of Scan ST1 Algo Scheduler (replaces all old schedulers)"""
    try:
        try:
            from backend.services.scan_st1_algo import scan_st1_algo_scheduler
        except ImportError as import_err:
            logger.error(f"Failed to import scan_st1_algo_scheduler: {import_err}")
            return {
                "success": False,
                "error": f"Import error: {str(import_err)}",
                "all_running": False,
                "total_jobs": 0,
                "scan_st1_algo": {
                    "running": False,
                    "jobs_count": 0,
                    "next_jobs": []
                }
            }
        except Exception as init_err:
            logger.error(f"Error initializing scan_st1_algo_scheduler: {init_err}")
            import traceback
            logger.error(traceback.format_exc())
            return {
                "success": False,
                "error": f"Initialization error: {str(init_err)}",
                "all_running": False,
                "total_jobs": 0,
                "scan_st1_algo": {
                    "running": False,
                    "jobs_count": 0,
                    "next_jobs": []
                }
            }
        
        if not scan_st1_algo_scheduler:
            return {
                "success": False,
                "error": "Scan ST1 Algo Scheduler not initialized",
                "all_running": False,
                "total_jobs": 0,
                "scan_st1_algo": {
                    "running": False,
                    "jobs_count": 0,
                    "next_jobs": []
                }
            }
        
        try:
            is_running = False
            jobs_count = 0
            next_jobs = []
            
            if scan_st1_algo_scheduler.scheduler:
                is_running = scan_st1_algo_scheduler.is_running and scan_st1_algo_scheduler.scheduler.running
                if is_running:
                    jobs = scan_st1_algo_scheduler.scheduler.get_jobs()
                    jobs_count = len(jobs)
                    next_jobs = [
                        {"name": job.name, "next_run": str(job.next_run_time) if job.next_run_time else "Not scheduled"} 
                        for job in sorted(jobs, key=lambda x: x.next_run_time if x.next_run_time else float('inf'))[:10]
                    ]
        except Exception as status_err:
            logger.error(f"Error checking scheduler status: {status_err}")
            import traceback
            logger.error(traceback.format_exc())
            is_running = False
            jobs_count = 0
            next_jobs = []
        
        return {
            "success": True,
            "all_running": is_running,
            "total_jobs": jobs_count,
            "scan_st1_algo": {
                "running": is_running,
                "jobs_count": jobs_count,
                "next_jobs": next_jobs
            }
        }
        
    except Exception as e:
        logger.error(f"Error getting scheduler status: {e}")
        import traceback
        logger.error(traceback.format_exc())
        return {
            "success": False,
            "error": str(e),
            "all_running": False,
            "total_jobs": 0,
            "scan_st1_algo": {
                "running": False,
                "jobs_count": 0,
                "next_jobs": []
            }
        }


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
            logger.info(f"Database health check failed: {e}")
        
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
        logger.info(f"Health check endpoint failed: {str(e)}")
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
    try:
        # Try to read JSON body with timeout protection (increased timeout to reduce client disconnect errors)
        data = await asyncio.wait_for(request.json(), timeout=5.0)
        
        # Enhanced logging: Log full payload for debugging
        stocks_count = len(data.get('stocks', '').split(',')) if isinstance(data.get('stocks'), str) else len(data.get('stocks', []))
        logger.info(f"📥 Received bullish webhook with {stocks_count} stocks")
        logger.info(f"📦 Full webhook payload: {json.dumps(data, indent=2)}")
        logger.info(f"📥 Received bullish webhook at {datetime.now().isoformat()}")
        logger.info(f"📦 Payload: {json.dumps(data, indent=2)}")
        logger.info(f"📦 Raw stocks field: {repr(data.get('stocks'))}")
        logger.info(f"📦 Raw trigger_prices field: {repr(data.get('trigger_prices'))}")
        
        # Process SYNCHRONOUSLY for reliability - webhooks are critical and must be processed
        # Synchronous processing ensures data is saved before response is sent
        # FastAPI's async nature means this won't block other requests significantly
        try:
            result = await process_webhook_data(data, db, 'bullish')
            # Track webhook success
            if health_monitor:
                health_monitor.record_webhook_success()
            
            # Return the result from process_webhook_data
            if isinstance(result, JSONResponse):
                return result
            else:
                return JSONResponse(content={
                    "status": "success",
                    "message": "Bullish webhook processed successfully",
            "alert_type": "bullish",
            "timestamp": datetime.now().isoformat()
                }, status_code=200)
        except Exception as e:
            logger.error(f"❌ CRITICAL: Failed to process bullish webhook: {str(e)}")
            logger.error(f"   Stock names in webhook: {data.get('stocks', 'N/A')}")
            import traceback
            logger.error(f"   Traceback: {traceback.format_exc()}")
            logger.info(f"❌ CRITICAL: Failed to process bullish webhook: {str(e)}")
            traceback.print_exc()
            # Track webhook failure
            if health_monitor:
                health_monitor.record_webhook_failure()
            
            # Return error response but still acknowledge receipt
            return JSONResponse(content={
                "status": "error",
                "message": f"Webhook received but processing failed: {str(e)}",
                "alert_type": "bullish",
                "timestamp": datetime.now().isoformat()
            }, status_code=500)
        
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
    try:
        # Try to read JSON body with timeout protection (increased timeout to reduce client disconnect errors)
        data = await asyncio.wait_for(request.json(), timeout=5.0)
        
        # Enhanced logging: Log full payload for debugging
        stocks_count = len(data.get('stocks', '').split(',')) if isinstance(data.get('stocks'), str) else len(data.get('stocks', []))
        logger.info(f"📥 Received bearish webhook with {stocks_count} stocks")
        logger.info(f"📦 Full webhook payload: {json.dumps(data, indent=2)}")
        logger.info(f"📥 Received bearish webhook at {datetime.now().isoformat()}")
        logger.info(f"📦 Payload: {json.dumps(data, indent=2)}")
        logger.info(f"📦 Raw stocks field: {repr(data.get('stocks'))}")
        logger.info(f"📦 Raw trigger_prices field: {repr(data.get('trigger_prices'))}")
        
        # Process SYNCHRONOUSLY for reliability - webhooks are critical and must be processed
        # Synchronous processing ensures data is saved before response is sent
        # FastAPI's async nature means this won't block other requests significantly
        try:
            result = await process_webhook_data(data, db, 'bearish')
            # Track webhook success
            if health_monitor:
                health_monitor.record_webhook_success()
            
            # Return the result from process_webhook_data
            if isinstance(result, JSONResponse):
                return result
            else:
                return JSONResponse(content={
                    "status": "success",
                    "message": "Bearish webhook processed successfully",
            "alert_type": "bearish",
            "timestamp": datetime.now().isoformat()
                }, status_code=200)
        except Exception as e:
            logger.error(f"❌ CRITICAL: Failed to process bearish webhook: {str(e)}")
            logger.error(f"   Stock names in webhook: {data.get('stocks', 'N/A')}")
            import traceback
            logger.error(f"   Traceback: {traceback.format_exc()}")
            logger.info(f"❌ CRITICAL: Failed to process bearish webhook: {str(e)}")
            traceback.print_exc()
            # Track webhook failure
            if health_monitor:
                health_monitor.record_webhook_failure()
            
            # Return error response but still acknowledge receipt
            return JSONResponse(content={
                "status": "error",
                "message": f"Webhook received but processing failed: {str(e)}",
                "alert_type": "bearish",
                "timestamp": datetime.now().isoformat()
            }, status_code=500)
        
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
    try:
        # Try to read JSON body with timeout protection (increased timeout to reduce client disconnect errors)
        data = await asyncio.wait_for(request.json(), timeout=5.0)
        
        # Enhanced logging: Log full payload for debugging
        stocks_count = len(data.get('stocks', '').split(',')) if isinstance(data.get('stocks'), str) else len(data.get('stocks', []))
        alert_name = data.get('alert_name', 'N/A')
        scan_name = data.get('scan_name', 'N/A')
        scan_url = data.get('scan_url', 'N/A')
        
        logger.info(f"📥 Received webhook (auto-detect) with {stocks_count} stocks")
        logger.info(f"📦 Alert details: alert_name='{alert_name}', scan_name='{scan_name}', scan_url='{scan_url}'")
        logger.info(f"📦 Full webhook payload: {json.dumps(data, indent=2)}")
        logger.info(f"📥 Received webhook (auto-detect) at {datetime.now().isoformat()}")
        logger.info(f"📦 Alert details: alert_name='{alert_name}', scan_name='{scan_name}', scan_url='{scan_url}'")
        logger.info(f"📦 Payload: {json.dumps(data, indent=2)}")
        
        # Process SYNCHRONOUSLY for reliability
        try:
            result = await process_webhook_data(data, db, forced_type=None)  # Auto-detect
            # Track webhook success
            if health_monitor:
                health_monitor.record_webhook_success()
            
            # Return the result from process_webhook_data
            if isinstance(result, JSONResponse):
                return result
            else:
                return JSONResponse(content={
                    "status": "success",
                    "message": "Webhook processed successfully (auto-detect)",
                    "timestamp": datetime.now().isoformat()
                }, status_code=200)
        except Exception as e:
            logger.error(f"❌ CRITICAL: Failed to process webhook (auto-detect): {str(e)}")
            logger.error(f"   Stock names in webhook: {data.get('stocks', 'N/A')}")
            import traceback
            logger.error(f"   Traceback: {traceback.format_exc()}")
            logger.info(f"❌ CRITICAL: Failed to process webhook (auto-detect): {str(e)}")
            traceback.print_exc()
            # Track webhook failure
            if health_monitor:
                health_monitor.record_webhook_failure()
            
            # Return error response but still acknowledge receipt
            return JSONResponse(content={
                "status": "error",
                "message": f"Failed to process webhook: {str(e)}",
                "timestamp": datetime.now().isoformat()
            }, status_code=500)
    except asyncio.TimeoutError:
        logger.error("⚠️ Timeout reading webhook body")
        return JSONResponse(
            content={"status": "error", "message": "Timeout reading request body"},
            status_code=408
        )
    except Exception as e:
        logger.error(f"❌ Error processing webhook: {str(e)}")
        return JSONResponse(
            content={"status": "error", "message": f"Error: {str(e)}"},
            status_code=500
        )


@router.post("/manual-entry")
async def manual_stock_entry(request: Request, db: Session = Depends(get_db)):
    """
    Manual stock entry endpoint for processing stocks when webhook fails
    
    Expected JSON format:
    {
        "bullishStocks": "RELIANCE,TCS,INFY",
        "bearishStocks": "ADANIENT,ADANIPORTS",
        "alertTime": "11:15",
        "alertDate": "2026-01-01" (optional, defaults to today)
    }
    """
    try:
        data = await request.json()
        
        bullish_stocks_str = data.get("bullishStocks", "").strip()
        bearish_stocks_str = data.get("bearishStocks", "").strip()
        alert_time_str = data.get("alertTime", "").strip()
        alert_date_str = data.get("alertDate", "").strip()
        
        # Validate inputs
        if not bullish_stocks_str and not bearish_stocks_str:
            return JSONResponse(
                status_code=400,
                content={
                    "status": "error",
                    "message": "At least one stock list (Bullish or Bearish) must be provided"
                }
            )
        
        if not alert_time_str:
            return JSONResponse(
                status_code=400,
                content={
                    "status": "error",
                    "message": "Alert time is required"
                }
            )
        
        # Parse alert time and date
        import pytz
        from dateutil import parser
        
        ist = pytz.timezone('Asia/Kolkata')
        now = datetime.now(ist)
        
        # Parse date (default to today if not provided)
        if alert_date_str:
            try:
                alert_date = datetime.strptime(alert_date_str, '%Y-%m-%d').date()
                trading_date = datetime.combine(alert_date, datetime.min.time()).replace(tzinfo=ist)
            except ValueError:
                return JSONResponse(
                    status_code=400,
                    content={
                        "status": "error",
                        "message": f"Invalid date format. Expected YYYY-MM-DD, got: {alert_date_str}"
                    }
                )
        else:
            trading_date = now.replace(hour=0, minute=0, second=0, microsecond=0)
            alert_date = trading_date.date()
        
        # Parse time (format: HH:MM)
        try:
            time_parts = alert_time_str.split(':')
            if len(time_parts) != 2:
                raise ValueError("Invalid time format")
            hour = int(time_parts[0])
            minute = int(time_parts[1])
            
            if not (0 <= hour <= 23) or not (0 <= minute <= 59):
                raise ValueError("Invalid time values")
            
            # Create datetime for triggered_at
            triggered_datetime = trading_date.replace(hour=hour, minute=minute, second=0, microsecond=0)
            
            # Format time for display (12-hour format)
            if hour == 0:
                time_display = f"12:{minute:02d} AM"
            elif hour < 12:
                time_display = f"{hour}:{minute:02d} AM"
            elif hour == 12:
                time_display = f"12:{minute:02d} PM"
            else:
                time_display = f"{hour-12}:{minute:02d} PM"
            
        except (ValueError, IndexError) as e:
            return JSONResponse(
                status_code=400,
                content={
                    "status": "error",
                    "message": f"Invalid time format. Expected HH:MM (24-hour format), got: {alert_time_str}"
                }
            )
        
        # Process bullish stocks if provided
        bullish_result = None
        bullish_count = 0
        if bullish_stocks_str:
            stock_list = [s.strip() for s in bullish_stocks_str.split(",") if s.strip()]
            trigger_prices_str = ",".join(["0.0"] * len(stock_list))  # Trigger prices not provided, use 0.0
            
            webhook_data = {
                "stocks": ",".join(stock_list),
                "trigger_prices": trigger_prices_str,
                "triggered_at": time_display.lower(),
                "scan_name": "Manual Entry - Bullish",
                "alert_name": "Manual Entry - Bullish"
            }
            
            try:
                bullish_result = await process_webhook_data(webhook_data, db, forced_type='bullish')
                # process_webhook_data returns JSONResponse, extract the data
                if isinstance(bullish_result, JSONResponse):
                    try:
                        result_data = json.loads(bullish_result.body.decode())
                        bullish_count = result_data.get('saved_to_database', 0)
                    except (json.JSONDecodeError, AttributeError, UnicodeDecodeError) as decode_error:
                        logger.warning(f"Could not decode bullish result: {str(decode_error)}")
                        # If we can't decode, check if it was successful by status code
                        if bullish_result.status_code == 200:
                            bullish_count = len([s.strip() for s in bullish_stocks_str.split(",") if s.strip()])
                else:
                    # If not JSONResponse, assume success and count stocks
                    bullish_count = len([s.strip() for s in bullish_stocks_str.split(",") if s.strip()])
            except Exception as e:
                logger.error(f"Error processing bullish stocks: {str(e)}")
                import traceback
                logger.error(traceback.format_exc())
                # Don't fail the entire request if one type fails
        
        # Process bearish stocks if provided
        bearish_result = None
        bearish_count = 0
        if bearish_stocks_str:
            stock_list = [s.strip() for s in bearish_stocks_str.split(",") if s.strip()]
            trigger_prices_str = ",".join(["0.0"] * len(stock_list))  # Trigger prices not provided, use 0.0
            
            webhook_data = {
                "stocks": ",".join(stock_list),
                "trigger_prices": trigger_prices_str,
                "triggered_at": time_display.lower(),
                "scan_name": "Manual Entry - Bearish",
                "alert_name": "Manual Entry - Bearish"
            }
            
            try:
                bearish_result = await process_webhook_data(webhook_data, db, forced_type='bearish')
                # process_webhook_data returns JSONResponse, extract the data
                if isinstance(bearish_result, JSONResponse):
                    try:
                        # JSONResponse stores content directly - access it
                        if hasattr(bearish_result, 'body') and bearish_result.body:
                            # body is bytes, decode it
                            result_data = json.loads(bearish_result.body.decode('utf-8'))
                            bearish_count = result_data.get('saved_to_database', 0)
                        elif hasattr(bearish_result, 'body') and callable(bearish_result.body):
                            # body is a callable (async generator), this shouldn't happen but handle it
                            logger.warning("Bearish result body is callable, using fallback count")
                            if bearish_result.status_code == 200:
                                bearish_count = len([s.strip() for s in bearish_stocks_str.split(",") if s.strip()])
                        else:
                            # Fallback: count stocks if status is 200
                            if bearish_result.status_code == 200:
                                bearish_count = len([s.strip() for s in bearish_stocks_str.split(",") if s.strip()])
                    except (json.JSONDecodeError, AttributeError, UnicodeDecodeError, TypeError) as decode_error:
                        logger.warning(f"Could not decode bearish result: {str(decode_error)}")
                        # If we can't decode, check if it was successful by status code
                        if bearish_result.status_code == 200:
                            bearish_count = len([s.strip() for s in bearish_stocks_str.split(",") if s.strip()])
                else:
                    # If not JSONResponse, assume success and count stocks
                    bearish_count = len([s.strip() for s in bearish_stocks_str.split(",") if s.strip()])
            except Exception as e:
                logger.error(f"Error processing bearish stocks: {str(e)}")
                import traceback
                logger.error(traceback.format_exc())
                # Don't fail the entire request if one type fails
        
        total_processed = bullish_count + bearish_count
        
        return JSONResponse(
            status_code=200,
            content={
                "status": "success",
                "message": f"Successfully processed {total_processed} stock(s)",
                "bullish_count": bullish_count,
                "bearish_count": bearish_count,
                "processed_count": total_processed,
                "alert_time": triggered_datetime.isoformat(),
                "timestamp": datetime.now(ist).isoformat()
            }
        )
        
    except Exception as e:
        logger.error(f"Error in manual stock entry: {str(e)}")
        import traceback
        logger.error(traceback.format_exc())
        return JSONResponse(
            status_code=500,
            content={
                "status": "error",
                "message": f"Failed to process manual entry: {str(e)}"
            }
        )

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
            logger.info(f"Current time: {now.strftime('%Y-%m-%d %H:%M:%S')} IST - Showing TODAY's data (after 9:00 AM)")
        else:
            # Before 9:00 AM - show yesterday's data
            filter_date_start = today - timedelta(days=1)
            filter_date_end = today
            logger.info(f"Current time: {now.strftime('%Y-%m-%d %H:%M:%S')} IST - Showing YESTERDAY's data (before 9:00 AM)")
        
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
                    # For 10:15 AM alerts, Cycle 1 (10:30 AM) should recalculate VWAP slope
                    # Only show "Skipped" if Cycle 1 hasn't run yet or failed to calculate
                    # Check if saved values exist (Cycle 1 should have calculated them)
                    if record.vwap_slope_status and record.vwap_slope_status != "Skipped":
                        # Cycle 1 already calculated it - use saved value
                        vwap_slope_status = record.vwap_slope_status
                        vwap_slope_angle = record.vwap_slope_angle
                        vwap_slope_direction = record.vwap_slope_direction
                    else:
                        # Not calculated yet - show "Skipped" only if before 10:30 AM
                        if now.hour > 10 or (now.hour == 10 and now.minute >= 30):
                            # After 10:30 AM - Cycle 1 should have run, but no data means it failed
                            vwap_slope_status = "Skipped"
                        else:
                            # Before 10:30 AM - will be calculated in Cycle 1
                            vwap_slope_status = "Skipped"
                
                # Calculate candle size if data is available
                # For 10:15 AM alerts, Cycle 1 (10:30 AM) should have recalculated candle size
                # Use saved values if available from Cycle 1
                if record.candle_size_ratio is not None and record.candle_size_status:
                    # Use saved values from Cycle 1 recalculation
                    candle_size_ratio = record.candle_size_ratio
                    candle_size_status = record.candle_size_status
                elif record.option_current_candle_high and record.option_current_candle_low and record.option_previous_candle_high and record.option_previous_candle_low:
                    try:
                        current_size = abs(record.option_current_candle_high - record.option_current_candle_low)
                        previous_size = abs(record.option_previous_candle_high - record.option_previous_candle_low)
                        if previous_size > 0:
                            candle_size_ratio = current_size / previous_size
                            candle_size_status = "Pass" if candle_size_ratio < 7.5 else "Fail"
                    except:
                        pass
                elif is_10_15_alert:
                    # For 10:15 AM alerts, Cycle 1 (10:30 AM) should recalculate candle size
                    # Only show "Skipped" if Cycle 1 hasn't run yet or failed to calculate
                    if now.hour > 10 or (now.hour == 10 and now.minute >= 30):
                        # After 10:30 AM - Cycle 1 should have run, but no data means it failed
                        candle_size_status = "Skipped"
                    else:
                        # Before 10:30 AM - will be calculated in Cycle 1
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
                                logger.info(f"✅ Retried and found option contract for {record.stock_name}: {retry_contract}")
                            except Exception as commit_error:
                                db.rollback()
                                logger.info(f"⚠️ Failed to commit option contract for {record.stock_name}: {str(commit_error)}")
                    except Exception as retry_error:
                        logger.info(f"⚠️ Retry option contract determination failed for {record.stock_name}: {str(retry_error)}")
                
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
                    "status": record.status,  # Include status to identify no_entry trades
                    "no_entry_reason": record.no_entry_reason or None  # Reason for no entry if status is no_entry
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
                    # For 10:15 AM alerts, Cycle 1 (10:30 AM) should recalculate VWAP slope
                    # Only show "Skipped" if Cycle 1 hasn't run yet or failed to calculate
                    # Check if saved values exist (Cycle 1 should have calculated them)
                    if record.vwap_slope_status and record.vwap_slope_status != "Skipped":
                        # Cycle 1 already calculated it - use saved value
                        vwap_slope_status = record.vwap_slope_status
                        vwap_slope_angle = record.vwap_slope_angle
                        vwap_slope_direction = record.vwap_slope_direction
                    else:
                        # Not calculated yet - show "Skipped" only if before 10:30 AM
                        if now.hour > 10 or (now.hour == 10 and now.minute >= 30):
                            # After 10:30 AM - Cycle 1 should have run, but no data means it failed
                            vwap_slope_status = "Skipped"
                        else:
                            # Before 10:30 AM - will be calculated in Cycle 1
                            vwap_slope_status = "Skipped"
                
                # Calculate candle size if data is available
                # For 10:15 AM alerts, Cycle 1 (10:30 AM) should have recalculated candle size
                # Use saved values if available from Cycle 1
                if record.candle_size_ratio is not None and record.candle_size_status:
                    # Use saved values from Cycle 1 recalculation
                    candle_size_ratio = record.candle_size_ratio
                    candle_size_status = record.candle_size_status
                elif record.option_current_candle_high and record.option_current_candle_low and record.option_previous_candle_high and record.option_previous_candle_low:
                    try:
                        current_size = abs(record.option_current_candle_high - record.option_current_candle_low)
                        previous_size = abs(record.option_previous_candle_high - record.option_previous_candle_low)
                        if previous_size > 0:
                            candle_size_ratio = current_size / previous_size
                            candle_size_status = "Pass" if candle_size_ratio < 7.5 else "Fail"
                    except:
                        pass
                elif is_10_15_alert:
                    # For 10:15 AM alerts, Cycle 1 (10:30 AM) should recalculate candle size
                    # Only show "Skipped" if Cycle 1 hasn't run yet or failed to calculate
                    if now.hour > 10 or (now.hour == 10 and now.minute >= 30):
                        # After 10:30 AM - Cycle 1 should have run, but no data means it failed
                        candle_size_status = "Skipped"
                    else:
                        # Before 10:30 AM - will be calculated in Cycle 1
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
                                logger.info(f"✅ Retried and found option contract for {record.stock_name}: {retry_contract}")
                            except Exception as commit_error:
                                db.rollback()
                                logger.info(f"⚠️ Failed to commit option contract for {record.stock_name}: {str(commit_error)}")
                    except Exception as retry_error:
                        logger.info(f"⚠️ Retry option contract determination failed for {record.stock_name}: {str(retry_error)}")
                
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
                    "status": record.status,  # Include status to identify no_entry trades
                    "no_entry_reason": record.no_entry_reason or None  # Reason for no entry if status is no_entry
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
        logger.info(f"Error fetching latest data from database: {str(e)}")
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
            logger.info(f"⏰ TIME-BASED EXIT: Current time {current_time.strftime('%H:%M')} >= 15:25 - Exiting all open trades")
        
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
        
        logger.info(f"Refreshing {len(records)} records (skipped {skipped_no_entry} 'no_entry' trades)...")
        
        for record in records:
            try:
                # SAFETY CHECK: Skip if trade already has exit_reason (should be filtered by query, but double-check)
                if record.exit_reason is not None:
                    logger.info(f"⚠️ Skipping {record.stock_name} - already exited with reason: {record.exit_reason}")
                    continue
                
                # Load instruments JSON if needed
                from pathlib import Path
                import json as json_lib
                
                instruments_file = Path("/home/ubuntu/trademanthan/data/instruments/nse_instruments.json")
                
                if not instruments_file.exists():
                    logger.info(f"Instruments JSON not found")
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
                                        logger.info(f"✅ Found EXACT match for {option_contract}: {instrument_key} (strike: {inst_strike})")
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
                        logger.info(f"⚠️ WARNING: Using BEST MATCH (not exact) for {option_contract}: {instrument_key} (strike: {inst_strike}, requested: {strike_value})")
                    
                    if not instrument_key:
                        logger.info(f"❌ ERROR: Could not find instrument_key for {option_contract}")
                    
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
                                logger.info(f"Could not update stock LTP/VWAP for {record.stock_name}: {str(e)}")
                            
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
                                    logger.info(f"⏰ TIME EXIT CONDITION MET for {record.stock_name}: Current time >= 3:25 PM")
                                
                                # 2. CHECK STOP LOSS
                                if record.stop_loss and new_option_ltp <= record.stop_loss:
                                    exit_conditions['stop_loss'] = True
                                    logger.info(f"🛑 STOP LOSS CONDITION MET for {record.stock_name}: LTP ₹{new_option_ltp} <= SL ₹{record.stop_loss}")
                                
                                # 3. CHECK VWAP CROSS (only after 11:15 AM)
                                vwap_check_time = datetime.strptime("11:15", "%H:%M").time()
                                current_time_check = now.time()
                                
                                if current_time_check >= vwap_check_time and record.stock_ltp and record.stock_vwap and record.option_type:
                                    # Enhanced logging for debugging
                                    logger.info(f"📊 VWAP CHECK for {record.stock_name} ({record.option_type}): Stock LTP=₹{record.stock_ltp}, VWAP=₹{record.stock_vwap}, Time={current_time_check.strftime('%H:%M')}")
                                    
                                    if record.option_type == 'CE' and record.stock_ltp < record.stock_vwap:
                                        # Bullish trade: stock went below VWAP (bearish signal)
                                        exit_conditions['vwap_cross'] = True
                                        logger.info(f"📉 VWAP CROSS CONDITION MET for {record.stock_name} (CE): Stock LTP ₹{record.stock_ltp} < VWAP ₹{record.stock_vwap}")
                                    elif record.option_type == 'PE' and record.stock_ltp > record.stock_vwap:
                                        # Bearish trade: stock went above VWAP (bullish signal)
                                        exit_conditions['vwap_cross'] = True
                                        logger.info(f"📈 VWAP CROSS CONDITION MET for {record.stock_name} (PE): Stock LTP ₹{record.stock_ltp} > VWAP ₹{record.stock_vwap}")
                                    else:
                                        logger.info(f"✅ VWAP OK for {record.stock_name} - Stock {record.stock_ltp} {'>' if record.option_type == 'CE' else '<'} VWAP {record.stock_vwap}")
                                elif current_time_check < vwap_check_time:
                                    logger.info(f"⏰ VWAP check skipped for {record.stock_name} (time {current_time_check.strftime('%H:%M')} < 11:15 AM)")
                                
                                # 4. CHECK PROFIT TARGET (50% gain)
                                if record.buy_price and new_option_ltp >= (record.buy_price * 1.5):
                                    exit_conditions['profit_target'] = True
                                    logger.info(f"🎯 PROFIT TARGET CONDITION MET for {record.stock_name}: LTP ₹{new_option_ltp} >= Target ₹{record.buy_price * 1.5}")
                                
                                # APPLY THE HIGHEST PRIORITY EXIT CONDITION
                                exit_applied = False
                                
                                if exit_conditions['time_based']:
                                    record.sell_price = new_option_ltp
                                    record.sell_time = now
                                    record.exit_reason = 'time_based'
                                    record.status = 'sold'
                                    if record.buy_price and record.qty:
                                        record.pnl = (new_option_ltp - record.buy_price) * record.qty
                                    logger.info(f"✅ APPLIED: TIME EXIT for {record.stock_name}: PnL=₹{record.pnl}")
                                    exit_applied = True
                                
                                elif exit_conditions['stop_loss']:
                                    record.sell_price = new_option_ltp
                                    record.sell_time = now
                                    record.exit_reason = 'stop_loss'
                                    record.status = 'sold'
                                    if record.buy_price and record.qty:
                                        record.pnl = (new_option_ltp - record.buy_price) * record.qty
                                    logger.info(f"✅ APPLIED: STOP LOSS EXIT for {record.stock_name}: PnL=₹{record.pnl}")
                                    exit_applied = True
                                
                                elif exit_conditions['vwap_cross']:
                                    record.sell_price = new_option_ltp
                                    record.sell_time = now
                                    record.exit_reason = 'stock_vwap_cross'
                                    record.status = 'sold'
                                    if record.buy_price and record.qty:
                                        record.pnl = (new_option_ltp - record.buy_price) * record.qty
                                    logger.info(f"✅ APPLIED: VWAP CROSS EXIT for {record.stock_name}: PnL=₹{record.pnl}")
                                    exit_applied = True
                                
                                elif exit_conditions['profit_target']:
                                    record.sell_price = new_option_ltp
                                    record.sell_time = now
                                    record.exit_reason = 'profit_target'
                                    record.status = 'sold'
                                    if record.qty:
                                        record.pnl = (new_option_ltp - record.buy_price) * record.qty
                                    logger.info(f"✅ APPLIED: PROFIT TARGET EXIT for {record.stock_name}: PnL=₹{record.pnl}")
                                    exit_applied = True
                                
                                # If no exit was applied, just update current price and PnL (trade still OPEN)
                                if not exit_applied:
                                    old_sell_price = record.sell_price or 0.0
                                    record.sell_price = new_option_ltp  # Update current Option LTP
                                    logger.info(f"📝 PRICE UPDATE for {record.stock_name}: sell_price ₹{old_sell_price:.2f} → ₹{new_option_ltp:.2f} (OPEN trade)")
                                    
                                    # DO NOT update sell_time here - only set when trade exits
                                    if record.buy_price and record.qty:
                                        record.pnl = (new_option_ltp - record.buy_price) * record.qty  # Current unrealized P&L
                                        
                                    # Sanity check for unrealistic prices
                                    if record.buy_price and new_option_ltp > record.buy_price * 3:
                                        logger.info(f"🚨 WARNING: Unrealistic option price for {record.stock_name}!")
                                        logger.info(f"   Buy: ₹{record.buy_price:.2f}, Current: ₹{new_option_ltp:.2f} ({new_option_ltp/record.buy_price:.1f}x)")
                                        logger.info(f"   Previous sell_price: ₹{old_sell_price:.2f}")
                                        logger.info(f"   This may indicate data corruption!")
                            else:
                                # Trade already closed - this should NOT happen due to query filter
                                # But if it does, log it and skip
                                logger.info(f"🚨 ERROR: {record.stock_name} already has exit_reason='{record.exit_reason}' but was still in query results!")
                                logger.info(f"   This indicates query filter bug. Skipping update.")
                                continue
                            
                            updated_count += 1
                            logger.info(f"✅ Updated {record.stock_name}: option_ltp=₹{new_option_ltp}, PnL=₹{record.pnl}, Exit={record.exit_reason or 'Open'}")
                        else:
                            logger.info(f"❌ Could not fetch LTP for {option_contract}")
                            failed_count += 1
                    else:
                        logger.info(f"❌ Could not find instrument key for {option_contract}")
                        failed_count += 1
            except Exception as e:
                logger.info(f"❌ Error processing {record.stock_name}: {str(e)}")
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
                    logger.info(f"Could not get LTP for {stock_name}, keeping old values")
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
            logger.info(f"Refreshing Bullish alerts...")
            for alert in bullish_data['alerts']:
                refresh_stocks_in_alert(alert, 'CE')
                bullish_count += len(alert.get('stocks', []))
        
        # Refresh bearish data (always PE)
        bearish_count = 0
        if has_bearish:
            logger.info(f"Refreshing Bearish alerts...")
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
        
        logger.info(f"Successfully refreshed LTP and option strikes")
        
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
        logger.info(f"Error refreshing LTP and strikes: {str(e)}")
        return JSONResponse(
            status_code=500,
            content={
                "status": "error",
                "message": f"Failed to refresh VWAP: {str(e)}"
            }
        )

@router.get("/index-prices")
async def get_index_prices(db: Session = Depends(get_db)):
    """
    Get current NIFTY and BANKNIFTY prices with trends
    - During market hours (9:15 AM - 3:30 PM): Fetches from Upstox API (real-time)
    - Before 9:15 AM or after 3:30 PM: Returns last stored price from database
    """
    try:
        from services.index_price_scheduler import index_price_scheduler
        
        # Check if during market hours (9:15 AM - 3:30 PM)
        import pytz
        ist = pytz.timezone('Asia/Kolkata')
        current_time = datetime.now(ist)
        current_hour = current_time.hour
        current_minute = current_time.minute
        
        # Check if market is open (9:15 AM - 3:30 PM)
        market_open = (current_hour > 9 or (current_hour == 9 and current_minute >= 15)) and \
                     (current_hour < 15 or (current_hour == 15 and current_minute <= 30))
        
        # If market is closed, fetch from database
        if not market_open:
            logger.info(f"⏰ Market closed ({current_time.strftime('%H:%M:%S IST')}) - fetching stored prices from database")
            
            # Get latest stored prices from database
            from services.index_price_scheduler import index_price_scheduler
            nifty_stored = index_price_scheduler.get_latest_stored_price('NIFTY50', db)
            banknifty_stored = index_price_scheduler.get_latest_stored_price('BANKNIFTY', db)
            
            # Process NIFTY data from database
            if nifty_stored:
                nifty_close = nifty_stored.get('close_price', nifty_stored.get('ltp', 0))
                nifty_open = nifty_stored.get('day_open', 0)
                nifty_trend = nifty_stored.get('trend', 'unknown')
                nifty_change = nifty_stored.get('change', 0)
                nifty_change_percent = nifty_stored.get('change_percent', 0)
            else:
                # Fallback if no stored data
                nifty_close = 0
                nifty_open = 0
                nifty_trend = 'unknown'
                nifty_change = 0
                nifty_change_percent = 0
            
            # Process BANKNIFTY data from database
            if banknifty_stored:
                banknifty_close = banknifty_stored.get('close_price', banknifty_stored.get('ltp', 0))
                banknifty_open = banknifty_stored.get('day_open', 0)
                banknifty_trend = banknifty_stored.get('trend', 'unknown')
                banknifty_change = banknifty_stored.get('change', 0)
                banknifty_change_percent = banknifty_stored.get('change_percent', 0)
            else:
                # Fallback if no stored data
                banknifty_close = 0
                banknifty_open = 0
                banknifty_trend = 'unknown'
                banknifty_change = 0
                banknifty_change_percent = 0
            
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
                            "change": nifty_change,
                            "change_percent": nifty_change_percent,
                            "market_status": "Closed"
                        },
                        "banknifty": {
                            "name": "BANKNIFTY",
                            "ltp": banknifty_close,
                            "close_price": banknifty_close,
                            "day_open": banknifty_open,
                            "trend": banknifty_trend,
                            "change": banknifty_change,
                            "change_percent": banknifty_change_percent,
                            "market_status": "Closed"
                        },
                        "timestamp": datetime.now().isoformat(),
                        "data_source": "database",
                        "market_status": "closed",
                        "message": "Market closed - showing last stored prices from database"
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
            logger.info("Real-time data not available, falling back to historical data")
            
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
        logger.info(f"Error fetching index prices: {str(e)}")
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
        # Also log to scan_st1_algo.log for visibility in scanlog.html
        try:
            from backend.services.scan_st1_algo import logger as scan_st1_logger
            scan_st1_logger.info("🔄 Updating Upstox API token (manual update)...")
        except:
            pass  # Continue even if scan_st1_logger is not available
        
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
        
        # Log to scan_st1_algo.log
        try:
            from backend.services.scan_st1_algo import logger as scan_st1_logger
            scan_st1_logger.info("✅ Upstox token updated successfully (manual update)")
        except:
            pass
        
        logger.info("✅ Upstox token updated successfully (manual update)")
        
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
        # Log error to scan_st1_algo.log
        try:
            from backend.services.scan_st1_algo import logger as scan_st1_logger
            scan_st1_logger.error(f"❌ Failed to update Upstox token: {str(e)}")
        except:
            pass
        logger.error(f"❌ Failed to update Upstox token: {str(e)}")
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
        
        # Also log to scan_st1_algo.log for visibility in scanlog.html
        try:
            from backend.services.scan_st1_algo import logger as scan_st1_logger
            scan_st1_logger.info("🔄 Updating Upstox API token (OAuth callback)...")
        except:
            pass  # Continue even if scan_st1_logger is not available
        
        # Save token using token manager (persistent storage)
        try:
            from services.token_manager import save_upstox_token
            if save_upstox_token(access_token, expires_at):
                logger.info("✅ Upstox token saved to token manager")
                try:
                    from backend.services.scan_st1_algo import logger as scan_st1_logger
                    scan_st1_logger.info("✅ Upstox token saved to token manager")
                except:
                    pass
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
            try:
                from backend.services.scan_st1_algo import logger as scan_st1_logger
                scan_st1_logger.info("✅ Upstox token updated in service file")
            except:
                pass
        except Exception as e:
            logger.warning(f"⚠️ Could not update service file: {str(e)}")
        
        # Update the token in memory (so it works immediately without restart)
        if hasattr(vwap_service, 'access_token'):
            vwap_service.access_token = access_token
            logger.info("✅ Upstox token updated in memory")
            try:
                from backend.services.scan_st1_algo import logger as scan_st1_logger
                scan_st1_logger.info("✅ Upstox token updated in memory - token refresh complete")
            except:
                pass
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
                IntradayStockOption.status != 'no_entry',
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
                    
                    # CRITICAL: Calculate and update PnL whenever sell_price is updated
                    if position.buy_price and position.qty:
                        old_pnl = position.pnl or 0.0
                        new_pnl = (new_option_ltp - position.buy_price) * position.qty
                        position.pnl = new_pnl
                        from sqlalchemy.orm.attributes import flag_modified
                        flag_modified(position, 'pnl')
                        update_info["pnl"] = {"old": round(old_pnl, 2), "new": round(new_pnl, 2)}
                        updates_made.append(f"P&L: ₹{old_pnl:.2f}→₹{new_pnl:.2f}")
                        logger.info(f"📊 {stock_name}: PnL updated to ₹{new_pnl:.2f} (Buy: ₹{position.buy_price:.2f}, Sell: ₹{new_option_ltp:.2f}, Qty: {position.qty})")
                
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
        
        logger.info(f"\n{'='*80}")
        logger.info(f"VWAP BACKFILL FOR {date_str}")
        logger.info(f"{'='*80}\n")
        
        # Get all records from the specified date
        records = db.query(IntradayStockOption).filter(
            IntradayStockOption.trade_date >= target_date,
            IntradayStockOption.trade_date < next_date
        ).all()
        
        total_records = len(records)
        logger.info(f"📊 Found {total_records} total records from {date_str}")
        
        # Filter records with missing VWAP
        empty_vwap_records = [r for r in records if not r.stock_vwap or r.stock_vwap == 0.0]
        empty_count = len(empty_vwap_records)
        
        logger.info(f"⚠️  Records with missing/zero stock_vwap: {empty_count}")
        
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
        
        logger.info(f"📈 Processing {len(unique_stocks)} unique stocks...\n")
        
        updated_count = 0
        failed_count = 0
        results = []
        
        for stock_name, stock_records in unique_stocks.items():
            try:
                logger.info(f"Fetching VWAP for {stock_name} ({len(stock_records)} records)...")
                
                # Fetch VWAP from Upstox API
                vwap = vwap_service.get_stock_vwap(stock_name)
                
                if vwap and vwap > 0:
                    # Update all records for this stock
                    for record in stock_records:
                        record.stock_vwap = vwap
                        record.updated_at = datetime.now(ist)
                    
                    logger.info(f"  ✅ Updated {len(stock_records)} records with VWAP = ₹{vwap:.2f}")
                    updated_count += len(stock_records)
                    results.append({
                        "stock": stock_name,
                        "status": "success",
                        "vwap": vwap,
                        "records_updated": len(stock_records)
                    })
                else:
                    logger.info(f"  ⚠️  Could not fetch VWAP for {stock_name} (API returned 0 or failed)")
                    failed_count += len(stock_records)
                    results.append({
                        "stock": stock_name,
                        "status": "failed",
                        "reason": "API returned 0 or failed",
                        "records": len(stock_records)
                    })
                    
            except Exception as e:
                logger.info(f"  ❌ Error processing {stock_name}: {str(e)}")
                failed_count += len(stock_records)
                results.append({
                    "stock": stock_name,
                    "status": "error",
                    "error": str(e),
                    "records": len(stock_records)
                })
        
        # Commit all changes
        db.commit()
        
        logger.info(f"\n{'='*80}")
        logger.info("BACKFILL COMPLETE")
        logger.info(f"{'='*80}")
        logger.info(f"✅ Successfully updated: {updated_count} records")
        logger.info(f"❌ Failed: {failed_count} records\n")
        
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
        logger.info(f"❌ Error in backfill: {str(e)}")
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
async def get_scan_logs(
    lines: int = Query(100, ge=1, le=10000, description="Number of log lines to retrieve (1-10000)"),
    log_type: str = Query("scan_st1_algo", description="Log file type: 'scan_st1_algo' (default) or 'trademanthan'")
):
    """
    Get the last N lines from the log file
    For scanlog.html, reads from scan_st1_algo.log (scan algorithm logs)
    For other uses, can read from trademanthan.log
    
    Args:
        lines: Number of lines to return (default 100, max 10000)
        log_type: Type of log file - 'scan_st1_algo' (default) or 'trademanthan'
    
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
        
        # Use scan_st1_algo.log for scan algorithm logs (default for scanlog.html)
        if log_type == "scan_st1_algo":
            log_file = log_dir / 'scan_st1_algo.log'
        else:
            # Fallback to trademanthan.log for other uses
            log_file = log_dir / 'trademanthan.log'
        
        # If scan_st1_algo.log doesn't exist, create it (empty file)
        if log_type == "scan_st1_algo" and not log_file.exists():
            try:
                log_file.parent.mkdir(parents=True, exist_ok=True)
                log_file.touch()
                logger.info(f"Created scan_st1_algo.log file at {log_file}")
            except Exception as e:
                logger.error(f"Error creating scan_st1_algo.log: {e}")
        
        # Alternative: try to find from logging configuration (only for trademanthan.log if main file doesn't exist)
        if log_type != "scan_st1_algo" and not log_file.exists():
            # Try alternative locations (EXCLUDE /tmp/uvicorn.log - that's not the proper log file)
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
        # Return empty logs array instead of error - log file might not exist yet
        return {
            "success": True,
            "message": "Log file not found - will be created when scheduler starts",
            "log_file": str(log_file) if 'log_file' in locals() else "Unknown",
            "total_lines": 0,
            "logs": []
        }
    except PermissionError as perm_err:
        logger.error(f"Permission error reading log file: {perm_err}")
        return {
            "success": False,
            "message": f"Permission denied reading log file: {str(perm_err)}",
            "log_file": str(log_file) if 'log_file' in locals() else "Unknown",
            "total_lines": 0,
            "logs": []
        }
    except Exception as e:
        logger.error(f"Error reading logs: {str(e)}")
        import traceback
        logger.error(traceback.format_exc())
        return {
            "success": False,
            "message": f"Error reading logs: {str(e)}",
            "log_file": str(log_file) if 'log_file' in locals() else "Unknown",
            "total_lines": 0,
            "logs": []
        }


@router.get("/diagnose-bearish-trades")
async def diagnose_bearish_trades(db: Session = Depends(get_db)):
    """
    Diagnostic endpoint to check why bearish trades are not entering
    Shows all today's bearish trades with their entry conditions status
    """
    try:
        import pytz
        from datetime import timedelta
        from backend.services.upstox_service import upstox_service
        
        ist = pytz.timezone('Asia/Kolkata')
        now = datetime.now(ist)
        today = now.replace(hour=0, minute=0, second=0, microsecond=0)
        
        # Get all bearish trades for today
        bearish_trades = db.query(IntradayStockOption).filter(
            IntradayStockOption.alert_type == 'Bearish',
            IntradayStockOption.trade_date >= today,
            IntradayStockOption.trade_date < today + timedelta(days=1)
        ).all()
        
        diagnostics = []
        
        for trade in bearish_trades:
            stock_name = trade.stock_name
            
            # Get current market data
            stock_data = upstox_service.get_stock_ltp_and_vwap(stock_name)
            current_stock_ltp = stock_data.get('ltp', 0) if stock_data else None
            current_stock_vwap = stock_data.get('vwap', 0) if stock_data else None
            
            # Get index trends
            index_trends = upstox_service.check_index_trends()
            nifty_trend = index_trends.get("nifty_trend", "unknown")
            banknifty_trend = index_trends.get("banknifty_trend", "unknown")
            
            # Check time condition
            is_before_3pm = now.hour < 15
            
            # Check index trends alignment
            option_type = trade.option_type or 'PE'
            both_bullish = (nifty_trend == "bullish" and banknifty_trend == "bullish")
            both_bearish = (nifty_trend == "bearish" and banknifty_trend == "bearish")
            opposite_directions = not both_bullish and not both_bearish
            
            can_enter_by_index = False
            if option_type == 'PE':  # Bearish alert
                can_enter_by_index = both_bullish or both_bearish
            
            # Check VWAP slope
            vwap_slope_passed = False
            vwap_slope_angle = trade.vwap_slope_angle
            if vwap_slope_angle is not None:
                vwap_slope_passed = vwap_slope_angle >= 45.0
            
            # Check candle size
            candle_size_passed = False
            candle_size_ratio = trade.candle_size_ratio
            if candle_size_ratio is not None:
                candle_size_passed = candle_size_ratio < 7.5
            elif trade.alert_time and trade.alert_time.hour == 10 and trade.alert_time.minute == 15:
                candle_size_passed = True  # Skipped for 10:15 alerts
            
            # Check option data
            has_option_contract = bool(trade.option_contract)
            has_instrument_key = bool(trade.instrument_key)
            
            # Try to fetch option LTP
            option_ltp_available = False
            option_ltp_value = None
            if trade.instrument_key:
                try:
                    option_quote = upstox_service.get_market_quote_by_key(trade.instrument_key)
                    if option_quote and option_quote.get('last_price', 0) > 0:
                        option_ltp_available = True
                        option_ltp_value = float(option_quote.get('last_price', 0))
                except Exception as e:
                    pass
            
            # Determine if all conditions are met
            all_conditions_met = (
                is_before_3pm and
                can_enter_by_index and
                vwap_slope_passed and
                candle_size_passed and
                has_option_contract and
                has_instrument_key and
                option_ltp_available
            )
            
            diagnostics.append({
                "stock_name": stock_name,
                "status": trade.status,
                "alert_time": trade.alert_time.strftime("%H:%M:%S") if trade.alert_time else None,
                "option_contract": trade.option_contract,
                "instrument_key": trade.instrument_key,
                "conditions": {
                    "time_before_3pm": {
                        "passed": is_before_3pm,
                        "value": now.strftime("%H:%M:%S")
                    },
                    "index_trends_aligned": {
                        "passed": can_enter_by_index,
                        "nifty_trend": nifty_trend,
                        "banknifty_trend": banknifty_trend,
                        "option_type": option_type,
                        "reason": "Both bullish" if both_bullish else ("Both bearish" if both_bearish else "Opposite directions")
                    },
                    "vwap_slope": {
                        "passed": vwap_slope_passed,
                        "angle": float(vwap_slope_angle) if vwap_slope_angle is not None else None,
                        "status": trade.vwap_slope_status
                    },
                    "candle_size": {
                        "passed": candle_size_passed,
                        "ratio": float(candle_size_ratio) if candle_size_ratio is not None else None,
                        "status": trade.candle_size_status
                    },
                    "has_option_contract": {
                        "passed": has_option_contract,
                        "value": trade.option_contract
                    },
                    "has_instrument_key": {
                        "passed": has_instrument_key,
                        "value": trade.instrument_key
                    },
                    "option_ltp_available": {
                        "passed": option_ltp_available,
                        "value": option_ltp_value
                    }
                },
                "all_conditions_met": all_conditions_met,
                "current_stock_ltp": float(current_stock_ltp) if current_stock_ltp else None,
                "current_stock_vwap": float(current_stock_vwap) if current_stock_vwap else None
            })
        
        return {
            "success": True,
            "timestamp": now.isoformat(),
            "total_bearish_trades": len(bearish_trades),
            "diagnostics": diagnostics
        }
        
    except Exception as e:
        logger.error(f"Error diagnosing bearish trades: {str(e)}")
        import traceback
        traceback.print_exc()
        return {
            "success": False,
            "message": f"Error: {str(e)}",
            "diagnostics": []
        }


@router.get("/historical-market-data")
async def get_historical_market_data(
    date: str = Query(None, description="Date in YYYY-MM-DD format (defaults to today)"),
    stock_name: str = Query(None, description="Filter by stock name"),
    db: Session = Depends(get_db)
):
    """
    Get historical market data for a specific date (defaults to today)
    Shows snapshots of market data captured at various times during the day
    """
    try:
        import pytz
        from datetime import timedelta
        from backend.models.trading import HistoricalMarketData
        
        ist = pytz.timezone('Asia/Kolkata')
        now = datetime.now(ist)
        
        # Parse date or use today
        if date:
            try:
                target_date = datetime.strptime(date, '%Y-%m-%d')
                target_date = ist.localize(target_date.replace(hour=0, minute=0, second=0, microsecond=0))
            except ValueError:
                return {
                    "success": False,
                    "message": "Invalid date format. Use YYYY-MM-DD",
                    "data": []
                }
        else:
            target_date = now.replace(hour=0, minute=0, second=0, microsecond=0)
        
        # Build query
        query = db.query(HistoricalMarketData).filter(
            HistoricalMarketData.scan_date >= target_date,
            HistoricalMarketData.scan_date < target_date + timedelta(days=1)
        )
        
        # Filter by stock name if provided
        if stock_name:
            query = query.filter(HistoricalMarketData.stock_name == stock_name.upper())
        
        # Order by scan_date (time) ascending
        records = query.order_by(HistoricalMarketData.scan_date.asc()).all()
        
        # Format data
        historical_data = []
        for record in records:
            historical_data.append({
                "id": record.id,
                "stock_name": record.stock_name,
                "stock_vwap": float(record.stock_vwap) if record.stock_vwap else None,
                "stock_ltp": float(record.stock_ltp) if record.stock_ltp else None,
                "vwap_slope_angle": float(record.vwap_slope_angle) if record.vwap_slope_angle else None,
                "vwap_slope_status": record.vwap_slope_status,
                "vwap_slope_direction": record.vwap_slope_direction,
                "vwap_slope_time": record.vwap_slope_time.strftime('%Y-%m-%d %H:%M:%S') if record.vwap_slope_time else None,
                "option_contract": record.option_contract,
                "option_instrument_key": record.option_instrument_key,
                "option_ltp": float(record.option_ltp) if record.option_ltp else None,
                "scan_date": record.scan_date.strftime('%Y-%m-%d %H:%M:%S') if record.scan_date else None,
                "scan_time": record.scan_time,
                "created_at": record.created_at.strftime('%Y-%m-%d %H:%M:%S') if record.created_at else None
            })
        
        # Group by stock for summary
        stocks_summary = {}
        for record in records:
            stock = record.stock_name
            if stock not in stocks_summary:
                stocks_summary[stock] = {
                    "stock_name": stock,
                    "total_records": 0,
                    "scan_times": []
                }
            stocks_summary[stock]["total_records"] += 1
            if record.scan_time:
                stocks_summary[stock]["scan_times"].append(record.scan_time)
        
        return {
            "success": True,
            "date": target_date.strftime('%Y-%m-%d'),
            "total_records": len(historical_data),
            "stocks_count": len(stocks_summary),
            "stocks_summary": list(stocks_summary.values()),
            "data": historical_data
        }
        
    except Exception as e:
        logger.error(f"Error fetching historical market data: {str(e)}")
        import traceback
        traceback.print_exc()
        return {
            "success": False,
            "message": f"Error: {str(e)}",
            "data": []
        }


@router.get("/analyze-historical-vwap-slope")
async def analyze_historical_vwap_slope(
    date: str = Query(None, description="Date in YYYY-MM-DD format (defaults to today)"),
    stock_name: str = Query(None, description="Filter by stock name"),
    db: Session = Depends(get_db)
):
    """
    Analyze VWAP slope and candle size from historical_market_data table
    Shows data for each stock at every relevant cycle time in tabular format
    """
    try:
        import pytz
        from datetime import timedelta
        
        ist = pytz.timezone('Asia/Kolkata')
        now = datetime.now(ist)
        
        # Parse date or use today
        if date:
            try:
                target_date = datetime.strptime(date, '%Y-%m-%d')
                target_date = ist.localize(target_date.replace(hour=0, minute=0, second=0, microsecond=0))
            except ValueError:
                return {
                    "success": False,
                    "message": "Invalid date format. Use YYYY-MM-DD",
                    "data": []
                }
        else:
            target_date = now.replace(hour=0, minute=0, second=0, microsecond=0)
        
        # Query historical market data for the target date
        query = db.query(HistoricalMarketData).filter(
            HistoricalMarketData.scan_date >= target_date,
            HistoricalMarketData.scan_date < target_date + timedelta(days=1)
        )
        
        # Filter by stock name if provided
        if stock_name:
            query = query.filter(HistoricalMarketData.stock_name == stock_name.upper())
        
        # Order by stock_name, then scan_date (time) ascending
        records = query.order_by(HistoricalMarketData.stock_name.asc(), HistoricalMarketData.scan_date.asc()).all()
        
        if not records:
            return {
                "success": True,
                "message": f"No historical data found for {target_date.strftime('%Y-%m-%d')}",
                "date": target_date.strftime('%Y-%m-%d'),
                "data": []
            }
        
        # Group data by stock and cycle time
        analysis_data = {}
        
        # Define cycle times for reference
        cycle_times = {
            "10:15 am": "10:15 AM (Webhook)",
            "10:30 am": "10:30 AM (Cycle 1)",
            "11:15 am": "11:15 AM (Cycle 2)",
            "12:15 pm": "12:15 PM (Cycle 3)",
            "01:15 pm": "01:15 PM (Cycle 4)",
            "02:15 pm": "02:15 PM (Cycle 5)",
            "03:15 pm": "03:15 PM (Hourly Update)",
            "03:25 pm": "03:25 PM (EOD)"
        }
        
        for record in records:
            stock = record.stock_name
            scan_time = record.scan_time or "Unknown"
            
            if stock not in analysis_data:
                analysis_data[stock] = {}
            
            # Use scan_time as key (e.g., "10:15 am", "10:30 am")
            cycle_key = scan_time.lower() if scan_time else "unknown"
            
            analysis_data[stock][cycle_key] = {
                "scan_time": scan_time,
                "scan_date": record.scan_date.strftime('%Y-%m-%d %H:%M:%S') if record.scan_date else None,
                "vwap_slope_angle": float(record.vwap_slope_angle) if record.vwap_slope_angle else None,
                "vwap_slope_status": record.vwap_slope_status,
                "vwap_slope_direction": record.vwap_slope_direction,
                "vwap_slope_time": record.vwap_slope_time.strftime('%Y-%m-%d %H:%M:%S') if record.vwap_slope_time else None,
                "stock_vwap": float(record.stock_vwap) if record.stock_vwap else None,
                "stock_ltp": float(record.stock_ltp) if record.stock_ltp else None,
                "option_ltp": float(record.option_ltp) if record.option_ltp else None,
                "option_contract": record.option_contract
            }
        
        # Format data for tabular display
        table_data = []
        cycle_order = ["10:15 am", "10:30 am", "11:15 am", "12:15 pm", "01:15 pm", "02:15 pm", "03:15 pm", "03:25 pm"]
        
        for stock in sorted(analysis_data.keys()):
            stock_data = analysis_data[stock]
            row = {
                "stock_name": stock,
                "cycles": {}
            }
            
            for cycle_time in cycle_order:
                if cycle_time in stock_data:
                    cycle_info = stock_data[cycle_time]
                    row["cycles"][cycle_time] = {
                        "vwap_slope_angle": cycle_info["vwap_slope_angle"],
                        "vwap_slope_status": cycle_info["vwap_slope_status"],
                        "vwap_slope_direction": cycle_info["vwap_slope_direction"],
                        "stock_vwap": cycle_info["stock_vwap"],
                        "stock_ltp": cycle_info["stock_ltp"],
                        "option_ltp": cycle_info["option_ltp"],
                        "scan_time": cycle_info["scan_time"]
                    }
                else:
                    row["cycles"][cycle_time] = None
            
            table_data.append(row)
        
        # Also get candle size data from intraday_stock_options for comparison
        candle_size_data = {}
        trades = db.query(IntradayStockOption).filter(
            IntradayStockOption.trade_date >= target_date,
            IntradayStockOption.trade_date < target_date + timedelta(days=1)
        ).all()
        
        for trade in trades:
            stock = trade.stock_name
            if stock not in candle_size_data:
                candle_size_data[stock] = {
                    "candle_size_ratio": trade.candle_size_ratio,
                    "candle_size_status": trade.candle_size_status,
                    "vwap_slope_angle": trade.vwap_slope_angle,
                    "vwap_slope_status": trade.vwap_slope_status,
                    "vwap_slope_direction": trade.vwap_slope_direction
                }
        
        # Merge candle size data from trade records into table
        for row in table_data:
            stock = row["stock_name"]
            if stock in candle_size_data:
                row["candle_size_ratio"] = candle_size_data[stock]["candle_size_ratio"]
                row["candle_size_status"] = candle_size_data[stock]["candle_size_status"]
                # Also add latest VWAP slope from trade record if not in historical
                if not any(cycle and cycle.get("vwap_slope_angle") for cycle in row["cycles"].values()):
                    row["latest_vwap_slope_angle"] = candle_size_data[stock]["vwap_slope_angle"]
                    row["latest_vwap_slope_status"] = candle_size_data[stock]["vwap_slope_status"]
                    row["latest_vwap_slope_direction"] = candle_size_data[stock]["vwap_slope_direction"]
        
        # Create formatted table for display
        def format_cycle_data(cycle_data):
            """Helper function to format cycle data for display"""
            if not cycle_data:
                return None
            
            vwap_angle = cycle_data.get("vwap_slope_angle")
            vwap_status = cycle_data.get("vwap_slope_status")
            vwap_direction = cycle_data.get("vwap_slope_direction")
            
            vwap_display = None
            if vwap_angle is not None:
                vwap_display = f"{vwap_angle:.2f}° ({vwap_status or 'N/A'})"
            elif vwap_status:
                vwap_display = vwap_status
            else:
                vwap_display = "N/A"
            
            return {
                "vwap_slope": {
                    "angle": vwap_angle,
                    "status": vwap_status,
                    "direction": vwap_direction,
                    "display": vwap_display
                },
                "stock_vwap": cycle_data.get("stock_vwap"),
                "stock_ltp": cycle_data.get("stock_ltp"),
                "option_ltp": cycle_data.get("option_ltp")
            }
        
        formatted_table = []
        for row in table_data:
            candle_ratio = row.get("candle_size_ratio")
            candle_status = row.get("candle_size_status")
            candle_display = None
            if candle_ratio is not None:
                candle_display = f"{candle_ratio:.2f}x ({candle_status or 'N/A'})"
            elif candle_status:
                candle_display = candle_status
            else:
                candle_display = "N/A"
            
            stock_row = {
                "stock_name": row["stock_name"],
                "candle_size": {
                    "ratio": candle_ratio,
                    "status": candle_status,
                    "display": candle_display
                },
                "10:15 AM": format_cycle_data(row["cycles"].get("10:15 am")),
                "10:30 AM": format_cycle_data(row["cycles"].get("10:30 am")),
                "11:15 AM": format_cycle_data(row["cycles"].get("11:15 am")),
                "12:15 PM": format_cycle_data(row["cycles"].get("12:15 pm")),
                "01:15 PM": format_cycle_data(row["cycles"].get("01:15 pm")),
                "02:15 PM": format_cycle_data(row["cycles"].get("02:15 pm")),
                "03:15 PM": format_cycle_data(row["cycles"].get("03:15 pm")),
                "03:25 PM": format_cycle_data(row["cycles"].get("03:25 pm"))
            }
            formatted_table.append(stock_row)
        
        return {
            "success": True,
            "date": target_date.strftime('%Y-%m-%d'),
            "total_stocks": len(table_data),
            "cycle_times": cycle_times,
            "formatted_table": formatted_table,
            "detailed_data": table_data
        }
        
    except Exception as e:
        logger.error(f"Error analyzing historical VWAP slope: {str(e)}")
        import traceback
        traceback.print_exc()
        return {
            "success": False,
            "error": str(e)
        }


@router.post("/insert-jan6-index-prices")
async def insert_jan6_index_prices(db: Session = Depends(get_db)):
    """
    One-time endpoint to insert index prices for January 6th, 2026 at 9:15 AM
    NIFTY50: 26189.70
    BANKNIFTY: 59957.80
    """
    try:
        import pytz
        from models.trading import IndexPrice
        
        ist = pytz.timezone('Asia/Kolkata')
        price_time = ist.localize(datetime(2026, 1, 6, 9, 15, 0))
        
        results = {}
        
        # Check if NIFTY50 record already exists
        existing_nifty = db.query(IndexPrice).filter(
            IndexPrice.index_name == 'NIFTY50',
            IndexPrice.price_time == price_time
        ).first()
        
        if existing_nifty:
            # Update existing record
            existing_nifty.ltp = 26189.70
            existing_nifty.day_open = 26189.70
            existing_nifty.trend = 'neutral'
            existing_nifty.change = 0.0
            existing_nifty.change_percent = 0.0
            existing_nifty.is_special_time = True
            existing_nifty.is_market_open = True
            results["nifty50"] = "updated"
            logger.info(f"✅ Updated existing NIFTY50 record (ID: {existing_nifty.id})")
        else:
            # Insert new NIFTY50 record
            nifty_price = IndexPrice(
                index_name='NIFTY50',
                instrument_key='NSE_INDEX|Nifty 50',
                ltp=26189.70,
                day_open=26189.70,
                close_price=None,
                trend='neutral',
                change=0.0,
                change_percent=0.0,
                price_time=price_time,
                is_market_open=True,
                is_special_time=True
            )
            db.add(nifty_price)
            results["nifty50"] = "inserted"
            logger.info(f"✅ Inserted NIFTY50: ₹26189.70 at {price_time.strftime('%Y-%m-%d %H:%M:%S IST')}")
        
        # Check if BANKNIFTY record already exists
        existing_banknifty = db.query(IndexPrice).filter(
            IndexPrice.index_name == 'BANKNIFTY',
            IndexPrice.price_time == price_time
        ).first()
        
        if existing_banknifty:
            # Update existing record
            existing_banknifty.ltp = 59957.80
            existing_banknifty.day_open = 59957.80
            existing_banknifty.trend = 'neutral'
            existing_banknifty.change = 0.0
            existing_banknifty.change_percent = 0.0
            existing_banknifty.is_special_time = True
            existing_banknifty.is_market_open = True
            results["banknifty"] = "updated"
            logger.info(f"✅ Updated existing BANKNIFTY record (ID: {existing_banknifty.id})")
        else:
            # Insert new BANKNIFTY record
            banknifty_price = IndexPrice(
                index_name='BANKNIFTY',
                instrument_key='NSE_INDEX|Nifty Bank',
                ltp=59957.80,
                day_open=59957.80,
                close_price=None,
                trend='neutral',
                change=0.0,
                change_percent=0.0,
                price_time=price_time,
                is_market_open=True,
                is_special_time=True
            )
            db.add(banknifty_price)
            results["banknifty"] = "inserted"
            logger.info(f"✅ Inserted BANKNIFTY: ₹59957.80 at {price_time.strftime('%Y-%m-%d %H:%M:%S IST')}")
        
        # Commit changes
        db.commit()
        
        return {
            "success": True,
            "message": "Index prices inserted/updated for January 6th, 2026 at 9:15 AM",
            "price_time": price_time.strftime('%Y-%m-%d %H:%M:%S IST'),
            "prices": {
                "NIFTY50": 26189.70,
                "BANKNIFTY": 59957.80
            },
            "results": results
        }
        
    except Exception as e:
        db.rollback()
        logger.error(f"Error inserting index prices: {str(e)}")
        import traceback
        traceback.print_exc()
        return {
            "success": False,
            "error": str(e)
        }


@router.post("/insert-jan6-index-prices-330pm")
async def insert_jan6_index_prices_330pm(db: Session = Depends(get_db)):
    """
    One-time endpoint to insert index prices for January 6th, 2026 at 3:30 PM
    NIFTY50: Open=26189.70, Close/LTP=26178.70, Trend=bearish
    BANKNIFTY: Open=59957.80, Close/LTP=60118.40, Trend=bullish
    """
    try:
        import pytz
        from models.trading import IndexPrice
        
        ist = pytz.timezone('Asia/Kolkata')
        price_time = ist.localize(datetime(2026, 1, 6, 15, 30, 0))
        
        results = {}
        
        # NIFTY50 data
        nifty_open = 26189.70
        nifty_close = 26178.70
        nifty_ltp = 26178.70
        nifty_change = nifty_ltp - nifty_open  # -11.00
        nifty_change_percent = (nifty_change / nifty_open * 100) if nifty_open > 0 else 0  # -0.042%
        nifty_trend = 'bearish'  # LTP < Open
        
        # Check if NIFTY50 record already exists
        existing_nifty = db.query(IndexPrice).filter(
            IndexPrice.index_name == 'NIFTY50',
            IndexPrice.price_time == price_time
        ).first()
        
        if existing_nifty:
            # Update existing record
            existing_nifty.ltp = nifty_ltp
            existing_nifty.day_open = nifty_open
            existing_nifty.close_price = nifty_close
            existing_nifty.trend = nifty_trend
            existing_nifty.change = nifty_change
            existing_nifty.change_percent = nifty_change_percent
            existing_nifty.is_special_time = True
            existing_nifty.is_market_open = True
            results["nifty50"] = "updated"
            logger.info(f"✅ Updated existing NIFTY50 record (ID: {existing_nifty.id})")
        else:
            # Insert new NIFTY50 record
            nifty_price = IndexPrice(
                index_name='NIFTY50',
                instrument_key='NSE_INDEX|Nifty 50',
                ltp=nifty_ltp,
                day_open=nifty_open,
                close_price=nifty_close,
                trend=nifty_trend,
                change=nifty_change,
                change_percent=nifty_change_percent,
                price_time=price_time,
                is_market_open=True,
                is_special_time=True
            )
            db.add(nifty_price)
            results["nifty50"] = "inserted"
            logger.info(f"✅ Inserted NIFTY50: Open=₹{nifty_open:.2f}, Close=₹{nifty_close:.2f}, Trend={nifty_trend} at {price_time.strftime('%Y-%m-%d %H:%M:%S IST')}")
        
        # BANKNIFTY data
        banknifty_open = 59957.80
        banknifty_close = 60118.40
        banknifty_ltp = 60118.40
        banknifty_change = banknifty_ltp - banknifty_open  # 160.60
        banknifty_change_percent = (banknifty_change / banknifty_open * 100) if banknifty_open > 0 else 0  # 0.268%
        banknifty_trend = 'bullish'  # LTP > Open
        
        # Check if BANKNIFTY record already exists
        existing_banknifty = db.query(IndexPrice).filter(
            IndexPrice.index_name == 'BANKNIFTY',
            IndexPrice.price_time == price_time
        ).first()
        
        if existing_banknifty:
            # Update existing record
            existing_banknifty.ltp = banknifty_ltp
            existing_banknifty.day_open = banknifty_open
            existing_banknifty.close_price = banknifty_close
            existing_banknifty.trend = banknifty_trend
            existing_banknifty.change = banknifty_change
            existing_banknifty.change_percent = banknifty_change_percent
            existing_banknifty.is_special_time = True
            existing_banknifty.is_market_open = True
            results["banknifty"] = "updated"
            logger.info(f"✅ Updated existing BANKNIFTY record (ID: {existing_banknifty.id})")
        else:
            # Insert new BANKNIFTY record
            banknifty_price = IndexPrice(
                index_name='BANKNIFTY',
                instrument_key='NSE_INDEX|Nifty Bank',
                ltp=banknifty_ltp,
                day_open=banknifty_open,
                close_price=banknifty_close,
                trend=banknifty_trend,
                change=banknifty_change,
                change_percent=banknifty_change_percent,
                price_time=price_time,
                is_market_open=True,
                is_special_time=True
            )
            db.add(banknifty_price)
            results["banknifty"] = "inserted"
            logger.info(f"✅ Inserted BANKNIFTY: Open=₹{banknifty_open:.2f}, Close=₹{banknifty_close:.2f}, Trend={banknifty_trend} at {price_time.strftime('%Y-%m-%d %H:%M:%S IST')}")
        
        # Commit changes
        db.commit()
        
        return {
            "success": True,
            "message": "Index prices inserted/updated for January 6th, 2026 at 3:30 PM",
            "price_time": price_time.strftime('%Y-%m-%d %H:%M:%S IST'),
            "prices": {
                "NIFTY50": {
                    "open": nifty_open,
                    "close": nifty_close,
                    "ltp": nifty_ltp,
                    "trend": nifty_trend,
                    "change": nifty_change,
                    "change_percent": round(nifty_change_percent, 3)
                },
                "BANKNIFTY": {
                    "open": banknifty_open,
                    "close": banknifty_close,
                    "ltp": banknifty_ltp,
                    "trend": banknifty_trend,
                    "change": banknifty_change,
                    "change_percent": round(banknifty_change_percent, 3)
                }
            },
            "results": results
        }
        
    except Exception as e:
        db.rollback()
        logger.error(f"Error inserting index prices: {str(e)}")
        import traceback
        traceback.print_exc()
        return {
            "success": False,
            "error": str(e)
        }
