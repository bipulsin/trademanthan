"""
Delta Exchange Options API Client
Extends the existing DeltaAPI for Bitcoin options trading
"""

import asyncio
import websockets
import json
import time
import logging
from typing import Dict, List, Any, Optional, Tuple
from datetime import datetime, timedelta
import pandas as pd
import sys
import os

# Add parent directory to path to import existing delta_api
sys.path.append(os.path.join(os.path.dirname(__file__), '..', '..', 'trademanthan', 'backend'))
try:
    from delta_api import DeltaAPI, TradingParams
except ImportError:
    # Fallback for different path structures
    sys.path.append(os.path.join(os.path.dirname(__file__), '..', '..', 'trademanthan'))
    from backend.delta_api import DeltaAPI, TradingParams

logger = logging.getLogger(__name__)

class DeltaOptionsAPI(DeltaAPI):
    """
    Extended Delta API client specifically for Bitcoin options trading
    """
    
    def __init__(self, trading_params: TradingParams, config: Dict[str, Any] = None):
        super().__init__(trading_params)
        self.config = config or {}
        self.websocket = None
        self.websocket_connected = False
        self.underlying_price = None
        self.options_data = {}
        self.products_cache = {}
        self.products_cache_time = 0
        self.cache_duration = 300  # 5 minutes
        
    async def connect_websocket(self):
        """Connect to Delta Exchange WebSocket for real-time data"""
        try:
            # Use the same base URL as the REST API but with wss://
            base_url = self.api_url.replace("https://", "wss://").replace("http://", "ws://")
            ws_url = f"{base_url}/ws"
            
            logger.info(f"🔌 Attempting WebSocket connection to: {ws_url}")
            self.websocket = await websockets.connect(ws_url)
            self.websocket_connected = True
            logger.info("✅ WebSocket connected to Delta Exchange")
            
            # Subscribe to BTCUSD futures for underlying price (not BTCUSD.P)
            subscribe_msg = {
                "type": "subscribe",
                "payload": {
                    "channels": [
                        {"name": "ticker", "symbols": ["BTCUSD"]}
                    ]
                }
            }
            await self.websocket.send(json.dumps(subscribe_msg))
            logger.info("📡 Subscribed to BTCUSD ticker")
            
        except Exception as e:
            logger.error(f"❌ Failed to connect WebSocket: {e}")
            logger.warning("⚠️ Continuing without WebSocket - will use REST API polling")
            self.websocket_connected = False
    
    async def disconnect_websocket(self):
        """Disconnect from WebSocket"""
        if self.websocket:
            await self.websocket.close()
            self.websocket_connected = False
            logger.info("🔌 WebSocket disconnected")
    
    async def listen_websocket(self):
        """Listen to WebSocket messages for real-time data"""
        try:
            async for message in self.websocket:
                data = json.loads(message)
                
                if data.get("type") == "ticker":
                    ticker_data = data.get("payload", {})
                    if ticker_data.get("symbol") == "BTCUSD.P":
                        self.underlying_price = float(ticker_data.get("close", 0))
                        logger.debug(f"📊 BTCUSD.P price: ${self.underlying_price}")
                        
        except websockets.exceptions.ConnectionClosed:
            logger.warning("⚠️ WebSocket connection closed")
            self.websocket_connected = False
        except Exception as e:
            logger.error(f"❌ WebSocket error: {e}")
            self.websocket_connected = False
    
    def get_btc_futures_data(self, interval: str = "90m", limit: int = 100) -> pd.DataFrame:
        """
        Get BTCUSD.P futures data for SuperTrend calculation
        
        Args:
            interval: Time interval (default: 90m)
            limit: Number of candles
            
        Returns:
            DataFrame with OHLC data
        """
        try:
            # Get candles from Delta Exchange
            candles = self.get_candles(
                symbol="BTCUSD",
                interval=interval,
                limit=limit
            )
            
            if not candles:
                logger.error("❌ No candles data received")
                return pd.DataFrame()
            
            # Convert to DataFrame
            df = pd.DataFrame(candles)
            df['timestamp'] = pd.to_datetime(df['time'], unit='s')
            df.set_index('timestamp', inplace=True)
            
            # Rename columns to standard format
            df = df.rename(columns={
                'open': 'open',
                'high': 'high', 
                'low': 'low',
                'close': 'close',
                'volume': 'volume'
            })
            
            logger.info(f"📊 Retrieved {len(df)} BTCUSD.P candles")
            return df[['open', 'high', 'low', 'close', 'volume']]
            
        except Exception as e:
            logger.error(f"❌ Failed to get BTC futures data: {e}")
            return pd.DataFrame()
    
    def get_btc_options(self, expiry_date: str = None) -> List[Dict[str, Any]]:
        """
        Get Bitcoin options for a specific expiry date
        
        Args:
            expiry_date: Expiry date in YYYY-MM-DD format
            
        Returns:
            List of option contracts
        """
        try:
            # Get all products first
            products = self.get_all_products()
            
            # Filter for Bitcoin options
            btc_options = []
            for product in products:
                symbol = product.get('symbol', '').upper()
                
                # Check if it's a BTC option (C-BTC- or P-BTC- format)
                if 'BTC' in symbol and ('C-BTC-' in symbol or 'P-BTC-' in symbol):
                    
                    # Check expiry date if specified
                    if expiry_date:
                        # Handle both YYMMDD format (230925) and YYYY-MM-DD format
                        if expiry_date in symbol or expiry_date in product.get('settlement_time', ''):
                            btc_options.append(product)
                    else:
                        btc_options.append(product)
            
            logger.info(f"📋 Found {len(btc_options)} Bitcoin options")
            return btc_options
            
        except Exception as e:
            logger.error(f"❌ Failed to get Bitcoin options: {e}")
            return []
    
    def get_option_chain(self, underlying_price: float, option_type: str = "both") -> List[Dict[str, Any]]:
        """
        Get option chain around current underlying price
        
        Args:
            underlying_price: Current BTC price
            option_type: "call", "put", or "both"
            
        Returns:
            List of option contracts with strikes around current price
        """
        try:
            # Get all Bitcoin options
            all_options = self.get_btc_options()
            
            # Filter by option type
            if option_type != "both":
                filtered_options = [
                    opt for opt in all_options 
                    if opt.get('contract_type') == f"{option_type}_options"
                ]
            else:
                filtered_options = all_options
            
            # Sort by strike price and find options around current price
            filtered_options.sort(key=lambda x: float(x.get('strike_price', 0)))
            
            # Find options within reasonable range (±20% of current price)
            price_range = underlying_price * 0.2
            relevant_options = [
                opt for opt in filtered_options
                if abs(float(opt.get('strike_price', 0)) - underlying_price) <= price_range
            ]
            
            logger.info(f"📊 Found {len(relevant_options)} relevant options around ${underlying_price}")
            return relevant_options
            
        except Exception as e:
            logger.error(f"❌ Failed to get option chain: {e}")
            return []
    
    def find_nearest_strike(self, options: List[Dict[str, Any]], target_price: float, 
                          option_type: str, direction: str = "at_or_below") -> Optional[Dict[str, Any]]:
        """
        Find the nearest strike option to target price
        
        Args:
            options: List of option contracts
            target_price: Target price (SuperTrend level)
            option_type: "call" or "put"
            direction: "at_or_below" for puts, "at_or_above" for calls
            
        Returns:
            Option contract or None
        """
        try:
            # Filter by option type using symbol format
            filtered_options = []
            for opt in options:
                symbol = opt.get('symbol', '').upper()
                if option_type == 'call' and 'C-BTC-' in symbol:
                    filtered_options.append(opt)
                elif option_type == 'put' and 'P-BTC-' in symbol:
                    filtered_options.append(opt)
            
            if not filtered_options:
                logger.warning(f"⚠️ No {option_type} options found")
                return None
            
            # Sort by strike price
            filtered_options.sort(key=lambda x: float(x.get('strike_price', 0)))
            
            if direction == "at_or_below":
                # For puts, find strike at or below target price
                suitable_options = [
                    opt for opt in filtered_options
                    if float(opt.get('strike_price', 0)) <= target_price
                ]
                if suitable_options:
                    return suitable_options[-1]  # Highest strike at or below
            else:
                # For calls, find strike at or above target price
                suitable_options = [
                    opt for opt in filtered_options
                    if float(opt.get('strike_price', 0)) >= target_price
                ]
                if suitable_options:
                    return suitable_options[0]  # Lowest strike at or above
            
            # If no suitable option found, return closest one
            closest_option = min(
                filtered_options,
                key=lambda x: abs(float(x.get('strike_price', 0)) - target_price)
            )
            
            logger.info(f"🎯 Selected {option_type} option: Strike ${closest_option.get('strike_price')}")
            return closest_option
            
        except Exception as e:
            logger.error(f"❌ Failed to find nearest strike: {e}")
            return None
    
    def get_option_premium(self, option_id: int) -> Optional[float]:
        """
        Get current premium for an option contract
        
        Args:
            option_id: Option contract ID
            
        Returns:
            Current premium or None
        """
        try:
            # Get ticker data for the option
            url = f"{self.api_url}/v2/tickers/{option_id}"
            response = self.session.get(url, timeout=10)
            response.raise_for_status()
            
            data = response.json()
            if data.get("success") and data.get("result"):
                premium = float(data["result"].get("mark_price", 0))
                logger.debug(f"💰 Option {option_id} premium: ${premium}")
                return premium
            
            return None
            
        except Exception as e:
            logger.error(f"❌ Failed to get option premium: {e}")
            return None
    
    def check_premium_threshold(self, premium: float, config: Dict[str, Any]) -> bool:
        """
        Check if premium meets minimum threshold requirements
        
        Args:
            premium: Option premium
            config: Configuration with threshold settings
            
        Returns:
            True if premium is acceptable
        """
        min_threshold = config.get('premium_filters', {}).get('min_premium_threshold', 250.0)
        max_threshold = config.get('premium_filters', {}).get('max_premium_threshold', 300.0)
        
        if premium < min_threshold:
            logger.warning(f"⚠️ Premium ${premium} below minimum threshold ${min_threshold}")
            return False
        
        if premium > max_threshold:
            logger.warning(f"⚠️ Premium ${premium} above maximum threshold ${max_threshold}")
            return False
        
        return True
    
    def get_expiry_dates(self) -> List[str]:
        """
        Get available expiry dates for Bitcoin options
        
        Returns:
            List of expiry dates in YYYY-MM-DD format
        """
        try:
            options = self.get_btc_options()
            expiry_dates = set()
            
            for option in options:
                settlement_time = option.get('settlement_time', '')
                if settlement_time:
                    # Extract date from settlement_time
                    expiry_date = settlement_time.split('T')[0]
                    expiry_dates.add(expiry_date)
            
            sorted_dates = sorted(list(expiry_dates))
            logger.info(f"📅 Available expiry dates: {sorted_dates}")
            return sorted_dates
            
        except Exception as e:
            logger.error(f"❌ Failed to get expiry dates: {e}")
            return []
    
    def select_expiry_date(self, config: Dict[str, Any]) -> str:
        """
        Select appropriate expiry date based on configuration
        
        Args:
            config: Configuration with expiry preferences
            
        Returns:
            Selected expiry date
        """
        try:
            expiry_dates = self.get_expiry_dates()
            if not expiry_dates:
                raise Exception("No expiry dates available")
            
            # Prefer 23/09/2025 (230925) format for SuperTrend strategy
            preferred_expiry = "230925"  # 23/09/2025 in YYMMDD format
            if preferred_expiry in expiry_dates:
                logger.info(f"📅 Selected preferred expiry: {preferred_expiry} (23/09/2025)")
                return preferred_expiry
            
            today = datetime.now().date()
            tomorrow = today + timedelta(days=1)
            
            # Check for 0DTE (today's expiry)
            today_str = today.strftime('%Y-%m-%d')
            if today_str in expiry_dates and config.get('expiry', {}).get('prefer_0dte', True):
                # Check if premium is acceptable for 0DTE
                # This would require checking actual premiums, simplified here
                logger.info(f"📅 Selected 0DTE expiry: {today_str}")
                return today_str
            
            # Check for 1DTE (tomorrow's expiry)
            tomorrow_str = tomorrow.strftime('%Y-%m-%d')
            if tomorrow_str in expiry_dates and config.get('expiry', {}).get('fallback_to_1dte', True):
                logger.info(f"📅 Selected 1DTE expiry: {tomorrow_str}")
                return tomorrow_str
            
            # Fallback to earliest available expiry
            selected_date = expiry_dates[0]
            logger.info(f"📅 Selected earliest expiry: {selected_date}")
            return selected_date
            
        except Exception as e:
            logger.error(f"❌ Failed to select expiry date: {e}")
            return ""
    
    async def place_option_order(self, option_contract: Dict[str, Any], 
                               side: str, quantity: int, price: float = None) -> Dict[str, Any]:
        """
        Place an option order
        
        Args:
            option_contract: Option contract details
            side: "buy" or "sell"
            quantity: Number of contracts
            price: Limit price (optional)
            
        Returns:
            Order result
        """
        try:
            product_id = option_contract.get('id')
            order_type = "limit_order" if price else "market_order"
            
            order_data = {
                'product_id': product_id,
                'side': side,
                'order_type': order_type,
                'size': quantity,
                'time_in_force': 'gtc'
            }
            
            if price:
                order_data['limit_price'] = price
            
            # Use the parent class place_order method
            result = self.place_order(
                symbol=option_contract.get('symbol'),
                side=side,
                qty=quantity,
                order_type=order_type,
                price=price
            )
            
            logger.info(f"✅ Option order placed: {side} {quantity} {option_contract.get('symbol')}")
            return result
            
        except Exception as e:
            logger.error(f"❌ Failed to place option order: {e}")
            raise
    
    def calculate_position_size(self, config: Dict[str, Any], available_margin: float) -> int:
        """
        Calculate position size based on configuration
        
        Args:
            config: Configuration with position sizing rules
            available_margin: Available margin for trading
            
        Returns:
            Number of contracts to trade
        """
        try:
            position_config = config.get('position_sizing', {})
            method = position_config.get('position_size_method', 'fixed_premium')
            target_premium = position_config.get('target_premium', 500.0)
            max_contracts = position_config.get('max_total_contracts', 10)
            max_margin_util = position_config.get('max_margin_utilization', 0.8)
            
            if method == 'fixed_premium':
                # Simple calculation based on target premium
                # This is simplified - in reality, you'd need to calculate based on option price
                contracts = min(max_contracts, int(target_premium / 100))  # Assuming $100 per contract
            else:
                # Percentage of available margin
                margin_to_use = available_margin * max_margin_util
                contracts = min(max_contracts, int(margin_to_use / 1000))  # Assuming $1000 margin per contract
            
            logger.info(f"📊 Calculated position size: {contracts} contracts")
            return max(1, contracts)  # At least 1 contract
            
        except Exception as e:
            logger.error(f"❌ Failed to calculate position size: {e}")
            return 1
    
    def get_option_candle_data(self, option_id: str) -> Optional[Dict[str, Any]]:
        """
        Get option candle data for detailed logging
        
        Args:
            option_id: Option contract ID
            
        Returns:
            Dictionary with option candle data or None
        """
        try:
            # For paper trading, generate mock candle data
            if hasattr(self, 'paper_trading') and self.paper_trading:
                # Generate realistic mock data
                import random
                base_price = random.uniform(200, 800)  # Mock option price
                
                return {
                    'open': base_price,
                    'high': base_price * random.uniform(1.02, 1.08),
                    'low': base_price * random.uniform(0.92, 0.98),
                    'close': base_price * random.uniform(0.95, 1.05),
                    'volume': random.randint(100, 1000),
                    'timestamp': datetime.now().isoformat()
                }
            
            # For real trading, fetch actual option candle data
            # Note: This would need to be implemented based on Delta Exchange API
            # For now, return None as we're in paper trading mode
            logger.warning("⚠️ Option candle data not available for real trading mode")
            return None
            
        except Exception as e:
            logger.error(f"❌ Failed to get option candle data: {e}")
            return None
