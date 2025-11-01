#!/usr/bin/env python3
"""
Enhanced Delta API Client for Trade Manthan
Provides trading functionality with comprehensive logging and error handling
"""

import requests
import time
import hashlib
import hmac
import json
import threading
import concurrent.futures
import logging
import traceback
from typing import Optional, Dict, List, Any, Union
from dataclasses import dataclass
from datetime import datetime

# Configure logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s',
    handlers=[
        logging.FileHandler('delta_api.log'),
        logging.StreamHandler()
    ]
)
logger = logging.getLogger(__name__)

@dataclass
class TradingParams:
    """Trading parameters container for initialization"""
    api_key: str
    api_secret: str
    api_url: str
    asset_id: str = "14"  # Default to USD asset ID

class DeltaAPI:
    """
    Enhanced Delta API client with comprehensive logging and error handling
    """
    
    def __init__(self, trading_params: TradingParams):
        """
        Initialize Delta API client with basic API credentials
        
        Args:
            trading_params (TradingParams): API configuration parameters (api_key, api_secret, api_url)
        """
        start_time = time.time()
        logger.info(f"🚀 Initializing Delta API client")
        
        self.api_key = trading_params.api_key
        self.api_secret = trading_params.api_secret
        self.api_url = trading_params.api_url
        self.params = trading_params  # Store params for compatibility
        
        self.session = requests.Session()
        self.session.headers.update({
            'User-Agent': 'TradeManthan/1.0',
            'Accept': 'application/json'
        })
        
        # Cache management
        self._balance_cache = None
        self._balance_cache_time = 0
        self._balance_cache_duration = 30
        self._price_cache = None
        self._price_cache_time = 0
        self._price_cache_duration = 5
        self._cache_lock = threading.Lock()
        
        execution_time = time.time() - start_time
        logger.info(f"✅ Delta API client initialized successfully in {execution_time:.3f}s")
    
    def _log_method_entry(self, method_name: str, **kwargs):
        """Log method entry with parameters"""
        logger.info(f"📥 {method_name} called with params: {kwargs}")
        return time.time()
    
    def _log_method_exit(self, method_name: str, start_time: float, result: Any = None, error: Exception = None):
        """Log method exit with execution time and result"""
        execution_time = time.time() - start_time
        
        if error:
            logger.error(f"❌ {method_name} failed after {execution_time:.3f}s with error: {str(error)}")
            logger.error(f"🔍 Full traceback: {traceback.format_exc()}")
        else:
            logger.info(f"📤 {method_name} completed successfully in {execution_time:.3f}s")
            if result is not None:
                logger.debug(f"📊 {method_name} returned: {result}")
    
    def _handle_api_error(self, response: requests.Response, operation: str) -> Exception:
        """Handle API errors with detailed logging"""
        try:
            error_data = response.json()
            error_msg = error_data.get('message', 'Unknown API error')
            logger.error(f"🚨 API Error in {operation}: HTTP {response.status_code} - {error_msg}")
            logger.error(f"🔍 Response headers: {dict(response.headers)}")
            logger.error(f"🔍 Response body: {error_data}")
        except Exception as e:
            logger.error(f"🚨 Failed to parse API error response: {e}")
            error_msg = f"HTTP {response.status_code}: {response.text}"
        
        return Exception(f"{operation} failed: {error_msg}")
    
    def get_latest_price(self, symbol: Optional[str] = None) -> Optional[float]:
        """
        Get latest price using market data API
        
        Args:
            symbol (str, optional): Symbol to get price for. Defaults to configured symbol.
            
        Returns:
            float: Latest price or None if failed
        """
        start_time = self._log_method_entry("get_latest_price", symbol=symbol or self.params.symbol)
        
        try:
            current_time = time.time()
            with self._cache_lock:
                if (self._price_cache is None or 
                    current_time - self._price_cache_time > self._price_cache_duration):
                    
                    url = f"{self.params.api_url}/v2/tickers/{self.params.symbol_id}"
                    logger.debug(f"🌐 Fetching price from: {url}")
                    
                    response = self.session.get(url, timeout=5)
                    response.raise_for_status()
                    
                    data = response.json()
                    if data.get("success") and data.get("result"):
                        self._price_cache = float(data["result"]["mark_price"])
                        self._price_cache_time = current_time
                        logger.info(f"💰 Latest price for {symbol or self.params.symbol}: ${self._price_cache}")
                    else:
                        raise Exception("API response indicates failure")
                else:
                    logger.debug(f"📦 Using cached price: ${self._price_cache}")
            
            self._log_method_exit("get_latest_price", start_time, self._price_cache)
            return self._price_cache
            
        except Exception as e:
            self._log_method_exit("get_latest_price", start_time, error=e)
            logger.error(f"❌ Failed to get latest price: {str(e)}")
            return None
    
    def get_candles(self, 
                    symbol: Optional[str] = None, 
                    interval: Optional[str] = None, 
                    limit: int = 100, 
                    start: Optional[int] = None, 
                    end: Optional[int] = None) -> List[Dict[str, Any]]:
        """
        Get candle data using market data API
        
        Args:
            symbol (str, optional): Symbol to get candles for
            interval (str, optional): Candle interval (e.g., '1m', '5m', '1h', '1d')
            limit (int): Number of candles to fetch
            start (int, optional): Start timestamp
            end (int, optional): End timestamp
            
        Returns:
            List[Dict]: List of candle data
        """
        start_time = self._log_method_entry(
            "get_candles", 
            symbol=symbol or self.params.symbol,
            interval=interval or self.params.candle_interval,
            limit=limit,
            start=start,
            end=end
        )
        
        try:
            url = f"{self.params.api_url}/v2/history/candles"
            
            # Convert interval format if needed
            resolution = interval or self.params.candle_interval
            if resolution == '90m':
                resolution = '1h'  # Use 1h as closest valid resolution
            
            # Set default start and end times if not provided
            import time
            current_time = int(time.time())
            if start is None:
                start = current_time - (limit * 3600)  # Go back in time based on limit
            if end is None:
                end = current_time
            
            params = {
                'symbol': symbol or self.params.symbol,
                'resolution': resolution,
                'limit': limit,
                'start': start,
                'end': end
            }
            
            logger.debug(f"🌐 Fetching candles from: {url} with params: {params}")
            
            response = self.session.get(url, params=params, timeout=20)
            response.raise_for_status()
            
            data = response.json()
            if data.get("success"):
                candles = data['result']
                logger.info(f"📊 Retrieved {len(candles)} candles for {symbol or self.params.symbol}")
                self._log_method_exit("get_candles", start_time, len(candles))
                return candles
            else:
                raise Exception("API response indicates failure")
                
        except Exception as e:
            self._log_method_exit("get_candles", start_time, error=e)
            logger.error(f"❌ Failed to get candles: {str(e)}")
            raise
    
    def get_candles_binance(self, 
                           symbol: str = 'BTCUSDT', 
                           interval: str = '5m', 
                           limit: int = 100) -> List[Dict[str, Any]]:
        """
        Get candle data from Binance (external API)
        
        Args:
            symbol (str): Symbol to get candles for
            interval (str): Candle interval
            limit (int): Number of candles to fetch
            
        Returns:
            List[Dict]: List of candle data
        """
        start_time = self._log_method_entry("get_candles_binance", symbol=symbol, interval=interval, limit=limit)
        
        try:
            url = 'https://api.binance.com/api/v3/klines'
            params = {
                'symbol': symbol,
                'interval': interval,
                'limit': limit
            }
            
            logger.debug(f"🌐 Fetching Binance candles from: {url} with params: {params}")
            
            response = requests.get(url, params=params, timeout=10)
            response.raise_for_status()
            
            klines = response.json()
            candles = []
            
            for k in klines:
                candles.append({
                    'time': int(k[0] // 1000),
                    'open': float(k[1]),
                    'high': float(k[2]),
                    'low': float(k[3]),
                    'close': float(k[4]),
                    'volume': float(k[5])
                })
            
            logger.info(f"📊 Retrieved {len(candles)} Binance candles for {symbol}")
            self._log_method_exit("get_candles_binance", start_time, len(candles))
            return candles
            
        except Exception as e:
            self._log_method_exit("get_candles_binance", start_time, error=e)
            logger.error(f"❌ Failed to get Binance candles: {str(e)}")
            return []
    
    def get_balance(self) -> float:
        """
        Get account balance using trading API
        
        Returns:
            float: Available balance
        """
        start_time = self._log_method_entry("get_balance")
        
        try:
            current_time = time.time()
            with self._cache_lock:
                if (self._balance_cache is None or 
                    current_time - self._balance_cache_time > self._balance_cache_duration):
                    
                    path = "/v2/wallet/balances"
                    headers, timestamp, message, signature = self._sign_request("GET", path)
                    
                    url = f"{self.params.api_url}{path}"
                    logger.debug(f"🌐 Fetching balance from: {url}")
                    
                    response = self.session.get(url, headers=headers, timeout=10)
                    response.raise_for_status()
                    
                    data = response.json()
                    wallet_balance = 0
                    
                    for bal in data["result"]:
                        if str(bal.get("asset_id")) == str(self.params.asset_id):
                            wallet_balance = float(bal['available_balance'])
                            break
                    
                    self._balance_cache = wallet_balance
                    self._balance_cache_time = current_time
                    logger.info(f"💰 Current balance: ${wallet_balance}")
                else:
                    logger.debug(f"📦 Using cached balance: ${self._balance_cache}")
            
            self._log_method_exit("get_balance", start_time, self._balance_cache)
            return self._balance_cache
            
        except Exception as e:
            self._log_method_exit("get_balance", start_time, error=e)
            logger.error(f"❌ Failed to get balance: {str(e)}")
            return 0
    
    def _sign_request(self, method: str, path: str, body: Optional[Dict] = None) -> tuple:
        """
        Sign request for authenticated API calls
        
        Args:
            method (str): HTTP method
            path (str): API path
            body (Dict, optional): Request body
            
        Returns:
            tuple: (headers, timestamp, message, signature)
        """
        timestamp = str(int(time.time()))
        if body is None:
            body = ""
        else:
            body = json.dumps(body)
        
        message = method + timestamp + path + body
        signature = hmac.new(
            self.params.api_secret.encode(), 
            message.encode(), 
            hashlib.sha256
        ).hexdigest()
        
        headers = {
            "api-key": self.params.api_key,
            "timestamp": timestamp,
            "signature": signature,
            "Content-Type": "application/json"
        }
        
        return headers, timestamp, message, signature
    
    def place_order(self, 
                   symbol: Optional[str] = None,
                   side: Optional[str] = None,
                   qty: Optional[int] = None,
                   order_type: Optional[str] = None,
                   price: Optional[float] = None,
                   stop_loss: Optional[float] = None,
                   take_profit: Optional[float] = None,
                   post_only: bool = False,
                   max_retries: int = 3) -> Dict[str, Any]:
        """
        Place order using trading API
        
        Args:
            symbol (str, optional): Trading symbol
            side (str, optional): Order side ('buy' or 'sell')
            qty (int, optional): Order quantity
            order_type (str, optional): Order type
            price (float, optional): Order price
            stop_loss (float, optional): Stop loss price
            take_profit (float, optional): Take profit price
            post_only (bool): Post only flag
            max_retries (int): Maximum retry attempts
            
        Returns:
            Dict: Order placement result
        """
        # Use parameters from trading_params if not provided
        symbol = symbol or self.params.symbol
        side = side or self.params.order_side
        qty = qty or self.params.order_qty
        order_type = order_type or self.params.order_type
        price = price or self.params.order_price
        stop_loss = stop_loss or self.params.stop_loss
        take_profit = take_profit or self.params.take_profit
        
        start_time = self._log_method_entry(
            "place_order",
            symbol=symbol,
            side=side,
            qty=qty,
            order_type=order_type,
            price=price,
            stop_loss=stop_loss,
            take_profit=take_profit,
            post_only=post_only
        )
        
        try:
            # Set default stop loss and take profit if not provided
            if price is not None:
                if stop_loss is None:
                    stop_loss = round(float(price) * 0.98, 2)  # 2% below price
                if take_profit is None:
                    take_profit = round(float(price) * 1.02, 2)  # 2% above price
            
            url = f"{self.params.api_url}/v2/orders"
            path = "/v2/orders"
            qty = int(qty)
            
            data = {
                'product_id': self.params.symbol_id,
                'side': side,
                'order_type': order_type,
                'size': qty,
                'time_in_force': 'gtc',
                'post_only': post_only
            }
            
            if price is not None:
                data['limit_price'] = price
                data['bracket_stop_loss_price'] = stop_loss
                data['bracket_stop_loss_limit_price'] = stop_loss
                data['bracket_take_profit_price'] = take_profit
                data['bracket_take_profit_limit_price'] = take_profit
            
            logger.info(f"📝 Placing {side} order: {qty} {symbol} @ ${price} (SL: ${stop_loss}, TP: ${take_profit})")
            
            for attempt in range(max_retries):
                try:
                    headers, timestamp, message, signature = self._sign_request('POST', path, data)
                    
                    response = self.session.post(url, headers=headers, json=data, timeout=30)
                    response.raise_for_status()
                    
                    response_data = response.json()
                    
                    if not response_data.get('success'):
                        raise self._handle_api_error(response, "Order placement")
                    
                    result = response_data['result']
                    logger.info(f"✅ Order placed successfully: {result.get('id')}")
                    
                    # Return clean result
                    clean_result = {
                        'id': result.get('id'),
                        'side': result.get('side'),
                        'size': result.get('size'),
                        'limit_price': result.get('limit_price'),
                        'state': result.get('state'),
                        'product_symbol': result.get('product_symbol'),
                        'bracket_stop_loss_price': result.get('bracket_stop_loss_price'),
                        'bracket_take_profit_price': result.get('bracket_take_profit_price'),
                        'created_at': result.get('created_at'),
                        'average_fill_price': result.get('average_fill_price')
                    }
                    
                    self._log_method_exit("place_order", start_time, clean_result)
                    return clean_result
                    
                except requests.exceptions.Timeout as e:
                    if attempt < max_retries - 1:
                        wait_time = 2 ** attempt
                        logger.warning(f"⏰ Order placement timed out, retrying in {wait_time}s (attempt {attempt + 1}/{max_retries})")
                        time.sleep(wait_time)
                        continue
                    else:
                        raise Exception(f"Order placement timed out after {max_retries} attempts")
                        
                except Exception as e:
                    if attempt < max_retries - 1:
                        logger.warning(f"⚠️ Order placement failed, retrying in 1s (attempt {attempt + 1}/{max_retries}): {str(e)}")
                        time.sleep(1)
                        continue
                    else:
                        raise
                        
        except Exception as e:
            self._log_method_exit("place_order", start_time, error=e)
            logger.error(f"❌ Order placement failed: {str(e)}")
            raise
    
    def get_live_orders(self) -> List[Dict[str, Any]]:
        """
        Get live orders using trading API
        
        Returns:
            List[Dict]: List of live orders
        """
        start_time = self._log_method_entry("get_live_orders")
        
        try:
            path = "/v2/orders"
            headers, timestamp, message, signature = self._sign_request("GET", path)
            
            url = f"{self.params.api_url}{path}"
            logger.debug(f"🌐 Fetching live orders from: {url}")
            
            response = self.session.get(url, headers=headers, timeout=10)
            response.raise_for_status()
            
            data = response.json()
            if data.get("success"):
                orders = data['result']
                logger.info(f"📋 Retrieved {len(orders)} live orders")
                self._log_method_exit("get_live_orders", start_time, len(orders))
                return orders
            else:
                raise self._handle_api_error(response, "Get live orders")
                
        except Exception as e:
            self._log_method_exit("get_live_orders", start_time, error=e)
            logger.error(f"❌ Failed to get live orders: {str(e)}")
            raise
    
    def cancel_order(self, order_id: str) -> Dict[str, Any]:
        """
        Cancel a specific order
        
        Args:
            order_id (str): Order ID to cancel
            
        Returns:
            Dict: Cancellation result
        """
        start_time = self._log_method_entry("cancel_order", order_id=order_id)
        
        try:
            path = f"/v2/orders/{order_id}/cancel"
            headers, timestamp, message, signature = self._sign_request("POST", path)
            
            url = f"{self.params.api_url}{path}"
            logger.info(f"❌ Cancelling order: {order_id}")
            
            response = self.session.post(url, headers=headers, timeout=10)
            
            # Handle 404 errors gracefully
            if response.status_code == 404:
                result = {"id": order_id, "status": "already_cancelled"}
                logger.warning(f"⚠️ Order {order_id} already cancelled or doesn't exist")
                self._log_method_exit("cancel_order", start_time, result)
                return result
            
            response.raise_for_status()
            data = response.json()
            
            if data.get("success"):
                result = data['result']
                logger.info(f"✅ Order {order_id} cancelled successfully")
                self._log_method_exit("cancel_order", start_time, result)
                return result
            else:
                raise self._handle_api_error(response, "Cancel order")
                
        except Exception as e:
            self._log_method_exit("cancel_order", start_time, error=e)
            logger.error(f"❌ Failed to cancel order {order_id}: {str(e)}")
            raise Exception(f"Failed to cancel order {order_id}: {e}")
    
    def cancel_all_orders(self) -> bool:
        """
        Cancel all active orders
        
        Returns:
            bool: True if successful, False otherwise
        """
        start_time = self._log_method_entry("cancel_all_orders")
        
        try:
            live_orders = self.get_live_orders()
            active_orders = [
                order for order in live_orders 
                if order.get('state') not in ['filled', 'cancelled', 'rejected']
            ]
            
            if not active_orders:
                logger.info("ℹ️ No active orders to cancel")
                self._log_method_exit("cancel_all_orders", start_time, True)
                return True
            
            logger.info(f"❌ Cancelling {len(active_orders)} active orders")
            
            cancelled_count = 0
            failed_count = 0
            
            for order in active_orders:
                try:
                    order_id = order['id']
                    result = self.cancel_order(order_id)
                    
                    if result and (isinstance(result, dict) and result.get('id')):
                        cancelled_count += 1
                    else:
                        failed_count += 1
                        
                    time.sleep(0.5)  # Rate limiting
                    
                except Exception as e:
                    failed_count += 1
                    logger.error(f"❌ Failed to cancel order {order.get('id')}: {str(e)}")
            
            success = cancelled_count > 0
            logger.info(f"📊 Order cancellation complete: {cancelled_count} cancelled, {failed_count} failed")
            
            self._log_method_exit("cancel_all_orders", start_time, success)
            return success
            
        except Exception as e:
            self._log_method_exit("cancel_all_orders", start_time, error=e)
            logger.error(f"❌ Failed to cancel all orders: {str(e)}")
            return False
    
    def get_positions(self, product_id: Optional[int] = None) -> List[Dict[str, Any]]:
        """
        Get current positions
        
        Args:
            product_id (int, optional): Product ID. Defaults to configured symbol_id.
            
        Returns:
            List[Dict]: List of positions
        """
        start_time = self._log_method_entry("get_positions", product_id=product_id or self.params.symbol_id)
        
        try:
            product_id = product_id or self.params.symbol_id
            path = f"/v2/positions?product_id={product_id}"
            headers, timestamp, message, signature = self._sign_request("GET", path)
            
            url = f"{self.params.api_url}{path}"
            logger.debug(f"🌐 Fetching positions from: {url}")
            
            response = self.session.get(url, headers=headers, timeout=10)
            response.raise_for_status()
            
            data = response.json()
            if data.get("success"):
                positions = data['result']
                logger.info(f"📊 Retrieved {len(positions)} positions for product {product_id}")
                self._log_method_exit("get_positions", start_time, len(positions))
                return positions
            else:
                raise self._handle_api_error(response, "Get positions")
                
        except Exception as e:
            self._log_method_exit("get_positions", start_time, error=e)
            logger.error(f"❌ Failed to get positions: {str(e)}")
            raise
    
    def get_margined_positions(self) -> List[Dict[str, Any]]:
        """
        Get margined positions (open option positions)
        
        Returns:
            List[Dict]: List of margined positions
        """
        start_time = self._log_method_entry("get_margined_positions")
        
        try:
            path = "/v2/positions/margined"
            headers, timestamp, message, signature = self._sign_request("GET", path)
            
            url = f"{self.params.api_url}{path}"
            logger.debug(f"🌐 Fetching margined positions from: {url}")
            
            response = self.session.get(url, headers=headers, timeout=10)
            response.raise_for_status()
            
            data = response.json()
            if data.get("success"):
                positions = data['result']
                logger.info(f"📊 Retrieved {len(positions)} margined positions")
                self._log_method_exit("get_margined_positions", start_time, len(positions))
                return positions
            else:
                raise self._handle_api_error(response, "Get margined positions")
                
        except Exception as e:
            self._log_method_exit("get_margined_positions", start_time, error=e)
            logger.error(f"❌ Failed to get margined positions: {str(e)}")
            raise
    
    def close_all_positions(self, product_id: Optional[int] = None) -> bool:
        """
        Close all open positions
        
        Args:
            product_id (int, optional): Product ID. Defaults to configured symbol_id.
            
        Returns:
            bool: True if successful, False otherwise
        """
        start_time = self._log_method_entry("close_all_positions", product_id=product_id or self.params.symbol_id)
        
        try:
            product_id = product_id or self.params.symbol_id
            positions = self.get_positions(product_id)
            
            if not positions:
                logger.info(f"ℹ️ No positions to close for product {product_id}")
                self._log_method_exit("close_all_positions", start_time, True)
                return True
            
            if isinstance(positions, dict):
                positions = [positions]
            
            open_positions = []
            for pos in positions:
                if isinstance(pos, dict) and float(pos.get('size', 0)) != 0:
                    open_positions.append(pos)
            
            if not open_positions:
                logger.info(f"ℹ️ No open positions to close for product {product_id}")
                self._log_method_exit("close_all_positions", start_time, True)
                return True
            
            logger.info(f"🔒 Closing {len(open_positions)} open positions for product {product_id}")
            
            with concurrent.futures.ThreadPoolExecutor(max_workers=3) as executor:
                future_to_position = {}
                for pos in open_positions:
                    size = float(pos.get('size', 0))
                    if size > 0:
                        side = 'sell'
                    else:
                        side = 'buy'
                    close_size = abs(size)
                    
                    future = executor.submit(
                        self.place_order,
                        self.params.symbol, side, close_size, 'market_order', None, None, None
                    )
                    future_to_position[future] = pos
                
                success_count = 0
                for future in concurrent.futures.as_completed(future_to_position):
                    pos = future_to_position[future]
                    try:
                        result = future.result(timeout=10)
                        success_count += 1
                        logger.info(f"✅ Position closed successfully: {pos.get('id')}")
                    except Exception as e:
                        logger.error(f"❌ Failed to close position {pos.get('id')}: {str(e)}")
            
            success = success_count == len(open_positions)
            logger.info(f"📊 Position closing complete: {success_count}/{len(open_positions)} successful")
            
            self._log_method_exit("close_all_positions", start_time, success)
            return success
            
        except Exception as e:
            self._log_method_exit("close_all_positions", start_time, error=e)
            logger.error(f"❌ Failed to close all positions: {str(e)}")
            return False
    
    def get_account_state(self, product_id: Optional[int] = None) -> Dict[str, Any]:
        """
        Get comprehensive account state
        
        Args:
            product_id (int, optional): Product ID. Defaults to configured symbol_id.
            
        Returns:
            Dict: Account state information
        """
        start_time = self._log_method_entry("get_account_state", product_id=product_id or self.params.symbol_id)
        
        try:
            product_id = product_id or self.params.symbol_id
            logger.info(f"📊 Getting account state for product {product_id}")
            
            with concurrent.futures.ThreadPoolExecutor(max_workers=2) as executor:
                orders_future = executor.submit(self.get_live_orders)
                positions_future = executor.submit(self.get_positions, product_id)
                
                orders = orders_future.result(timeout=20)
                positions = positions_future.result(timeout=20)
            
            # Check for active orders
            active_orders = [
                order for order in orders 
                if order.get('state') not in ['filled', 'cancelled', 'rejected']
            ]
            
            # Check for open positions
            if positions:
                if isinstance(positions, dict):
                    positions = [positions]
                open_positions = [
                    pos for pos in positions 
                    if isinstance(pos, dict) and float(pos.get('size', 0)) != 0
                ]
            else:
                open_positions = []
            
            account_state = {
                'orders': active_orders,
                'positions': open_positions,
                'has_orders': len(active_orders) > 0,
                'has_positions': len(open_positions) > 0,
                'is_clean': len(active_orders) == 0 and len(open_positions) == 0
            }
            
            logger.info(f"📊 Account state: {len(active_orders)} active orders, {len(open_positions)} open positions")
            
            self._log_method_exit("get_account_state", start_time, account_state)
            return account_state
            
        except Exception as e:
            self._log_method_exit("get_account_state", start_time, error=e)
            logger.error(f"❌ Failed to get account state: {str(e)}")
            return {
                'orders': [],
                'positions': [],
                'has_orders': False,
                'has_positions': False,
                'is_clean': False,
                'error': str(e)
            }
    
    def get_all_products(self) -> List[Dict[str, Any]]:
        """
        Get all available products
        
        Returns:
            List[Dict]: List of products
        """
        start_time = self._log_method_entry("get_all_products")
        
        try:
            url = f"{self.params.api_url}/v2/products"
            logger.debug(f"🌐 Fetching products from: {url}")
            
            response = self.session.get(url, timeout=10)
            response.raise_for_status()
            
            data = response.json()
            if data.get("success"):
                products = data['result']
                logger.info(f"📦 Retrieved {len(products)} products")
                self._log_method_exit("get_all_products", start_time, len(products))
                return products
            else:
                raise self._handle_api_error(response, "Get products")
                
        except Exception as e:
            self._log_method_exit("get_all_products", start_time, error=e)
            logger.error(f"❌ Failed to get products: {str(e)}")
            raise
    
    def clear_cache(self):
        """Clear all cached data"""
        start_time = self._log_method_entry("clear_cache")
        
        try:
            with self._cache_lock:
                self._balance_cache = None
                self._balance_cache_time = 0
                self._price_cache = None
                self._price_cache_time = 0
            
            logger.info("🧹 Cache cleared successfully")
            self._log_method_exit("clear_cache", start_time)
            
        except Exception as e:
            self._log_method_exit("clear_cache", start_time, error=e)
            logger.error(f"❌ Failed to clear cache: {str(e)}")
    
    def edit_bracket_order(self, 
                          order_id: str, 
                          stop_loss: Optional[float] = None, 
                          take_profit: Optional[float] = None) -> Dict[str, Any]:
        """
        Edit bracket order parameters
        
        Args:
            order_id (str): Order ID to edit
            stop_loss (float, optional): New stop loss price
            take_profit (float, optional): New take profit price
            
        Returns:
            Dict: Edit result
        """
        start_time = self._log_method_entry(
            "edit_bracket_order", 
            order_id=order_id,
            stop_loss=stop_loss,
            take_profit=take_profit
        )
        
        try:
            path = f"/v2/orders/{order_id}/edit_bracket"
            body = {}
            
            if stop_loss is not None:
                body['stop_loss_price'] = stop_loss
            if take_profit is not None:
                body['take_profit_price'] = take_profit
            
            headers, timestamp, message, signature = self._sign_request("POST", path, body)
            
            url = f"{self.params.api_url}{path}"
            logger.info(f"✏️ Editing bracket order {order_id}: SL={stop_loss}, TP={take_profit}")
            
            response = self.session.post(url, headers=headers, json=body, timeout=10)
            
            if response.status_code == 404:
                raise Exception(f"Order {order_id} not found")
            
            response.raise_for_status()
            data = response.json()
            
            if data.get("success"):
                result = data['result']
                logger.info(f"✅ Bracket order {order_id} edited successfully")
                self._log_method_exit("edit_bracket_order", start_time, result)
                return result
            else:
                raise self._handle_api_error(response, "Edit bracket order")
                
        except Exception as e:
            self._log_method_exit("edit_bracket_order", start_time, error=e)
            logger.error(f"❌ Failed to edit bracket order {order_id}: {str(e)}")
            raise Exception(f"Failed to edit bracket order {order_id}: {e}")
    
    def __del__(self):
        """Cleanup on object destruction"""
        try:
            if hasattr(self, 'session'):
                self.session.close()
                logger.info("🧹 Delta API client session closed")
        except Exception as e:
            logger.error(f"❌ Error during cleanup: {str(e)}")


# Example usage and testing
if __name__ == "__main__":
    # Example API parameters (only the three required ones)
    example_params = TradingParams(
        api_key="your_api_key_here",
        api_secret="your_api_secret_here",
        api_url="https://api.delta.exchange"
    )
    
    # Initialize API client
    api_client = DeltaAPI(example_params)
    
    try:
        # Test basic functionality
        print("🧪 Testing Delta API Client...")
        
        # Note: These methods now require additional parameters when called
        # For example: get_latest_price(symbol_id), get_account_state(), etc.
        print("✅ Delta API Client initialized successfully!")
        print("📝 Note: Individual methods require specific parameters when called")
        
    except Exception as e:
        print(f"❌ Test failed: {str(e)}")
        logger.error(f"Test execution failed: {str(e)}")
    
    finally:
        # Cleanup
        if 'api_client' in locals():
            del api_client

