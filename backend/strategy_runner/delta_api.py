"""
Delta API Mock Module

This module provides a mock implementation of the Delta Exchange API
for testing and development purposes. In production, this would be
replaced with the actual Delta Exchange API client.
"""

import logging
import time
import random
from typing import Dict, List, Any, Optional, Union
from datetime import datetime, timezone
import pandas as pd


class DeltaAPI:
    """
    Mock Delta Exchange API client for strategy testing.
    
    This class simulates the behavior of the actual Delta Exchange API
    for development and testing purposes.
    """
    
    def __init__(self, base_url: str, api_key: str, api_secret: str):
        """
        Initialize the Delta API client.
        
        Args:
            base_url: Base URL for the API
            api_key: API key for authentication
            api_secret: API secret for authentication
        """
        self.base_url = base_url
        self.api_key = api_key
        self.api_secret = api_secret
        self.logger = logging.getLogger(__name__)
        
        # Mock data storage
        self.positions = {}
        self.orders = {}
        self.order_counter = 1000
        
        # Simulate network latency
        self.min_latency = 0.05  # 50ms
        self.max_latency = 0.2   # 200ms
        
        self.logger.info(f"Delta API initialized with base URL: {base_url}")
    
    def _simulate_latency(self):
        """Simulate realistic API latency."""
        latency = random.uniform(self.min_latency, self.max_latency)
        time.sleep(latency)
    
    def _generate_mock_candles(self, symbol: str, timeframe: str, limit: int = 200) -> pd.DataFrame:
        """
        Generate mock candlestick data.
        
        Args:
            symbol: Trading symbol
            timeframe: Timeframe for candles
            limit: Number of candles to generate
            
        Returns:
            DataFrame with OHLCV data
        """
        # Base price for different symbols
        base_prices = {
            "BTCUSD": 50000,
            "ETHUSD": 3000,
            "NIFTY": 20000,
            "BANKNIFTY": 45000
        }
        
        base_price = base_prices.get(symbol, 100)
        
        # Generate timestamps
        end_time = datetime.now(timezone.utc)
        
        # Convert timeframe to timedelta
        if timeframe.endswith('m'):
            minutes = int(timeframe[:-1])
            delta = pd.Timedelta(minutes=minutes)
        elif timeframe.endswith('h'):
            hours = int(timeframe[:-1])
            delta = pd.Timedelta(hours=hours)
        elif timeframe.endswith('d'):
            days = int(timeframe[:-1])
            delta = pd.Timedelta(days=days)
        else:
            delta = pd.Timedelta(minutes=1)
        
        timestamps = [end_time - (i * delta) for i in range(limit)]
        timestamps.reverse()
        
        # Generate price data with some randomness
        prices = []
        current_price = base_price
        
        for i in range(limit):
            # Add some volatility
            change = random.uniform(-0.02, 0.02)  # ±2% change
            current_price *= (1 + change)
            
            # Generate OHLCV
            high = current_price * random.uniform(1.0, 1.01)
            low = current_price * random.uniform(0.99, 1.0)
            open_price = current_price
            close_price = current_price * random.uniform(0.995, 1.005)
            volume = random.uniform(1000, 10000)
            
            prices.append({
                'timestamp': timestamps[i],
                'open': open_price,
                'high': high,
                'low': low,
                'close': close_price,
                'volume': volume
            })
        
        df = pd.DataFrame(prices)
        df.set_index('timestamp', inplace=True)
        
        return df
    
    def get_candles(self, symbol: str, timeframe: str, limit: int = 200) -> pd.DataFrame:
        """
        Get candlestick data for a symbol.
        
        Args:
            symbol: Trading symbol
            timeframe: Timeframe for candles
            limit: Number of candles to return
            
        Returns:
            DataFrame with OHLCV data
        """
        self._simulate_latency()
        
        try:
            candles = self._generate_mock_candles(symbol, timeframe, limit)
            self.logger.debug(f"Retrieved {len(candles)} candles for {symbol} ({timeframe})")
            return candles
            
        except Exception as e:
            self.logger.error(f"Failed to get candles for {symbol}: {e}")
            raise
    
    def get_position(self, symbol: str) -> Optional[Dict[str, Any]]:
        """
        Get current position for a symbol.
        
        Args:
            symbol: Trading symbol
            
        Returns:
            Position data or None if no position
        """
        self._simulate_latency()
        
        try:
            position = self.positions.get(symbol)
            
            if position:
                self.logger.debug(f"Retrieved position for {symbol}: {position}")
            else:
                self.logger.debug(f"No position found for {symbol}")
            
            return position
            
        except Exception as e:
            self.logger.error(f"Failed to get position for {symbol}: {e}")
            raise
    
    def get_open_orders(self, symbol: str) -> List[Dict[str, Any]]:
        """
        Get open orders for a symbol.
        
        Args:
            symbol: Trading symbol
            
        Returns:
            List of open orders
        """
        self._simulate_latency()
        
        try:
            symbol_orders = [order for order in self.orders.values() 
                           if order['symbol'] == symbol and order['status'] == 'open']
            
            self.logger.debug(f"Retrieved {len(symbol_orders)} open orders for {symbol}")
            return symbol_orders
            
        except Exception as e:
            self.logger.error(f"Failed to get open orders for {symbol}: {e}")
            raise
    
    def place_order(self, symbol: str, side: str, order_type: str, 
                   size: float, price: Optional[float] = None) -> Dict[str, Any]:
        """
        Place a new order.
        
        Args:
            symbol: Trading symbol
            side: "BUY" or "SELL"
            order_type: "MARKET" or "LIMIT"
            size: Order size
            price: Limit price (required for LIMIT orders)
            
        Returns:
            Order details
        """
        self._simulate_latency()
        
        try:
            # Validate inputs
            if side not in ["BUY", "SELL"]:
                raise ValueError("Side must be 'BUY' or 'SELL'")
            
            if order_type not in ["MARKET", "LIMIT"]:
                raise ValueError("Order type must be 'MARKET' or 'LIMIT'")
            
            if order_type == "LIMIT" and price is None:
                raise ValueError("Price is required for LIMIT orders")
            
            # Create order
            order_id = str(self.order_counter)
            self.order_counter += 1
            
            order = {
                'id': order_id,
                'symbol': symbol,
                'side': side,
                'type': order_type,
                'size': size,
                'price': price,
                'status': 'open',
                'filled_size': 0.0,
                'created_at': datetime.now(timezone.utc).isoformat(),
                'updated_at': datetime.now(timezone.utc).isoformat()
            }
            
            self.orders[order_id] = order
            
            self.logger.info(f"Placed {order_type} {side} order for {symbol}: {order_id}")
            return order
            
        except Exception as e:
            self.logger.error(f"Failed to place order for {symbol}: {e}")
            raise
    
    def cancel_order(self, order_id: str) -> Dict[str, Any]:
        """
        Cancel an existing order.
        
        Args:
            order_id: ID of the order to cancel
            
        Returns:
            Cancelled order details
        """
        self._simulate_latency()
        
        try:
            if order_id not in self.orders:
                raise ValueError(f"Order {order_id} not found")
            
            order = self.orders[order_id]
            
            if order['status'] != 'open':
                raise ValueError(f"Cannot cancel {order['status']} order")
            
            # Cancel the order
            order['status'] = 'cancelled'
            order['updated_at'] = datetime.now(timezone.utc).isoformat()
            
            self.logger.info(f"Cancelled order {order_id}")
            return order
            
        except Exception as e:
            self.logger.error(f"Failed to cancel order {order_id}: {e}")
            raise
    
    def get_order_status(self, order_id: str) -> Dict[str, Any]:
        """
        Get the status of an order.
        
        Args:
            order_id: ID of the order
            
        Returns:
            Order status details
        """
        self._simulate_latency()
        
        try:
            if order_id not in self.orders:
                raise ValueError(f"Order {order_id} not found")
            
            return self.orders[order_id]
            
        except Exception as e:
            self.logger.error(f"Failed to get order status for {order_id}: {e}")
            raise
    
    def get_account_balance(self) -> Dict[str, Any]:
        """
        Get account balance information.
        
        Returns:
            Account balance details
        """
        self._simulate_latency()
        
        try:
            balance = {
                'total_balance': 100000.0,
                'available_balance': 95000.0,
                'margin_used': 5000.0,
                'unrealized_pnl': 0.0,
                'realized_pnl': 0.0,
                'currency': 'USD'
            }
            
            self.logger.debug("Retrieved account balance")
            return balance
            
        except Exception as e:
            self.logger.error(f"Failed to get account balance: {e}")
            raise
    
    def get_ticker(self, symbol: str) -> Dict[str, Any]:
        """
        Get current ticker information for a symbol.
        
        Args:
            symbol: Trading symbol
            
        Returns:
            Ticker information
        """
        self._simulate_latency()
        
        try:
            # Generate mock ticker data
            base_prices = {
                "BTCUSD": 50000,
                "ETHUSD": 3000,
                "NIFTY": 20000,
                "BANKNIFTY": 45000
            }
            
            base_price = base_prices.get(symbol, 100)
            current_price = base_price * random.uniform(0.98, 1.02)
            
            ticker = {
                'symbol': symbol,
                'last_price': current_price,
                'bid': current_price * 0.999,
                'ask': current_price * 1.001,
                'high_24h': current_price * 1.05,
                'low_24h': current_price * 0.95,
                'volume_24h': random.uniform(1000000, 10000000),
                'change_24h': random.uniform(-0.05, 0.05),
                'timestamp': datetime.now(timezone.utc).isoformat()
            }
            
            self.logger.debug(f"Retrieved ticker for {symbol}")
            return ticker
            
        except Exception as e:
            self.logger.error(f"Failed to get ticker for {symbol}: {e}")
            raise
    
    def close_position(self, symbol: str, side: str, size: float) -> Dict[str, Any]:
        """
        Close a position.
        
        Args:
            symbol: Trading symbol
            side: "BUY" or "SELL" (opposite of position side)
            size: Size to close
            
        Returns:
            Close position result
        """
        self._simulate_latency()
        
        try:
            position = self.positions.get(symbol)
            
            if not position:
                raise ValueError(f"No position found for {symbol}")
            
            if position['side'] == side:
                raise ValueError(f"Cannot close position with same side: {side}")
            
            # Update position
            closed_size = min(size, position['size'])
            position['size'] -= closed_size
            
            if position['size'] <= 0:
                del self.positions[symbol]
            
            result = {
                'symbol': symbol,
                'closed_size': closed_size,
                'remaining_size': max(0, position['size']),
                'closed_at': datetime.now(timezone.utc).isoformat()
            }
            
            self.logger.info(f"Closed position for {symbol}: {result}")
            return result
            
        except Exception as e:
            self.logger.error(f"Failed to close position for {symbol}: {e}")
            raise
    
    def get_trade_history(self, symbol: str, limit: int = 100) -> List[Dict[str, Any]]:
        """
        Get trade history for a symbol.
        
        Args:
            symbol: Trading symbol
            limit: Number of trades to return
            
        Returns:
            List of trades
        """
        self._simulate_latency()
        
        try:
            # Generate mock trade history
            trades = []
            base_price = 100
            
            for i in range(limit):
                trade = {
                    'id': f"trade_{i}",
                    'symbol': symbol,
                    'side': random.choice(['BUY', 'SELL']),
                    'size': random.uniform(0.1, 10.0),
                    'price': base_price * random.uniform(0.95, 1.05),
                    'timestamp': datetime.now(timezone.utc).isoformat(),
                    'fee': random.uniform(0.001, 0.01)
                }
                trades.append(trade)
            
            self.logger.debug(f"Retrieved {len(trades)} trades for {symbol}")
            return trades
            
        except Exception as e:
            self.logger.error(f"Failed to get trade history for {symbol}: {e}")
            raise
    
    def health_check(self) -> Dict[str, Any]:
        """
        Check API health status.
        
        Returns:
            Health status information
        """
        try:
            health = {
                'status': 'healthy',
                'timestamp': datetime.now(timezone.utc).isoformat(),
                'version': '1.0.0',
                'uptime': random.uniform(99.5, 99.9)
            }
            
            return health
            
        except Exception as e:
            self.logger.error(f"Health check failed: {e}")
            return {
                'status': 'unhealthy',
                'error': str(e),
                'timestamp': datetime.now(timezone.utc).isoformat()
            }

