"""
Mock API Client for Paper Trading Mode
Provides simulated market data for testing the SuperTrend strategy
"""

import asyncio
import logging
import pandas as pd
import numpy as np
from datetime import datetime, timedelta
from typing import Dict, List, Any, Optional

logger = logging.getLogger(__name__)

class MockAPI:
    """
    Mock API client for paper trading mode
    Provides simulated Bitcoin futures data for SuperTrend calculation
    """
    
    def __init__(self, config: Dict[str, Any] = None):
        self.config = config or {}
        self.websocket_connected = False
        self.underlying_price = 50000.0  # Starting BTC price
        self.price_history = []
        self._generate_initial_data()
        
    def _generate_initial_data(self):
        """Generate initial price history for SuperTrend calculation"""
        # Generate 500 candles of realistic Bitcoin price data
        np.random.seed(42)  # For reproducible results
        
        # Start with a base price
        base_price = 50000.0
        prices = [base_price]
        
        # Generate realistic price movements
        for i in range(499):
            # Random walk with slight upward bias
            change_pct = np.random.normal(0.001, 0.02)  # 0.1% mean, 2% std
            new_price = prices[-1] * (1 + change_pct)
            prices.append(max(new_price, 1000))  # Minimum price floor
        
        # Create OHLCV data
        for i, close in enumerate(prices):
            high = close * (1 + abs(np.random.normal(0, 0.01)))
            low = close * (1 - abs(np.random.normal(0, 0.01)))
            open_price = prices[i-1] if i > 0 else close
            volume = np.random.uniform(1000, 10000)
            
            timestamp = datetime.now() - timedelta(minutes=90 * (200 - i))
            
            self.price_history.append({
                'timestamp': timestamp,
                'open': open_price,
                'high': high,
                'low': low,
                'close': close,
                'volume': volume
            })
    
    async def connect_websocket(self):
        """Mock WebSocket connection"""
        self.websocket_connected = True
        logger.info("🎭 Mock WebSocket connected (Paper Trading Mode)")
        
    async def disconnect_websocket(self):
        """Mock WebSocket disconnection"""
        self.websocket_connected = False
        logger.info("🎭 Mock WebSocket disconnected")
    
    def get_btc_futures_data(self, interval: str = '90m', limit: int = 100) -> pd.DataFrame:
        """Get simulated BTC futures data for SuperTrend calculation"""
        try:
            # Convert interval to minutes
            interval_minutes = self._parse_interval(interval)
            
            # Generate data for the requested interval
            data = []
            current_time = datetime.now()
            
            # Use at least 100 candles for SuperTrend calculation
            actual_limit = max(limit, 100)
            
            for i in range(actual_limit):
                # Use historical data if available, otherwise generate new
                if i < len(self.price_history):
                    candle = self.price_history[-(actual_limit - i)]
                else:
                    # Generate new realistic data
                    last_price = self.price_history[-1]['close'] if self.price_history else 50000.0
                    change_pct = np.random.normal(0.001, 0.02)
                    new_price = last_price * (1 + change_pct)
                    
                    high = new_price * (1 + abs(np.random.normal(0, 0.01)))
                    low = new_price * (1 - abs(np.random.normal(0, 0.01)))
                    volume = np.random.uniform(1000, 10000)
                    
                    candle = {
                        'timestamp': current_time - timedelta(minutes=interval_minutes * (actual_limit - i)),
                        'open': last_price,
                        'high': high,
                        'low': low,
                        'close': new_price,
                        'volume': volume
                    }
                
                data.append(candle)
            
            # Create DataFrame
            df = pd.DataFrame(data)
            df['timestamp'] = pd.to_datetime(df['timestamp'])
            df.set_index('timestamp', inplace=True)
            
            # Ensure no NaN values
            df = df.ffill().bfill()
            
            # Update current underlying price
            if not df.empty:
                self.underlying_price = df['close'].iloc[-1]
            
            logger.debug(f"📊 Generated {len(df)} mock candles for SuperTrend calculation")
            return df
            
        except Exception as e:
            logger.error(f"❌ Failed to generate mock data: {e}")
            return pd.DataFrame()
    
    def _parse_interval(self, interval: str) -> int:
        """Parse interval string to minutes"""
        interval_map = {
            '1m': 1,
            '5m': 5,
            '15m': 15,
            '30m': 30,
            '60m': 60,
            '90m': 90,
            '1h': 60,
            '4h': 240,
            '1d': 1440
        }
        return interval_map.get(interval, 90)
    
    def get_options_chain(self, underlying_symbol: str = "BTCUSD.P") -> List[Dict[str, Any]]:
        """Get mock options chain data"""
        try:
            # Generate mock options around current price
            current_price = self.underlying_price
            strikes = []
            
            # Generate strikes around current price
            for i in range(-10, 11):
                strike = current_price * (1 + i * 0.05)  # 5% intervals
                strikes.append({
                    'id': f"mock_option_{i}",
                    'symbol': f"BTC-{strike:.0f}-C" if i >= 0 else f"BTC-{strike:.0f}-P",
                    'strike': strike,
                    'option_type': 'call' if i >= 0 else 'put',
                    'expiry': (datetime.now() + timedelta(days=1)).isoformat(),
                    'premium': max(100, abs(current_price - strike) * 0.1),
                    'delta': 0.5 if i == 0 else (0.5 + i * 0.05),
                    'gamma': 0.01,
                    'theta': -0.1,
                    'vega': 0.5
                })
            
            logger.debug(f"📊 Generated {len(strikes)} mock options")
            return strikes
            
        except Exception as e:
            logger.error(f"❌ Failed to generate mock options: {e}")
            return []
    
    def get_account_info(self) -> Dict[str, Any]:
        """Get mock account information"""
        return {
            'balance': 10000.0,
            'free_margin': 8000.0,
            'used_margin': 2000.0,
            'equity': 10000.0,
            'currency': 'USD'
        }
    
    def place_order(self, order_data: Dict[str, Any]) -> Dict[str, Any]:
        """Mock order placement"""
        logger.info(f"🎭 Mock order placed: {order_data}")
        return {
            'order_id': f"mock_{int(datetime.now().timestamp())}",
            'status': 'filled',
            'filled_quantity': order_data.get('quantity', 1),
            'filled_price': order_data.get('price', 100.0),
            'timestamp': datetime.now().isoformat()
        }
    
    def cancel_order(self, order_id: str) -> bool:
        """Mock order cancellation"""
        logger.info(f"🎭 Mock order cancelled: {order_id}")
        return True
    
    def get_open_orders(self) -> List[Dict[str, Any]]:
        """Get mock open orders"""
        return []
    
    def get_positions(self) -> List[Dict[str, Any]]:
        """Get mock positions"""
        return []
