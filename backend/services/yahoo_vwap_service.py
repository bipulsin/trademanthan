"""
Yahoo Finance VWAP Service - No authentication required
"""
import requests
import logging
from datetime import datetime, timedelta
from typing import Dict, List, Optional
import pytz

logger = logging.getLogger(__name__)

class YahooVWAPService:
    """Service to fetch VWAP data using Yahoo Finance"""
    
    def __init__(self):
        self.base_url = "https://query1.finance.yahoo.com/v8/finance/chart"
        
    def get_symbol_suffix(self, symbol: str) -> str:
        """
        Convert Indian stock symbol to Yahoo Finance format
        NSE stocks: {SYMBOL}.NS
        BSE stocks: {SYMBOL}.BO
        """
        symbol = symbol.strip().upper()
        # Default to NSE
        return f"{symbol}.NS"
    
    def calculate_vwap(self, candle_data: List[Dict]) -> float:
        """
        Calculate VWAP from candle data
        VWAP = Σ(Price × Volume) / Σ(Volume)
        Price = (High + Low + Close) / 3
        """
        if not candle_data:
            return 0.0
        
        try:
            total_pv = 0.0  # Price × Volume
            total_volume = 0.0
            
            for candle in candle_data:
                # Typical price
                typical_price = (candle['high'] + candle['low'] + candle['close']) / 3
                volume = candle['volume']
                
                total_pv += typical_price * volume
                total_volume += volume
            
            if total_volume > 0:
                return round(total_pv / total_volume, 2)
            return 0.0
            
        except Exception as e:
            logger.error(f"Error calculating VWAP: {str(e)}")
            return 0.0
    
    def get_historical_candles(self, symbol: str, period: str = "2d", interval: str = "1h") -> Optional[List[Dict]]:
        """
        Fetch historical candle data from Yahoo Finance
        
        Args:
            symbol: Stock symbol (e.g., "RELIANCE")
            period: Data period (1d, 2d, 5d, 1mo)
            interval: Candle interval (1h, 30m, 1d)
            
        Returns:
            List of candle data or None
        """
        try:
            # Convert to Yahoo Finance symbol
            yahoo_symbol = self.get_symbol_suffix(symbol)
            
            # Build URL
            url = f"{self.base_url}/{yahoo_symbol}"
            params = {
                "period": period,
                "interval": interval,
                "includePrePost": "false"
            }
            
            # Make request
            response = requests.get(url, params=params, timeout=10)
            
            if response.status_code == 200:
                data = response.json()
                
                if 'chart' in data and 'result' in data['chart'] and len(data['chart']['result']) > 0:
                    result = data['chart']['result'][0]
                    
                    if 'timestamp' not in result or 'indicators' not in result:
                        logger.warning(f"Incomplete data for {symbol}")
                        return None
                    
                    timestamps = result['timestamp']
                    indicators = result['indicators']['quote'][0]
                    
                    # Extract OHLCV data
                    candles = []
                    for i in range(len(timestamps)):
                        try:
                            candle = {
                                'timestamp': timestamps[i],
                                'open': float(indicators['open'][i]) if indicators['open'][i] is not None else 0.0,
                                'high': float(indicators['high'][i]) if indicators['high'][i] is not None else 0.0,
                                'low': float(indicators['low'][i]) if indicators['low'][i] is not None else 0.0,
                                'close': float(indicators['close'][i]) if indicators['close'][i] is not None else 0.0,
                                'volume': float(indicators['volume'][i]) if indicators['volume'][i] is not None else 0.0
                            }
                            
                            # Skip candles with zero/null data
                            if candle['high'] > 0 and candle['low'] > 0 and candle['volume'] > 0:
                                candles.append(candle)
                        except (TypeError, ValueError, IndexError) as e:
                            continue
                    
                    logger.info(f"Fetched {len(candles)} candles for {symbol}")
                    return candles
                else:
                    logger.warning(f"No data in Yahoo Finance response for {symbol}")
                    return None
            else:
                logger.error(f"Yahoo Finance API error for {symbol}: {response.status_code}")
                return None
                
        except Exception as e:
            logger.error(f"Error fetching candles for {symbol}: {str(e)}")
            return None
    
    def get_vwap_data(self, symbol: str) -> Dict[str, float]:
        """
        Get current hour VWAP and previous hour VWAP for a symbol
        
        Returns:
            {
                'current_hour_vwap': float,
                'previous_hour_vwap': float
            }
        """
        try:
            # Fetch last 2 days of 1-hour candles
            candles = self.get_historical_candles(symbol, period="2d", interval="1h")
            
            if not candles or len(candles) < 2:
                logger.warning(f"Insufficient candle data for {symbol}, got {len(candles) if candles else 0} candles")
                return {
                    'current_hour_vwap': 0.0,
                    'previous_hour_vwap': 0.0
                }
            
            # Sort candles by timestamp (most recent first)
            candles.sort(key=lambda x: x['timestamp'], reverse=True)
            
            # Current hour VWAP (most recent candle)
            current_hour_vwap = self.calculate_vwap([candles[0]])
            
            # Previous hour VWAP (second most recent candle)
            previous_hour_vwap = self.calculate_vwap([candles[1]]) if len(candles) > 1 else 0.0
            
            logger.info(f"VWAP for {symbol}: Current={current_hour_vwap}, Previous={previous_hour_vwap}")
            
            return {
                'current_hour_vwap': current_hour_vwap,
                'previous_hour_vwap': previous_hour_vwap
            }
            
        except Exception as e:
            logger.error(f"Error getting VWAP data for {symbol}: {str(e)}")
            return {
                'current_hour_vwap': 0.0,
                'previous_hour_vwap': 0.0
            }
    
    def enrich_stocks_with_vwap(self, stocks: List[Dict]) -> List[Dict]:
        """
        Enrich stock list with VWAP data
        
        Args:
            stocks: List of stock dicts with 'stock_name' and 'trigger_price'
            
        Returns:
            List of enriched stock dicts with VWAP data added
        """
        enriched_stocks = []
        
        for stock in stocks:
            stock_name = stock.get('stock_name', '')
            
            # Get VWAP data
            vwap_data = self.get_vwap_data(stock_name)
            
            # Add VWAP data to stock
            enriched_stock = stock.copy()
            enriched_stock['current_hour_vwap'] = vwap_data['current_hour_vwap']
            enriched_stock['previous_hour_vwap'] = vwap_data['previous_hour_vwap']
            
            enriched_stocks.append(enriched_stock)
        
        return enriched_stocks


# Initialize Yahoo VWAP service
yahoo_vwap_service = YahooVWAPService()


