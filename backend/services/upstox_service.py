"""
Upstox API Service for fetching market data
"""
import requests
import logging
import time
from datetime import datetime, timedelta
from typing import Dict, Optional, List
import pytz

logger = logging.getLogger(__name__)

class UpstoxService:
    """Service to interact with Upstox API"""
    
    # Index instrument keys
    NIFTY50_KEY = "NSE_INDEX|Nifty 50"
    BANKNIFTY_KEY = "NSE_INDEX|Nifty Bank"
    
    def __init__(self, api_key: str, api_secret: str, access_token: str = None):
        self.api_key = api_key
        self.api_secret = api_secret
        
        # Try to load token from token manager first, fallback to provided token
        try:
            from services.token_manager import load_upstox_token
            loaded_token = load_upstox_token()
            self.access_token = loaded_token or access_token
            if loaded_token:
                logger.info("✅ Using Upstox token from token manager")
            elif access_token:
                logger.info("✅ Using Upstox token from initialization parameter")
            else:
                logger.warning("⚠️ No Upstox token available")
        except Exception as e:
            logger.warning(f"Could not load token from manager: {e}, using provided token")
            self.access_token = access_token
        
        self.base_url = "https://api.upstox.com/v3"
    
    def reload_token_from_storage(self) -> bool:
        """
        Reload access token from token manager storage
        This is called to refresh the token without restarting the service
        
        Returns:
            True if token was loaded successfully
        """
        try:
            from services.token_manager import load_upstox_token
            loaded_token = load_upstox_token()
            
            if loaded_token:
                old_token_preview = self.access_token[:20] if self.access_token else "None"
                new_token_preview = loaded_token[:20]
                
                self.access_token = loaded_token
                logger.info(f"🔄 Token reloaded: {old_token_preview}... → {new_token_preview}...")
                return True
            else:
                logger.warning("⚠️ No token found in storage")
                return False
                
        except Exception as e:
            logger.error(f"❌ Failed to reload token: {str(e)}")
            return False
        
    def refresh_access_token(self) -> Optional[str]:
        """
        Refresh the Upstox access token using API key and secret
        
        Returns:
            New access token if successful, None otherwise
        """
        try:
            # Try different redirect URIs that might be configured
            redirect_uris = [
                "https://trademanthan.in",
                "https://trademanthan.in/",
                "http://localhost:3000",
                "https://localhost:3000",
                "https://api.upstox.com",
                "https://upstox.com"
            ]
            
            for redirect_uri in redirect_uris:
                try:
                    # Upstox login endpoint
                    url = "https://api.upstox.com/v2/login/authorization/token"
                    
                    headers = {
                        "Accept": "application/json",
                        "Content-Type": "application/x-www-form-urlencoded"
                    }
                    
                    data = {
                        "code": self.api_key,  # Using API key as code
                        "client_id": self.api_key,
                        "client_secret": self.api_secret,
                        "redirect_uri": redirect_uri,
                        "grant_type": "authorization_code"
                    }
                    
                    logger.info(f"Trying token refresh with redirect_uri: {redirect_uri}")
                    response = requests.post(url, headers=headers, data=data, timeout=10)
                    
                    if response.status_code == 200:
                        result = response.json()
                        if result.get('status') == 'success':
                            new_token = result.get('access_token')
                            if new_token:
                                logger.info(f"Successfully refreshed Upstox access token with redirect_uri: {redirect_uri}")
                                # Update the instance token
                                self.access_token = new_token
                                return new_token
                        else:
                            logger.warning(f"Token refresh failed with {redirect_uri}: {result}")
                    else:
                        logger.warning(f"Token refresh API error with {redirect_uri}: {response.status_code} - {response.text}")
                        
                except Exception as e:
                    logger.warning(f"Error with redirect_uri {redirect_uri}: {str(e)}")
                    continue
                    
        except Exception as e:
            logger.error(f"Error refreshing access token: {str(e)}")
            
        return None

    def get_headers(self) -> Dict[str, str]:
        """Get request headers with authentication"""
        return {
            "Accept": "application/json",
            "Authorization": f"Bearer {self.access_token}"
        }
    
    def make_api_request(
        self,
        url: str,
        method: str = "GET",
        params: Optional[Dict] = None,
        data: Optional[Dict] = None,
        timeout: int = 10,
        max_retries: int = 2
    ) -> Optional[Dict]:
        """
        Make a request to Upstox API with built-in retry, token refresh, and error handling
        
        Args:
            url: API endpoint URL
            method: HTTP method (GET or POST)
            params: Query parameters
            data: Request body data
            timeout: Request timeout in seconds
            max_retries: Maximum number of retries
        
        Returns:
            API response data or None
        """
        last_error = None
        
        for attempt in range(max_retries):
            try:
                headers = self.get_headers()
                
                # Make request
                if method.upper() == "GET":
                    response = requests.get(
                        url,
                        headers=headers,
                        params=params,
                        timeout=timeout
                    )
                elif method.upper() == "POST":
                    response = requests.post(
                        url,
                        headers=headers,
                        json=data,
                        timeout=timeout
                    )
                else:
                    raise ValueError(f"Unsupported HTTP method: {method}")
                
                # Handle different status codes
                if response.status_code == 200:
                    response_data = response.json()
                    
                    # Return data (let caller validate success status)
                    return response_data
                
                elif response.status_code == 401:
                    # Token expired
                    logger.warning(f"🔑 Token expired (attempt {attempt + 1}/{max_retries}) for {url}")
                    
                    # Try to reload token from storage (faster)
                    if self.reload_token_from_storage():
                        logger.info("✅ Token reloaded from storage, retrying...")
                        time.sleep(0.5)  # Small delay before retry
                        continue
                    
                    # If not available in storage, log warning
                    logger.error("❌ Token reload failed - manual token refresh needed")
                    last_error = "Token expired and reload failed"
                    break
                
                elif response.status_code == 429:
                    # Rate limit exceeded
                    wait_time = min(2 ** attempt, 8)  # Exponential backoff, max 8s
                    logger.warning(f"⏱️ Rate limit (429) for {url}, waiting {wait_time}s...")
                    time.sleep(wait_time)
                    continue
                
                elif response.status_code == 400:
                    # Bad request - don't retry
                    logger.error(f"❌ Bad request (400) for {url}: {response.text[:200]}")
                    last_error = f"Bad request: {response.text[:100]}"
                    break
                
                elif response.status_code == 404:
                    # Not found - don't retry
                    logger.error(f"❌ Resource not found (404): {url}")
                    last_error = "Resource not found"
                    break
                
                elif response.status_code >= 500:
                    # Server error - retry
                    logger.warning(f"⚠️ Server error ({response.status_code}) for {url}, retrying...")
                    time.sleep(1)
                    continue
                
                else:
                    # Other errors
                    logger.error(f"❌ HTTP {response.status_code} for {url}: {response.text[:200]}")
                    last_error = f"HTTP {response.status_code}"
                    break
            
            except requests.exceptions.Timeout:
                logger.warning(f"⏱️ Request timeout (attempt {attempt + 1}/{max_retries}) for {url}")
                last_error = "Timeout"
                if attempt < max_retries - 1:
                    time.sleep(1)
                    continue
                break
            
            except requests.exceptions.ConnectionError as e:
                logger.warning(f"🔌 Connection error (attempt {attempt + 1}/{max_retries}) for {url}")
                last_error = "Connection error"
                if attempt < max_retries - 1:
                    time.sleep(2)
                    continue
                break
            
            except requests.exceptions.RequestException as e:
                logger.error(f"❌ Request exception for {url}: {str(e)}")
                last_error = str(e)
                break
            
            except Exception as e:
                logger.error(f"❌ Unexpected error for {url}: {str(e)}")
                last_error = str(e)
                break
        
        # All retries failed
        logger.error(f"❌ All {max_retries} attempts failed for {url}: {last_error}")
        return None
    
    def get_market_holidays(self, year: int = None) -> List[str]:
        """
        Get list of market holidays from Upstox API
        
        Args:
            year: Year for which to fetch holidays (default: current year)
            
        Returns:
            List of holiday dates in 'YYYY-MM-DD' format
        """
        try:
            if year is None:
                ist = pytz.timezone('Asia/Kolkata')
                year = datetime.now(ist).year
            
            # Upstox v2 API endpoint for market holidays (requires date format, not just year)
            # Use January 1st of the year to get all holidays for that year
            date_param = f"{year}-01-01"
            url = f"https://api.upstox.com/v2/market/holidays/{date_param}"
            
            response = requests.get(url, headers=self.get_headers(), timeout=10)
            
            if response.status_code == 200:
                data = response.json()
                
                if data.get('status') == 'success' and 'data' in data:
                    holidays = []
                    for holiday in data['data']:
                        # Extract date from holiday entry
                        if 'date' in holiday:
                            holidays.append(holiday['date'])
                    
                    logger.info(f"Fetched {len(holidays)} market holidays for {year}")
                    return holidays
                else:
                    logger.warning(f"No holiday data in response: {data}")
                    return []
            else:
                logger.warning(f"Failed to fetch holidays: {response.status_code} - {response.text}")
                return []
                
        except Exception as e:
            logger.error(f"Error fetching market holidays: {str(e)}")
            return []
    
    def get_last_trading_date(self, reference_date: datetime = None) -> datetime:
        """
        Get the last trading date (excluding weekends and holidays)
        
        Args:
            reference_date: Date to check from (default: today)
            
        Returns:
            Last trading date as datetime object
        """
        try:
            ist = pytz.timezone('Asia/Kolkata')
            
            if reference_date is None:
                reference_date = datetime.now(ist)
            elif reference_date.tzinfo is None:
                reference_date = ist.localize(reference_date)
            
            # Get market holidays for current year
            holidays = self.get_market_holidays(reference_date.year)
            holiday_dates = set(holidays)
            
            # Start from reference date and go backwards
            current_date = reference_date.replace(hour=0, minute=0, second=0, microsecond=0)
            
            # Go back maximum 10 days to find last trading day
            for _ in range(10):
                # Check if it's a weekday (Monday=0, Sunday=6)
                if current_date.weekday() < 5:  # Monday to Friday
                    # Check if it's not a holiday
                    date_str = current_date.strftime('%Y-%m-%d')
                    if date_str not in holiday_dates:
                        logger.info(f"Last trading date: {date_str} (from {reference_date.strftime('%Y-%m-%d')})")
                        return current_date
                
                # Go to previous day
                current_date = current_date - timedelta(days=1)
            
            # If we couldn't find a trading day in last 10 days, return reference date
            logger.warning(f"Could not find trading day in last 10 days, using reference date")
            return reference_date
            
        except Exception as e:
            logger.error(f"Error getting last trading date: {str(e)}")
            return reference_date if reference_date else datetime.now(ist)
    
    def is_trading_day(self, date: datetime = None) -> bool:
        """
        Check if a given date is a trading day (not weekend or holiday)
        
        Args:
            date: Date to check (default: today)
            
        Returns:
            True if trading day, False otherwise
        """
        try:
            ist = pytz.timezone('Asia/Kolkata')
            
            if date is None:
                date = datetime.now(ist)
            elif date.tzinfo is None:
                date = ist.localize(date)
            
            # Check if weekend
            if date.weekday() >= 5:  # Saturday or Sunday
                return False
            
            # Check if holiday
            holidays = self.get_market_holidays(date.year)
            date_str = date.strftime('%Y-%m-%d')
            
            if date_str in holidays:
                logger.info(f"{date_str} is a market holiday")
                return False
            
            return True
            
        except Exception as e:
            logger.error(f"Error checking if trading day: {str(e)}")
            # Default to True if we can't determine (conservative approach)
            return True
    
    def check_index_trends(self) -> Dict[str, any]:
        """
        Check if NIFTY50 and BANKNIFTY are in opposite trends
        Uses historical candle data since market quote requires special permissions
        
        Returns:
            {
                'nifty_trend': 'bullish' | 'bearish' | 'neutral',
                'banknifty_trend': 'bullish' | 'bearish' | 'neutral',
                'opposite_trends': bool,
                'allow_trading': bool,
                'message': str,
                'nifty_data': {...},
                'banknifty_data': {...}
            }
        """
        try:
            # Use OHLC endpoint for more accurate and up-to-date data
            logger.info("Fetching NIFTY and BANKNIFTY data using OHLC endpoint")
            
            # Get OHLC data for both indices
            nifty_ohlc = self.get_ohlc_data(self.NIFTY50_KEY)
            banknifty_ohlc = self.get_ohlc_data(self.BANKNIFTY_KEY)
            
            nifty_day_open = 0
            nifty_ltp = 0
            banknifty_day_open = 0
            banknifty_ltp = 0
            
            # Process NIFTY OHLC data
            if nifty_ohlc and nifty_ohlc.get('open', 0) > 0:
                nifty_day_open = nifty_ohlc['open']
                nifty_ltp = nifty_ohlc['last_price'] if nifty_ohlc['last_price'] > 0 else nifty_ohlc['close']
                logger.info(f"NIFTY OHLC: Day Open={nifty_day_open}, LTP={nifty_ltp}")
            else:
                logger.warning("NIFTY OHLC data not available, using market quote data")
                # Fallback to market quote API
                nifty_quote = self.get_market_quote_by_key(self.NIFTY50_KEY)
                if nifty_quote:
                    nifty_ltp = nifty_quote.get('last_price', 0)
                    nifty_day_open = nifty_quote.get('ohlc', {}).get('open', 0)
                else:
                    nifty_day_open = 0
                    nifty_ltp = 0
            
            # Process BANKNIFTY OHLC data
            if banknifty_ohlc and banknifty_ohlc.get('open', 0) > 0:
                banknifty_day_open = banknifty_ohlc['open']
                banknifty_ltp = banknifty_ohlc['last_price'] if banknifty_ohlc['last_price'] > 0 else banknifty_ohlc['close']
                logger.info(f"BANKNIFTY OHLC: Day Open={banknifty_day_open}, LTP={banknifty_ltp}")
            else:
                logger.warning("BANKNIFTY OHLC data not available, using market quote data")
                # Fallback to market quote API
                banknifty_quote = self.get_market_quote_by_key(self.BANKNIFTY_KEY)
                if banknifty_quote:
                    banknifty_ltp = banknifty_quote.get('last_price', 0)
                    banknifty_day_open = banknifty_quote.get('ohlc', {}).get('open', 0)
                else:
                    banknifty_day_open = 0
                    banknifty_ltp = 0
            
            if nifty_day_open == 0 or banknifty_day_open == 0:
                logger.warning("Could not fetch index data")
                return {
                    'nifty_trend': 'unknown',
                    'banknifty_trend': 'unknown',
                    'opposite_trends': False,
                    'allow_trading': True,
                    'message': 'Index data unavailable',
                    'nifty_data': None,
                    'banknifty_data': None
                }
            
            # Determine trends by comparing day open vs current LTP
            # Bullish: LTP > Day Open
            # Bearish: LTP < Day Open
            nifty_trend = 'bullish' if nifty_ltp > nifty_day_open else ('bearish' if nifty_ltp < nifty_day_open else 'neutral')
            banknifty_trend = 'bullish' if banknifty_ltp > banknifty_day_open else ('bearish' if banknifty_ltp < banknifty_day_open else 'neutral')
            
            # Check if opposite
            opposite_trends = (nifty_trend == 'bullish' and banknifty_trend == 'bearish') or \
                             (nifty_trend == 'bearish' and banknifty_trend == 'bullish')
            
            allow_trading = not opposite_trends
            
            message = f"NIFTY: {nifty_trend.upper()} | BANKNIFTY: {banknifty_trend.upper()}"
            if opposite_trends:
                message = "NIFTY & BANKNIFTY in opposite trend, so NO NEW trade allowed"
            
            logger.info(f"Index check: NIFTY={nifty_trend} (Open:{nifty_day_open}, LTP:{nifty_ltp}), BANKNIFTY={banknifty_trend} (Open:{banknifty_day_open}, LTP:{banknifty_ltp}), Allow={allow_trading}")
            
            return {
                'nifty_trend': nifty_trend,
                'banknifty_trend': banknifty_trend,
                'opposite_trends': opposite_trends,
                'allow_trading': allow_trading,
                'message': message,
                'nifty_data': {
                    'day_open': nifty_day_open,
                    'ltp': nifty_ltp,
                    'last_price': nifty_ltp
                },
                'banknifty_data': {
                    'day_open': banknifty_day_open,
                    'ltp': banknifty_ltp,
                    'last_price': banknifty_ltp
                }
            }
            
        except Exception as e:
            logger.error(f"Error checking index trends: {str(e)}")
            return {
                'nifty_trend': 'unknown',
                'banknifty_trend': 'unknown',
                'opposite_trends': False,
                'allow_trading': True,  # Allow on error to not block
                'message': 'Index check failed',
                'nifty_data': None,
                'banknifty_data': None
            }
    
    def get_option_instrument_key(self, symbol: str, expiry_date: datetime, strike: float, option_type: str) -> Optional[str]:
        """
        Create option instrument key for Upstox API
        
        Format: NSE_FO|SYMBOL{YY}{MMM}{STRIKE}{CE/PE}
        Example: NSE_FO|RELIANCE25NOV1450CE
        
        Args:
            symbol: Stock symbol
            expiry_date: Expiry datetime
            strike: Strike price
            option_type: 'CE' or 'PE'
            
        Returns:
            Option instrument key or None
        """
        try:
            # Format: NSE_FO|SYMBOL{YY}{MMM}{STRIKE}{CE/PE}
            year_short = expiry_date.strftime('%y')  # Last 2 digits of year
            month_short = expiry_date.strftime('%b').upper()  # Short month name
            strike_int = int(strike)
            
            # Option instrument key format
            option_key = f"NSE_FO|{symbol}{year_short}{month_short}{strike_int}{option_type}"
            
            logger.info(f"Option instrument key: {option_key}")
            return option_key
            
        except Exception as e:
            logger.error(f"Error creating option instrument key: {str(e)}")
            return None
    
    def get_option_ltp(self, symbol: str, expiry_date: datetime, strike: float, option_type: str) -> Optional[float]:
        """
        Get LTP (Last Traded Price) for an option contract
        
        Args:
            symbol: Stock symbol
            expiry_date: Expiry datetime
            strike: Strike price
            option_type: 'CE' or 'PE'
            
        Returns:
            Option LTP or None
        """
        try:
            # Get option instrument key
            option_key = self.get_option_instrument_key(symbol, expiry_date, strike, option_type)
            
            if not option_key:
                return None
            
            # Try to get market quote for the option
            url = f"https://api.upstox.com/v2/market-quote/ltp"
            params = {'instrument_key': option_key}
            
            response = requests.get(url, headers=self.get_headers(), params=params, timeout=10)
            
            if response.status_code == 200:
                data = response.json()
                
                if data.get('status') == 'success' and 'data' in data:
                    option_data = data['data'].get(option_key, {})
                    ltp = option_data.get('last_price', 0)
                    
                    if ltp > 0:
                        logger.info(f"Option LTP for {option_key}: ₹{ltp}")
                        return float(ltp)
            
            # Fallback: Try historical candles for the option
            candles = self.get_historical_candles_by_instrument_key(option_key, interval="hours/1", days_back=2)
            
            if candles and len(candles) > 0:
                candles.sort(key=lambda x: x['timestamp'], reverse=True)
                ltp = round(candles[0]['close'], 2)
                logger.info(f"Option LTP from candle for {option_key}: ₹{ltp}")
                return ltp
            
            logger.warning(f"Could not get option LTP for {option_key}")
            return None
            
        except Exception as e:
            logger.error(f"Error getting option LTP: {str(e)}")
            return None
    
    def get_option_vwap(self, symbol: str, expiry_date: datetime, strike: float, option_type: str) -> Optional[float]:
        """
        Get VWAP for an option contract
        
        Args:
            symbol: Stock symbol
            expiry_date: Expiry datetime
            strike: Strike price
            option_type: 'CE' or 'PE'
            
        Returns:
            Option VWAP or None
        """
        try:
            # Get option instrument key
            option_key = self.get_option_instrument_key(symbol, expiry_date, strike, option_type)
            
            if not option_key:
                return None
            
            # Fetch hourly candles for the option
            candles = self.get_historical_candles_by_instrument_key(option_key, interval="hours/1", days_back=5)
            
            if not candles or len(candles) == 0:
                logger.warning(f"No candle data for option VWAP: {option_key}")
                return None
            
            # Calculate VWAP
            vwap = self.calculate_vwap(candles)
            
            if vwap > 0:
                logger.info(f"Option VWAP for {option_key}: ₹{vwap}")
                return vwap
            
            return None
            
        except Exception as e:
            logger.error(f"Error getting option VWAP: {str(e)}")
            return None
    
    def get_historical_candles_by_instrument_key(self, instrument_key: str, interval: str = "hours/1", days_back: int = 2) -> Optional[List[Dict]]:
        """
        Fetch historical candle data using instrument key directly
        Uses improved API request with automatic retry and token refresh
        
        Args:
            instrument_key: Full instrument key (e.g., "NSE_FO|RELIANCE25NOV1450CE")
            interval: Candle interval
            days_back: Number of days to fetch
            
        Returns:
            List of candle data or None
        """
        try:
            # Calculate date range
            ist = pytz.timezone('Asia/Kolkata')
            end_date = datetime.now(ist)
            start_date = end_date - timedelta(days=days_back)
            
            # Format dates for API
            to_date = end_date.strftime("%Y-%m-%d")
            from_date = start_date.strftime("%Y-%m-%d")
            
            # Upstox V3 API endpoint
            url = f"{self.base_url}/historical-candle/{instrument_key}/{interval}/{to_date}/{from_date}"
            
            logger.info(f"📊 Fetching {interval} candles for {instrument_key} ({from_date} to {to_date})")
            
            # Use improved API request with retry and token refresh
            data = self.make_api_request(url, method="GET", timeout=15, max_retries=2)
            
            if data and data.get('status') == 'success' and 'data' in data:
                candles = data['data'].get('candles', [])
                
                # Convert to structured format
                structured_candles = []
                for candle in candles:
                    if len(candle) >= 6:
                        structured_candles.append({
                            'timestamp': candle[0],
                            'open': float(candle[1]),
                            'high': float(candle[2]),
                            'low': float(candle[3]),
                            'close': float(candle[4]),
                            'volume': float(candle[5])
                        })
                
                logger.info(f"✅ Fetched {len(structured_candles)} candles for {instrument_key}")
                return structured_candles
            else:
                logger.warning(f"⚠️ No candle data for {instrument_key}")
                return None
                
        except Exception as e:
            logger.error(f"❌ Error fetching candles for {instrument_key}: {str(e)}")
            return None
    
    def get_option_chain(self, symbol: str) -> Optional[Dict]:
        """
        Get option chain data for a symbol from Upstox
        
        Args:
            symbol: Stock symbol (e.g., "RELIANCE")
            
        Returns:
            Option chain data or None
        """
        try:
            instrument_key = self.get_instrument_key(symbol)
            
            if not instrument_key:
                logger.error(f"Could not get instrument key for {symbol}")
                return None
            
            # Get monthly expiry date
            monthly_expiry = self.get_monthly_expiry()
            expiry_date_str = monthly_expiry.strftime('%Y-%m-%d')
            
            # Upstox v2 API endpoint for option chain
            url = f"https://api.upstox.com/v2/option/chain"
            
            params = {
                'instrument_key': instrument_key,
                'expiry_date': expiry_date_str  # Monthly expiry date
            }
            
            logger.info(f"Fetching option chain for {symbol} with expiry {expiry_date_str}")
            response = requests.get(url, headers=self.get_headers(), params=params, timeout=10)
            
            if response.status_code == 200:
                data = response.json()
                
                if data.get('status') == 'success':
                    logger.info(f"Fetched option chain for {symbol}")
                    return data.get('data', {})
                else:
                    logger.warning(f"No option chain data for {symbol}: {data}")
                    return None
            else:
                logger.error(f"Option chain API error for {symbol}: {response.status_code} - {response.text}")
                return None
                
        except Exception as e:
            logger.error(f"Error fetching option chain for {symbol}: {str(e)}")
            return None
    
    def calculate_strike_interval(self, spot_price: float) -> float:
        """
        Calculate standard NSE strike interval based on spot price
        
        Returns:
            Strike interval (10, 50, or 100)
        """
        if spot_price < 1000:
            return 10
        elif spot_price < 5000:
            return 50
        else:
            return 100
    
    def get_monthly_expiry(self, reference_date: datetime = None) -> datetime:
        """
        Get the monthly derivative expiry date (last TUESDAY of the month)
        
        Logic:
        - If current date <= 18th → Use current month's expiry
        - If current date > 18th → Use next month's expiry
        
        Args:
            reference_date: Reference date (default: today)
            
        Returns:
            Expiry datetime (last Tuesday of the month)
        """
        try:
            ist = pytz.timezone('Asia/Kolkata')
            
            if reference_date is None:
                reference_date = datetime.now(ist)
            elif reference_date.tzinfo is None:
                reference_date = ist.localize(reference_date)
            
            # Determine which month's expiry to use
            if reference_date.day <= 18:
                # Use current month's expiry
                expiry_month = reference_date.month
                expiry_year = reference_date.year
            else:
                # Use next month's expiry
                if reference_date.month == 12:
                    expiry_month = 1
                    expiry_year = reference_date.year + 1
                else:
                    expiry_month = reference_date.month + 1
                    expiry_year = reference_date.year
            
            # Find last Tuesday of the expiry month
            # Start from last day of month and go backwards
            import calendar
            last_day = calendar.monthrange(expiry_year, expiry_month)[1]
            
            # Start from last day of month
            expiry_date = ist.localize(datetime(expiry_year, expiry_month, last_day))
            
            # Go backwards to find last Tuesday (weekday 1)
            while expiry_date.weekday() != 1:  # 1 = Tuesday
                expiry_date = expiry_date - timedelta(days=1)
            
            logger.info(f"Monthly expiry: {expiry_date.strftime('%d %b %Y %A')} (from reference: {reference_date.strftime('%d %b %Y')})")
            
            return expiry_date
            
        except Exception as e:
            logger.error(f"Error calculating monthly expiry: {str(e)}")
            # Fallback to end of month
            return reference_date if reference_date else datetime.now(ist)
    
    def get_otm1_strike(self, symbol: str, option_type: str = 'CE', spot_price: float = None) -> Optional[float]:
        """
        Get the OTM-1 strike price for a symbol
        
        Args:
            symbol: Stock symbol
            option_type: 'CE' for Call or 'PE' for Put
            spot_price: Current spot price (if not provided, will fetch)
            
        Returns:
            OTM-1 strike price or None
            
        OTM-1 Strike:
            - For CE (Call): First strike above current spot price
            - For PE (Put): First strike below current spot price
        """
        try:
            # Get current spot price if not provided
            if spot_price is None or spot_price == 0:
                spot_price = self.get_current_ltp(symbol)
                
            if not spot_price or spot_price == 0:
                logger.warning(f"Could not get spot price for {symbol}")
                return None
            
            # Try to get option chain from API first
            option_chain = self.get_option_chain(symbol)
            
            # If option chain API works, use it
            if option_chain and 'data' in option_chain:
                strikes = []
                
                for item in option_chain.get('data', []):
                    if 'strike_price' in item:
                        strikes.append(float(item['strike_price']))
                
                if strikes:
                    strikes = sorted(set(strikes))
                    
                    # Find OTM-1 strike
                    if option_type.upper() == 'CE':
                        otm1_strike = next((s for s in strikes if s > spot_price), None)
                    else:  # PE
                        otm1_strike = next((s for s in reversed(strikes) if s < spot_price), None)
                    
                    if otm1_strike:
                        logger.info(f"OTM-1 {option_type} strike for {symbol} (spot: ₹{spot_price}): ₹{otm1_strike} (from option chain)")
                        return otm1_strike
            
            # Fallback: Calculate OTM-1 strike based on standard NSE intervals
            logger.info(f"Using calculated strike for {symbol} (option chain not available)")
            
            strike_interval = self.calculate_strike_interval(spot_price)
            
            if option_type.upper() == 'CE':
                # For Call: Round up to next strike
                otm1_strike = (int(spot_price / strike_interval) + 1) * strike_interval
            else:  # PE
                # For Put: Round down to previous strike
                otm1_strike = int(spot_price / strike_interval) * strike_interval
            
            logger.info(f"Calculated OTM-1 {option_type} strike for {symbol} (spot: ₹{spot_price}, interval: {strike_interval}): ₹{otm1_strike}")
            return otm1_strike
                
        except Exception as e:
            logger.error(f"Error getting OTM-1 strike for {symbol}: {str(e)}")
            return None
    
    def get_simple_vwap(self, symbol: str) -> Optional[float]:
        """
        Get a simple VWAP for the stock using hourly candles
        Used for comparing with LTP to determine option type
        
        Args:
            symbol: Stock symbol
            
        Returns:
            VWAP value or None
        """
        try:
            # Fetch hourly candles for last 5 days to ensure sufficient data
            candles = self.get_historical_candles(symbol, interval="hours/1", days_back=5)
            
            if not candles or len(candles) == 0:
                logger.warning(f"No candle data for VWAP calculation for {symbol}")
                return None
            
            # Calculate VWAP from available candles (even if just 1 candle)
            vwap = self.calculate_vwap(candles)
            
            if vwap > 0:
                logger.info(f"VWAP for {symbol}: ₹{vwap} (from {len(candles)} candles)")
                return vwap
            else:
                logger.warning(f"VWAP calculation returned 0 for {symbol}")
                return None
            
        except Exception as e:
            logger.error(f"Error calculating VWAP for {symbol}: {str(e)}")
            return None
    
    def get_current_ltp(self, symbol: str) -> Optional[float]:
        """
        Get current Last Traded Price for a symbol
        Always returns the most recent LTP regardless of date
        
        Args:
            symbol: Stock symbol
            
        Returns:
            Last traded price or None
        """
        try:
            # Try market quote first (works during market hours)
            market_quote = self.get_market_quote(symbol)
            
            if market_quote and market_quote.get('last_price', 0) > 0:
                ltp = market_quote['last_price']
                logger.info(f"Current LTP for {symbol}: ₹{ltp} (from market quote)")
                return ltp
            
            # Fallback 1: Try 1-hour candles for last 5 days
            hour_candles = self.get_historical_candles(symbol, interval="hours/1", days_back=5)
            
            if hour_candles and len(hour_candles) > 0:
                # Sort by timestamp (most recent first)
                hour_candles.sort(key=lambda x: x['timestamp'], reverse=True)
                ltp = round(hour_candles[0]['close'], 2)
                logger.info(f"LTP for {symbol}: ₹{ltp} (from last 1-hour candle)")
                return ltp
            
            # Fallback 2: Try daily candles
            day_candles = self.get_historical_candles(symbol, interval="days/1", days_back=7)
            
            if day_candles and len(day_candles) > 0:
                day_candles.sort(key=lambda x: x['timestamp'], reverse=True)
                ltp = round(day_candles[0]['close'], 2)
                logger.info(f"LTP for {symbol}: ₹{ltp} (from last daily candle)")
                return ltp
            
            logger.warning(f"Could not get LTP for {symbol} - no candle data available")
            return None
            
        except Exception as e:
            logger.error(f"Error getting LTP for {symbol}: {str(e)}")
            return None
    
    def get_stock_ltp_from_market_quote(self, stock_name: str) -> Optional[float]:
        """
        Fetch LTP for a stock using the working market-quote endpoint
        
        Args:
            stock_name: Stock symbol (e.g., 'RELIANCE', 'TATASTEEL')
            
        Returns:
            LTP price as float, or None if not found
        """
        try:
            # Use the existing working get_market_quote method
            market_data = self.get_market_quote(stock_name)
            
            if market_data and market_data.get('last_price', 0) > 0:
                ltp = market_data['last_price']
                logger.info(f"LTP for {stock_name}: ₹{ltp}")
                return float(ltp)
            else:
                logger.warning(f"No LTP data found for {stock_name}")
                return None
                
        except Exception as e:
            logger.error(f"Error fetching LTP for {stock_name}: {str(e)}")
            return None
    
    def get_day_close_price(self, symbol: str) -> Optional[float]:
        """
        Get the most recent day's close price
        This gives the actual market close price
        
        Returns:
            Close price as float or None
        """
        try:
            # Get instrument key
            instrument_key = self.get_instrument_key(symbol)
            
            # Calculate date range - last 5 trading days
            ist = pytz.timezone('Asia/Kolkata')
            end_date = datetime.now(ist)
            start_date = end_date - timedelta(days=7)
            
            to_date = end_date.strftime("%Y-%m-%d")
            from_date = start_date.strftime("%Y-%m-%d")
            
            # Fetch daily candles (V3 API uses days/1 format)
            url = f"{self.base_url}/historical-candle/{instrument_key}/days/1/{to_date}/{from_date}"
            
            response = requests.get(url, headers=self.get_headers(), timeout=10)
            
            if response.status_code == 200:
                data = response.json()
                if data.get('status') == 'success' and 'data' in data:
                    candles = data['data'].get('candles', [])
                    if candles and len(candles) > 0:
                        # Most recent day's close price (index 4 in array)
                        close_price = float(candles[0][4])
                        logger.info(f"Day close price for {symbol}: ₹{close_price}")
                        return close_price
            
            return None
            
        except Exception as e:
            logger.error(f"Error fetching day close for {symbol}: {str(e)}")
            return None
    
    def get_ohlc_data(self, instrument_key: str) -> Optional[Dict]:
        """
        Get OHLC data for an instrument using the OHLC endpoint
        
        Returns:
            {
                'open': float,
                'high': float,
                'low': float,
                'close': float,
                'last_price': float
            }
        """
        try:
            # OHLC endpoint (using v2 like other market-quote endpoints)
            url = f"https://api.upstox.com/v2/market-quote/ohlc?instrument_key={instrument_key}&interval=1d"
            
            # Make request
            response = requests.get(url, headers=self.get_headers(), timeout=10)
            
            if response.status_code == 200:
                data = response.json()
                
                if data.get('status') == 'success' and 'data' in data:
                    # Try different key formats:
                    ohlc_data = None
                    
                    # Try to find the OHLC data
                    for key in data['data']:
                        if instrument_key in key:
                            ohlc_data = data['data'][key]
                            break
                    
                    if not ohlc_data and len(data['data']) > 0:
                        # Use first available data if exact match not found
                        ohlc_data = list(data['data'].values())[0]
                    
                    if ohlc_data:
                        ohlc = ohlc_data.get('ohlc', {})
                        last_price = float(ohlc_data.get('last_price', 0))
                        
                        result = {
                            'open': float(ohlc.get('open', 0)),
                            'high': float(ohlc.get('high', 0)),
                            'low': float(ohlc.get('low', 0)),
                            'close': float(ohlc.get('close', 0)),
                            'last_price': last_price
                        }
                        
                        logger.info(f"OHLC data for {instrument_key}: Open={result['open']}, Close={result['close']}, Last={result['last_price']}")
                        return result
                else:
                    logger.warning(f"No OHLC data found for {instrument_key} in response")
            elif response.status_code == 401:
                logger.warning(f"Token expired for {instrument_key}, attempting to refresh...")
                # Try to refresh token and retry once
                if self.refresh_access_token():
                    logger.info(f"Token refreshed, retrying OHLC request for {instrument_key}")
                    response = requests.get(url, headers=self.get_headers(), timeout=10)
                    if response.status_code == 200:
                        data = response.json()
                        if data.get('status') == 'success' and 'data' in data:
                            ohlc_data = None
                            for key in data['data']:
                                if instrument_key in key:
                                    ohlc_data = data['data'][key]
                                    break
                            if not ohlc_data and len(data['data']) > 0:
                                ohlc_data = list(data['data'].values())[0]
                            if ohlc_data:
                                ohlc = ohlc_data.get('ohlc', {})
                                last_price = float(ohlc_data.get('last_price', 0))
                                result = {
                                    'open': float(ohlc.get('open', 0)),
                                    'high': float(ohlc.get('high', 0)),
                                    'low': float(ohlc.get('low', 0)),
                                    'close': float(ohlc.get('close', 0)),
                                    'last_price': last_price
                                }
                                logger.info(f"OHLC data for {instrument_key} after token refresh: Open={result['open']}, Close={result['close']}")
                                return result
                else:
                    logger.error(f"Failed to refresh token for {instrument_key}")
            else:
                logger.error(f"OHLC API error for {instrument_key}: {response.status_code} - {response.text}")
                
        except Exception as e:
            logger.error(f"Error fetching OHLC data for {instrument_key}: {str(e)}")
            
        return None

    def get_market_quote_by_key(self, instrument_key: str) -> Optional[Dict]:
        """
        Get real-time market quote (LTP) using instrument key directly
        Uses improved API request with automatic retry and token refresh
        
        Returns:
            {
                'last_price': float,
                'close_price': float,
                'ohlc': {...}
            }
        """
        try:
            # Market quote endpoint (V2 API)
            url = f"https://api.upstox.com/v2/market-quote/quotes?instrument_key={instrument_key}"
            
            # Use improved API request with retry and token refresh
            data = self.make_api_request(url, method="GET", timeout=10, max_retries=2)
            
            if data and data.get('status') == 'success' and 'data' in data:
                # Try different key formats to find exact match
                # Upstox API may return keys in different formats than requested:
                # - Request: "NSE_FO|104500" (pipe separator)
                # - Response: "NSE_FO|104500" or "NSE_FO:104500" (colon separator) or other formats
                quote_data = None
                available_keys = list(data['data'].keys())
                
                # Strategy 1: Exact match
                if instrument_key in data['data']:
                    quote_data = data['data'][instrument_key]
                    logger.debug(f"✅ Found exact match for {instrument_key}")
                
                # Strategy 2: Handle pipe vs colon separator mismatch
                # Convert pipe to colon and vice versa for comparison
                if not quote_data:
                    # Try with colon instead of pipe
                    alt_key1 = instrument_key.replace('|', ':')
                    if alt_key1 in data['data']:
                        quote_data = data['data'][alt_key1]
                        logger.info(f"✅ Found match with colon separator: {alt_key1} (requested: {instrument_key})")
                    
                    # Try with pipe instead of colon
                    if not quote_data:
                        alt_key2 = instrument_key.replace(':', '|')
                        if alt_key2 in data['data']:
                            quote_data = data['data'][alt_key2]
                            logger.info(f"✅ Found match with pipe separator: {alt_key2} (requested: {instrument_key})")
                
                # Strategy 3: Case-insensitive normalized match (handles spaces, case differences)
                if not quote_data:
                    # Normalize: remove spaces, convert to uppercase, normalize separators to pipe
                    def normalize_key(key):
                        return key.replace(' ', '').replace(':', '|').upper()
                    
                    normalized_request = normalize_key(instrument_key)
                    for key in data['data']:
                        normalized_response = normalize_key(key)
                        if normalized_request == normalized_response:
                            quote_data = data['data'][key]
                            logger.info(f"✅ Found normalized match for {instrument_key} (response key: {key})")
                            break
                
                # Strategy 4: Extract core identifier and match (e.g., "NSE_FO|104500" -> "104500")
                if not quote_data:
                    # Extract the core identifier (token/number after separator)
                    core_id = None
                    for sep in ['|', ':']:
                        if sep in instrument_key:
                            core_id = instrument_key.split(sep)[-1]
                            break
                    if not core_id:
                        core_id = instrument_key.split()[-1] if ' ' in instrument_key else instrument_key
                    
                    if core_id:
                        for key in data['data']:
                            # Check if core_id appears in the key (at end or as substring)
                            if core_id in key or key.endswith(core_id) or key.endswith(f'|{core_id}') or key.endswith(f':{core_id}'):
                                quote_data = data['data'][key]
                                logger.info(f"✅ Found core ID match for {instrument_key} (core: {core_id}, response key: {key})")
                                break
                
                # Strategy 5: If still no match, use the first (and likely only) entry if response has exactly one key
                # This handles cases where API returns data in a different format but it's the correct instrument
                if not quote_data and len(data['data']) == 1:
                    # Only use this if we have exactly one response - it's likely the correct one
                    single_key = list(data['data'].keys())[0]
                    quote_data = data['data'][single_key]
                    logger.warning(f"⚠️ Using single response entry for {instrument_key} (response key: {single_key})")
                    logger.warning(f"   This assumes the API returned the correct instrument despite key format mismatch")
                
                # CRITICAL FIX: Remove dangerous fallback that uses first value
                # If we can't find a match, return None instead of using wrong data
                if not quote_data:
                    logger.error(f"❌ CRITICAL: No match found for instrument_key '{instrument_key}'")
                    logger.error(f"   Requested format: {instrument_key}")
                    logger.error(f"   Available keys in response ({len(available_keys)} total):")
                    for i, key in enumerate(available_keys[:10]):  # Show first 10 keys
                        logger.error(f"     [{i+1}] {key}")
                    if len(available_keys) > 10:
                        logger.error(f"     ... and {len(available_keys) - 10} more")
                    logger.error(f"   This prevents using wrong data for different instruments")
                    logger.error(f"   Possible causes:")
                    logger.error(f"     1. Instrument expired or no longer traded")
                    logger.error(f"     2. Format mismatch (pipe vs colon separator)")
                    logger.error(f"     3. Stale instrument_key in database")
                    logger.error(f"     4. API returned data for different instrument")
                    return None
                
                if quote_data:
                    ohlc = quote_data.get('ohlc', {})
                    ltp = float(quote_data.get('last_price', 0))
                    close_price = float(ohlc.get('close', ltp))
                    
                    # Additional validation: Ensure LTP is reasonable (not zero or negative)
                    if ltp <= 0:
                        logger.warning(f"⚠️ Invalid LTP (₹{ltp}) for {instrument_key} - returning None")
                        return None
                    
                    logger.info(f"✅ Market quote for {instrument_key}: LTP=₹{ltp}, Close=₹{close_price}")
                    
                    return {
                        'last_price': ltp,
                        'close_price': close_price,
                        'ohlc': ohlc,
                        'open': float(ohlc.get('open', 0)),
                        'high': float(ohlc.get('high', 0)),
                        'low': float(ohlc.get('low', 0))
                    }
            
            logger.warning(f"⚠️ No valid quote data for {instrument_key}")
                
        except Exception as e:
            logger.error(f"❌ Error fetching market quote for {instrument_key}: {str(e)}")
            
        return None

    def get_market_quote(self, symbol: str) -> Optional[Dict]:
        """
        Get real-time market quote (LTP) for a symbol
        Uses V2 API as market quote is in V2
        
        Returns:
            {
                'last_price': float,
                'close_price': float,
                'ohlc': {...}
            }
        """
        try:
            # Get instrument key
            instrument_key = self.get_instrument_key(symbol)
            
            # Market quote endpoint (V2 API)
            url = f"https://api.upstox.com/v2/market-quote/quotes?instrument_key={instrument_key}"
            
            # Make request
            response = requests.get(url, headers=self.get_headers(), timeout=10)
            
            if response.status_code == 200:
                data = response.json()
                
                if data.get('status') == 'success' and 'data' in data:
                    # Try different key formats:
                    # 1. NSE_EQ|INE002A01018 (request format)
                    # 2. NSE_EQ:RELIANCE (response format with symbol)
                    # 3. Just iterate through data keys
                    
                    quote_data = None
                    
                    # Strategy 1: Exact match with instrument_key
                    if instrument_key in data['data']:
                        quote_data = data['data'][instrument_key]
                    
                    # Strategy 2: Try to find by symbol or instrument_key substring
                    if not quote_data:
                        for key in data['data'].keys():
                            if symbol.upper() in key.upper() or instrument_key in key:
                                quote_data = data['data'][key]
                                break
                    
                    # CRITICAL FIX: Remove dangerous fallback
                    # If we can't find a match, return None instead of using wrong data
                    if not quote_data:
                        available_keys = list(data['data'].keys())
                        logger.warning(f"⚠️ No match found for symbol '{symbol}' (instrument_key: {instrument_key})")
                        logger.warning(f"   Available keys in response: {available_keys[:3]}...")
                        # Return None instead of using first value
                        return None
                    
                    if quote_data:
                        ohlc = quote_data.get('ohlc', {})
                        ltp = float(quote_data.get('last_price', 0))
                        close = float(ohlc.get('close', 0))
                        
                        logger.info(f"Market quote for {symbol}: LTP={ltp}, Close={close}")
                        
                        return {
                            'last_price': ltp if ltp > 0 else close,
                            'close_price': close,
                            'open': float(ohlc.get('open', 0)),
                            'high': float(ohlc.get('high', 0)),
                            'low': float(ohlc.get('low', 0))
                        }
                
                logger.warning(f"No quote data found for {symbol} in response")
                return None
            else:
                logger.error(f"Market quote API error for {symbol}: {response.status_code} - {response.text}")
                return None
                
        except Exception as e:
            logger.error(f"Error fetching market quote for {symbol}: {str(e)}")
            return None
    
    def get_instrument_key(self, symbol: str) -> Optional[str]:
        """
        Convert stock symbol to Upstox instrument key
        Format: NSE_EQ|INE{ISIN_CODE}
        
        Uses symbol_isin_mapping for proper ISIN codes
        """
        try:
            from services.symbol_isin_mapping import get_instrument_key as get_isin_key
            return get_isin_key(symbol)
        except ImportError:
            # Fallback if import fails
            from symbol_isin_mapping import get_instrument_key as get_isin_key
            return get_isin_key(symbol)
    
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
    
    def vwap_slope(self, vwap1: float, time1: datetime, vwap2: float, time2: datetime) -> str:
        """
        Calculate the inclination (slope angle) in degrees between two VWAP points
        for the same stock on the same day.
        
        The first VWAP (vwap1) is set as the origin (0) on the price axis.
        The angle is calculated between:
        - Point 1: (time1, 0) - starting point where price change = 0
        - Point 2: (time2, vwap_change) - end point where price change = vwap2 - vwap1
        
        Both positive (upward) and negative (downward) inclinations are considered.
        
        Args:
            vwap1: First VWAP value (origin point, set as 0 for calculation)
            time1: Timestamp of first VWAP (datetime object)
            vwap2: Second VWAP value (end point)
            time2: Timestamp of second VWAP (datetime object, should be later than time1)
        
        Returns:
            "Yes" if absolute inclination is 45 degrees or more (either upward or downward), "No" otherwise
        
        Formula:
            - Set first VWAP as origin: price_change_at_start = 0
            - Calculate price change: vwap_change = vwap2 - vwap1
            - Calculate time difference in hours: time_diff_hours = (time2 - time1) in hours
            - For 45-degree angle: rise (price_change) should equal run (time_diff) when properly scaled
            - Scale time to match price units: normalized_time = time_diff_hours * scaling_factor
            - Scaling factor: 0.2% of starting price per hour (for visual chart matching)
            - Angle = arctan(|vwap_change| / normalized_time) * (180 / π)
            - Returns "Yes" if absolute angle >= 45 degrees
            
        Coordinate System:
            - X-axis: Time (normalized)
            - Y-axis: Price change from origin (vwap1 = 0)
            - Point 1: (0, 0) - time1, no price change
            - Point 2: (normalized_time, vwap_change) - time2, price change from origin
        """
        import math
        
        try:
            # Validate inputs
            if vwap1 <= 0 or vwap2 <= 0:
                logger.warning("Invalid VWAP values (must be > 0)")
                return "No"
            
            # Ensure both datetimes are timezone-aware (IST)
            ist = pytz.timezone('Asia/Kolkata')
            if time1.tzinfo is None:
                time1 = ist.localize(time1)
            elif time1.tzinfo != ist:
                time1 = time1.astimezone(ist)
            
            if time2.tzinfo is None:
                time2 = ist.localize(time2)
            elif time2.tzinfo != ist:
                time2 = time2.astimezone(ist)
            
            if time1 >= time2:
                logger.warning("time1 must be earlier than time2")
                return "No"
            
            # Calculate time difference in hours
            time_diff = time2 - time1
            time_diff_hours = time_diff.total_seconds() / 3600.0  # Convert to hours
            
            if time_diff_hours <= 0:
                logger.warning("Invalid time difference")
                return "No"
            
            # Set first VWAP as origin (0)
            # Calculate price change from origin
            vwap_change = vwap2 - vwap1
            vwap_change_absolute = abs(vwap_change)
            
            # Calculate scaling factor to normalize time axis to match price axis
            # For visual 45-degree angle: use 0.2% of starting price per hour as baseline
            # This means: if price changes by 0.2% of starting price per hour = 45 degrees
            # Scaling factor converts hours to price-equivalent units
            scaling_factor_per_hour = vwap1 * 0.002  # 0.2% of starting price per hour
            
            # Normalize time to price-equivalent units
            # This allows us to compare price_change (Y-axis) with normalized_time (X-axis)
            normalized_time = time_diff_hours * scaling_factor_per_hour
            
            # Calculate angle using rise (price_change) and run (normalized_time)
            # For 45 degrees: rise = run, so ratio = 1
            # Angle = arctan(rise / run) = arctan(price_change / normalized_time)
            if normalized_time > 0:
                slope_ratio = vwap_change_absolute / normalized_time
            else:
                slope_ratio = 0
            
            # Determine direction
            direction = "upward" if vwap_change > 0 else "downward" if vwap_change < 0 else "flat"
            
            # Calculate angle in degrees
            # arctan gives angle in radians, convert to degrees
            # When slope_ratio = 1 (rise = run), angle = 45 degrees
            angle_radians = math.atan(slope_ratio)
            angle_degrees = math.degrees(angle_radians)
            
            logger.debug(f"VWAP Slope Calculation (Origin-Based):")
            logger.debug(f"  Origin point (VWAP1 = 0): ₹{vwap1:.2f} at {time1.strftime('%H:%M:%S')}")
            logger.debug(f"  End point (VWAP2): ₹{vwap2:.2f} at {time2.strftime('%H:%M:%S')}")
            logger.debug(f"  Price change from origin: ₹{vwap_change:.2f}")
            logger.debug(f"  Time difference: {time_diff_hours:.2f} hours")
            logger.debug(f"  Scaling factor: ₹{scaling_factor_per_hour:.2f} per hour")
            logger.debug(f"  Normalized time: ₹{normalized_time:.2f} (time-equivalent in price units)")
            logger.debug(f"  Slope ratio (rise/run): {slope_ratio:.4f}")
            logger.debug(f"  Angle: {angle_degrees:.2f} degrees ({direction})")
            
            # Return "Yes" if absolute angle is 45 degrees or more (for both upward and downward slopes)
            if angle_degrees >= 45.0:
                return "Yes"
            else:
                return "No"
                
        except Exception as e:
            logger.error(f"Error calculating VWAP slope: {str(e)}")
            import traceback
            traceback.print_exc()
            return "No"
    
    def get_historical_candles(self, symbol: str, interval: str = "hours/1", days_back: int = 2) -> Optional[List[Dict]]:
        """
        Fetch historical candle data from Upstox V3 API
        Uses improved API request with automatic retry and token refresh
        
        Args:
            symbol: Stock symbol (e.g., "RELIANCE")
            interval: Candle interval (hours/1 for 1-hour candles in V3 API)
            days_back: Number of days to fetch
            
        Returns:
            List of candle data or None
        """
        try:
            # Get instrument key (NSE_EQ|INE002A01018)
            instrument_key = self.get_instrument_key(symbol)
            
            if not instrument_key:
                logger.error(f"Could not get instrument key for {symbol}")
                return None
            
            # Calculate date range
            ist = pytz.timezone('Asia/Kolkata')
            end_date = datetime.now(ist)
            start_date = end_date - timedelta(days=days_back)
            
            # Format dates for API (YYYY-MM-DD)
            to_date = end_date.strftime("%Y-%m-%d")
            from_date = start_date.strftime("%Y-%m-%d")
            
            # Upstox V3 API endpoint
            # Format: /v3/historical-candle/{instrument_key}/hours/1/{to_date}/{from_date}
            url = f"{self.base_url}/historical-candle/{instrument_key}/{interval}/{to_date}/{from_date}"
            
            # Log the request for debugging
            logger.info(f"📊 Fetching {interval} candles for {symbol} ({from_date} to {to_date})")
            
            # Use improved API request with retry and token refresh
            data = self.make_api_request(url, method="GET", timeout=15, max_retries=2)
            
            if data and data.get('status') == 'success' and 'data' in data:
                candles = data['data'].get('candles', [])
                
                # Convert to structured format
                structured_candles = []
                for candle in candles:
                    # Upstox format: [timestamp, open, high, low, close, volume, oi]
                    if len(candle) >= 6:
                        structured_candles.append({
                            'timestamp': candle[0],
                            'open': float(candle[1]),
                            'high': float(candle[2]),
                            'low': float(candle[3]),
                            'close': float(candle[4]),
                            'volume': float(candle[5])
                        })
                
                logger.info(f"✅ Fetched {len(structured_candles)} candles for {symbol}")
                return structured_candles
            else:
                logger.warning(f"⚠️ No candle data for {symbol}")
                return None
                
        except Exception as e:
            logger.error(f"❌ Error fetching candles for {symbol}: {str(e)}")
            return None
    
    def get_vwap_data(self, symbol: str, triggered_at: str = None) -> Dict[str, float]:
        """
        Get current hour VWAP, previous hour VWAP, and Last Traded Price for a symbol
        
        Args:
            symbol: Stock symbol
            triggered_at: Time when alert was triggered (e.g., "10:15 AM")
                         If provided, Last Price will be from the candle at this time
        
        Logic:
        - If triggered_at provided: Fetch Last Price from the candle at trigger time
        - During market hours: Uses real-time data with market quote API
        - After market close: Uses completed candle data
        - VWAP is cumulative from market open (matches TradingView)
        
        Returns:
            {
                'current_hour_vwap': float,
                'previous_hour_vwap': float,
                'last_traded_price': float,
                'close_price': float
            }
        """
        try:
            # Import and check if symbol is supported
            try:
                from services.symbol_isin_mapping import is_symbol_supported
            except ImportError:
                from symbol_isin_mapping import is_symbol_supported
            
            # Check if symbol is supported
            if not is_symbol_supported(symbol):
                logger.warning(f"Symbol {symbol} not in ISIN mapping - skipping")
                return {
                    'current_hour_vwap': 0.0,
                    'previous_hour_vwap': 0.0,
                    'last_traded_price': 0.0,
                    'close_price': 0.0
                }
            
            # Step 1: Try to get real-time market quote (works during market hours)
            market_quote = self.get_market_quote(symbol)
            
            # Step 2: Fetch 1-minute candles for VWAP calculation
            minute_candles = self.get_historical_candles(symbol, interval="minutes/1", days_back=2)
            
            if not minute_candles or len(minute_candles) < 60:
                logger.warning(f"Insufficient minute candle data for {symbol}")
                return {
                    'current_hour_vwap': 0.0,
                    'previous_hour_vwap': 0.0,
                    'last_traded_price': 0.0,
                    'close_price': 0.0
                }
            
            # Sort candles by timestamp (oldest first for cumulative VWAP)
            minute_candles.sort(key=lambda x: x['timestamp'], reverse=False)
            
            # Filter candles from current trading day only (9:15 AM to 3:30 PM)
            from datetime import datetime
            ist = pytz.timezone('Asia/Kolkata')
            current_time = datetime.now(ist)
            
            # Determine the trading day to use
            trading_day = current_time.strftime("%Y-%m-%d")
            # If weekend or after hours, use last Friday
            if current_time.weekday() >= 5 or (current_time.weekday() < 5 and current_time.hour < 9):
                # Use last trading day
                days_back = current_time.weekday() - 4 if current_time.weekday() > 4 else 1
                trading_day = (current_time - timedelta(days=days_back)).strftime("%Y-%m-%d")
            
            # Filter candles for the trading session
            session_candles = []
            for candle in minute_candles:
                ts_str = candle['timestamp']
                if trading_day in ts_str:
                    try:
                        hour = int(ts_str.split('T')[1].split(':')[0])
                        minute = int(ts_str.split(':')[1])
                        # Trading hours: 9:15 AM to 3:30 PM
                        if (hour == 9 and minute >= 15) or (hour >= 10 and hour < 15) or (hour == 15 and minute <= 30):
                            session_candles.append(candle)
                    except:
                        continue
            
            if len(session_candles) < 60:
                logger.warning(f"Insufficient session candles for {symbol}: {len(session_candles)}")
                session_candles = minute_candles[-375:] if len(minute_candles) >= 375 else minute_candles
            
            logger.info(f"Using {len(session_candles)} session candles for {symbol}")
            
            # Calculate cumulative VWAP from market open to now (TradingView style)
            # Current hour VWAP: All candles from market open to latest time
            current_hour_vwap = self.calculate_vwap(session_candles)
            
            # Previous hour VWAP: All candles from market open to 1 hour ago
            # Remove last 60 candles to get "previous hour" cumulative VWAP
            if len(session_candles) > 60:
                prev_hour_candles = session_candles[:-60]
            else:
                prev_hour_candles = session_candles[:max(1, len(session_candles)//2)]
            previous_hour_vwap = self.calculate_vwap(prev_hour_candles)
            
            # Determine Last Traded Price
            # If triggered_at is provided, use the candle at that specific time
            if triggered_at:
                try:
                    # Parse the triggered_at time (e.g., "10:15 AM" or "2:34 pm")
                    from dateutil import parser
                    import re
                    
                    # Clean up the time string and parse it
                    time_str = triggered_at.strip()
                    
                    # Try to parse various time formats
                    # Handle formats like "10:15 AM", "2:34 pm", "14:30"
                    try:
                        # Parse the time string
                        parsed_time = parser.parse(time_str, fuzzy=True)
                        # Combine with today's date in IST
                        trigger_datetime = current_time.replace(
                            hour=parsed_time.hour,
                            minute=parsed_time.minute,
                            second=0,
                            microsecond=0
                        )
                        
                        # Convert trigger time to timestamp for comparison
                        trigger_timestamp = int(trigger_datetime.timestamp() * 1000)
                        
                        logger.info(f"Looking for candle at trigger time {trigger_datetime.strftime('%Y-%m-%d %H:%M:%S')} (timestamp: {trigger_timestamp}) for {symbol}")
                        
                        # Find the candle closest to (but not after) the trigger time
                        closest_candle = None
                        min_diff = float('inf')
                        
                        for candle in session_candles:
                            # Convert candle timestamp to milliseconds
                            if isinstance(candle['timestamp'], str):
                                # Handle ISO date string format (e.g., '2025-10-17T09:15:00+05:30')
                                candle_dt = parser.parse(candle['timestamp'])
                                candle_timestamp = int(candle_dt.timestamp() * 1000)
                            else:
                                # Already numeric timestamp
                                candle_timestamp = int(candle['timestamp'])
                            
                            # Only consider candles at or before the trigger time
                            if candle_timestamp <= trigger_timestamp:
                                diff = trigger_timestamp - candle_timestamp
                                if diff < min_diff:
                                    min_diff = diff
                                    closest_candle = candle
                        
                        if closest_candle:
                            last_traded_price = round(closest_candle['close'], 2)
                            close_price = round(closest_candle['close'], 2)
                            # Convert timestamp for display
                            if isinstance(closest_candle['timestamp'], str):
                                candle_time = parser.parse(closest_candle['timestamp'])
                            else:
                                candle_time = datetime.fromtimestamp(int(closest_candle['timestamp'])/1000, ist)
                            logger.info(f"Using trigger-time candle close for {symbol}: ₹{last_traded_price} (candle at {candle_time.strftime('%H:%M')})")
                        else:
                            # Fallback to latest candle if no match found
                            last_traded_price = round(session_candles[-1]['close'], 2)
                            close_price = round(session_candles[-1]['close'], 2)
                            logger.warning(f"No candle found at trigger time for {symbol}, using latest: ₹{last_traded_price}")
                    except Exception as parse_error:
                        logger.warning(f"Could not parse triggered_at '{triggered_at}' for {symbol}: {str(parse_error)}")
                        # Fallback to latest candle
                        last_traded_price = round(session_candles[-1]['close'], 2)
                        close_price = round(session_candles[-1]['close'], 2)
                        logger.info(f"Fallback: Using last session candle close for {symbol}: ₹{last_traded_price}")
                        
                except Exception as trigger_error:
                    logger.warning(f"Error processing triggered_at for {symbol}: {str(trigger_error)}")
                    # Fallback to latest candle
                    last_traded_price = round(session_candles[-1]['close'], 2)
                    close_price = round(session_candles[-1]['close'], 2)
                    logger.info(f"Fallback: Using last session candle close for {symbol}: ₹{last_traded_price}")
            
            # If no triggered_at, use current logic
            elif market_quote and market_quote['last_price'] > 0:
                # Market is open - use real-time LTP
                last_traded_price = market_quote['last_price']
                close_price = market_quote['close_price']
                logger.info(f"Using real-time LTP for {symbol}: ₹{last_traded_price}")
            else:
                # Market closed - use close from last completed candle
                # Use the most recent candle's close
                last_traded_price = round(session_candles[-1]['close'], 2)
                close_price = round(session_candles[-1]['close'], 2)
                logger.info(f"Using last session candle close for {symbol}: ₹{last_traded_price}")
            
            logger.info(f"Data for {symbol}: LTP={last_traded_price}, Current VWAP={current_hour_vwap}, Previous VWAP={previous_hour_vwap}")
            
            return {
                'current_hour_vwap': current_hour_vwap,
                'previous_hour_vwap': previous_hour_vwap,
                'last_traded_price': last_traded_price,
                'close_price': close_price
            }
            
        except Exception as e:
            logger.error(f"Error getting VWAP data for {symbol}: {str(e)}")
            return {
                'current_hour_vwap': 0.0,
                'previous_hour_vwap': 0.0,
                'last_traded_price': 0.0,
                'close_price': 0.0
            }
    
    def enrich_stocks_with_options(self, stocks: List[Dict], alert_name: str = "", forced_option_type: str = None) -> List[Dict]:
        """
        Enrich stock list with current LTP and OTM-1 option strike
        
        Args:
            stocks: List of stock dicts with 'stock_name' and 'trigger_price'
            alert_name: Alert name (not used for option type anymore)
            forced_option_type: Force a specific option type ('CE' or 'PE'). If None, determine based on LTP vs VWAP
            
        Returns:
            List of enriched stock dicts with LTP and option strike added
            
        Option Type Logic:
            - If forced_option_type is provided, use that
            - Otherwise: LTP > VWAP → CE (Call), LTP < VWAP → PE (Put)
        """
        enriched_stocks = []
        
        if forced_option_type:
            logger.info(f"Enriching stocks with forced option type: {forced_option_type}")
        else:
            logger.info(f"Enriching stocks with option strikes based on LTP vs VWAP comparison")
        
        for stock in stocks:
            stock_name = stock.get('stock_name', '')
            
            # Get current LTP (always latest, not historical)
            ltp = self.get_current_ltp(stock_name)
            
            if not ltp or ltp == 0:
                logger.warning(f"Could not get LTP for {stock_name}, skipping option strike")
                enriched_stock = stock.copy()
                enriched_stock['last_traded_price'] = 0.0
                enriched_stock['otm1_strike'] = 0.0
                enriched_stock['option_type'] = forced_option_type if forced_option_type else 'CE'
                enriched_stock['vwap'] = 0.0
                enriched_stocks.append(enriched_stock)
                continue
            
            # Get VWAP for comparison (but don't display it)
            # Use simple VWAP from hourly candles
            vwap = self.get_simple_vwap(stock_name)
            
            # Determine option type
            if forced_option_type:
                # Use forced option type
                option_type = forced_option_type
                logger.info(f"{stock_name}: Using forced option type {option_type}")
            elif not vwap or vwap == 0:
                # Default to CE if VWAP unavailable
                option_type = 'CE'
                logger.warning(f"Could not get VWAP for {stock_name}, defaulting to CE")
            else:
                # Determine option type based on LTP vs VWAP
                if ltp > vwap:
                    option_type = 'CE'
                    logger.info(f"{stock_name}: LTP (₹{ltp}) > VWAP (₹{vwap}) → CE")
                else:
                    option_type = 'PE'
                    logger.info(f"{stock_name}: LTP (₹{ltp}) < VWAP (₹{vwap}) → PE")
            
            # Get OTM-1 strike based on determined option type
            otm1_strike = self.get_otm1_strike(stock_name, option_type=option_type, spot_price=ltp)
            
            # Get monthly expiry date
            expiry_date = self.get_monthly_expiry()
            
            # Format the option contract string and fetch option data
            option_contract = ""
            option_ltp = 0.0
            option_vwap = 0.0
            
            if otm1_strike and otm1_strike > 0:
                strike_int = int(otm1_strike)  # Remove decimal
                option_text = "CALL" if option_type == 'CE' else "PUT"
                expiry_str = expiry_date.strftime('%d %b').upper()
                
                option_contract = f"{stock_name} {expiry_str} {strike_int} {option_text}"
                
                # Fetch option LTP and VWAP
                logger.info(f"Fetching option data for {option_contract}")
                option_ltp = self.get_option_ltp(stock_name, expiry_date, otm1_strike, option_type)
                option_vwap = self.get_option_vwap(stock_name, expiry_date, otm1_strike, option_type)
            
            # Add data to stock
            enriched_stock = stock.copy()
            enriched_stock['last_traded_price'] = ltp
            enriched_stock['otm1_strike'] = otm1_strike if otm1_strike else 0.0
            enriched_stock['option_type'] = option_type
            enriched_stock['vwap'] = vwap  # Store but don't display (stock VWAP)
            enriched_stock['option_contract'] = option_contract
            enriched_stock['option_ltp'] = option_ltp if option_ltp else 0.0
            enriched_stock['option_vwap'] = option_vwap if option_vwap else 0.0
            
            enriched_stocks.append(enriched_stock)
        
        return enriched_stocks
    
    def get_stock_ltp_and_vwap(self, stock_symbol: str) -> Optional[Dict]:
        """
        Get both LTP and VWAP for a stock in a single call with fallback mechanisms
        Uses improved API request with automatic retry and token refresh
        
        Args:
            stock_symbol: Stock symbol (e.g., "RELIANCE")
            
        Returns:
            {'ltp': float, 'vwap': float} or None if completely unable to fetch
        """
        try:
            # Get proper instrument key with ISIN code
            instrument_key = self.get_instrument_key(stock_symbol)
            
            if not instrument_key:
                logger.warning(f"⚠️ Could not get instrument key for {stock_symbol}")
                return None
            
            # Try Method 1: Use market quote API (includes VWAP if available)
            url = f"https://api.upstox.com/v2/market-quote/quotes?instrument_key={instrument_key}"
            
            # Use improved API request with retry and token refresh
            data = self.make_api_request(url, method="GET", timeout=10, max_retries=2)
            
            if data and data.get('status') == 'success' and 'data' in data:
                quote_data = None
                
                for key in data['data']:
                    if instrument_key in key or stock_symbol in key:
                        quote_data = data['data'][key]
                        break
                
                if not quote_data and len(data['data']) > 0:
                    quote_data = list(data['data'].values())[0]
                
                if quote_data:
                    ltp = float(quote_data.get('last_price', 0))
                    
                    # Try to get VWAP from quote (some responses include it)
                    vwap = quote_data.get('vwap', 0) or quote_data.get('average_price', 0)
                    
                    # If VWAP not in quote, calculate from historical candles
                    if not vwap or vwap == 0:
                        logger.info(f"VWAP not in market quote for {stock_symbol}, fetching from candles")
                        vwap = self.get_stock_vwap(stock_symbol)
                    
                    if ltp > 0:
                        logger.info(f"✅ Fetched LTP and VWAP for {stock_symbol}: LTP=₹{ltp:.2f}, VWAP=₹{vwap:.2f}")
                        return {
                            'ltp': ltp,
                            'vwap': float(vwap) if vwap else 0.0
                        }
            else:
                logger.warning(f"Market quote API failed for {stock_symbol}, falling back to historical candles")
                
            # Method 2 fallback: Use historical candles for both LTP and VWAP
            vwap = self.get_stock_vwap(stock_symbol)
            ltp = self.get_stock_ltp_from_market_quote(stock_symbol)
            
            # Ensure we have valid values (not None)
            ltp_value = ltp if ltp is not None else 0.0
            vwap_value = vwap if vwap is not None else 0.0
            
            if ltp_value > 0 or vwap_value > 0:
                logger.info(f"✅ Fallback success for {stock_symbol}: LTP=₹{ltp_value:.2f}, VWAP=₹{vwap_value:.2f}")
                return {
                    'ltp': ltp_value,
                    'vwap': vwap_value
                }
                
        except Exception as e:
            logger.error(f"❌ Error getting LTP and VWAP for {stock_symbol}: {str(e)}")
            import traceback
            logger.debug(traceback.format_exc())
        
        return None
    
    def get_stock_vwap(self, stock_symbol: str) -> float:
        """
        Calculate VWAP for a stock using Upstox historical candle data
        Uses improved API request with automatic retry and token refresh
        
        Args:
            stock_symbol: Stock symbol (e.g., "RELIANCE")
            
        Returns:
            VWAP value or 0.0 if unable to fetch
        """
        try:
            # Get proper instrument key with ISIN code
            instrument_key = self.get_instrument_key(stock_symbol)
            
            if not instrument_key:
                logger.warning(f"⚠️ Could not get instrument key for {stock_symbol}")
                return 0.0
            
            # Fetch intraday historical candles (1 hour interval)
            url = f"https://api.upstox.com/v3/historical-candle/intraday/{instrument_key}/hours/1"
            
            # Use improved API request with retry and token refresh
            data = self.make_api_request(url, method="GET", timeout=10, max_retries=2)
            
            if data and data.get('status') == 'success':
                candles = data.get('data', {}).get('candles', [])
                
                if candles and len(candles) > 0:
                    # Calculate VWAP from candles
                    total_pv = 0.0  # Price × Volume
                    total_volume = 0.0
                    
                    for candle in candles:
                        try:
                            # Candle format: [timestamp, open, high, low, close, volume]
                            if len(candle) >= 6:
                                open_price = float(candle[1])
                                high = float(candle[2])
                                low = float(candle[3])
                                close = float(candle[4])
                                volume = float(candle[5])
                                
                                # Typical price
                                typical_price = (high + low + close) / 3
                                
                                total_pv += typical_price * volume
                                total_volume += volume
                        except (ValueError, IndexError) as e:
                            logger.warning(f"⚠️ Error parsing candle for {stock_symbol}: {e}")
                            continue
                    
                    if total_volume > 0:
                        vwap = total_pv / total_volume
                        logger.info(f"✅ Calculated VWAP for {stock_symbol}: ₹{vwap:.2f} (from {len(candles)} candles)")
                        return round(vwap, 2)
                    else:
                        logger.warning(f"⚠️ Zero volume in candle data for {stock_symbol}")
                else:
                    logger.warning(f"⚠️ No candle data available for {stock_symbol}")
            else:
                logger.warning(f"⚠️ Failed to fetch VWAP candles for {stock_symbol}")
                
        except Exception as e:
            logger.error(f"❌ Error calculating VWAP for {stock_symbol}: {str(e)}")
            import traceback
            logger.debug(traceback.format_exc())
        
        return 0.0
    
    def get_stock_vwap_for_previous_hour(self, stock_symbol: str) -> Optional[Dict]:
        """
        Get stock VWAP for the previous 1-hour candle
        
        Args:
            stock_symbol: Stock symbol (e.g., "RELIANCE")
            
        Returns:
            Dict with 'vwap', 'timestamp', 'time' or None
            {
                'vwap': float,
                'timestamp': datetime,
                'time': datetime (timezone-aware)
            }
        """
        try:
            instrument_key = self.get_instrument_key(stock_symbol)
            if not instrument_key:
                logger.warning(f"⚠️ Could not get instrument key for {stock_symbol}")
                return None
            
            # Fetch last 2 hours of candles to get previous hour
            candles = self.get_historical_candles_by_instrument_key(
                instrument_key, 
                interval="hours/1", 
                days_back=1
            )
            
            if not candles or len(candles) < 2:
                logger.warning(f"⚠️ Not enough candles for {stock_symbol} to calculate previous hour VWAP")
                return None
            
            # Get previous hour candle (second to last)
            prev_candle = candles[-2]
            
            # Calculate VWAP for previous hour using that single candle
            # VWAP = (High + Low + Close) / 3 for single candle
            high = prev_candle.get('high', 0)
            low = prev_candle.get('low', 0)
            close = prev_candle.get('close', 0)
            volume = prev_candle.get('volume', 0)
            
            if volume > 0:
                # For single candle, typical price is (H+L+C)/3
                typical_price = (high + low + close) / 3
                vwap = typical_price  # For single candle, VWAP = typical price
            else:
                # Fallback: use close price
                vwap = close
            
            # Parse timestamp (handle both string and numeric formats)
            timestamp_ms = prev_candle.get('timestamp', 0)
            if isinstance(timestamp_ms, str):
                try:
                    timestamp_ms = float(timestamp_ms)
                except (ValueError, TypeError):
                    timestamp_ms = 0
            if timestamp_ms > 1e12:
                timestamp_ms = timestamp_ms / 1000
            
            ist = pytz.timezone('Asia/Kolkata')
            candle_time = datetime.fromtimestamp(timestamp_ms, tz=ist)
            
            logger.info(f"✅ Previous hour VWAP for {stock_symbol}: ₹{vwap:.2f} at {candle_time.strftime('%H:%M:%S')}")
            
            return {
                'vwap': round(vwap, 2),
                'timestamp': timestamp_ms,
                'time': candle_time
            }
            
        except Exception as e:
            logger.error(f"❌ Error getting previous hour VWAP for {stock_symbol}: {str(e)}")
            return None
    
    def get_option_candles_current_and_previous(self, instrument_key: str) -> Optional[Dict]:
        """
        Get current and previous 1-hour candles for an option contract
        
        Args:
            instrument_key: Option instrument key (e.g., "NSE_FO|RELIANCE25NOV1450CE")
            
        Returns:
            Dict with 'current_candle' and 'previous_candle' or None
            {
                'current_candle': {
                    'open': float,
                    'high': float,
                    'low': float,
                    'close': float,
                    'volume': float,
                    'timestamp': int,
                    'time': datetime
                },
                'previous_candle': {
                    'open': float,
                    'high': float,
                    'low': float,
                    'close': float,
                    'volume': float,
                    'timestamp': int,
                    'time': datetime
                }
            }
        """
        try:
            # Fetch last 2 hours of candles
            candles = self.get_historical_candles_by_instrument_key(
                instrument_key,
                interval="hours/1",
                days_back=1
            )
            
            if not candles or len(candles) < 2:
                logger.warning(f"⚠️ Not enough candles for {instrument_key} (need at least 2)")
                return None
            
            # Get last two candles
            current_candle_raw = candles[-1]
            previous_candle_raw = candles[-2]
            
            ist = pytz.timezone('Asia/Kolkata')
            
            # Parse current candle (handle both string and numeric formats)
            current_timestamp_ms = current_candle_raw.get('timestamp', 0)
            if isinstance(current_timestamp_ms, str):
                try:
                    current_timestamp_ms = float(current_timestamp_ms)
                except (ValueError, TypeError):
                    current_timestamp_ms = 0
            if current_timestamp_ms > 1e12:
                current_timestamp_ms = current_timestamp_ms / 1000
            current_time = datetime.fromtimestamp(current_timestamp_ms, tz=ist)
            
            # Parse previous candle (handle both string and numeric formats)
            previous_timestamp_ms = previous_candle_raw.get('timestamp', 0)
            if isinstance(previous_timestamp_ms, str):
                try:
                    previous_timestamp_ms = float(previous_timestamp_ms)
                except (ValueError, TypeError):
                    previous_timestamp_ms = 0
            if previous_timestamp_ms > 1e12:
                previous_timestamp_ms = previous_timestamp_ms / 1000
            previous_time = datetime.fromtimestamp(previous_timestamp_ms, tz=ist)
            
            current_candle = {
                'open': current_candle_raw.get('open', 0),
                'high': current_candle_raw.get('high', 0),
                'low': current_candle_raw.get('low', 0),
                'close': current_candle_raw.get('close', 0),
                'volume': current_candle_raw.get('volume', 0),
                'timestamp': current_timestamp_ms,
                'time': current_time
            }
            
            previous_candle = {
                'open': previous_candle_raw.get('open', 0),
                'high': previous_candle_raw.get('high', 0),
                'low': previous_candle_raw.get('low', 0),
                'close': previous_candle_raw.get('close', 0),
                'volume': previous_candle_raw.get('volume', 0),
                'timestamp': previous_timestamp_ms,
                'time': previous_time
            }
            
            logger.info(f"✅ Fetched candles for {instrument_key}")
            logger.info(f"   Current: O={current_candle['open']:.2f}, H={current_candle['high']:.2f}, L={current_candle['low']:.2f}, C={current_candle['close']:.2f} at {current_time.strftime('%H:%M:%S')}")
            logger.info(f"   Previous: O={previous_candle['open']:.2f}, H={previous_candle['high']:.2f}, L={previous_candle['low']:.2f}, C={previous_candle['close']:.2f} at {previous_time.strftime('%H:%M:%S')}")
            
            return {
                'current_candle': current_candle,
                'previous_candle': previous_candle
            }
            
        except Exception as e:
            logger.error(f"❌ Error getting option candles for {instrument_key}: {str(e)}")
            import traceback
            traceback.print_exc()
            return None
    
    def get_option_daily_candles_current_and_previous(self, instrument_key: str) -> Optional[Dict]:
        """
        Get aggregated daily candles for current day (up to current hour) and previous day (complete day)
        
        Args:
            instrument_key: Option instrument key (e.g., "NSE_FO|RELIANCE25NOV1450CE")
            
        Returns:
            Dict with 'current_day_candle' and 'previous_day_candle' or None
            {
                'current_day_candle': {
                    'open': float,
                    'high': float,
                    'low': float,
                    'close': float,
                    'time': datetime
                },
                'previous_day_candle': {
                    'open': float,
                    'high': float,
                    'low': float,
                    'close': float,
                    'time': datetime
                }
            }
        """
        try:
            ist = pytz.timezone('Asia/Kolkata')
            now = datetime.now(ist)
            current_hour = now.hour
            current_minute = now.minute
            
            # Fetch hourly candles for last 2 days
            candles = self.get_historical_candles_by_instrument_key(
                instrument_key,
                interval="hours/1",
                days_back=2
            )
            
            if not candles or len(candles) < 1:
                logger.warning(f"⚠️ Not enough candles for {instrument_key} to calculate daily candles")
                return None
            
            # Group candles by date
            current_day_candles = []
            previous_day_candles = []
            
            for candle in candles:
                timestamp_ms = candle.get('timestamp', 0)
                # Handle both string and numeric timestamp formats
                if isinstance(timestamp_ms, str):
                    try:
                        timestamp_ms = float(timestamp_ms)
                    except (ValueError, TypeError):
                        continue  # Skip invalid timestamp
                if timestamp_ms > 1e12:
                    timestamp_ms = timestamp_ms / 1000
                candle_time = datetime.fromtimestamp(timestamp_ms, tz=ist)
                candle_date = candle_time.date()
                candle_hour = candle_time.hour
                
                today = now.date()
                yesterday = today - timedelta(days=1)
                
                # Current day candles up to current hour
                if candle_date == today and candle_hour <= current_hour:
                    current_day_candles.append(candle)
                # Previous day candles - complete day (all hours)
                elif candle_date == yesterday:
                    previous_day_candles.append(candle)
            
            # Aggregate current day candles
            current_day_candle = None
            if current_day_candles:
                # Sort by timestamp
                current_day_candles.sort(key=lambda x: x.get('timestamp', 0))
                current_open = current_day_candles[0].get('open', 0)
                current_close = current_day_candles[-1].get('close', 0)
                current_high = max(c.get('high', 0) for c in current_day_candles)
                current_low = min(c.get('low', 0) for c in current_day_candles)
                
                current_day_candle = {
                    'open': current_open,
                    'high': current_high,
                    'low': current_low,
                    'close': current_close,
                    'time': now.replace(minute=0, second=0, microsecond=0)
                }
            
            # Aggregate previous day candles
            previous_day_candle = None
            if previous_day_candles:
                # Sort by timestamp
                previous_day_candles.sort(key=lambda x: x.get('timestamp', 0))
                previous_open = previous_day_candles[0].get('open', 0)
                previous_close = previous_day_candles[-1].get('close', 0)
                previous_high = max(c.get('high', 0) for c in previous_day_candles)
                previous_low = min(c.get('low', 0) for c in previous_day_candles)
                
                # Use same hour as current time for previous day
                previous_time = (now - timedelta(days=1)).replace(minute=0, second=0, microsecond=0)
                
                previous_day_candle = {
                    'open': previous_open,
                    'high': previous_high,
                    'low': previous_low,
                    'close': previous_close,
                    'time': previous_time
                }
            
            if not current_day_candle or not previous_day_candle:
                logger.warning(f"⚠️ Could not aggregate daily candles for {instrument_key} (Current: {len(current_day_candles)} candles, Previous: {len(previous_day_candles)} candles)")
                return None
            
            logger.info(f"✅ Fetched daily candles for {instrument_key}")
            logger.info(f"   Current Day (up to {current_hour}:00): O={current_day_candle['open']:.2f}, H={current_day_candle['high']:.2f}, L={current_day_candle['low']:.2f}, C={current_day_candle['close']:.2f}")
            logger.info(f"   Previous Day (complete day): O={previous_day_candle['open']:.2f}, H={previous_day_candle['high']:.2f}, L={previous_day_candle['low']:.2f}, C={previous_day_candle['close']:.2f}")
            
            return {
                'current_day_candle': current_day_candle,
                'previous_day_candle': previous_day_candle
            }
            
        except Exception as e:
            logger.error(f"❌ Error getting daily candles for {instrument_key}: {str(e)}")
            import traceback
            traceback.print_exc()
            return None
    
    def check_api_health(self) -> Dict[str, any]:
        """
        Check if Upstox API is accessible and token is valid
        Useful for monitoring API status during market hours
        
        Returns:
            {
                'api_accessible': bool,
                'token_valid': bool,
                'response_time_ms': int,
                'message': str,
                'timestamp': str
            }
        """
        try:
            start_time = time.time()
            
            # Try a simple API call (NIFTY quote)
            url = f"https://api.upstox.com/v2/market-quote/quotes?instrument_key={self.NIFTY50_KEY}"
            
            response = requests.get(
                url,
                headers=self.get_headers(),
                timeout=5
            )
            
            response_time = int((time.time() - start_time) * 1000)  # milliseconds
            ist = pytz.timezone('Asia/Kolkata')
            timestamp = datetime.now(ist).strftime('%Y-%m-%d %H:%M:%S')
            
            if response.status_code == 200:
                data = response.json()
                if data.get('status') == 'success':
                    logger.info(f"✅ API health check passed ({response_time}ms)")
                    return {
                        'api_accessible': True,
                        'token_valid': True,
                        'response_time_ms': response_time,
                        'message': 'API healthy',
                        'timestamp': timestamp
                    }
            
            elif response.status_code == 401:
                logger.warning(f"⚠️ API health check: Token expired")
                return {
                    'api_accessible': True,
                    'token_valid': False,
                    'response_time_ms': response_time,
                    'message': 'Token expired - refresh needed',
                    'timestamp': timestamp
                }
            
            else:
                logger.warning(f"⚠️ API health check: HTTP {response.status_code}")
                return {
                    'api_accessible': True,
                    'token_valid': False,
                    'response_time_ms': response_time,
                    'message': f'API error: {response.status_code}',
                    'timestamp': timestamp
                }
        
        except requests.exceptions.Timeout:
            logger.error(f"❌ API health check: Timeout")
            return {
                'api_accessible': False,
                'token_valid': False,
                'response_time_ms': 5000,
                'message': 'Request timeout',
                'timestamp': datetime.now(pytz.timezone('Asia/Kolkata')).strftime('%Y-%m-%d %H:%M:%S')
            }
        
        except requests.exceptions.ConnectionError:
            logger.error(f"❌ API health check: Connection failed")
            return {
                'api_accessible': False,
                'token_valid': False,
                'response_time_ms': 0,
                'message': 'Connection failed',
                'timestamp': datetime.now(pytz.timezone('Asia/Kolkata')).strftime('%Y-%m-%d %H:%M:%S')
            }
        
        except Exception as e:
            logger.error(f"❌ API health check error: {str(e)}")
            return {
                'api_accessible': False,
                'token_valid': False,
                'response_time_ms': 0,
                'message': f'Error: {str(e)}',
                'timestamp': datetime.now(pytz.timezone('Asia/Kolkata')).strftime('%Y-%m-%d %H:%M:%S')
            }


# Initialize Upstox service with credentials
UPSTOX_API_KEY = "dd1d3bcc-e1a4-4eed-be7c-1833d9301738"
UPSTOX_API_SECRET = "8lvpi8fb1f"
UPSTOX_ACCESS_TOKEN = "eyJ0eXAiOiJKV1QiLCJrZXlfaWQiOiJza192MS4wIiwiYWxnIjoiSFMyNTYifQ.eyJzdWIiOiI3RkFENUwiLCJqdGkiOiI2OGZlZjJjNDc1ODIwOTY3ZDdhZDlmOGIiLCJpc011bHRpQ2xpZW50IjpmYWxzZSwiaXNQbHVzUGxhbiI6dHJ1ZSwiaWF0IjoxNzYxNTM4NzU2LCJpc3MiOiJ1ZGFwaS1nYXRld2F5LXNlcnZpY2UiLCJleHAiOjE3NjE2MDI0MDB9.4I6bjhXxVoH9-RY6V-esAKC8yItcq4nrkQoKBgmS60Q"

upstox_service = UpstoxService(
    api_key=UPSTOX_API_KEY,
    api_secret=UPSTOX_API_SECRET,
    access_token=UPSTOX_ACCESS_TOKEN
)

