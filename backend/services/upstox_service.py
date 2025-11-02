"""
Upstox API Service for fetching market data
"""
import requests
import logging
from datetime import datetime, timedelta
from typing import Dict, Optional, List
import pytz

logger = logging.getLogger(__name__)

class UpstoxService:
    """Service to interact with Upstox API"""
    
    # Index instrument keys
    NIFTY50_KEY = "NSE_INDEX|Nifty 50"
    BANKNIFTY_KEY = "NSE_INDEX|Nifty Bank"
    
    def __init__(self, api_key: str, api_secret: str, access_token: str):
        self.api_key = api_key
        self.api_secret = api_secret
        self.access_token = access_token
        self.base_url = "https://api.upstox.com/v3"
        
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
            
            # Upstox v2 API endpoint for market holidays
            url = f"https://api.upstox.com/v2/market/holidays/{year}"
            
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
            
            logger.info(f"Fetching {interval} candles for {instrument_key} from {from_date} to {to_date}")
            
            # Make request
            response = requests.get(url, headers=self.get_headers(), timeout=10)
            
            if response.status_code == 200:
                data = response.json()
                
                if data.get('status') == 'success' and 'data' in data:
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
                    
                    return structured_candles
                else:
                    logger.warning(f"No candle data for {instrument_key}")
                    return None
            else:
                logger.error(f"API error for {instrument_key}: {response.status_code}")
                return None
                
        except Exception as e:
            logger.error(f"Error fetching candles for {instrument_key}: {str(e)}")
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
            url = f"https://api.upstox.com/v2/market-quote/ohlc?instrument_key={instrument_key}&interval=day"
            
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
            
            # Make request
            response = requests.get(url, headers=self.get_headers(), timeout=10)
            
            if response.status_code == 200:
                data = response.json()
                
                if data.get('status') == 'success' and 'data' in data:
                    # Try different key formats:
                    quote_data = None
                    
                    # Try to find the quote data
                    for key in data['data']:
                        if instrument_key in key:
                            quote_data = data['data'][key]
                            break
                    
                    if not quote_data and len(data['data']) > 0:
                        # Use first available data if exact match not found
                        quote_data = list(data['data'].values())[0]
                    
                    if quote_data:
                        ohlc = quote_data.get('ohlc', {})
                        ltp = float(quote_data.get('last_price', 0))
                        close_price = float(ohlc.get('close', ltp))
                        
                        logger.info(f"Market quote for {instrument_key}: LTP={ltp}, Close={close_price}")
                        
                        return {
                            'last_price': ltp,
                            'close_price': close_price,
                            'ohlc': ohlc
                        }
                else:
                    logger.warning(f"No quote data found for {instrument_key} in response")
            else:
                logger.error(f"Market quote API error for {instrument_key}: {response.status_code} - {response.text}")
                
        except Exception as e:
            logger.error(f"Error fetching market quote for {instrument_key}: {str(e)}")
            
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
                    
                    # Try to find the quote data
                    for key in data['data'].keys():
                        if symbol in key or instrument_key in key:
                            quote_data = data['data'][key]
                            break
                    
                    if not quote_data and len(data['data']) > 0:
                        # Just use the first (and likely only) entry
                        quote_data = list(data['data'].values())[0]
                    
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
    
    def get_historical_candles(self, symbol: str, interval: str = "hours/1", days_back: int = 2) -> Optional[List[Dict]]:
        """
        Fetch historical candle data from Upstox V3 API
        
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
            logger.info(f"Fetching {interval} candles for {symbol} (key: {instrument_key}) from {from_date} to {to_date}")
            
            # Make request
            response = requests.get(url, headers=self.get_headers(), timeout=10)
            
            if response.status_code == 200:
                data = response.json()
                
                if data.get('status') == 'success' and 'data' in data:
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
                    
                    return structured_candles
                else:
                    logger.warning(f"No candle data for {symbol}: {data}")
                    return None
            else:
                logger.error(f"Upstox API error for {symbol}: {response.status_code} - {response.text}")
                return None
                
        except Exception as e:
            logger.error(f"Error fetching candles for {symbol}: {str(e)}")
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
    
    def get_stock_vwap(self, stock_symbol: str) -> float:
        """
        Calculate VWAP for a stock using Upstox historical candle data
        
        Args:
            stock_symbol: Stock symbol (e.g., "RELIANCE")
            
        Returns:
            VWAP value or 0.0 if unable to fetch
        """
        try:
            # Get instrument key for the stock
            instrument_key = f"NSE_EQ|{stock_symbol}"
            
            # Fetch intraday historical candles (1 hour interval)
            url = f"https://api.upstox.com/v3/historical-candle/intraday/{instrument_key}/hours/1"
            headers = self.get_headers()
            
            # Get data for today
            today = datetime.now(pytz.timezone('Asia/Kolkata'))
            date_str = today.strftime('%Y-%m-%d')
            
            response = requests.get(url, headers=headers, params={'to_date': date_str})
            
            if response.status_code == 200:
                data = response.json()
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
                            logger.warning(f"Error parsing candle data: {e}")
                            continue
                    
                    if total_volume > 0:
                        vwap = total_pv / total_volume
                        logger.info(f"Calculated VWAP for {stock_symbol}: ₹{vwap:.2f}")
                        return round(vwap, 2)
            else:
                logger.warning(f"Failed to fetch historical candles for {stock_symbol}: {response.status_code}")
                
        except Exception as e:
            logger.error(f"Error calculating VWAP for {stock_symbol}: {str(e)}")
        
        return 0.0


# Initialize Upstox service with credentials
UPSTOX_API_KEY = "dd1d3bcc-e1a4-4eed-be7c-1833d9301738"
UPSTOX_API_SECRET = "8lvpi8fb1f"
UPSTOX_ACCESS_TOKEN = "eyJ0eXAiOiJKV1QiLCJrZXlfaWQiOiJza192MS4wIiwiYWxnIjoiSFMyNTYifQ.eyJzdWIiOiI3RkFENUwiLCJqdGkiOiI2OGZlZjJjNDc1ODIwOTY3ZDdhZDlmOGIiLCJpc011bHRpQ2xpZW50IjpmYWxzZSwiaXNQbHVzUGxhbiI6dHJ1ZSwiaWF0IjoxNzYxNTM4NzU2LCJpc3MiOiJ1ZGFwaS1nYXRld2F5LXNlcnZpY2UiLCJleHAiOjE3NjE2MDI0MDB9.4I6bjhXxVoH9-RY6V-esAKC8yItcq4nrkQoKBgmS60Q"

upstox_service = UpstoxService(
    api_key=UPSTOX_API_KEY,
    api_secret=UPSTOX_API_SECRET,
    access_token=UPSTOX_ACCESS_TOKEN
)

