"""
Comprehensive improvements for Upstox API calls
This file contains the enhanced methods to be integrated into upstox_service.py
"""

import requests
import logging
import time
from functools import wraps
from typing import Dict, Optional, Any, Callable
from datetime import datetime

logger = logging.getLogger(__name__)

def retry_with_token_refresh(max_retries: int = 2):
    """
    Decorator to automatically retry API calls with token refresh on 401 errors
    
    Args:
        max_retries: Maximum number of retries (default: 2)
    """
    def decorator(func: Callable) -> Callable:
        @wraps(func)
        def wrapper(self, *args, **kwargs):
            last_exception = None
            
            for attempt in range(max_retries):
                try:
                    result = func(self, *args, **kwargs)
                    return result
                    
                except requests.exceptions.HTTPError as e:
                    last_exception = e
                    
                    # Check for 401 Unauthorized
                    if e.response and e.response.status_code == 401:
                        logger.warning(f"🔄 Token expired (attempt {attempt + 1}/{max_retries}), refreshing...")
                        
                        # Try to reload token from storage first (faster)
                        if self.reload_token_from_storage():
                            logger.info("✅ Token reloaded from storage, retrying...")
                            continue
                        
                        # If reload fails, try refresh
                        if self.refresh_access_token():
                            logger.info("✅ Token refreshed successfully, retrying...")
                            continue
                        else:
                            logger.error("❌ Token refresh failed")
                            break
                    
                    # Check for 429 Rate Limit
                    elif e.response and e.response.status_code == 429:
                        wait_time = 2 ** attempt  # Exponential backoff: 1s, 2s, 4s
                        logger.warning(f"⏱️ Rate limit hit (429), waiting {wait_time}s before retry...")
                        time.sleep(wait_time)
                        continue
                    
                    else:
                        # Other HTTP errors, don't retry
                        logger.error(f"HTTP error {e.response.status_code if e.response else 'unknown'}: {str(e)}")
                        break
                
                except requests.exceptions.Timeout:
                    last_exception = Exception("Request timeout")
                    logger.warning(f"⏱️ Request timeout (attempt {attempt + 1}/{max_retries})")
                    if attempt < max_retries - 1:
                        time.sleep(1)
                        continue
                    break
                
                except requests.exceptions.ConnectionError:
                    last_exception = Exception("Connection error")
                    logger.warning(f"🔌 Connection error (attempt {attempt + 1}/{max_retries})")
                    if attempt < max_retries - 1:
                        time.sleep(2)
                        continue
                    break
                
                except Exception as e:
                    last_exception = e
                    logger.error(f"Unexpected error in {func.__name__}: {str(e)}")
                    break
            
            # All retries failed
            logger.error(f"❌ All retries failed for {func.__name__}")
            return None
        
        return wrapper
    return decorator


def make_upstox_request(
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
                data = response.json()
                
                # Validate response structure
                if data.get('status') == 'success':
                    return data
                else:
                    logger.warning(f"API returned non-success status: {data.get('status')} - {data.get('message', 'No message')}")
                    return data  # Return even if not success, let caller handle
            
            elif response.status_code == 401:
                # Token expired
                logger.warning(f"🔑 Token expired (attempt {attempt + 1}/{max_retries})")
                
                # Try to reload token from storage (faster)
                if self.reload_token_from_storage():
                    logger.info("✅ Token reloaded from storage, retrying...")
                    continue
                
                # If not available in storage, try token refresh (slower)
                logger.info("⚠️ Token not in storage, attempting refresh...")
                if self.refresh_access_token():
                    logger.info("✅ Token refreshed, retrying...")
                    continue
                else:
                    logger.error("❌ Token refresh failed - cannot proceed")
                    last_error = "Token refresh failed"
                    break
            
            elif response.status_code == 429:
                # Rate limit exceeded
                wait_time = 2 ** attempt  # Exponential backoff
                logger.warning(f"⏱️ Rate limit exceeded (429), waiting {wait_time}s...")
                time.sleep(wait_time)
                continue
            
            elif response.status_code == 400:
                # Bad request - don't retry
                logger.error(f"❌ Bad request (400): {response.text[:200]}")
                last_error = f"Bad request: {response.text[:100]}"
                break
            
            elif response.status_code == 404:
                # Not found - don't retry
                logger.error(f"❌ Resource not found (404): {url}")
                last_error = "Resource not found"
                break
            
            elif response.status_code >= 500:
                # Server error - retry
                logger.warning(f"⚠️ Server error ({response.status_code}), retrying...")
                time.sleep(1)
                continue
            
            else:
                # Other errors
                logger.error(f"❌ HTTP {response.status_code}: {response.text[:200]}")
                last_error = f"HTTP {response.status_code}"
                break
        
        except requests.exceptions.Timeout:
            logger.warning(f"⏱️ Request timeout (attempt {attempt + 1}/{max_retries})")
            last_error = "Timeout"
            if attempt < max_retries - 1:
                time.sleep(1)
                continue
            break
        
        except requests.exceptions.ConnectionError as e:
            logger.warning(f"🔌 Connection error (attempt {attempt + 1}/{max_retries}): {str(e)}")
            last_error = "Connection error"
            if attempt < max_retries - 1:
                time.sleep(2)
                continue
            break
        
        except requests.exceptions.RequestException as e:
            logger.error(f"❌ Request exception: {str(e)}")
            last_error = str(e)
            break
        
        except Exception as e:
            logger.error(f"❌ Unexpected error: {str(e)}")
            last_error = str(e)
            break
    
    # All retries failed
    logger.error(f"❌ All attempts failed for {url}: {last_error}")
    return None


# Example usage of improved methods:

def get_market_quote_improved(self, symbol: str) -> Optional[Dict]:
    """
    Enhanced version of get_market_quote with retry and token refresh
    """
    try:
        instrument_key = self.get_instrument_key(symbol)
        if not instrument_key:
            logger.error(f"Could not get instrument key for {symbol}")
            return None
        
        url = f"https://api.upstox.com/v2/market-quote/quotes?instrument_key={instrument_key}"
        
        # Use the improved request method
        data = make_upstox_request(self, url, method="GET", timeout=10, max_retries=2)
        
        if data and data.get('data'):
            # Parse response
            quote_data = None
            for key in data['data'].keys():
                if symbol in key or instrument_key in key:
                    quote_data = data['data'][key]
                    break
            
            if not quote_data and len(data['data']) > 0:
                quote_data = list(data['data'].values())[0]
            
            if quote_data:
                ohlc = quote_data.get('ohlc', {})
                ltp = float(quote_data.get('last_price', 0))
                close = float(ohlc.get('close', 0))
                
                logger.info(f"✅ Market quote for {symbol}: LTP=₹{ltp}, Close=₹{close}")
                
                return {
                    'last_price': ltp if ltp > 0 else close,
                    'close_price': close,
                    'open': float(ohlc.get('open', 0)),
                    'high': float(ohlc.get('high', 0)),
                    'low': float(ohlc.get('low', 0)),
                    'ohlc': ohlc
                }
        
        logger.warning(f"⚠️ No valid quote data for {symbol}")
        return None
    
    except Exception as e:
        logger.error(f"❌ Error in get_market_quote_improved for {symbol}: {str(e)}")
        return None


def get_historical_candles_improved(
    self,
    symbol: str,
    interval: str = "hours/1",
    days_back: int = 2
) -> Optional[list]:
    """
    Enhanced version of get_historical_candles with retry and token refresh
    """
    try:
        instrument_key = self.get_instrument_key(symbol)
        if not instrument_key:
            logger.error(f"Could not get instrument key for {symbol}")
            return None
        
        # Calculate date range
        import pytz
        from datetime import timedelta
        ist = pytz.timezone('Asia/Kolkata')
        end_date = datetime.now(ist)
        start_date = end_date - timedelta(days=days_back)
        
        to_date = end_date.strftime("%Y-%m-%d")
        from_date = start_date.strftime("%Y-%m-%d")
        
        url = f"https://api.upstox.com/v3/historical-candle/{instrument_key}/{interval}/{to_date}/{from_date}"
        
        logger.info(f"📊 Fetching {interval} candles for {symbol} ({from_date} to {to_date})")
        
        # Use improved request method
        data = make_upstox_request(self, url, method="GET", timeout=15, max_retries=2)
        
        if data and data.get('data', {}).get('candles'):
            candles = data['data']['candles']
            
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
            
            logger.info(f"✅ Fetched {len(structured_candles)} candles for {symbol}")
            return structured_candles
        
        logger.warning(f"⚠️ No candle data for {symbol}")
        return None
    
    except Exception as e:
        logger.error(f"❌ Error in get_historical_candles_improved for {symbol}: {str(e)}")
        return None


# Response validation helper
def validate_upstox_response(data: Dict, required_fields: list = None) -> bool:
    """
    Validate Upstox API response structure
    
    Args:
        data: Response data
        required_fields: List of required fields in data['data']
    
    Returns:
        True if valid, False otherwise
    """
    if not data:
        return False
    
    if data.get('status') != 'success':
        logger.warning(f"Response status is not 'success': {data.get('status')}")
        return False
    
    if 'data' not in data:
        logger.warning("Response missing 'data' field")
        return False
    
    if required_fields:
        response_data = data['data']
        for field in required_fields:
            if field not in response_data:
                logger.warning(f"Response missing required field: {field}")
                return False
    
    return True


# Connection pool configuration for better performance
def get_session_with_retry() -> requests.Session:
    """
    Create a requests Session with connection pooling and retry configuration
    """
    session = requests.Session()
    
    # Configure connection pooling
    adapter = requests.adapters.HTTPAdapter(
        pool_connections=10,
        pool_maxsize=20,
        max_retries=0,  # We handle retries manually
        pool_block=False
    )
    
    session.mount('http://', adapter)
    session.mount('https://', adapter)
    
    return session


# Health check for Upstox API
def check_api_health(self) -> Dict[str, Any]:
    """
    Check if Upstox API is accessible and token is valid
    
    Returns:
        {
            'api_accessible': bool,
            'token_valid': bool,
            'response_time_ms': int,
            'message': str
        }
    """
    try:
        start_time = time.time()
        
        # Try a simple API call (market status or quote for NIFTY)
        url = f"https://api.upstox.com/v2/market-quote/quotes?instrument_key={self.NIFTY50_KEY}"
        
        response = requests.get(
            url,
            headers=self.get_headers(),
            timeout=5
        )
        
        response_time = int((time.time() - start_time) * 1000)  # milliseconds
        
        if response.status_code == 200:
            data = response.json()
            if data.get('status') == 'success':
                return {
                    'api_accessible': True,
                    'token_valid': True,
                    'response_time_ms': response_time,
                    'message': 'API healthy'
                }
        
        elif response.status_code == 401:
            return {
                'api_accessible': True,
                'token_valid': False,
                'response_time_ms': response_time,
                'message': 'Token expired - refresh needed'
            }
        
        else:
            return {
                'api_accessible': True,
                'token_valid': False,
                'response_time_ms': response_time,
                'message': f'API error: {response.status_code}'
            }
    
    except requests.exceptions.Timeout:
        return {
            'api_accessible': False,
            'token_valid': False,
            'response_time_ms': 5000,
            'message': 'Request timeout'
        }
    
    except requests.exceptions.ConnectionError:
        return {
            'api_accessible': False,
            'token_valid': False,
            'response_time_ms': 0,
            'message': 'Connection failed'
        }
    
    except Exception as e:
        return {
            'api_accessible': False,
            'token_valid': False,
            'response_time_ms': 0,
            'message': f'Error: {str(e)}'
        }

