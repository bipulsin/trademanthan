"""
Dhan API Service for fetching market data
Uses direct HTTP API calls with Upstox as fallback
"""
import requests
import logging
from datetime import datetime
from typing import Dict, Optional, List
import pytz

logger = logging.getLogger(__name__)

class DhanService:
    """Service to interact with Dhan API (Primary) and Upstox API (Fallback)"""
    
    def __init__(self, dhan_client_id: str, dhan_access_token: str):
        self.dhan_client_id = dhan_client_id
        self.dhan_access_token = dhan_access_token
        self.base_url = "https://api.dhan.co"
        
    def get_headers(self) -> Dict[str, str]:
        """Get request headers with authentication"""
        return {
            "Accept": "application/json",
            "access-token": self.dhan_access_token,
            "dhanClientId": self.dhan_client_id,
            "Content-Type": "application/json"
        }
    
    def get_stock_ltp(self, stock_name: str, fallback_service=None) -> Optional[float]:
        """
        Fetch LTP for a stock using Dhan API with Upstox fallback
        
        Args:
            stock_name: Stock symbol (e.g., 'RELIANCE', 'TATASTEEL')
            fallback_service: Upstox service instance for fallback
            
        Returns:
            LTP price as float, or None if not found
        """
        try:
            # Try Dhan API first
            # Dhan API expects security IDs, not symbols
            # We'll need to look up the security ID first or use Upstox directly
            # For now, fallback to Upstox until we have Dhan security mapping
            
            logger.info(f"Dhan API: Using Upstox fallback for {stock_name}")
            if fallback_service:
                return fallback_service.get_stock_ltp_from_market_quote(stock_name)
            
            return None
            
        except Exception as e:
            logger.error(f"Error fetching LTP for {stock_name}: {str(e)}")
            return None
    
    def get_nifty_data(self, fallback_service=None) -> Optional[Dict]:
        """
        Get NIFTY50 index data using Dhan API with Upstox fallback
        
        Returns:
            Dict with ltp, open, close, trend info
        """
        try:
            # Try Dhan API - use Upstox format for now
            # Dhan uses different format, we'll need proper mapping
            logger.info("Dhan API: Getting NIFTY50 data via Upstox fallback")
            return self._get_nifty_from_upstox(fallback_service)
            
        except Exception as e:
            logger.error(f"Error fetching NIFTY50 data: {str(e)}")
            return None
    
    def get_banknifty_data(self, fallback_service=None) -> Optional[Dict]:
        """
        Get BANKNIFTY index data using Dhan API with Upstox fallback
        
        Returns:
            Dict with ltp, open, close, trend info
        """
        try:
            # Try Dhan API - use Upstox format for now
            logger.info("Dhan API: Getting BANKNIFTY data via Upstox fallback")
            return self._get_banknifty_from_upstox(fallback_service)
            
        except Exception as e:
            logger.error(f"Error fetching BANKNIFTY data: {str(e)}")
            return None
    
    def _get_nifty_from_upstox(self, upstox_service) -> Optional[Dict]:
        """Fallback method to get NIFTY50 data from Upstox"""
        try:
            quote = upstox_service.get_market_quote_by_key(upstox_service.NIFTY50_KEY)
            if quote:
                return {
                    'ltp': quote.get('last_price', 0),
                    'open': quote.get('open', 0),
                    'close': quote.get('close', 0),
                    'source': 'upstox'
                }
        except Exception as e:
            logger.error(f"Upstox fallback failed for NIFTY50: {str(e)}")
        return None
    
    def _get_banknifty_from_upstox(self, upstox_service) -> Optional[Dict]:
        """Fallback method to get BANKNIFTY data from Upstox"""
        try:
            quote = upstox_service.get_market_quote_by_key(upstox_service.BANKNIFTY_KEY)
            if quote:
                return {
                    'ltp': quote.get('last_price', 0),
                    'open': quote.get('open', 0),
                    'close': quote.get('close', 0),
                    'source': 'upstox'
                }
        except Exception as e:
            logger.error(f"Upstox fallback failed for BANKNIFTY: {str(e)}")
        return None


# Initialize Dhan service with credentials from token
DHAN_CLIENT_ID = "1100781317"
DHAN_ACCESS_TOKEN = "eyJ0eXAiOiJKV1QiLCJhbGciOiJIUzUxMiJ9.eyJpc3MiOiJkaGFuIiwicGFydG5lcklkIjoiIiwiZXhwIjoxNzYxNjU4ODY0LCJpYXQiOjE3NjE1NzI0NjQsInRva2VuQ29uc3VtZXJUeXBlIjoiU0VMRiIsIndlYmhvb2tVcmwiOiIiLCJkaGFuQ2xpZW50SWQiOiIxMTAwNzgxMzE3In0.0aVr7J4o_7oU-9qqBhGqY28fGPXKbi94W9QtbETPZIqq5JWwzuiUPj32kXvHTs3LYXfp2y8DaDrSJRVuqyi7Bw"

dhan_service = DhanService(
    dhan_client_id=DHAN_CLIENT_ID,
    dhan_access_token=DHAN_ACCESS_TOKEN
)