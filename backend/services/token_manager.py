"""
Token Manager for Upstox OAuth
Handles secure token storage and retrieval
"""

import os
import json
import logging
from datetime import datetime
from pathlib import Path
from typing import Optional

logger = logging.getLogger(__name__)

# Token storage file path
TOKEN_FILE = Path("/home/ubuntu/trademanthan/data/upstox_token.json")

def save_upstox_token(access_token: str, expires_at: Optional[int] = None) -> bool:
    """
    Save Upstox access token to secure file
    
    Args:
        access_token: The OAuth access token
        expires_at: Unix timestamp when token expires (optional)
    
    Returns:
        True if saved successfully, False otherwise
    """
    try:
        # Ensure data directory exists
        TOKEN_FILE.parent.mkdir(parents=True, exist_ok=True)
        
        # Prepare token data
        token_data = {
            "access_token": access_token,
            "updated_at": datetime.now().isoformat(),
            "expires_at": expires_at
        }
        
        # Write to file with restricted permissions
        with open(TOKEN_FILE, 'w') as f:
            json.dump(token_data, f, indent=2)
        
        # Set file permissions to read/write for owner only (600)
        os.chmod(TOKEN_FILE, 0o600)
        
        logger.info(f"✅ Upstox token saved to {TOKEN_FILE}")
        return True
        
    except Exception as e:
        logger.error(f"❌ Failed to save Upstox token: {str(e)}")
        return False


def load_upstox_token() -> Optional[str]:
    """
    Load Upstox access token from file or environment
    
    Priority:
    1. Token file (most recent OAuth login)
    2. Environment variable UPSTOX_ACCESS_TOKEN
    3. None (token not configured)
    
    Returns:
        Access token string or None
    """
    try:
        # Try to load from token file first (most recent)
        if TOKEN_FILE.exists():
            with open(TOKEN_FILE, 'r') as f:
                token_data = json.load(f)
            
            access_token = token_data.get("access_token")
            updated_at = token_data.get("updated_at")
            
            if access_token:
                logger.info(f"✅ Loaded Upstox token from file (updated: {updated_at})")
                return access_token
        
        # Fallback to environment variable
        env_token = os.getenv("UPSTOX_ACCESS_TOKEN")
        if env_token:
            logger.info("✅ Loaded Upstox token from environment variable")
            return env_token
        
        logger.warning("⚠️ No Upstox token found in file or environment")
        return None
        
    except Exception as e:
        logger.error(f"❌ Error loading Upstox token: {str(e)}")
        
        # Try environment variable as final fallback
        try:
            env_token = os.getenv("UPSTOX_ACCESS_TOKEN")
            if env_token:
                logger.info("✅ Using token from environment (file load failed)")
                return env_token
        except:
            pass
        
        return None


def get_token_info() -> dict:
    """
    Get information about the current token
    
    Returns:
        Dict with token status, source, and metadata
    """
    try:
        info = {
            "has_token": False,
            "source": None,
            "updated_at": None,
            "expires_at": None
        }
        
        # Check file
        if TOKEN_FILE.exists():
            with open(TOKEN_FILE, 'r') as f:
                token_data = json.load(f)
            
            if token_data.get("access_token"):
                info["has_token"] = True
                info["source"] = "file"
                info["updated_at"] = token_data.get("updated_at")
                info["expires_at"] = token_data.get("expires_at")
                return info
        
        # Check environment
        if os.getenv("UPSTOX_ACCESS_TOKEN"):
            info["has_token"] = True
            info["source"] = "environment"
            return info
        
        return info
        
    except Exception as e:
        logger.error(f"Error getting token info: {str(e)}")
        return {"has_token": False, "source": None, "error": str(e)}

