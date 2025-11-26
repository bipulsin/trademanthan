#!/usr/bin/env python3
"""
Check Upstox token expiration from JWT token
"""

import sys
import os
import json
import base64
from datetime import datetime
from pathlib import Path

# Add parent directory to path
script_dir = os.path.dirname(os.path.abspath(__file__))
parent_dir = os.path.dirname(os.path.dirname(script_dir))
sys.path.insert(0, parent_dir)

TOKEN_FILE = Path("/home/ubuntu/trademanthan/data/upstox_token.json")

def main():
    print("=" * 80)
    print("UPSTOX TOKEN EXPIRATION ANALYSIS")
    print("=" * 80)
    print()
    
    if not TOKEN_FILE.exists():
        print("❌ Token file not found")
        return
    
    # Read token file
    with open(TOKEN_FILE, 'r') as f:
        token_data = json.load(f)
    
    token = token_data.get('access_token', '')
    stored_expires_at = token_data.get('expires_at')
    updated_at = token_data.get('updated_at')
    
    print("Token File Data:")
    print(f"  Updated At: {updated_at}")
    print(f"  Expires At (stored): {stored_expires_at}")
    print()
    
    if not token:
        print("❌ No access token in file")
        return
    
    # Decode JWT to get actual expiration
    try:
        parts = token.split('.')
        if len(parts) < 2:
            print("❌ Invalid JWT token format")
            return
        
        # Decode payload (second part)
        payload = parts[1]
        # Add padding if needed
        padding = len(payload) % 4
        if padding:
            payload += '=' * (4 - padding)
        
        decoded = base64.urlsafe_b64decode(payload)
        jwt_data = json.loads(decoded)
        
        print("JWT Token Details (decoded):")
        iat = jwt_data.get('iat')
        exp = jwt_data.get('exp')
        
        if iat:
            iat_dt = datetime.fromtimestamp(iat)
            print(f"  Issued At (iat): {iat} = {iat_dt.strftime('%Y-%m-%d %H:%M:%S IST')}")
        else:
            print(f"  Issued At (iat): Not found")
        
        if exp:
            exp_dt = datetime.fromtimestamp(exp)
            now = datetime.now()
            is_expired = now.timestamp() > exp
            time_until_expiry = (exp_dt - now).total_seconds() / 3600
            
            print(f"  Expires At (exp): {exp} = {exp_dt.strftime('%Y-%m-%d %H:%M:%S IST')}")
            print()
            print("Current Status:")
            print(f"  Current Time: {now.strftime('%Y-%m-%d %H:%M:%S IST')}")
            print(f"  Token Expires: {exp_dt.strftime('%Y-%m-%d %H:%M:%S IST')}")
            print(f"  Is Expired: {'✅ YES' if is_expired else '❌ NO'}")
            print(f"  Time Until Expiry: {time_until_expiry:.2f} hours")
            
            if is_expired:
                print()
                print("⚠️ TOKEN HAS EXPIRED!")
                print(f"   Token expired at: {exp_dt.strftime('%Y-%m-%d %H:%M:%S IST')}")
                print(f"   Current time: {now.strftime('%Y-%m-%d %H:%M:%S IST')}")
                print(f"   Expired {(now.timestamp() - exp) / 3600:.2f} hours ago")
            else:
                print()
                print("✅ Token is still valid")
        else:
            print(f"  Expires At (exp): Not found in JWT")
        
        print()
        print("=" * 80)
        print("ANALYSIS:")
        print("=" * 80)
        print()
        
        if stored_expires_at is None:
            print("❌ PROBLEM: expires_at was NOT saved to token file")
            print("   This means when the token was saved, expires_in was missing or null")
            print("   The code should decode JWT expiration if expires_in is not provided")
        else:
            stored_exp_dt = datetime.fromtimestamp(stored_expires_at)
            print(f"✅ expires_at was saved: {stored_exp_dt.strftime('%Y-%m-%d %H:%M:%S IST')}")
            if exp and stored_expires_at != exp:
                print(f"⚠️ WARNING: Stored expiration ({stored_expires_at}) != JWT expiration ({exp})")
                print(f"   Difference: {abs(stored_expires_at - exp) / 3600:.2f} hours")
        
        print()
        print("RECOMMENDATION:")
        print("  The load_upstox_token() function should:")
        print("  1. Check if expires_at is stored, if not, decode JWT to get expiration")
        print("  2. Check if token is expired before returning it")
        print("  3. Return None if token is expired (so code knows to refresh)")
        
    except Exception as e:
        print(f"❌ Error decoding JWT: {e}")
        import traceback
        traceback.print_exc()

if __name__ == "__main__":
    main()

