from pydantic_settings import BaseSettings
from typing import Optional
import os
from dotenv import load_dotenv

load_dotenv()

class Settings(BaseSettings):
    DATABASE_URL: str = os.getenv("DATABASE_URL", "postgresql://trademanthan:trademanthan123@localhost/trademanthan")
    SECRET_KEY: str = os.getenv("SECRET_KEY", "your-secret-key-here-change-in-production")
    GOOGLE_CLIENT_ID: str = os.getenv("GOOGLE_CLIENT_ID", "your_google_client_id_here")
    GOOGLE_CLIENT_SECRET: str = os.getenv("GOOGLE_CLIENT_SECRET", "your_google_client_secret_here")
    REDIS_URL: str = os.getenv("REDIS_URL", "redis://localhost:6379")
    DOMAIN: str = os.getenv("DOMAIN", "trademanthan.in")
    ENVIRONMENT: str = os.getenv("ENVIRONMENT", "production")
    DEBUG: bool = os.getenv("DEBUG", "false").lower() == "true"
    ACCESS_TOKEN_EXPIRE_MINUTES: int = int(os.getenv("ACCESS_TOKEN_EXPIRE_MINUTES", "4320"))  # 72 hours default
    
    # Upstox OAuth Configuration
    UPSTOX_API_KEY: str = os.getenv("UPSTOX_API_KEY", "dd1d3bcc-e1a4-4eed-be7c-1833d9301738")
    UPSTOX_API_SECRET: str = os.getenv("UPSTOX_API_SECRET", "8lvpi8fb1f")
    UPSTOX_REDIRECT_URI: str = os.getenv("UPSTOX_REDIRECT_URI", "https://trademanthan.in/scan/upstox/callback")

settings = Settings()
