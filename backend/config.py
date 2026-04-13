import backend.env_bootstrap  # noqa: F401 — load `<project_root>/.env` before Settings

from pydantic_settings import BaseSettings
from typing import Optional
import os
from pathlib import Path


def get_instruments_file_path() -> Path:
    """
    Return path to nse_instruments.json with fallback for EC2 and local dev.
    Tries EC2 path first, then project-relative path.
    """
    ec2_path = Path("/home/ubuntu/trademanthan/data/instruments/nse_instruments.json")
    if ec2_path.exists():
        return ec2_path
    # Fallback: project root relative to backend package
    project_root = Path(__file__).resolve().parent.parent
    return project_root / "data" / "instruments" / "nse_instruments.json"


class Settings(BaseSettings):
    DATABASE_URL: str = os.getenv("DATABASE_URL", "postgresql://trademanthan:trademanthan123@localhost/trademanthan")
    SECRET_KEY: str = os.getenv("SECRET_KEY", "your-secret-key-here-change-in-production")
    GOOGLE_CLIENT_ID: str = os.getenv("GOOGLE_CLIENT_ID", "428560418671-t59riis4gqkhavnevt9ve6km54ltsba7.apps.googleusercontent.com")
    GOOGLE_CLIENT_SECRET: str = os.getenv("GOOGLE_CLIENT_SECRET", "your_google_client_secret_here")
    REDIS_URL: str = os.getenv("REDIS_URL", "redis://localhost:6379")
    DOMAIN: str = os.getenv("DOMAIN", "www.tradewithcto.com")
    GOOGLE_REDIRECT_URI: str = os.getenv("GOOGLE_REDIRECT_URI", "https://www.tradewithcto.com/login.html")
    ENVIRONMENT: str = os.getenv("ENVIRONMENT", "production")
    DEBUG: bool = os.getenv("DEBUG", "false").lower() == "true"
    ACCESS_TOKEN_EXPIRE_MINUTES: int = int(os.getenv("ACCESS_TOKEN_EXPIRE_MINUTES", "4320"))  # 72 hours default
    
    # Upstox OAuth Configuration (redirect URI must match Upstox "My Apps" redirect URL exactly)
    UPSTOX_API_KEY: str = os.getenv("UPSTOX_API_KEY", "dd1d3bcc-e1a4-4eed-be7c-1833d9301738")
    UPSTOX_API_SECRET: str = os.getenv("UPSTOX_API_SECRET", "8lvpi8fb1f")
    UPSTOX_REDIRECT_URI: str = os.getenv("UPSTOX_REDIRECT_URI", "https://tradewithcto.com/scan/upstox/callback")

    # CAR GPT Configuration
    CAR_NUMBER_OF_WEEKS: int = int(os.getenv("CAR_NUMBER_OF_WEEKS", "52"))

    # MarketAux (financial news + entity sentiment)
    MARKETAUX_API_TOKEN: str = os.getenv("MARKETAUX_API_TOKEN", "")

    # OpenAI (fin sentiment reason text from filings)
    OPENAI_API_KEY: str = os.getenv("OPENAI_API_KEY", "")

    # Live OI heatmap (Upstox-only: instruments JSON + batch market quotes)
    UPSTOX_OI_ENABLED: bool = os.getenv("UPSTOX_OI_ENABLED", "true").lower() in ("1", "true", "yes")
    OI_REFRESH_INTERVAL: int = int(os.getenv("OI_REFRESH_INTERVAL", "60"))
    OI_HEATMAP_TOP_N: int = int(os.getenv("OI_HEATMAP_TOP_N", "200"))
    OI_BATCH_CHUNK_SIZE: int = int(os.getenv("OI_BATCH_CHUNK_SIZE", "100"))

    # Pre-market watchlist schedule (IST, HH:MM)
    PREMKET_ENABLED: bool = os.getenv("PREMKET_ENABLED", "true").lower() in ("1", "true", "yes")
    PREMKET_RUN_TIME: str = os.getenv("PREMKET_RUN_TIME", "09:00")
    PREMKET_TOP_N: int = int(os.getenv("PREMKET_TOP_N", "10"))

settings = Settings()
