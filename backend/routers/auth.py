from fastapi import APIRouter, Body, Depends, HTTPException, Request, status
from fastapi.security import OAuth2PasswordBearer
from sqlalchemy.orm import Session
from datetime import datetime, timedelta
import requests
from jose import jwt
from typing import Any, Dict, Optional
import os
from pydantic import BaseModel, Field
from urllib.parse import urlparse
import logging

import backend.models as models
from backend.database import get_db
from backend.config import settings

router = APIRouter(prefix="/auth", tags=["authentication"])
logger = logging.getLogger(__name__)

oauth2_scheme = OAuth2PasswordBearer(tokenUrl="token")


def _to_iso(dt):
    return dt.isoformat() if dt else None


def _extract_client_ip(request: Request) -> Optional[str]:
    """
    Capture user IP behind reverse proxy.
    Priority: X-Forwarded-For first hop, then connecting client host.
    """
    try:
        xff = (request.headers.get("x-forwarded-for") or "").strip()
        if xff:
            return xff.split(",")[0].strip()[:64]
        if request.client and request.client.host:
            return str(request.client.host).strip()[:64]
    except Exception:
        pass
    return None


def _is_user_blocked(user: models.User) -> bool:
    return bool(getattr(user, "is_blocked", False))


def _assert_not_blocked(user: models.User):
    if _is_user_blocked(user):
        raise HTTPException(status_code=403, detail="User is blocked. Contact administrator.")


def _register_login_metadata(user: models.User, request: Request):
    user.last_login_at = datetime.utcnow()
    user.last_login_ip = _extract_client_ip(request)
    user.last_page_visited = "login.html"
    user.last_page_visited_at = datetime.utcnow()
    user.last_activity_ip = user.last_login_ip


def user_to_client_dict(user: models.User) -> dict:
    """Serialize user for login /me responses (includes admin flags)."""
    return {
        "id": user.id,
        "email": user.email,
        "name": user.full_name,
        "picture": user.avatar_url,
        "isAdmin": (getattr(user, "is_admin", None) or "").strip(),
        "page_permitted": (getattr(user, "page_permitted", None) or "").strip(),
        "is_blocked": bool(getattr(user, "is_blocked", False)),
        "is_paid_user": bool(getattr(user, "is_paid_user", False)),
        "last_login_at": _to_iso(getattr(user, "last_login_at", None)),
        "last_login_ip": (getattr(user, "last_login_ip", None) or "").strip(),
        "last_page_visited": (getattr(user, "last_page_visited", None) or "").strip(),
        "last_page_visited_at": _to_iso(getattr(user, "last_page_visited_at", None)),
    }


def get_user_from_token(token: str, db: Session) -> models.User:
    """Resolve authenticated user from Bearer JWT (same subject as /me)."""
    try:
        payload = jwt.decode(token, settings.SECRET_KEY, algorithms=["HS256"])
    except jwt.ExpiredSignatureError:
        raise HTTPException(status_code=401, detail="Token expired")
    except jwt.JWTError:
        raise HTTPException(status_code=401, detail="Invalid token")
    user_id = payload.get("sub")
    if user_id is None:
        raise HTTPException(status_code=401, detail="Invalid token")
    user = db.query(models.User).filter(models.User.id == int(user_id)).first()
    if user is None:
        raise HTTPException(status_code=401, detail="User not found")
    return user


def _send_telegram_trade_channel_message(text: str) -> bool:
    """Post to Telegram channel (bot must be admin). Env: TELEGRAM_BOT_TOKEN, TELEGRAM_TRADEWITHCTO_CHAT_ID (@TradeWithCTO)."""
    bot_token = os.getenv("TELEGRAM_BOT_TOKEN") or os.getenv("TELEGRAM_NOTIFY_BOT_TOKEN")
    chat_id = os.getenv("TELEGRAM_TRADEWITHCTO_CHAT_ID", "@TradeWithCTO")
    if not bot_token or not chat_id:
        logger.warning(
            "Telegram notify skipped: TELEGRAM_BOT_TOKEN / TELEGRAM_TRADEWITHCTO_CHAT_ID not set in "
            "environment after load (check /home/ubuntu/trademanthan/.env and backend.env_bootstrap)."
        )
        return False
    try:
        url = f"https://api.telegram.org/bot{bot_token}/sendMessage"
        r = requests.post(url, json={"chat_id": chat_id, "text": text}, timeout=15)
        if r.status_code != 200:
            logger.warning(
                "Telegram sendMessage failed: status=%s body=%s",
                r.status_code,
                (r.text or "")[:800],
            )
        return r.status_code == 200
    except Exception as e:
        logger.warning("Telegram sendMessage exception: %s", e)
        return False


class NotifyTradeChannelRequest(BaseModel):
    context: str  # intraoption | pivot_breakout


class NotifyTelegramUserMessageRequest(BaseModel):
    """User-typed message; server appends DB user display name and posts to TradeWithCTO channel."""

    message: str = Field(..., min_length=1, max_length=2000)


class PageViewRequest(BaseModel):
    page: str = Field(..., min_length=1, max_length=255)
    title: Optional[str] = Field(default=None, max_length=255)


class UserFlagsUpdateRequest(BaseModel):
    is_blocked: Optional[bool] = None
    is_paid_user: Optional[bool] = None
    is_admin: Optional[bool] = None


_NOTIFY_TRADE_CHANNEL_MESSAGES = {
    "intraoption": "Check the Broker API for Intraday Stock Options. Notified by user {name}.",
    "pivot_breakout": "Check the Broker API for Pivot Breakout. Notified by user {name}.",
}

class GoogleOAuthRequest(BaseModel):
    credential: str

class GoogleOAuthCodeRequest(BaseModel):
    code: str
    redirect_uri: Optional[str] = None

# Google OAuth endpoints
GOOGLE_TOKEN_URL = "https://oauth2.googleapis.com/token"
GOOGLE_USERINFO_URL = "https://www.googleapis.com/oauth2/v2/userinfo"


def _require_admin_user(token: str, db: Session) -> models.User:
    user = get_user_from_token(token, db)
    if (getattr(user, "is_admin", "") or "").strip().lower() != "yes":
        raise HTTPException(status_code=403, detail="Admin access required")
    return user


def _get_google_oauth_credentials() -> tuple[str, str]:
    """
    Resolve Google OAuth credentials on each request.
    - Client ID can come from env or settings fallback.
    - Client secret must come from env (or non-placeholder settings) for code-exchange flow.
    """
    client_id = (os.getenv("GOOGLE_CLIENT_ID") or settings.GOOGLE_CLIENT_ID or "").strip()
    client_secret = (os.getenv("GOOGLE_CLIENT_SECRET") or settings.GOOGLE_CLIENT_SECRET or "").strip()
    # Ignore placeholder defaults
    if client_secret.lower().startswith("your_google_client_secret"):
        client_secret = ""
    return client_id, client_secret

@router.get("/config")
async def get_oauth_config():
    """Return public OAuth config for frontend (client_id, redirect_uri, domain)"""
    google_client_id, _ = _get_google_oauth_credentials()
    return {
        "google_client_id": google_client_id or "",
        "redirect_uri": settings.GOOGLE_REDIRECT_URI,
        "domain": settings.DOMAIN,
    }


def validate_google_id_token_login() -> str:
    """Validate Google client id for JWT credential verification flow (/auth/google)."""
    google_client_id, _ = _get_google_oauth_credentials()
    if not google_client_id:
        raise HTTPException(
            status_code=status.HTTP_500_INTERNAL_SERVER_ERROR,
            detail="Google OAuth is not configured. GOOGLE_CLIENT_ID must be set in environment variables",
        )
    return google_client_id


def validate_google_code_exchange_login() -> tuple[str, str]:
    """Validate Google client id+secret for authorization-code flow (/auth/google-code)."""
    google_client_id, google_client_secret = _get_google_oauth_credentials()
    if not google_client_id or not google_client_secret:
        raise HTTPException(
            status_code=status.HTTP_500_INTERNAL_SERVER_ERROR,
            detail="Google OAuth code flow is not configured. GOOGLE_CLIENT_ID and GOOGLE_CLIENT_SECRET must be set in environment variables",
        )
    return google_client_id, google_client_secret

def resolve_google_redirect_uri(requested_redirect_uri: Optional[str]) -> str:
    """Allow redirect URIs only from trusted hosted login pages."""
    allowed_redirect_uris = {
        "https://tradentical.com/login.html",
        "https://www.tradentical.com/login.html",
        "https://tradewithcto.com/login.html",
        "https://www.tradewithcto.com/login.html",
        "http://localhost:8000/login.html",
        "http://localhost:3000/login.html",
        settings.GOOGLE_REDIRECT_URI,
    }

    if requested_redirect_uri:
        parsed = urlparse(requested_redirect_uri)
        if parsed.scheme in {"http", "https"} and requested_redirect_uri in allowed_redirect_uris:
            return requested_redirect_uri

    return settings.GOOGLE_REDIRECT_URI

def create_access_token(data: dict, expires_delta: Optional[timedelta] = None):
    to_encode = data.copy()
    if expires_delta:
        expire = datetime.utcnow() + expires_delta
    else:
        expire = datetime.utcnow() + timedelta(minutes=15)
    to_encode.update({"exp": expire})
    encoded_jwt = jwt.encode(to_encode, settings.SECRET_KEY, algorithm="HS256")
    return encoded_jwt

@router.post("/google")
async def google_oauth(
    request: GoogleOAuthRequest,
    req: Request,
    db: Session = Depends(get_db),
):
    """Handle Google OAuth login/signup using JWT credential"""
    google_client_id = validate_google_id_token_login()
    try:
        # Verify the JWT credential with Google
        verify_url = f"https://oauth2.googleapis.com/tokeninfo?id_token={request.credential}"
        response = requests.get(verify_url)
        
        if response.status_code != 200:
            raise HTTPException(status_code=400, detail="Invalid Google credential")
        
        userinfo = response.json()
        
        # Verify the audience matches our client ID
        if userinfo.get('aud') != google_client_id:
            raise HTTPException(status_code=400, detail="Invalid client ID")
        
        # Check if user exists
        user = db.query(models.User).filter(models.User.google_id == userinfo['sub']).first()
        
        if not user:
            # Create new user
            user = models.User(
                google_id=userinfo['sub'],
                email=userinfo['email'],
                full_name=userinfo['name'],
                avatar_url=userinfo.get('picture'),
                created_at=datetime.utcnow()
            )
            db.add(user)
            db.commit()
            db.refresh(user)
            
            # Create default strategies for new user
            try:
                from utils.strategy_creator import create_default_strategies_for_user
                print(f"Creating default strategies for user: {user.id}")
                created_strategies = create_default_strategies_for_user(db, user.id)
                print(f"Successfully created {len(created_strategies)} default strategies")
            except Exception as strategy_error:
                print(f"Warning: Failed to create default strategies: {strategy_error}")
                # Don't fail the user creation if strategy creation fails
        else:
            user.updated_at = datetime.utcnow()
        _assert_not_blocked(user)
        _register_login_metadata(user, req)
        db.commit()
        
        # Create access token
        access_token_expires = timedelta(minutes=settings.ACCESS_TOKEN_EXPIRE_MINUTES)
        access_token = create_access_token(
            data={"sub": str(user.id)}, expires_delta=access_token_expires
        )
        
        return {
            "access_token": access_token,
            "token_type": "bearer",
            "user": user_to_client_dict(user),
        }
        
    except HTTPException:
        # Re-raise HTTP exceptions (like 400 errors) as-is
        raise
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))

@router.post("/google-verify")
async def google_oauth_verify(
    req: Request,
    db: Session = Depends(get_db),
    user_data: Dict[str, Any] = Body(...),
):
    """Handle Google OAuth verification from frontend (JSON body must bind correctly)."""
    validate_google_id_token_login()
    try:
        google_id = user_data.get('google_id')
        email = user_data.get('email')
        name = user_data.get('name')
        picture = user_data.get('picture')
        
        if not all([google_id, email, name]):
            raise HTTPException(status_code=400, detail="Missing required user data")
        
        print(f"Google OAuth verification for user: {email} (ID: {google_id})")
        
        # Check if user exists by google_id or email
        user = db.query(models.User).filter(
            (models.User.google_id == google_id) | 
            (models.User.email == email)
        ).first()
        
        if not user:
            # Create new user
            print(f"Creating new user: {email}")
            user = models.User(
                google_id=google_id,
                email=email,
                full_name=name,
                avatar_url=picture,
                created_at=datetime.utcnow()
            )
            db.add(user)
            db.commit()
            db.refresh(user)
            print(f"New user created with ID: {user.id}")
            
            # Create default Binance broker for new user
            try:
                print(f"Creating default Binance broker for user: {user.id}")
                default_broker = models.Broker(
                    user_id=user.id,
                    name="Binance",
                    type="crypto",
                    api_url="https://api.binance.com",
                    api_key="",  # Empty - user needs to fill
                    api_secret="",  # Empty - user needs to fill
                    is_connected=False,
                    connection_status="disconnected",
                    test_mode=True,
                    created_at=datetime.utcnow(),
                    updated_at=datetime.utcnow()
                )
                db.add(default_broker)
                db.commit()
                print(f"Default Binance broker created with ID: {default_broker.id}")
            except Exception as broker_error:
                print(f"Warning: Failed to create default broker: {broker_error}")
                # Don't fail the user creation if broker creation fails
            
            # Create default strategies for new user
            try:
                from utils.strategy_creator import create_default_strategies_for_user
                print(f"Creating default strategies for user: {user.id}")
                created_strategies = create_default_strategies_for_user(db, user.id)
                print(f"Successfully created {len(created_strategies)} default strategies")
            except Exception as strategy_error:
                print(f"Warning: Failed to create default strategies: {strategy_error}")
                # Don't fail the user creation if strategy creation fails
        else:
            # Update existing user
            print(f"Updating existing user: {user.id}")
            user.google_id = google_id  # Update google_id if it changed
            user.full_name = name  # Update name if it changed
            user.avatar_url = picture  # Update picture if it changed
            print(f"User updated: {user.id}")
        _assert_not_blocked(user)
        _register_login_metadata(user, req)
        db.commit()
        db.refresh(user)
        
        # Create access token
        access_token_expires = timedelta(minutes=settings.ACCESS_TOKEN_EXPIRE_MINUTES)
        access_token = create_access_token(
            data={"sub": str(user.id)}, expires_delta=access_token_expires
        )
        
        print(f"Authentication successful for user: {user.id}")
        
        u = user_to_client_dict(user)
        u["google_id"] = user.google_id
        return {
            "access_token": access_token,
            "token_type": "bearer",
            "user": u,
        }
        
    except Exception as e:
        print(f"Error in google_oauth_verify: {str(e)}")
        raise HTTPException(status_code=500, detail=str(e))

@router.post("/google-code")
async def google_oauth_code(
    request: GoogleOAuthCodeRequest,
    req: Request,
    db: Session = Depends(get_db),
):
    """Handle Google OAuth code exchange for mobile browsers"""
    google_client_id, google_client_secret = validate_google_code_exchange_login()
    try:
        redirect_uri = resolve_google_redirect_uri(request.redirect_uri)

        # Exchange authorization code for access token
        token_response = requests.post(GOOGLE_TOKEN_URL, data={
            'client_id': google_client_id,
            'client_secret': google_client_secret,
            'code': request.code,
            'grant_type': 'authorization_code',
            'redirect_uri': redirect_uri
        })
        
        if token_response.status_code != 200:
            raise HTTPException(status_code=400, detail="Failed to exchange code for token")
        
        token_data = token_response.json()
        access_token = token_data.get('access_token')
        
        if not access_token:
            raise HTTPException(status_code=400, detail="No access token received")
        
        # Get user info from Google
        userinfo_response = requests.get(
            GOOGLE_USERINFO_URL,
            headers={'Authorization': f'Bearer {access_token}'}
        )
        
        if userinfo_response.status_code != 200:
            raise HTTPException(status_code=400, detail="Failed to get user info")
        
        userinfo = userinfo_response.json()
        
        # Check if user exists
        user = db.query(models.User).filter(models.User.google_id == userinfo['id']).first()
        
        if not user:
            # Create new user
            user = models.User(
                google_id=userinfo['id'],
                email=userinfo['email'],
                full_name=userinfo['name'],
                avatar_url=userinfo.get('picture'),
                created_at=datetime.utcnow()
            )
            db.add(user)
            db.commit()
            db.refresh(user)
            
            # Create default strategies for new user
            try:
                from utils.strategy_creator import create_default_strategies_for_user
                print(f"Creating default strategies for user: {user.id}")
                created_strategies = create_default_strategies_for_user(db, user.id)
                print(f"Successfully created {len(created_strategies)} default strategies")
            except Exception as strategy_error:
                print(f"Warning: Failed to create default strategies: {strategy_error}")
        else:
            user.updated_at = datetime.utcnow()
        _assert_not_blocked(user)
        _register_login_metadata(user, req)
        db.commit()
        
        # Create access token
        access_token_expires = timedelta(minutes=settings.ACCESS_TOKEN_EXPIRE_MINUTES)
        jwt_token = create_access_token(
            data={"sub": str(user.id)}, expires_delta=access_token_expires
        )
        
        return {
            "access_token": jwt_token,
            "token_type": "bearer",
            "user": user_to_client_dict(user),
        }
        
    except HTTPException:
        raise
    except Exception as e:
        print(f"Error in google_oauth_code: {str(e)}")
        raise HTTPException(status_code=500, detail=str(e))

@router.get("/me")
async def get_current_user(token: str = Depends(oauth2_scheme), db: Session = Depends(get_db)):
    """Get current user information"""
    try:
        user = get_user_from_token(token, db)
        _assert_not_blocked(user)
        return user_to_client_dict(user)
    except HTTPException:
        raise
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))


@router.post("/notify-trade-channel")
async def notify_trade_channel(
    body: NotifyTradeChannelRequest,
    token: str = Depends(oauth2_scheme),
    db: Session = Depends(get_db),
):
    """
    Send admin alert to Telegram channel TradeWithCTO (configure TELEGRAM_BOT_TOKEN + TELEGRAM_TRADEWITHCTO_CHAT_ID).
    Authenticated users only; message includes DB user display name.
    """
    raw = (body.context or "").strip().lower().replace("-", "_")
    if raw not in _NOTIFY_TRADE_CHANNEL_MESSAGES:
        raise HTTPException(
            status_code=400,
            detail="Invalid context (use intraoption or pivot_breakout)",
        )
    user = get_user_from_token(token, db)
    display_name = (user.full_name or user.username or user.email or f"user_{user.id}").strip()
    text = _NOTIFY_TRADE_CHANNEL_MESSAGES[raw].format(name=display_name)
    if not _send_telegram_trade_channel_message(text):
        raise HTTPException(
            status_code=503,
            detail="Telegram channel notify is not configured or failed. Set TELEGRAM_BOT_TOKEN and TELEGRAM_TRADEWITHCTO_CHAT_ID.",
        )
    return {"success": True, "message": "Notification sent to TradeWithCTO channel"}


@router.post("/notify-telegram-user-message")
async def notify_telegram_user_message(
    body: NotifyTelegramUserMessageRequest,
    token: str = Depends(oauth2_scheme),
    db: Session = Depends(get_db),
):
    """
    Send a custom user message to Telegram channel TradeWithCTO, concatenated with the logged-in user's name.
    Uses TELEGRAM_BOT_TOKEN and TELEGRAM_TRADEWITHCTO_CHAT_ID (same as other notify endpoints).
    """
    text = (body.message or "").strip()
    if not text:
        raise HTTPException(status_code=400, detail="Message cannot be empty")
    user = get_user_from_token(token, db)
    display_name = (user.full_name or user.username or user.email or f"user_{user.id}").strip()
    full_text = f"{text}\n\n— {display_name}"
    if not _send_telegram_trade_channel_message(full_text):
        raise HTTPException(
            status_code=503,
            detail="Telegram channel notify is not configured or failed. Set TELEGRAM_BOT_TOKEN and TELEGRAM_TRADEWITHCTO_CHAT_ID.",
        )
    return {"success": True, "message": "Message sent to TradeWithCTO channel"}


@router.post("/activity/page-view")
async def track_page_view(
    body: PageViewRequest,
    request: Request,
    token: str = Depends(oauth2_scheme),
    db: Session = Depends(get_db),
):
    """
    Track latest visited page and activity metadata for authenticated user.
    Called from frontend on page loads/navigation.
    """
    user = get_user_from_token(token, db)
    _assert_not_blocked(user)
    page = (body.page or "").strip()[:255]
    if not page:
        raise HTTPException(status_code=400, detail="page is required")
    user.last_page_visited = page
    user.last_page_visited_at = datetime.utcnow()
    user.last_activity_ip = _extract_client_ip(request)
    db.commit()
    return {"success": True}


@router.get("/admin/user-activity")
async def get_admin_user_activity(
    token: str = Depends(oauth2_scheme),
    db: Session = Depends(get_db),
):
    _require_admin_user(token, db)
    users = (
        db.query(models.User)
        .order_by(models.User.last_login_at.desc().nullslast(), models.User.created_at.desc())
        .all()
    )
    rows = []
    for u in users:
        rows.append(
            {
                "id": u.id,
                "email": u.email,
                "name": u.full_name or u.username or "",
                "is_admin": ((u.is_admin or "").strip().lower() == "yes"),
                "is_paid_user": bool(getattr(u, "is_paid_user", False)),
                "is_blocked": bool(getattr(u, "is_blocked", False)),
                "last_login_at": _to_iso(getattr(u, "last_login_at", None)),
                "last_login_ip": (getattr(u, "last_login_ip", None) or "").strip(),
                "last_page_visited": (getattr(u, "last_page_visited", None) or "").strip(),
                "last_page_visited_at": _to_iso(getattr(u, "last_page_visited_at", None)),
                "created_at": _to_iso(getattr(u, "created_at", None)),
            }
        )
    return {"status": "success", "users": rows}


@router.patch("/admin/users/{user_id}/flags")
async def update_user_flags(
    user_id: int,
    body: UserFlagsUpdateRequest,
    token: str = Depends(oauth2_scheme),
    db: Session = Depends(get_db),
):
    admin_user = _require_admin_user(token, db)
    target = db.query(models.User).filter(models.User.id == user_id).first()
    if not target:
        raise HTTPException(status_code=404, detail="User not found")

    if body.is_blocked is not None:
        target.is_blocked = bool(body.is_blocked)
    if body.is_paid_user is not None:
        target.is_paid_user = bool(body.is_paid_user)
    if body.is_admin is not None:
        # Prevent accidental full admin lockout
        if int(target.id) == int(admin_user.id) and body.is_admin is False:
            raise HTTPException(status_code=400, detail="You cannot remove your own admin access")
        target.is_admin = "Yes" if body.is_admin else None

    target.updated_at = datetime.utcnow()
    db.commit()
    db.refresh(target)

    return {
        "status": "success",
        "user": {
            "id": target.id,
            "is_admin": ((target.is_admin or "").strip().lower() == "yes"),
            "is_paid_user": bool(getattr(target, "is_paid_user", False)),
            "is_blocked": bool(getattr(target, "is_blocked", False)),
        },
    }
