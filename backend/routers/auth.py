from fastapi import APIRouter, Depends, HTTPException, status
from fastapi.security import OAuth2PasswordBearer
from sqlalchemy.orm import Session
from datetime import datetime, timedelta
import requests
from jose import jwt
from typing import Optional
import os
from pydantic import BaseModel

import backend.models as models
from backend.database import get_db
from backend.config import settings

router = APIRouter(prefix="/auth", tags=["authentication"])

oauth2_scheme = OAuth2PasswordBearer(tokenUrl="token")

class GoogleOAuthRequest(BaseModel):
    credential: str

class GoogleOAuthCodeRequest(BaseModel):
    code: str

# Google OAuth configuration
GOOGLE_CLIENT_ID = os.getenv("GOOGLE_CLIENT_ID")
GOOGLE_CLIENT_SECRET = os.getenv("GOOGLE_CLIENT_SECRET")
GOOGLE_TOKEN_URL = "https://oauth2.googleapis.com/token"
GOOGLE_USERINFO_URL = "https://www.googleapis.com/oauth2/v2/userinfo"

if not GOOGLE_CLIENT_ID or not GOOGLE_CLIENT_SECRET:
    raise ValueError("GOOGLE_CLIENT_ID and GOOGLE_CLIENT_SECRET must be set in environment variables")

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
async def google_oauth(request: GoogleOAuthRequest, db: Session = Depends(get_db)):
    """Handle Google OAuth login/signup using JWT credential"""
    try:
        # Verify the JWT credential with Google
        verify_url = f"https://oauth2.googleapis.com/tokeninfo?id_token={request.credential}"
        response = requests.get(verify_url)
        
        if response.status_code != 200:
            raise HTTPException(status_code=400, detail="Invalid Google credential")
        
        userinfo = response.json()
        
        # Verify the audience matches our client ID
        if userinfo.get('aud') != GOOGLE_CLIENT_ID:
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
            # Update last login
            user.updated_at = datetime.utcnow()
            db.commit()
        
        # Create access token
        access_token_expires = timedelta(minutes=settings.ACCESS_TOKEN_EXPIRE_MINUTES)
        access_token = create_access_token(
            data={"sub": str(user.id)}, expires_delta=access_token_expires
        )
        
        return {
            "access_token": access_token,
            "token_type": "bearer",
            "user": {
                "id": user.id,
                "email": user.email,
                "name": user.full_name,
                "picture": user.avatar_url
            }
        }
        
    except HTTPException:
        # Re-raise HTTP exceptions (like 400 errors) as-is
        raise
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))

@router.post("/google-verify")
async def google_oauth_verify(user_data: dict, db: Session = Depends(get_db)):
    """Handle Google OAuth verification from frontend"""
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
            db.commit()
            db.refresh(user)
            print(f"User updated: {user.id}")
        
        # Create access token
        access_token_expires = timedelta(minutes=settings.ACCESS_TOKEN_EXPIRE_MINUTES)
        access_token = create_access_token(
            data={"sub": str(user.id)}, expires_delta=access_token_expires
        )
        
        print(f"Authentication successful for user: {user.id}")
        
        return {
            "access_token": access_token,
            "token_type": "bearer",
            "user": {
                "id": user.id,
                "email": user.email,
                "name": user.full_name,
                "picture": user.avatar_url,
                "google_id": user.google_id
            }
        }
        
    except Exception as e:
        print(f"Error in google_oauth_verify: {str(e)}")
        raise HTTPException(status_code=500, detail=str(e))

@router.post("/google-code")
async def google_oauth_code(request: GoogleOAuthCodeRequest, db: Session = Depends(get_db)):
    """Handle Google OAuth code exchange for mobile browsers"""
    try:
        # Exchange authorization code for access token
        token_response = requests.post(GOOGLE_TOKEN_URL, data={
            'client_id': GOOGLE_CLIENT_ID,
            'client_secret': GOOGLE_CLIENT_SECRET,
            'code': request.code,
            'grant_type': 'authorization_code',
            'redirect_uri': 'https://trademanthan.in/login.html'
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
            # Update last login
            user.updated_at = datetime.utcnow()
            db.commit()
        
        # Create access token
        access_token_expires = timedelta(minutes=settings.ACCESS_TOKEN_EXPIRE_MINUTES)
        jwt_token = create_access_token(
            data={"sub": str(user.id)}, expires_delta=access_token_expires
        )
        
        return {
            "access_token": jwt_token,
            "token_type": "bearer",
            "user": {
                "id": user.id,
                "email": user.email,
                "name": user.full_name,
                "picture": user.avatar_url
            }
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
        payload = jwt.decode(token, settings.SECRET_KEY, algorithms=["HS256"])
        user_id: str = payload.get("sub")
        if user_id is None:
            raise HTTPException(status_code=401, detail="Invalid token")
        
        user = db.query(models.User).filter(models.User.id == int(user_id)).first()
        if user is None:
            raise HTTPException(status_code=401, detail="User not found")
        
        return {
            "id": user.id,
            "email": user.email,
            "name": user.full_name,
            "picture": user.avatar_url
        }
        
    except jwt.ExpiredSignatureError:
        raise HTTPException(status_code=401, detail="Token expired")
    except jwt.JWTError:
        raise HTTPException(status_code=401, detail="Invalid token")
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))
