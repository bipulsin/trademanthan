from fastapi import APIRouter, Depends, HTTPException, status
from fastapi.responses import JSONResponse
from sqlalchemy.orm import Session
from typing import List, Optional

import backend.models.user
import backend.models.trading
from backend.database import get_db
import backend.routers.auth

router = APIRouter(prefix="/dashboard", tags=["dashboard"])

async def get_current_user(token: str = Depends(backend.routers.auth.oauth2_scheme), db: Session = Depends(get_db)):
    """Get current user from token"""
    try:
        from jose import jwt
        from backend.config import settings
        
        payload = jwt.decode(token, settings.SECRET_KEY, algorithms=["HS256"])
        user_id: str = payload.get("sub")
        if user_id is None:
            raise HTTPException(status_code=401, detail="Invalid token")
        
        user = db.query(backend.models.user.User).filter(backend.models.user.User.id == int(user_id)).first()
        if user is None:
            raise HTTPException(status_code=401, detail="User not found")
        
        return user
        
    except Exception as e:
        raise HTTPException(status_code=401, detail="Invalid token")

@router.get("/brokers")
async def get_user_brokers(current_user: backend.models.user.User = Depends(get_current_user), db: Session = Depends(get_db)):
    """Get all brokers for the current user"""
    brokers = db.query(backend.models.trading.Broker).filter(backend.models.trading.Broker.user_id == current_user.id).all()
    return [
        {
            "id": broker.id,
            "name": broker.name,
            "wallet_balance": broker.wallet_balance,
            "is_connected": broker.is_connected,
            "last_sync": broker.last_sync
        }
        for broker in brokers
    ]

@router.post("/brokers")
async def create_broker(
    name: str,
    api_key: str,
    api_secret: str,
    current_user: backend.models.user.User = Depends(get_current_user),
    db: Session = Depends(get_db)
):
    """Create a new broker connection"""
    broker = backend.models.trading.Broker(
        user_id=current_user.id,
        name=name,
        api_key=api_key,
        api_secret=api_secret
    )
    db.add(broker)
    db.commit()
    db.refresh(broker)
    
    return {
        "id": broker.id,
        "name": broker.name,
        "wallet_balance": broker.wallet_balance,
        "is_connected": broker.is_connected
    }

@router.get("/strategies")
async def get_user_strategies(current_user: backend.models.user.User = Depends(get_current_user), db: Session = Depends(get_db)):
    """Get all strategies for the current user"""
    strategies = db.query(backend.models.trading.Strategy).filter(backend.models.trading.Strategy.user_id == current_user.id).all()
    return [
        {
            "id": strategy.id,
            "name": strategy.name,
            "description": strategy.description,
            "total_pnl": strategy.total_pnl,
            "is_active": strategy.is_active,
            "broker_name": strategy.broker.name if strategy.broker else None
        }
        for strategy in strategies
    ]

@router.post("/strategies")
async def create_strategy(
    name: str,
    description: Optional[str] = None,
    broker_id: Optional[int] = None,
    current_user: backend.models.user.User = Depends(get_current_user),
    db: Session = Depends(get_db)
):
    """Create a new trading strategy"""
    strategy = backend.models.trading.Strategy(
        user_id=current_user.id,
        name=name,
        description=description,
        broker_id=broker_id
    )
    db.add(strategy)
    db.commit()
    db.refresh(strategy)
    
    return {
        "id": strategy.id,
        "name": strategy.name,
        "description": strategy.description,
        "total_pnl": strategy.total_pnl
    }

@router.get("/crypto-prices")
async def get_crypto_prices():
    """Live desktop crypto cards from Delta Exchange India public tickers."""
    from backend.services.delta_india_live_prices import build_desktop_crypto_prices

    payload = build_desktop_crypto_prices()
    status_code = 200 if payload.get("ok") else 503
    return JSONResponse(
        status_code=status_code,
        content=payload,
        headers={"Cache-Control": "no-store"},
    )

@router.get("/summary")
async def get_dashboard_summary(current_user: backend.models.user.User = Depends(get_current_user), db: Session = Depends(get_db)):
    """Get dashboard summary data"""
    brokers = db.query(backend.models.trading.Broker).filter(backend.models.trading.Broker.user_id == current_user.id).all()
    strategies = db.query(backend.models.trading.Strategy).filter(backend.models.trading.Strategy.user_id == current_user.id).all()
    
    total_brokers = len(brokers)
    connected_brokers = len([b for b in brokers if b.is_connected])
    total_strategies = len(strategies)
    total_pnl = sum([s.total_pnl for s in strategies])
    
    return {
        "brokers": {
            "total": total_brokers,
            "connected": connected_brokers
        },
        "strategies": {
            "total": total_strategies,
            "total_pnl": total_pnl
        }
    }
