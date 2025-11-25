from fastapi import APIRouter, Depends, HTTPException, status
from sqlalchemy.orm import Session
from datetime import datetime
from typing import List, Optional
import backend.models as models
from backend.database import get_db
from backend.config import settings

router = APIRouter(prefix="/broker", tags=["broker management"])

@router.get("/")
async def get_user_brokers(user_id: int, db: Session = Depends(get_db)):
    """Get all brokers for a specific user"""
    try:
        brokers = db.query(models.Broker).filter(models.Broker.user_id == user_id).all()
        return {
            "success": True,
            "brokers": [
                {
                    "id": broker.id,
                    "name": broker.name,
                    "type": broker.type,
                    "api_url": broker.api_url,
                    "api_key": broker.api_key,
                    "api_secret": broker.api_secret,
                    "is_connected": broker.is_connected,
                    "connection_status": broker.connection_status,
                    "last_connection": broker.last_connection,
                    "test_mode": broker.test_mode,
                    "created_at": broker.created_at,
                    "updated_at": broker.updated_at
                }
                for broker in brokers
            ]
        }
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))

@router.post("/")
async def create_broker(broker_data: dict, db: Session = Depends(get_db)):
    """Create a new broker for a user"""
    try:
        required_fields = ["user_id", "name", "type"]
        for field in required_fields:
            if field not in broker_data:
                raise HTTPException(status_code=400, detail=f"Missing required field: {field}")
        
        # Create new broker
        new_broker = models.Broker(
            user_id=broker_data["user_id"],
            name=broker_data["name"],
            type=broker_data["type"],
            api_url=broker_data.get("api_url"),
            api_key=broker_data.get("api_key"),
            api_secret=broker_data.get("api_secret"),
            is_connected=broker_data.get("is_connected", False),
            connection_status=broker_data.get("connection_status", "disconnected"),
            test_mode=broker_data.get("test_mode", True),
            created_at=datetime.utcnow(),
            updated_at=datetime.utcnow()
        )
        
        db.add(new_broker)
        db.commit()
        db.refresh(new_broker)
        
        return {
            "success": True,
            "message": "Broker created successfully",
            "broker_id": new_broker.id
        }
        
    except Exception as e:
        db.rollback()
        raise HTTPException(status_code=500, detail=str(e))

@router.put("/{broker_id}")
async def update_broker(broker_id: int, broker_data: dict, db: Session = Depends(get_db)):
    """Update an existing broker"""
    try:
        broker = db.query(models.Broker).filter(models.Broker.id == broker_id).first()
        if not broker:
            raise HTTPException(status_code=404, detail="Broker not found")
        
        # Update fields
        for field, value in broker_data.items():
            if hasattr(broker, field) and field not in ["id", "user_id", "created_at"]:
                setattr(broker, field, value)
        
        broker.updated_at = datetime.utcnow()
        db.commit()
        
        return {
            "success": True,
            "message": "Broker updated successfully"
        }
        
    except Exception as e:
        db.rollback()
        raise HTTPException(status_code=500, detail=str(e))

@router.delete("/{broker_id}")
async def delete_broker(broker_id: int, db: Session = Depends(get_db)):
    """Delete a broker"""
    try:
        broker = db.query(models.Broker).filter(models.Broker.id == broker_id).first()
        if not broker:
            raise HTTPException(status_code=404, detail="Broker not found")
        
        db.delete(broker)
        db.commit()
        
        return {
            "success": True,
            "message": "Broker deleted successfully"
        }
        
    except Exception as e:
        db.rollback()
        raise HTTPException(status_code=500, detail=str(e))

@router.post("/create-default")
async def create_default_broker(user_id: int, db: Session = Depends(get_db)):
    """Create a default Binance broker for a new user"""
    try:
        # Check if user already has a default broker
        existing_broker = db.query(models.Broker).filter(
            models.Broker.user_id == user_id,
            models.Broker.name == "Binance"
        ).first()
        
        if existing_broker:
            return {
                "success": True,
                "message": "Default broker already exists",
                "broker_id": existing_broker.id
            }
        
        # Create default Binance broker
        default_broker = models.Broker(
            user_id=user_id,
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
        db.refresh(default_broker)
        
        return {
            "success": True,
            "message": "Default Binance broker created successfully",
            "broker_id": default_broker.id
        }
        
    except Exception as e:
        db.rollback()
        raise HTTPException(status_code=500, detail=str(e))
