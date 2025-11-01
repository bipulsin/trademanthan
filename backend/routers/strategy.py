from fastapi import APIRouter, Depends, HTTPException, status
from sqlalchemy.orm import Session
from typing import List
import models
from database import get_db
from datetime import datetime
from models import User, Strategy, Broker
from dependencies import get_current_user
import logging
import sys
import os

logger = logging.getLogger(__name__)

router = APIRouter(prefix="/strategy", tags=["strategies"])

@router.get("/user/{user_id}")
async def get_user_strategies(user_id: int, db: Session = Depends(get_db)):
    """Get all strategies for a specific user"""
    try:
        # Verify user exists
        user = db.query(models.User).filter(models.User.id == user_id).first()
        if not user:
            raise HTTPException(status_code=404, detail="User not found")
        
        # Get all strategies for the user (excluding test strategies with null user_id)
        strategies = db.query(models.Strategy).filter(
            models.Strategy.user_id == user_id,
            models.Strategy.user_id.is_not(None)
        ).all()
        
        strategy_list = []
        for strategy in strategies:
            # Convert indicators from text[] to list for frontend compatibility
            indicators_list = strategy.indicators if strategy.indicators else []
            if isinstance(indicators_list, str):
                # Handle case where indicators might be stored as string
                indicators_list = [indicators_list]
            
            strategy_data = {
                "id": strategy.id,
                "name": strategy.name,
                "description": strategy.description,
                "product": getattr(strategy, 'product', None),
                "platform": getattr(strategy, 'platform', None),
                "product_id": getattr(strategy, 'product_id', None),
                "candle_duration": getattr(strategy, 'candle_duration', None),
                "indicators": indicators_list,
                "logic_operator": strategy.logic_operator,
                "parameters": strategy.parameters or {},
                "entry_criteria": strategy.entry_criteria,
                "exit_criteria": strategy.exit_criteria,
                "trade_conditions": strategy.trade_conditions or {},
                "stop_loss": getattr(strategy, 'stop_loss', None) or {},
                "trailing_stop": getattr(strategy, 'trailing_stop', None) or {},
                "is_active": strategy.is_active,
                "is_live": strategy.is_live,
                "broker_connected": strategy.broker_connected,
                "broker_connection_date": strategy.broker_connection_date,
                "execution_status": strategy.execution_status,
                "last_execution": strategy.last_execution,
                "next_execution": strategy.next_execution,
                "total_pnl": float(strategy.total_pnl) if strategy.total_pnl else 0.0,
                "last_trade_pnl": float(strategy.last_trade_pnl) if strategy.last_trade_pnl else 0.0,
                "created_at": strategy.created_at,
                "updated_at": strategy.updated_at
            }
            
            # Add broker information if connected
            if strategy.broker_id:
                broker = db.query(models.Broker).filter(models.Broker.id == strategy.broker_id).first()
                if broker:
                    strategy_data["broker"] = {
                        "id": broker.id,
                        "name": broker.name,
                        "type": broker.type
                    }
            
            strategy_list.append(strategy_data)
        
        return {
            "success": True,
            "strategies": strategy_list,
            "count": len(strategy_list)
        }
        
    except HTTPException:
        raise
    except Exception as e:
        print(f"Error fetching strategies for user {user_id}: {e}")
        # Return mock data for development when database is not available
        if "connection" in str(e).lower() or "refused" in str(e).lower():
            mock_strategies = [
                {
                    "id": 1,
                    "name": "Test Strategy",
                    "description": "Mock strategy for testing",
                    "product": "NIFTY",
                    "platform": "testnet",
                    "product_id": "123",
                    "candle_duration": "5m",
                    "indicators": ["rsi", "supertrend"],
                    "logic_operator": "AND",
                    "parameters": {},
                    "entry_criteria": "RSI oversold and Supertrend bullish",
                    "exit_criteria": "RSI overbought or Supertrend bearish",
                    "trade_conditions": {},
                    "stop_loss": {},
                    "trailing_stop": {},
                    "is_active": True,
                    "is_live": False,
                    "broker_connected": False,
                    "broker_connection_date": None,
                    "execution_status": "STOPPED",
                    "last_execution": None,
                    "next_execution": None,
                    "total_pnl": 0.0,
                    "last_trade_pnl": 0.0,
                    "created_at": "2024-01-01T00:00:00Z",
                    "updated_at": "2024-01-01T00:00:00Z"
                }
            ]
            return {
                "success": True,
                "strategies": mock_strategies,
                "count": len(mock_strategies),
                "note": "Using mock data - database not available"
            }
        raise HTTPException(status_code=500, detail="Internal server error")

@router.get("/{strategy_id}")
async def get_strategy(strategy_id: int, db: Session = Depends(get_db)):
    """Get a specific strategy by ID"""
    try:
        strategy = db.query(models.Strategy).filter(models.Strategy.id == strategy_id).first()
        if not strategy:
            raise HTTPException(status_code=404, detail="Strategy not found")
        
        # Convert indicators from text[] to list for frontend compatibility
        indicators_list = strategy.indicators if strategy.indicators else []
        if isinstance(indicators_list, str):
            # Handle case where indicators might be stored as string
            indicators_list = [indicators_list]
        
        strategy_data = {
            "id": strategy.id,
            "name": strategy.name,
            "description": strategy.description,
            "product": getattr(strategy, 'product', None),
            "platform": getattr(strategy, 'platform', None),
            "product_id": getattr(strategy, 'product_id', None),
            "candle_duration": getattr(strategy, 'candle_duration', None),
            "indicators": indicators_list,
            "logic_operator": strategy.logic_operator,
            "parameters": strategy.parameters or {},
            "entry_criteria": strategy.entry_criteria,
            "exit_criteria": strategy.exit_criteria,
            "trade_conditions": strategy.trade_conditions or {},
            "stop_loss": getattr(strategy, 'stop_loss', None) or {},
            "trailing_stop": getattr(strategy, 'trailing_stop', None) or {},
            "is_active": strategy.is_active,
            "is_live": strategy.is_live,
            "broker_connected": strategy.broker_connected,
            "broker_connection_date": strategy.broker_connection_date,
            "execution_status": strategy.execution_status,
            "last_execution": strategy.last_execution,
            "next_execution": strategy.next_execution,
            "total_pnl": float(strategy.total_pnl) if strategy.total_pnl else 0.0,
            "last_trade_pnl": float(strategy.last_trade_pnl) if strategy.last_trade_pnl else 0.0,
            "created_at": strategy.created_at,
            "updated_at": strategy.updated_at
        }
        
        return {
            "success": True,
            "strategy": strategy_data
        }
        
    except HTTPException:
        raise
    except Exception as e:
        print(f"Error fetching strategy {strategy_id}: {e}")
        raise HTTPException(status_code=500, detail="Internal server error")

@router.post("/")
async def create_strategy(strategy_data: dict, db: Session = Depends(get_db)):
    """Create a new strategy"""
    try:
        print(f"🆕 ===== STRATEGY CREATION REQUEST RECEIVED ======")
        print(f"🆕 Received data: {strategy_data}")
        print(f"🆕 Data type: {type(strategy_data)}")
        print(f"🆕 Data keys: {list(strategy_data.keys()) if isinstance(strategy_data, dict) else 'Not a dict'}")
        
        # Log specific fields that should be created
        print(f"🔍 Key fields in request:")
        print(f"  - user_id: {strategy_data.get('user_id')}")
        print(f"  - name: {strategy_data.get('name')}")
        print(f"  - platform: {strategy_data.get('platform')}")
        print(f"  - product: {strategy_data.get('product')}")
        print(f"  - product_id: {strategy_data.get('product_id')}")
        print(f"  - candle_duration: {strategy_data.get('candle_duration')}")
        print(f"  - stop_loss: {strategy_data.get('stop_loss')}")
        print(f"  - indicators: {strategy_data.get('indicators')}")
        
        # Validate required fields
        required_fields = ["user_id", "name", "indicators", "entry_criteria", "exit_criteria"]
        print(f"🔍 Validating required fields: {required_fields}")
        
        missing_fields = []
        for field in required_fields:
            if field not in strategy_data:
                missing_fields.append(field)
                print(f"  ❌ Missing required field: {field}")
            else:
                print(f"  ✅ Required field present: {field} = {strategy_data[field]}")
        
        if missing_fields:
            print(f"❌ Missing required fields: {missing_fields}")
            raise HTTPException(status_code=400, detail=f"Missing required fields: {', '.join(missing_fields)}")
        
        print(f"✅ All required fields are present")
        
        # Create new strategy
        print(f"🔄 Creating new Strategy object...")
        strategy = models.Strategy(
            user_id=strategy_data["user_id"],
            broker_id=strategy_data.get("broker_id"),
            name=strategy_data["name"],
            description=strategy_data.get("description", ""),
            product=strategy_data.get("product"),
            platform=strategy_data.get("platform"),
            product_id=strategy_data.get("product_id"),
            candle_duration=strategy_data.get("candle_duration"),
            indicators=strategy_data["indicators"],
            logic_operator=strategy_data.get("logic_operator", "AND"),
            parameters=strategy_data.get("parameters", {}),
            entry_criteria=strategy_data["entry_criteria"],
            exit_criteria=strategy_data["exit_criteria"],
            trade_conditions=strategy_data.get("trade_conditions", {}),
            stop_loss=strategy_data.get("stop_loss"),
            trailing_stop=strategy_data.get("trailing_stop"),
            is_active=strategy_data.get("is_active", True),
            is_live=strategy_data.get("is_live", False),
            is_backtested=strategy_data.get("is_backtested", False),
            broker_connected=strategy_data.get("broker_connected", False),
            execution_status=strategy_data.get("execution_status", "STOPPED"),
            created_at=datetime.utcnow(),
            updated_at=datetime.utcnow()
        )
        
        print(f"✅ Strategy object created successfully")
        print(f"🔍 Strategy object details:")
        print(f"  - platform: {strategy.platform}")
        print(f"  - product: {strategy.product}")
        print(f"  - product_id: {strategy.product_id}")
        print(f"  - candle_duration: {strategy.candle_duration}")
        print(f"  - stop_loss: {strategy.stop_loss}")
        
        # Add to database
        print(f"🔄 Adding strategy to database...")
        db.add(strategy)
        print(f"✅ Strategy added to session")
        
        # Commit to database
        print(f"🔄 Committing to database...")
        db.commit()
        print(f"✅ Database commit successful")
        
        # Refresh to get the ID
        print(f"🔄 Refreshing strategy object to get ID...")
        db.refresh(strategy)
        print(f"✅ Strategy refreshed, ID: {strategy.id}")
        
        print(f"✅ ===== STRATEGY CREATION COMPLETED SUCCESSFULLY ======")
        
        return {
            "success": True,
            "strategy_id": strategy.id,
            "message": "Strategy created successfully"
        }
        
    except HTTPException:
        print(f"❌ HTTP Exception raised during creation")
        raise
    except Exception as e:
        print(f"❌ ===== STRATEGY CREATION FAILED ======")
        print(f"❌ Error creating strategy: {e}")
        print(f"❌ Error type: {type(e)}")
        print(f"❌ Error details: {str(e)}")
        import traceback
        print(f"❌ Traceback: {traceback.format_exc()}")
        db.rollback()
        raise HTTPException(status_code=500, detail=f"Internal server error: {str(e)}")

@router.put("/{strategy_id}")
async def update_strategy(strategy_id: int, strategy_data: dict, db: Session = Depends(get_db)):
    """Update an existing strategy"""
    try:
        print(f"🔄 ===== STRATEGY UPDATE REQUEST RECEIVED ======")
        print(f"🔄 Strategy ID: {strategy_id}")
        print(f"🔄 Received data: {strategy_data}")
        print(f"🔄 Data type: {type(strategy_data)}")
        print(f"🔄 Data keys: {list(strategy_data.keys()) if isinstance(strategy_data, dict) else 'Not a dict'}")
        
        # Log specific fields that should be updated
        print(f"🔍 Key fields in request:")
        print(f"  - platform: {strategy_data.get('platform')}")
        print(f"  - product: {strategy_data.get('product')}")
        print(f"  - product_id: {strategy_data.get('product_id')}")
        print(f"  - candle_duration: {strategy_data.get('candle_duration')}")
        print(f"  - stop_loss: {strategy_data.get('stop_loss')}")
        
        # Find existing strategy
        print(f"🔍 Querying database for strategy ID: {strategy_id}")
        strategy = db.query(models.Strategy).filter(models.Strategy.id == strategy_id).first()
        
        if not strategy:
            print(f"❌ Strategy not found in database for ID: {strategy_id}")
            raise HTTPException(status_code=404, detail="Strategy not found")
        
        print(f"✅ Strategy found in database:")
        print(f"  - Current platform: {getattr(strategy, 'platform', 'Not set')}")
        print(f"  - Current product: {getattr(strategy, 'product', 'Not set')}")
        print(f"  - Current product_id: {getattr(strategy, 'product_id', 'Not set')}")
        print(f"  - Current candle_duration: {getattr(strategy, 'candle_duration', 'Not set')}")
        print(f"  - Current stop_loss: {getattr(strategy, 'stop_loss', 'Not set')}")
        
        # Update allowed fields
        allowed_fields = [
            "name", "description", "product", "platform", "product_id", "candle_duration", "indicators", 
            "logic_operator", "parameters", "entry_criteria", "exit_criteria", 
            "stop_loss", "trailing_stop", "trade_conditions", "is_active", 
            "is_live", "broker_id"
        ]
        
        print(f"🔍 Allowed fields for update: {allowed_fields}")
        print(f"🔍 Fields that will be updated:")
        
        updated_fields = []
        for field in allowed_fields:
            if field in strategy_data:
                old_value = getattr(strategy, field, 'Not set')
                new_value = strategy_data[field]
                print(f"  - {field}: {old_value} -> {new_value}")
                setattr(strategy, field, new_value)
                updated_fields.append(field)
            else:
                print(f"  - {field}: Not in request data (keeping current value)")
        
        print(f"✅ Total fields to be updated: {len(updated_fields)}")
        print(f"✅ Fields updated: {updated_fields}")
        
        # Update timestamp
        strategy.updated_at = datetime.utcnow()
        print(f"🔄 Updated timestamp to: {strategy.updated_at}")
        
        # Flush changes to session before commit
        print(f"🔄 Flushing changes to session...")
        db.flush()
        print(f"✅ Session flush successful")
        
        # Commit changes to database with proper error handling
        print(f"🔄 Committing changes to database...")
        try:
            db.commit()
            print(f"✅ Database commit successful")
        except Exception as commit_error:
            print(f"❌ Database commit failed: {commit_error}")
            print(f"❌ Rolling back session...")
            db.rollback()
            print(f"❌ Session rolled back")
            raise HTTPException(status_code=500, detail=f"Database commit failed: {str(commit_error)}")
        
        # Verify the update by querying again
        print(f"🔍 Verifying update by querying database again...")
        try:
            db.refresh(strategy)
            print(f"✅ Strategy data after update:")
            print(f"  - platform: {getattr(strategy, 'platform', 'Not set')}")
            print(f"  - product: {getattr(strategy, 'product', 'Not set')}")
            print(f"  - product_id: {getattr(strategy, 'product_id', 'Not set')}")
            print(f"  - candle_duration: {getattr(strategy, 'candle_duration', 'Not set')}")
            print(f"  - stop_loss: {getattr(strategy, 'stop_loss', 'Not set')}")
        except Exception as refresh_error:
            print(f"⚠️ Warning: Could not refresh strategy object: {refresh_error}")
            print(f"⚠️ This is not critical, but may indicate a session issue")
        
        print(f"✅ ===== STRATEGY UPDATE COMPLETED SUCCESSFULLY ======")
        
        return {
            "success": True,
            "message": "Strategy updated successfully",
            "updated_fields": updated_fields,
            "strategy_id": strategy_id
        }
        
    except HTTPException:
        print(f"❌ HTTP Exception raised during update")
        raise
    except Exception as e:
        print(f"❌ ===== STRATEGY UPDATE FAILED ======")
        print(f"❌ Error updating strategy {strategy_id}: {e}")
        print(f"❌ Error type: {type(e)}")
        print(f"❌ Error details: {str(e)}")
        import traceback
        print(f"❌ Traceback: {traceback.format_exc()}")
        
        # Ensure session is rolled back on any error
        try:
            db.rollback()
            print(f"❌ Session rolled back due to error")
        except Exception as rollback_error:
            print(f"⚠️ Warning: Could not rollback session: {rollback_error}")
        
        raise HTTPException(status_code=500, detail=f"Internal server error: {str(e)}")

@router.delete("/{strategy_id}")
async def delete_strategy(strategy_id: int, db: Session = Depends(get_db)):
    """Delete a strategy"""
    try:
        strategy = db.query(models.Strategy).filter(models.Strategy.id == strategy_id).first()
        if not strategy:
            raise HTTPException(status_code=404, detail="Strategy not found")
        
        db.delete(strategy)
        db.commit()
        
        return {
            "success": True,
            "message": "Strategy deleted successfully"
        }
        
    except HTTPException:
        raise
    except Exception as e:
        print(f"Error deleting strategy {strategy_id}: {e}")
        db.rollback()
        raise HTTPException(status_code=500, detail="Internal server error")

@router.post("/{strategy_id}/connect-broker")
async def connect_broker_to_strategy(strategy_id: int, broker_data: dict, db: Session = Depends(get_db)):
    """Connect a broker to a strategy"""
    try:
        strategy = db.query(models.Strategy).filter(models.Strategy.id == strategy_id).first()
        if not strategy:
            raise HTTPException(status_code=404, detail="Strategy not found")
        
        broker_id = broker_data.get("broker_id")
        if not broker_id:
            raise HTTPException(status_code=400, detail="Broker ID is required")
        
        # Verify broker exists
        broker = db.query(models.Broker).filter(models.Broker.id == broker_id).first()
        if not broker:
            raise HTTPException(status_code=404, detail="Broker not found")
        
        # Update strategy with broker connection
        strategy.broker_id = broker_id
        strategy.broker_connected = True
        strategy.broker_connection_date = datetime.utcnow()
        strategy.updated_at = datetime.utcnow()
        
        db.commit()
        
        return {
            "success": True,
            "message": "Broker connected to strategy successfully",
            "strategy_id": strategy_id,
            "broker_id": broker_id
        }
        
    except HTTPException:
        raise
    except Exception as e:
        print(f"Error connecting broker to strategy {strategy_id}: {e}")
        db.rollback()
        raise HTTPException(status_code=500, detail="Internal server error")

@router.post("/{strategy_id}/disconnect-broker")
async def disconnect_broker_from_strategy(strategy_id: int, db: Session = Depends(get_db)):
    """Disconnect broker from a strategy"""
    try:
        strategy = db.query(models.Strategy).filter(models.Strategy.id == strategy_id).first()
        if not strategy:
            raise HTTPException(status_code=404, detail="Strategy not found")
        
        # Remove broker connection
        strategy.broker_id = None
        strategy.broker_connected = False
        strategy.broker_connection_date = None
        strategy.is_live = False  # Stop strategy if it was running
        strategy.execution_status = "STOPPED"
        strategy.updated_at = datetime.utcnow()
        
        db.commit()
        
        return {
            "success": True,
            "message": "Broker disconnected from strategy successfully",
            "strategy_id": strategy_id
        }
        
    except HTTPException:
        raise
    except Exception as e:
        print(f"Error disconnecting broker from strategy {strategy_id}: {e}")
        db.rollback()
        raise HTTPException(status_code=500, detail="Internal server error")

@router.post("/{strategy_id}/build")
async def build_strategy_template(
    strategy_id: int,
    db: Session = Depends(get_db),
    current_user: User = Depends(get_current_user)
):
    """Build a strategy template for the given strategy ID."""
    try:
        # Get the strategy
        strategy = db.query(Strategy).filter(
            Strategy.id == strategy_id,
            Strategy.user_id == current_user.id
        ).first()
        
        if not strategy:
            raise HTTPException(status_code=404, detail="Strategy not found")
        
        # Get the broker for this strategy
        broker = None
        if strategy.broker_id:
            broker = db.query(Broker).filter(
                Broker.id == strategy.broker_id,
                Broker.user_id == current_user.id
            ).first()
        
        # Import strategy generator
        import sys
        import os
        sys.path.append(os.path.join(os.path.dirname(__file__), '..', 'strategy_runner'))
        
        from strategy_generator import StrategyGenerator
        
        # Create a mock broker if none exists (for template generation)
        if not broker:
            class MockBroker:
                def __init__(self):
                    self.id = 0
                    self.name = "Mock Broker"
                    self.base_url = "https://api.example.com"
                    self.api_key = "mock_key"
                    self.api_secret = "mock_secret"
            
            broker = MockBroker()
        
        # Generate the strategy template
        generator = StrategyGenerator()
        strategy_file_path = generator.generate(strategy, broker)
        
        # Read the generated template
        with open(strategy_file_path, 'r') as f:
            template_content = f.read()
        
        # Clean up the generated file
        os.remove(strategy_file_path)
        
        return {
            "success": True,
            "message": "Strategy template built successfully",
            "template": template_content,
            "strategy_id": strategy_id
        }
        
    except Exception as e:
        logger.error(f"Error building strategy template: {str(e)}")
        raise HTTPException(status_code=500, detail=f"Failed to build strategy template: {str(e)}")


@router.post("/{strategy_id}/deploy")
async def deploy_strategy_to_runner(
    strategy_id: int,
    db: Session = Depends(get_db),
    current_user: User = Depends(get_current_user)
):
    """Deploy a strategy to the strategy runner."""
    try:
        # Get the strategy
        strategy = db.query(Strategy).filter(
            Strategy.id == strategy_id,
            Strategy.user_id == current_user.id
        ).first()
        
        if not strategy:
            raise HTTPException(status_code=404, detail="Strategy not found")
        
        # Check if broker is connected
        if not strategy.broker_id:
            raise HTTPException(status_code=400, detail="Strategy must have a broker connected before deployment")
        
        # Get the broker
        broker = db.query(Broker).filter(
            Broker.id == strategy.broker_id,
            Broker.user_id == current_user.id
        ).first()
        
        if not broker:
            raise HTTPException(status_code=404, detail="Broker not found")
        
        # Import strategy generator
        import sys
        import os
        sys.path.append(os.path.join(os.path.dirname(__file__), '..', 'strategy_runner'))
        
        from strategy_generator import StrategyGenerator
        
        # Generate the strategy file
        generator = StrategyGenerator()
        strategy_file_path = generator.generate(strategy, broker)
        
        # Update strategy status
        strategy.is_active = True
        strategy.is_live = False  # Start as stopped, user can start manually
        strategy.execution_status = 'READY'
        db.commit()
        
        return {
            "success": True,
            "message": "Strategy deployed to runner successfully",
            "strategy_id": strategy_id,
            "file_path": str(strategy_file_path),
            "status": "READY"
        }
        
    except Exception as e:
        logger.error(f"Error deploying strategy: {str(e)}")
        raise HTTPException(status_code=500, detail=f"Failed to deploy strategy: {str(e)}")


@router.get("/{strategy_id}/template")
async def get_strategy_template(
    strategy_id: int,
    db: Session = Depends(get_db),
    current_user: User = Depends(get_current_user)
):
    """Get the raw Python script for a deployed strategy."""
    try:
        # Get the strategy
        strategy = db.query(Strategy).filter(
            Strategy.id == strategy_id,
            Strategy.user_id == current_user.id
        ).first()
        
        if not strategy:
            raise HTTPException(status_code=404, detail="Strategy not found")
        
        # Check if strategy is deployed
        if not strategy.is_active:
            raise HTTPException(status_code=400, detail="Strategy must be deployed before viewing template")
        
        # Import strategy generator
        import sys
        import os
        sys.path.append(os.path.join(os.path.dirname(__file__), '..', 'strategy_runner'))
        
        from strategy_generator import StrategyGenerator
        
        # Get the broker
        broker = None
        if strategy.broker_id:
            broker = db.query(Broker).filter(
                Broker.id == strategy.broker_id,
                Broker.user_id == current_user.id
            ).first()
        
        if not broker:
            raise HTTPException(status_code=404, detail="Broker not found")
        
        # Generate the strategy template
        generator = StrategyGenerator()
        strategy_file_path = generator.generate(strategy, broker)
        
        # Read the generated template
        with open(strategy_file_path, 'r') as f:
            template_content = f.read()
        
        # Clean up the generated file
        os.remove(strategy_file_path)
        
        return {
            "success": True,
            "template": template_content,
            "strategy": {
                "id": strategy.id,
                "name": strategy.name,
                "product": strategy.product,
                "platform": getattr(strategy, 'platform', 'testnet'),
                "candle_duration": strategy.candle_duration,
                "indicators": strategy.indicators,
                "parameters": strategy.parameters,
                "trade_conditions": strategy.trade_conditions,
                "logic_operator": strategy.logic_operator,
                "entry_criteria": strategy.entry_criteria,
                "exit_criteria": strategy.exit_criteria
            }
        }
        
    except Exception as e:
        logger.error(f"Error getting strategy template: {str(e)}")
        raise HTTPException(status_code=500, detail=f"Failed to get strategy template: {str(e)}")


@router.post("/{strategy_id}/start")
async def start_strategy_execution(
    strategy_id: int,
    db: Session = Depends(get_db),
    current_user: User = Depends(get_current_user)
):
    """Start strategy execution on the runner."""
    try:
        # Get the strategy
        strategy = db.query(Strategy).filter(
            Strategy.id == strategy_id,
            Strategy.user_id == current_user.id
        ).first()
        
        if not strategy:
            raise HTTPException(status_code=404, detail="Strategy not found")
        
        # Check if strategy is deployed
        if not strategy.is_active:
            raise HTTPException(status_code=400, detail="Strategy must be deployed before starting execution")
        
        # Check if broker is connected
        if not strategy.broker_id:
            raise HTTPException(status_code=400, detail="Strategy must have a broker connected before starting execution")
        
        # Update strategy status
        strategy.is_live = True
        strategy.execution_status = 'RUNNING'
        db.commit()
        
        # TODO: Send command to strategy runner to start execution
        # This would involve calling the EC2 strategy runner service
        
        return {
            "success": True,
            "message": "Strategy execution started successfully",
            "strategy_id": strategy_id,
            "status": "RUNNING"
        }
        
    except Exception as e:
        logger.error(f"Error starting strategy execution: {str(e)}")
        raise HTTPException(status_code=500, detail=f"Failed to start strategy execution: {str(e)}")


@router.post("/{strategy_id}/stop")
async def stop_strategy_execution(
    strategy_id: int,
    db: Session = Depends(get_db),
    current_user: User = Depends(get_current_user)
):
    """Stop strategy execution on the runner."""
    try:
        # Get the strategy
        strategy = db.query(Strategy).filter(
            Strategy.id == strategy_id,
            Strategy.user_id == current_user.id
        ).first()
        
        if not strategy:
            raise HTTPException(status_code=404, detail="Strategy not found")
        
        # Update strategy status
        strategy.is_live = False
        strategy.execution_status = 'STOPPED'
        db.commit()
        
        # TODO: Send command to strategy runner to stop execution
        # This would involve calling the EC2 strategy runner service
        
        return {
            "success": True,
            "message": "Strategy execution stopped successfully",
            "strategy_id": strategy_id,
            "status": "STOPPED"
        }
        
    except Exception as e:
        logger.error(f"Error stopping strategy execution: {str(e)}")
        raise HTTPException(status_code=500, detail=f"Failed to stop strategy execution: {str(e)}")
