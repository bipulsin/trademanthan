"""
Algorithmic Trading API Routes
Handles strategy management and execution
"""

import asyncio
import logging
import sys
import os
from typing import Dict, Any, Optional
from fastapi import APIRouter, Depends, HTTPException, BackgroundTasks
from sqlalchemy.orm import Session
from pydantic import BaseModel

# Add algos directory to path
algos_path = '/home/ubuntu/trademanthan/algos'
sys.path.append(algos_path)

from backend.database import get_db
from backend.models import Broker
from backend.routers.auth import get_current_user
from backend.models.user import User
from utils.log_manager import log_manager

# Import strategy classes
try:
    from strategy.supertrend_options_strategy import SuperTrendOptionsStrategy
except ImportError as e:
    logging.error(f"Failed to import strategy: {e}")
    logging.error(f"Python path: {sys.path}")
    logging.error(f"Algos path: {algos_path}")
    SuperTrendOptionsStrategy = None

router = APIRouter(tags=["algorithmic-trading"])

# Global strategy instances
active_strategies: Dict[str, Any] = {}

class StrategyStartRequest(BaseModel):
    strategy: str
    broker_id: int
    paper_trading: bool = False

class StrategyStopRequest(BaseModel):
    strategy: str

class StrategyStatusResponse(BaseModel):
    running: bool
    strategy: str
    current_position: Optional[Dict[str, Any]] = None
    last_signal: Optional[str] = None
    paper_trading: bool = False
    error: Optional[str] = None

@router.post("/start")
async def start_strategy(
    request: StrategyStartRequest,
    background_tasks: BackgroundTasks,
    db: Session = Depends(get_db),
    current_user: User = Depends(get_current_user)
):
    """Start an algorithmic trading strategy"""
    try:
        # Check if strategy is already running
        strategy_key = f"{request.strategy}_{current_user['id']}"
        if strategy_key in active_strategies:
            raise HTTPException(status_code=400, detail="Strategy is already running")
        
        # Hardcoded Delta Exchange India API credentials for SuperTrend strategy
        DELTA_EXCHANGE_API_URL = 'https://api.india.delta.exchange'
        DELTA_EXCHANGE_API_KEY = 'Fp0bn5wr4qZ1A1AHz17NdVf8Pxp8Ct'
        DELTA_EXCHANGE_API_SECRET = 'SAsjx9iewya4yvLO5e3L7uKwVNOBQ7ernVhkMOU6BUaErxWNFLmE8m8ZLIiq'
        
        # Log hardcoded API selection for web interface
        logging.info(f"🌐 WEB INTERFACE - HARDCODED API SELECTION:")
        logging.info(f"   • User ID: {current_user['id']}")
        logging.info(f"   • API Provider: Delta Exchange India")
        logging.info(f"   • API URL: {DELTA_EXCHANGE_API_URL}")
        logging.info(f"   • API Key: {DELTA_EXCHANGE_API_KEY[:8]}...{DELTA_EXCHANGE_API_KEY[-4:]}")
        logging.info(f"   • Strategy: {request.strategy}")
        logging.info(f"   • Paper Trading: {request.paper_trading}")
        
        # Initialize strategy based on type
        if request.strategy == "supertrend_options":
            if not SuperTrendOptionsStrategy:
                raise HTTPException(status_code=500, detail="Strategy not available")
            
            # Create strategy instance
            logging.info(f"🚀 Initializing {request.strategy} strategy for user {current_user['id']}")
            config_path = os.path.join(algos_path, "config", "config.yaml")
            
            # Read paper trading setting from config file instead of request
            import yaml
            with open(config_path, 'r') as f:
                config = yaml.safe_load(f)
            paper_trading_from_config = config.get('paper_trading', {}).get('enabled', False)
            
            strategy = SuperTrendOptionsStrategy(
                config_path=config_path,
                api_key=DELTA_EXCHANGE_API_KEY,
                api_secret=DELTA_EXCHANGE_API_SECRET,
                api_url=DELTA_EXCHANGE_API_URL,
                paper_trading=paper_trading_from_config,
                strategy_id=strategy_key
            )
            
            # Add strategy to active strategies first
            active_strategies[strategy_key] = {
                'strategy': strategy,
                'running': False,  # Will be set to True when background task starts
                'paper_trading': paper_trading_from_config,
                'broker_id': 6,  # Hardcoded Delta Exchange broker ID
                'broker_name': 'Delta Exchange India',
                'started_at': asyncio.get_event_loop().time()
            }
            
            # Start strategy in background
            logging.info(f"🔄 Starting strategy background task for {strategy_key}")
            background_tasks.add_task(run_strategy_background, strategy_key, strategy)
            
            logging.info(f"✅ Successfully started {request.strategy} strategy for user {current_user['id']}")
            return {
                "message": "Strategy started successfully", 
                "strategy": request.strategy,
                "broker": "Delta Exchange India",
                "paper_trading": paper_trading_from_config
            }
        
        else:
            raise HTTPException(status_code=400, detail="Unknown strategy type")
    
    except HTTPException:
        raise
    except Exception as e:
        logging.error(f"Error starting strategy: {e}")
        raise HTTPException(status_code=500, detail=f"Failed to start strategy: {str(e)}")

@router.post("/stop")
async def stop_strategy(
    request: StrategyStopRequest,
    current_user: User = Depends(get_current_user)
):
    """Stop an algorithmic trading strategy"""
    try:
        strategy_key = f"{request.strategy}_{current_user['id']}"
        
        if strategy_key not in active_strategies:
            raise HTTPException(status_code=404, detail="Strategy not found or not running")
        
        # Stop the strategy
        strategy_data = active_strategies[strategy_key]
        strategy = strategy_data['strategy']
        
        # Only call stop_strategy if the strategy object exists
        if strategy is not None:
            strategy.stop_strategy()
        
        # Backup and clear logs for this strategy
        if hasattr(strategy, 'strategy_id') and log_manager:
            backup_result = log_manager.backup_and_clear_logs(strategy_id=strategy.strategy_id)
            if backup_result['success']:
                logging.info(f"📁 Logs backed up and cleared for strategy {strategy.strategy_id}: {backup_result['message']}")
            else:
                logging.warning(f"⚠️ Log backup failed for strategy {strategy.strategy_id}: {backup_result['message']}")
                # Fallback to simple clear if backup fails
                log_manager.clear_logs(strategy_id=strategy.strategy_id)
                logging.info(f"Cleared logs for strategy {strategy.strategy_id}")
            
            # Force clear any remaining logs to ensure clean restart
            log_manager.clear_logs(strategy_id=strategy.strategy_id)
        
        # Remove from active strategies
        del active_strategies[strategy_key]
        
        logging.info(f"Stopped {request.strategy} strategy for user {current_user['id']}")
        return {"message": "Strategy stopped successfully", "strategy": request.strategy}
    
    except HTTPException:
        raise
    except Exception as e:
        logging.error(f"Error stopping strategy: {e}")
        raise HTTPException(status_code=500, detail=f"Failed to stop strategy: {str(e)}")

@router.get("/status")
async def get_strategy_status(
    current_user: User = Depends(get_current_user)
):
    """Get status of all strategies for the current user"""
    try:
        user_strategies = {}
        
        for strategy_key, strategy_data in active_strategies.items():
            if strategy_key.endswith(f"_{current_user['id']}"):
                strategy = strategy_data['strategy']
                status = strategy.get_status()
                
                user_strategies[strategy_key] = {
                    'running': strategy_data['running'],
                    'strategy': strategy_key.split('_')[0],
                    'current_position': status.get('current_position'),
                    'last_signal': status.get('last_signal'),
                    'paper_trading': strategy_data['paper_trading']
                }
        
        # For backward compatibility, also return a single status object
        # Find the first running strategy or return default status
        running_strategy = None
        for strategy_key, strategy_data in active_strategies.items():
            if strategy_key.endswith(f"_{current_user['id']}") and strategy_data['running']:
                running_strategy = strategy_data
                break
        
        if running_strategy:
            strategy = running_strategy['strategy']
            status = strategy.get_status()
            
            # Get recent logs for this strategy
            strategy_id = getattr(strategy, 'strategy_id', None)
            recent_logs = []
            if strategy_id:
                recent_logs = log_manager.get_logs(strategy_id=strategy_id, limit=50)
                # Format logs for frontend
                recent_logs = [f"[{log['timestamp']}] {log['level']}: {log['message']}" for log in recent_logs]
            
            return {
                'running': True,
                'position': status.get('current_position'),
                'last_signal': status.get('last_signal'),
                'paper_trading': running_strategy['paper_trading'],
                'logs': recent_logs
            }
        else:
            return {
                'running': False,
                'position': None,
                'last_signal': 'N/A',
                'paper_trading': False,
                'logs': []
            }
    
    except Exception as e:
        logging.error(f"Error getting strategy status: {e}")
        raise HTTPException(status_code=500, detail=f"Failed to get strategy status: {str(e)}")

@router.get("/logs")
async def get_strategy_logs(
    strategy_id: str = None,
    since_id: str = None,
    limit: int = 100,
    current_user: User = Depends(get_current_user)
):
    """Get logs for a specific strategy"""
    try:
        # If no strategy_id provided, get the first running strategy for this user
        if not strategy_id:
            for strategy_key, strategy_data in active_strategies.items():
                if strategy_key.endswith(f"_{current_user['id']}") and strategy_data['running']:
                    strategy = strategy_data['strategy']
                    strategy_id = getattr(strategy, 'strategy_id', None)
                    break
        
        if not strategy_id:
            return {"logs": [], "latest_id": "0_0"}
        
        # Get logs from log manager
        logs = log_manager.get_logs(strategy_id=strategy_id, since_id=since_id, limit=limit)
        
        # Format logs for frontend
        formatted_logs = []
        for log in logs:
            formatted_logs.append({
                'id': log['id'],
                'timestamp': log['timestamp'],
                'level': log['level'],
                'message': log['message']
            })
        
        latest_id = log_manager.get_latest_log_id()
        
        return {
            "logs": formatted_logs,
            "latest_id": latest_id,
            "strategy_id": strategy_id
        }
    
    except Exception as e:
        logging.error(f"Error getting strategy logs: {e}")
        raise HTTPException(status_code=500, detail=f"Failed to get strategy logs: {str(e)}")

@router.get("/strategies")
async def list_available_strategies():
    """List all available algorithmic trading strategies"""
    strategies = [
        {
            "name": "supertrend_options",
            "display_name": "SuperTrend Bitcoin Options Strategy",
            "description": "Uses SuperTrend indicator on BTCUSD.P futures to trade Bitcoin options",
            "parameters": {
                "supertrend_length": 16,
                "supertrend_factor": 1.5,
                "timeframe": "90m",
                "expiry_preference": "0DTE/1DTE",
                "premium_threshold": "$250-$300"
            },
            "risk_level": "High",
            "paper_trading": True
        }
    ]
    
    return {"strategies": strategies}

@router.get("/test")
async def test_strategy_availability():
    """Test if strategy is properly configured for web execution"""
    try:
        if not SuperTrendOptionsStrategy:
            return {
                "available": False,
                "error": "Strategy class not available",
                "message": "SuperTrend strategy is not properly imported"
            }
        
        # Test strategy initialization with mock credentials
        config_path = os.path.join(algos_path, "config", "config.yaml")
        test_strategy = SuperTrendOptionsStrategy(
            config_path=config_path,
            api_key="test_key",
            api_secret="test_secret", 
            api_url="https://api.india.delta.exchange",
            paper_trading=True
        )
        
        return {
            "available": True,
            "message": "Strategy is properly configured for web execution",
            "config_loaded": test_strategy.config is not None,
            "api_initialized": test_strategy.api is not None
        }
        
    except Exception as e:
        logging.error(f"Strategy test failed: {e}")
        return {
            "available": False,
            "error": str(e),
            "message": "Strategy test failed"
        }

async def run_strategy_background(strategy_key: str, strategy: SuperTrendOptionsStrategy):
    """Run strategy in background task"""
    try:
        logging.info(f"🌐 WEB INTERFACE - Starting background strategy execution for {strategy_key}")
        # Update the running status in active_strategies
        if strategy_key in active_strategies:
            active_strategies[strategy_key]['running'] = True
        await strategy.run_strategy()
        logging.info(f"🌐 WEB INTERFACE - Strategy {strategy_key} completed successfully")
    except Exception as e:
        logging.error(f"❌ WEB INTERFACE - Strategy {strategy_key} error: {e}")
        logging.error(f"🔍 Full traceback: {str(e)}")
        # Remove from active strategies on error
        if strategy_key in active_strategies:
            active_strategies[strategy_key]['running'] = False
            active_strategies[strategy_key]['error'] = str(e)
            logging.info(f"🔄 Marked strategy {strategy_key} as stopped due to error")
    finally:
        # Ensure strategy is marked as stopped when it completes
        if strategy_key in active_strategies:
            active_strategies[strategy_key]['running'] = False
            logging.info(f"🔄 Strategy {strategy_key} execution completed")
