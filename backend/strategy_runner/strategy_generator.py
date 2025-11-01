"""
Strategy Generator Module

This module is responsible for generating executable Python strategy files
from database strategy and broker records. It uses Jinja2 templates to
create self-contained strategy runners.
"""

import json
import logging
import sys
import os
from pathlib import Path
from typing import Any, Dict, Optional

# Add parent directory to path to import models
sys.path.append(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

from models.strategy import Strategy
from models.trading import Broker


class StrategyGenerator:
    """
    Generates executable Python strategy files from database records.
    
    This class takes Strategy and Broker objects and generates a complete
    Python script that can be executed independently to run the strategy.
    """
    
    def __init__(self, output_dir: str = "generated_strategies"):
        """
        Initialize the strategy generator.
        
        Args:
            output_dir: Directory where generated strategy files will be saved
        """
        self.output_dir = Path(output_dir)
        self.output_dir.mkdir(exist_ok=True)
        self.logger = logging.getLogger(__name__)
    
    def generate(self, strategy: Strategy, broker: Broker) -> Path:
        """
        Generate a strategy file from the given strategy and broker data.
        
        Args:
            strategy: Strategy object from database
            broker: Broker object from database
            
        Returns:
            Path to the generated strategy file
            
        Raises:
            ValueError: If required data is missing
        """
        self.logger.info("Generating strategy file for strategy ID %d", strategy.id)
        
        # Validate required data
        self._validate_strategy_data(strategy)
        self._validate_broker_data(broker)
        
        # Prepare template context
        context = self._build_template_context(strategy, broker)
        
        # Generate the strategy file
        strategy_file_path = self.output_dir / f"strategy_{strategy.id}.py"
        
        # Use simple f-string template for now (can be upgraded to Jinja2)
        content = self._render_template(context)
        
        # Write the file
        strategy_file_path.write_text(content, encoding='utf-8')
        
        self.logger.info("Strategy file generated: %s", strategy_file_path)
        return strategy_file_path
    
    def _get_timestamp(self) -> str:
        """Get current timestamp for template generation."""
        from datetime import datetime
        return datetime.now().strftime("%Y-%m-%d %H:%M:%S UTC")
    
    def _validate_strategy_data(self, strategy: Strategy) -> None:
        """
        Validate that the strategy has all required data.
        
        Args:
            strategy: Strategy object to validate
            
        Raises:
            ValueError: If required data is missing
        """
        required_fields = [
            'id', 'product', 'candle_duration', 'indicators',
            'parameters', 'logic_operator',
            'entry_criteria', 'exit_criteria'
        ]
        
        for field in required_fields:
            if not hasattr(strategy, field) or getattr(strategy, field) is None:
                raise ValueError(f"Strategy missing required field: {field}")
        
        # Validate logic operator
        if strategy.logic_operator not in ['AND', 'OR']:
            raise ValueError(f"Invalid logic_operator: {strategy.logic_operator}")
        
        # Validate indicators and parameters
        if not isinstance(strategy.indicators, list) or len(strategy.indicators) == 0:
            raise ValueError("Strategy must have at least one indicator")
        
        if not isinstance(strategy.parameters, dict):
            raise ValueError("Strategy parameters must be a dictionary")
    
    def _validate_broker_data(self, broker: Broker) -> None:
        """
        Validate that the broker has all required data.
        
        Args:
            broker: Broker object to validate
            
        Raises:
            ValueError: If required data is missing
        """
        required_fields = ['id', 'name']
        
        for field in required_fields:
            if not hasattr(broker, field) or getattr(broker, field) is None:
                raise ValueError(f"Broker missing required field: {field}")
        
        # API credentials are optional for template generation
        if not hasattr(broker, 'api_key') or not broker.api_key:
            print("Warning: Broker API key not found, using placeholder")
        if not hasattr(broker, 'api_secret') or not broker.api_secret:
            print("Warning: Broker API secret not found, using placeholder")
    
    def _build_template_context(self, strategy: Strategy, broker: Broker) -> Dict[str, Any]:
        """
        Build the template context from strategy and broker data.
        
        Args:
            strategy: Strategy object
            broker: Broker object
            
        Returns:
            Dictionary containing all template variables
        """
        return {
            'strategy': {
                'id': strategy.id,
                'name': strategy.name,
                'product': strategy.product,
                'platform': getattr(strategy, 'platform', 'testnet'),
                'candle_duration': strategy.candle_duration,
                'indicators': strategy.indicators,
                'parameters': strategy.parameters,
                'trade_conditions': getattr(strategy, 'trade_conditions', {}),
                'logic_operator': strategy.logic_operator,
                'entry_criteria': strategy.entry_criteria,
                'exit_criteria': strategy.exit_criteria,
                'stop_loss': getattr(strategy, 'stop_loss', {}),
                'trailing_stop': getattr(strategy, 'trailing_stop', {})
            },
            'broker': {
                'id': broker.id,
                'name': broker.name,
                'base_url': getattr(broker, 'api_url', 'https://api.delta.exchange'),
                'api_key': getattr(broker, 'api_key', 'YOUR_API_KEY_HERE'),
                'api_secret': getattr(broker, 'api_secret', 'YOUR_API_SECRET_HERE')
            }
        }
    
    def _render_template(self, context: Dict[str, Any]) -> str:
        """
        Render the strategy template with the given context.
        
        Args:
            context: Template context dictionary
            
        Returns:
            Rendered template string
        """
        strategy = context['strategy']
        broker = context['broker']
        
        # Convert complex objects to JSON strings for template
        indicators_json = json.dumps(strategy['indicators'], indent=2)
        parameters_json = json.dumps(strategy['parameters'], indent=2)
        trade_conditions_json = json.dumps(strategy['trade_conditions'], indent=2)
        entry_criteria_json = json.dumps(strategy['entry_criteria'], indent=2)
        exit_criteria_json = json.dumps(strategy['exit_criteria'], indent=2)
        stop_loss_json = json.dumps(strategy['stop_loss'], indent=2)
        trailing_stop_json = json.dumps(strategy['trailing_stop'], indent=2)
        
        template = f'''#!/usr/bin/env python3
# Auto-generated strategy file for strategy ID {strategy['id']}
# Generated on: {self._get_timestamp()}
# DO NOT EDIT MANUALLY - Changes will be overwritten

"""
Strategy Runner: {strategy['name']}
Strategy ID: {strategy['id']}
Product: {strategy['product']}
Platform: {strategy['platform']}
Timeframe: {strategy['candle_duration']}
"""

import asyncio
import logging
import time
from datetime import datetime, timezone
from logging.handlers import RotatingFileHandler
from concurrent.futures import ThreadPoolExecutor
from typing import Dict, List, Optional, Tuple, Any

import pandas as pd
import numpy as np

# Import our modules
try:
    from delta_api import DeltaAPI
    from indicators import apply_indicators
    from conditions import evaluate_entry_exit
    from utils import timeframe_to_seconds, timed, mask_secret, setup_strategy_logger
except ImportError:
    # Fallback imports for when running from different directory
    import sys
    import os
    sys.path.append(os.path.dirname(os.path.abspath(__file__)))
    from delta_api import DeltaAPI
    from indicators import apply_indicators
    from conditions import evaluate_entry_exit
    from utils import timeframe_to_seconds, timed, mask_secret, setup_strategy_logger

# Strategy Configuration
STRATEGY_ID = {strategy['id']}
STRATEGY_NAME = "{strategy['name']}"
SYMBOL = "{strategy['product']}"
TIMEFRAME = "{strategy['candle_duration']}"
PLATFORM = "{strategy['platform']}"

# Broker Configuration
BROKER_URL = "{broker['base_url']}"
API_KEY = "{broker['api_key']}"
API_SECRET = "{broker['api_secret']}"

# Strategy Parameters
CFG = {{
    "indicators": {indicators_json},
    "parameters": {parameters_json},
    "trade_conditions": {trade_conditions_json},
    "logic_operator": "{strategy['logic_operator']}",
    "entry_criteria": {entry_criteria_json},
    "exit_criteria": {exit_criteria_json},
    "stop_loss": {stop_loss_json},
    "trailing_stop": {trailing_stop_json}
}}

# Global state
stale_order_tracker: Dict[str, int] = {{}}  # order_id -> stale_count
position_cache: Optional[Dict[str, Any]] = None
last_update_time: Optional[datetime] = None


class StrategyRunner:
    """Main strategy runner class."""
    
    def __init__(self):
        """Initialize the strategy runner."""
        self.logger = setup_strategy_logger(STRATEGY_ID, STRATEGY_NAME)
        self.api = DeltaAPI(BROKER_URL, API_KEY, API_SECRET)
        self.period_seconds = timeframe_to_seconds(TIMEFRAME)
        self.executor = ThreadPoolExecutor(max_workers=4)
        self.running = True
        
        self.logger.info("Strategy runner initialized")
        self.logger.info("Symbol: %s, Timeframe: %s, Period: %d seconds", 
                        SYMBOL, TIMEFRAME, self.period_seconds)
    
    async def run(self):
        """Main strategy execution loop."""
        self.logger.info("Starting strategy execution loop")
        
        try:
            while self.running:
                iteration_start = time.perf_counter()
                
                try:
                    await self._run_iteration()
                except Exception as e:
                    self.logger.exception("Iteration failed: %s", e)
                
                # Calculate sleep time to maintain period alignment
                iteration_duration = time.perf_counter() - iteration_start
                sleep_time = max(0, self.period_seconds - iteration_duration)
                
                if sleep_time > 0:
                    await asyncio.sleep(sleep_time)
                else:
                    self.logger.warning("Iteration took longer than period: %.3fs", iteration_duration)
        
        except KeyboardInterrupt:
            self.logger.info("Strategy execution interrupted by user")
        finally:
            self.cleanup()
    
    async def _run_iteration(self):
        """Execute one iteration of the strategy."""
        self.logger.debug("Starting iteration at %s", datetime.now(timezone.utc))
        
        # Fetch market data concurrently
        position, open_orders, candles_df = await self._fetch_market_data()
        
        # Compute indicators
        candles_with_indicators = await self._compute_indicators(candles_df)
        
        # Evaluate conditions
        decision = await self._evaluate_conditions(candles_with_indicators)
        
        # Manage positions and orders
        await self._manage_positions_and_orders(position, open_orders, decision, candles_with_indicators)
        
        # Update stop loss and trailing stop
        await self._update_risk_management(position, candles_with_indicators)
    
    async def _fetch_market_data(self) -> Tuple[Optional[Dict], List[Dict], pd.DataFrame]:
        """Fetch position, orders, and candles concurrently."""
        loop = asyncio.get_event_loop()
        
        # Fetch data concurrently
        position_task = loop.run_in_executor(self.executor, self.api.get_position, SYMBOL)
        orders_task = loop.run_in_executor(self.executor, self.api.get_open_orders, SYMBOL)
        candles_task = loop.run_in_executor(self.executor, self.api.get_candles, SYMBOL, TIMEFRAME, 200)
        
        position, open_orders, candles = await asyncio.gather(
            position_task, orders_task, candles_task, return_exceptions=True
        )
        
        # Handle exceptions
        if isinstance(position, Exception):
            self.logger.error("Failed to fetch position: %s", position)
            position = None
        if isinstance(open_orders, Exception):
            self.logger.error("Failed to fetch orders: %s", open_orders)
            open_orders = []
        if isinstance(candles, Exception):
            self.logger.error("Failed to fetch candles: %s", candles)
            raise RuntimeError("Cannot continue without candle data")
        
        return position, open_orders, candles
    
    async def _compute_indicators(self, candles_df: pd.DataFrame) -> pd.DataFrame:
        """Compute indicators for the candles data."""
        loop = asyncio.get_event_loop()
        
        # Run indicator computation in thread pool
        result = await loop.run_in_executor(
            self.executor, 
            apply_indicators, 
            candles_df, 
            CFG["indicators"], 
            CFG["parameters"]
        )
        
        return result
    
    async def _evaluate_conditions(self, candles_df: pd.DataFrame) -> Dict[str, Any]:
        """Evaluate entry/exit conditions."""
        loop = asyncio.get_event_loop()
        
        # Run condition evaluation in thread pool
        result = await loop.run_in_executor(
            self.executor,
            evaluate_entry_exit,
            candles_df,
            CFG["trade_conditions"],
            CFG["logic_operator"],
            CFG["entry_criteria"],
            CFG["exit_criteria"]
        )
        
        return result
    
    async def _manage_positions_and_orders(self, position: Optional[Dict], 
                                         open_orders: List[Dict], 
                                         decision: Dict[str, Any],
                                         candles_df: pd.DataFrame):
        """Manage positions and orders based on decisions."""
        # Handle stale orders
        await self._handle_stale_orders(open_orders)
        
        # Handle position management
        if position and decision.get("exit", False):
            await self._exit_position(position, candles_df)
        elif not position and decision.get("entry", False):
            await self._enter_position(candles_df)
    
    async def _handle_stale_orders(self, open_orders: List[Dict]):
        """Cancel orders that have been pending for too long."""
        for order in open_orders:
            order_id = order.get("id")
            if not order_id:
                continue
            
            # Increment stale count
            stale_order_tracker[order_id] = stale_order_tracker.get(order_id, 0) + 1
            
            # Cancel if stale for 3 iterations
            if stale_order_tracker[order_id] >= 3:
                try:
                    await asyncio.get_event_loop().run_in_executor(
                        self.executor, self.api.cancel_order, order_id
                    )
                    self.logger.info("Cancelled stale order: %s", order_id)
                    del stale_order_tracker[order_id]
                except Exception as e:
                    self.logger.error("Failed to cancel stale order %s: %s", order_id, e)
    
    async def _exit_position(self, position: Dict, candles_df: pd.DataFrame):
        """Exit the current position."""
        try:
            # Place exit order (market order for immediate execution)
            exit_order = await asyncio.get_event_loop().run_in_executor(
                self.executor,
                self.api.place_order,
                SYMBOL,
                "SELL" if position.get("side") == "BUY" else "BUY",
                "MARKET",
                position.get("size", 0),
                None  # No limit price for market orders
            )
            
            self.logger.info("Exited position: %s", exit_order)
            
        except Exception as e:
            self.logger.error("Failed to exit position: %s", e)
    
    async def _enter_position(self, candles_df: pd.DataFrame):
        """Enter a new position."""
        try:
            # Get current price for limit order
            current_price = candles_df.iloc[-1]["close"]
            
            # Place entry order (limit order slightly above current price for buy)
            entry_order = await asyncio.get_event_loop().run_in_executor(
                self.executor,
                self.api.place_order,
                SYMBOL,
                "BUY",  # Assuming long strategy
                "LIMIT",
                1.0,  # Default size
                current_price * 1.001  # Slightly above current price
            )
            
            self.logger.info("Entered position: %s", entry_order)
            
            # Reset stale tracker for new order
            stale_order_tracker[entry_order.get("id")] = 0
            
        except Exception as e:
            self.logger.error("Failed to enter position: %s", e)
    
    async def _update_risk_management(self, position: Optional[Dict], candles_df: pd.DataFrame):
        """Update stop loss and trailing stop."""
        if not position:
            return
        
        try:
            # Update stop loss
            if CFG["stop_loss"]:
                await self._update_stop_loss(position, candles_df)
            
            # Update trailing stop
            if CFG["trailing_stop"].get("enabled", False):
                await self._update_trailing_stop(position, candles_df)
                
        except Exception as e:
            self.logger.error("Failed to update risk management: %s", e)
    
    async def _update_stop_loss(self, position: Dict, candles_df: pd.DataFrame):
        """Update stop loss based on configuration."""
        stop_loss_cfg = CFG["stop_loss"]
        stop_loss_type = stop_loss_cfg.get("type")
        
        if stop_loss_type == "fixed":
            # Fixed stop loss - calculate absolute price
            entry_price = position.get("entry_price", 0)
            distance = stop_loss_cfg.get("distance", 0.02)  # Default 2%
            
            if entry_price > 0:
                stop_loss_price = entry_price * (1 - distance)
                self.logger.debug("Fixed stop loss: %s at %s", distance, stop_loss_price)
        
        elif stop_loss_type == "supertrend":
            # Supertrend-based stop loss
            distance = stop_loss_cfg.get("distance", "current")
            
            if distance == "current" and "st_lower" in candles_df.columns:
                stop_loss_price = candles_df.iloc[-1]["st_lower"]
                self.logger.debug("Supertrend stop loss: %s", stop_loss_price)
    
    async def _update_trailing_stop(self, position: Dict, candles_df: pd.DataFrame):
        """Update trailing stop based on configuration."""
        trailing_cfg = CFG["trailing_stop"]
        trailing_type = trailing_cfg.get("type", "price")
        distance = trailing_cfg.get("distance", 0.02)
        
        if trailing_type == "price":
            # Price-based trailing stop
            current_price = candles_df.iloc[-1]["close"]
            entry_price = position.get("entry_price", 0)
            
            if entry_price > 0:
                # Calculate new stop loss (never move away from reducing risk)
                new_stop_loss = current_price * (1 - distance)
                current_stop_loss = position.get("stop_loss_price", 0)
                
                if new_stop_loss > current_stop_loss:
                    # Update stop loss
                    self.logger.debug("Updated trailing stop: %s -> %s", 
                                    current_stop_loss, new_stop_loss)
    
    def cleanup(self):
        """Clean up resources."""
        self.logger.info("Cleaning up strategy runner")
        self.running = False
        self.executor.shutdown(wait=True)
        self.logger.info("Strategy runner cleanup complete")


async def main():
    """Main entry point for the generated strategy."""
    runner = StrategyRunner()
    await runner.run()


if __name__ == "__main__":
    asyncio.run(main())
'''
        
        return template
    
    def _get_timestamp(self) -> str:
        """Get current timestamp for template generation."""
        from datetime import datetime
        return datetime.now().isoformat()

