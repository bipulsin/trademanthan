"""
Strategy Runner Module

This module is responsible for executing generated strategy files.
It provides a clean interface to run strategies with proper error handling
and monitoring.
"""

import asyncio
import importlib.util
import logging
import sys
from pathlib import Path
from typing import Optional


class StrategyRunner:
    """
    Executes generated strategy files.
    
    This class loads and runs generated strategy files, providing
    proper error handling and monitoring capabilities.
    """
    
    def __init__(self, strategy_file_path: Path, strategy_id: int, dry_run: bool = False):
        """
        Initialize the strategy runner.
        
        Args:
            strategy_file_path: Path to the generated strategy file
            strategy_id: ID of the strategy being run
            dry_run: Whether to run in dry-run mode
        """
        self.strategy_file_path = Path(strategy_file_path)
        self.strategy_id = strategy_id
        self.dry_run = dry_run
        self.logger = logging.getLogger(__name__)
        
        if not self.strategy_file_path.exists():
            raise FileNotFoundError(f"Strategy file not found: {self.strategy_file_path}")
    
    async def run(self):
        """Run the strategy."""
        self.logger.info("Starting strategy execution for ID %d", self.strategy_id)
        
        try:
            # Load and execute the strategy module
            strategy_module = self._load_strategy_module()
            
            if self.dry_run:
                self.logger.info("Running in DRY-RUN mode - no actual orders will be placed")
                # Set dry-run flag in the module if it exists
                if hasattr(strategy_module, 'DRY_RUN'):
                    strategy_module.DRY_RUN = True
            
            # Run the strategy
            if hasattr(strategy_module, 'main'):
                await strategy_module.main()
            else:
                self.logger.error("Strategy module missing 'main' function")
                raise RuntimeError("Invalid strategy module: missing 'main' function")
                
        except Exception as e:
            self.logger.exception("Strategy execution failed: %s", e)
            raise
    
    def _load_strategy_module(self):
        """
        Load the strategy module from the generated file.
        
        Returns:
            Loaded module object
            
        Raises:
            ImportError: If the module cannot be loaded
        """
        try:
            # Load the module from file
            spec = importlib.util.spec_from_file_location(
                f"strategy_{self.strategy_id}", 
                self.strategy_file_path
            )
            
            if spec is None or spec.loader is None:
                raise ImportError(f"Could not create spec for {self.strategy_file_path}")
            
            module = importlib.util.module_from_spec(spec)
            spec.loader.exec_module(module)
            
            self.logger.info("Successfully loaded strategy module")
            return module
            
        except Exception as e:
            self.logger.error("Failed to load strategy module: %s", e)
            raise ImportError(f"Could not load strategy module: {e}")
    
    def stop(self):
        """Stop the strategy execution."""
        self.logger.info("Stopping strategy execution")
        # This would be implemented based on the specific strategy implementation
        # For now, we'll just log the request

