#!/usr/bin/env python3
"""
Strategy Runner + Generator - Main CLI Entry Point

This module provides the command-line interface to generate and run trading strategies.
It loads strategy and broker data from the database and generates executable strategy files.

Usage:
    python main.py --strategy-id <ID>
    python main.py --strategy-id <ID> --dry-run
    python main.py --strategy-id <ID> --generate-only
"""

import argparse
import asyncio
import logging
import sys
from pathlib import Path
from typing import Optional

from strategy_generator import StrategyGenerator
from strategy_runner import StrategyRunner
from database import get_db
from models.strategy import Strategy
from models.broker import Broker


def setup_logging() -> logging.Logger:
    """Set up basic logging for the main CLI."""
    logging.basicConfig(
        level=logging.INFO,
        format='%(asctime)s | %(levelname)s | %(name)s | %(message)s'
    )
    return logging.getLogger(__name__)


def load_strategy_and_broker(strategy_id: int) -> tuple[Strategy, Broker]:
    """
    Load strategy and broker data from database.
    
    Args:
        strategy_id: The ID of the strategy to load
        
    Returns:
        Tuple of (Strategy, Broker) objects
        
    Raises:
        ValueError: If strategy or broker not found
    """
    db = next(get_db())
    try:
        # Load strategy
        strategy = db.query(Strategy).filter(Strategy.id == strategy_id).first()
        if not strategy:
            raise ValueError(f"Strategy with ID {strategy_id} not found")
        
        # Load broker
        broker = db.query(Broker).filter(Broker.id == strategy.broker_id).first()
        if not broker:
            raise ValueError(f"Broker with ID {strategy.broker_id} not found")
        
        return strategy, broker
        
    finally:
        db.close()


async def main():
    """Main CLI function."""
    parser = argparse.ArgumentParser(
        description="Strategy Runner + Generator",
        formatter_class=argparse.RawDescriptionHelpFormatter,
        epilog="""
Examples:
  python main.py --strategy-id 123          # Generate and run strategy 123
  python main.py --strategy-id 123 --dry-run # Generate and run in dry-run mode
  python main.py --strategy-id 123 --generate-only # Only generate, don't run
        """
    )
    
    parser.add_argument(
        "--strategy-id",
        type=int,
        required=True,
        help="ID of the strategy to generate/run"
    )
    
    parser.add_argument(
        "--dry-run",
        action="store_true",
        help="Run in dry-run mode (no actual orders placed)"
    )
    
    parser.add_argument(
        "--generate-only",
        action="store_true",
        help="Only generate the strategy file, don't run it"
    )
    
    parser.add_argument(
        "--log-level",
        choices=["DEBUG", "INFO", "WARNING", "ERROR"],
        default="INFO",
        help="Logging level (default: INFO)"
    )
    
    args = parser.parse_args()
    
    # Set log level
    logging.getLogger().setLevel(getattr(logging, args.log_level))
    logger = setup_logging()
    
    try:
        logger.info("Loading strategy ID %d and broker data...", args.strategy_id)
        strategy, broker = load_strategy_and_broker(args.strategy_id)
        
        logger.info("Strategy: %s (ID: %d)", strategy.name, strategy.id)
        logger.info("Broker: %s (ID: %d)", broker.name, broker.id)
        
        # Initialize generator
        generator = StrategyGenerator()
        
        # Generate/update strategy file
        strategy_file_path = generator.generate(strategy, broker)
        logger.info("Strategy file generated/updated: %s", strategy_file_path)
        
        if args.generate_only:
            logger.info("Generation complete. Exiting.")
            return
        
        # Initialize and run strategy
        runner = StrategyRunner(
            strategy_file_path=strategy_file_path,
            strategy_id=args.strategy_id,
            dry_run=args.dry_run
        )
        
        if args.dry_run:
            logger.info("Starting strategy in DRY-RUN mode")
        else:
            logger.info("Starting strategy execution")
        
        await runner.run()
        
    except ValueError as e:
        logger.error("Validation error: %s", e)
        sys.exit(1)
    except Exception as e:
        logger.exception("Unexpected error: %s", e)
        sys.exit(1)


if __name__ == "__main__":
    asyncio.run(main())

