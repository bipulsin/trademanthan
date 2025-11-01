"""
Strategy Runner + Generator Package

A comprehensive Python solution for generating and executing automated trading strategies.
"""

__version__ = "1.0.0"
__author__ = "Trade Manthan Team"
__description__ = "Strategy Runner + Generator for automated trading"

from .strategy_generator import StrategyGenerator
from .strategy_runner import StrategyRunner
from .indicators import apply_indicators, get_indicator_columns
from .conditions import evaluate_entry_exit
from .utils import (
    timeframe_to_seconds, 
    setup_strategy_logger, 
    mask_secret,
    retry_with_backoff,
    circuit_breaker
)

__all__ = [
    "StrategyGenerator",
    "StrategyRunner", 
    "apply_indicators",
    "get_indicator_columns",
    "evaluate_entry_exit",
    "timeframe_to_seconds",
    "setup_strategy_logger",
    "mask_secret",
    "retry_with_backoff",
    "circuit_breaker"
]

