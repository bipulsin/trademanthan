"""
Utils Module

This module provides utility functions for the strategy runner including:
- Timeframe conversion utilities
- Timing and performance measurement
- Logging setup
- Retry and backoff mechanisms
- Security utilities
"""

import logging
import time
import random
from datetime import datetime, timezone
from logging.handlers import RotatingFileHandler
from pathlib import Path
from typing import Any, Callable, Optional, TypeVar, Union, Dict
from functools import wraps

T = TypeVar('T')


def timeframe_to_seconds(timeframe: str) -> int:
    """
    Convert timeframe string to seconds.
    
    Args:
        timeframe: Timeframe string (e.g., "1m", "5m", "15m", "1h", "4h", "1d")
        
    Returns:
        Timeframe in seconds
        
    Raises:
        ValueError: If timeframe format is invalid
    """
    timeframe = timeframe.lower().strip()
    
    # Parse timeframe
    if timeframe.endswith('m'):
        minutes = int(timeframe[:-1])
        return minutes * 60
    elif timeframe.endswith('h'):
        hours = int(timeframe[:-1])
        return hours * 3600
    elif timeframe.endswith('d'):
        days = int(timeframe[:-1])
        return days * 86400
    elif timeframe.endswith('w'):
        weeks = int(timeframe[:-1])
        return weeks * 604800
    else:
        raise ValueError(f"Invalid timeframe format: {timeframe}")


def seconds_to_timeframe(seconds: int) -> str:
    """
    Convert seconds to timeframe string.
    
    Args:
        seconds: Time in seconds
        
    Returns:
        Timeframe string
    """
    if seconds < 60:
        return f"{seconds}s"
    elif seconds < 3600:
        minutes = seconds // 60
        return f"{minutes}m"
    elif seconds < 86400:
        hours = seconds // 3600
        return f"{hours}h"
    elif seconds < 604800:
        days = seconds // 86400
        return f"{days}d"
    else:
        weeks = seconds // 604800
        return f"{weeks}w"


def timed(func: Callable[..., T]) -> Callable[..., T]:
    """
    Decorator to measure execution time of a function.
    
    Args:
        func: Function to time
        
    Returns:
        Wrapped function with timing
    """
    @wraps(func)
    def wrapper(*args, **kwargs):
        start_time = time.perf_counter()
        result = func(*args, **kwargs)
        end_time = time.perf_counter()
        
        logger = logging.getLogger(func.__module__)
        logger.debug(f"{func.__name__} executed in {end_time - start_time:.4f} seconds")
        
        return result
    return wrapper


def setup_strategy_logger(strategy_id: int, strategy_name: str, 
                         log_level: str = "INFO") -> logging.Logger:
    """
    Set up a logger for a specific strategy with rotating file handler.
    
    Args:
        strategy_id: ID of the strategy
        strategy_name: Name of the strategy
        log_level: Logging level
        
    Returns:
        Configured logger instance
    """
    # Create logs directory if it doesn't exist
    logs_dir = Path("logs")
    logs_dir.mkdir(exist_ok=True)
    
    # Create logger
    logger = logging.getLogger(f"strategy_{strategy_id}")
    logger.setLevel(getattr(logging, log_level.upper()))
    
    # Avoid duplicate handlers
    if logger.handlers:
        return logger
    
    # Create rotating file handler
    log_file = logs_dir / f"strategy_{strategy_id}.log"
    file_handler = RotatingFileHandler(
        log_file,
        maxBytes=5_000_000,  # 5MB
        backupCount=5
    )
    
    # Create formatter
    formatter = logging.Formatter(
        "%(asctime)s | %(levelname)s | strategy=%(name)s | %(message)s",
        datefmt="%Y-%m-%d %H:%M:%S"
    )
    file_handler.setFormatter(formatter)
    
    # Add handler to logger
    logger.addHandler(file_handler)
    
    # Also add console handler for development
    console_handler = logging.StreamHandler()
    console_handler.setFormatter(formatter)
    logger.addHandler(console_handler)
    
    logger.info(f"Logger initialized for strategy: {strategy_name} (ID: {strategy_id})")
    
    return logger


def mask_secret(secret: str, visible_chars: int = 4) -> str:
    """
    Mask a secret string for logging purposes.
    
    Args:
        secret: Secret string to mask
        visible_chars: Number of characters to show at the end
        
    Returns:
        Masked secret string
    """
    if not secret or len(secret) <= visible_chars:
        return "*" * len(secret) if secret else ""
    
    return "*" * (len(secret) - visible_chars) + secret[-visible_chars:]


def retry_with_backoff(max_retries: int = 3, 
                      base_delay: float = 1.0,
                      max_delay: float = 60.0,
                      exponential_base: float = 2.0,
                      jitter: bool = True) -> Callable:
    """
    Decorator for retrying functions with exponential backoff.
    
    Args:
        max_retries: Maximum number of retries
        base_delay: Base delay between retries in seconds
        max_delay: Maximum delay between retries in seconds
        exponential_base: Base for exponential backoff
        jitter: Whether to add random jitter to delays
        
    Returns:
        Decorated function with retry logic
    """
    def decorator(func: Callable[..., T]) -> Callable[..., T]:
        @wraps(func)
        def wrapper(*args, **kwargs) -> T:
            last_exception = None
            
            for attempt in range(max_retries + 1):
                try:
                    return func(*args, **kwargs)
                except Exception as e:
                    last_exception = e
                    
                    if attempt == max_retries:
                        # Last attempt failed, re-raise the exception
                        raise last_exception
                    
                    # Calculate delay with exponential backoff
                    delay = min(base_delay * (exponential_base ** attempt), max_delay)
                    
                    # Add jitter if enabled
                    if jitter:
                        delay *= (0.5 + random.random() * 0.5)
                    
                    # Log retry attempt
                    logger = logging.getLogger(func.__module__)
                    logger.warning(
                        f"Attempt {attempt + 1} failed for {func.__name__}: {e}. "
                        f"Retrying in {delay:.2f} seconds..."
                    )
                    
                    time.sleep(delay)
            
            # This should never be reached, but just in case
            raise last_exception
        
        return wrapper
    return decorator


def circuit_breaker(failure_threshold: int = 5,
                   recovery_timeout: float = 60.0,
                   expected_exception: type = Exception) -> Callable:
    """
    Circuit breaker decorator to prevent cascading failures.
    
    Args:
        failure_threshold: Number of failures before opening circuit
        recovery_timeout: Time to wait before attempting recovery
        expected_exception: Exception type to catch
        
    Returns:
        Decorated function with circuit breaker logic
    """
    def decorator(func: Callable[..., T]) -> Callable[..., T]:
        # Circuit breaker state
        failure_count = 0
        last_failure_time = 0
        circuit_open = False
        
        @wraps(func)
        def wrapper(*args, **kwargs) -> T:
            nonlocal failure_count, last_failure_time, circuit_open
            
            current_time = time.time()
            
            # Check if circuit is open
            if circuit_open:
                if current_time - last_failure_time < recovery_timeout:
                    raise RuntimeError("Circuit breaker is open")
                else:
                    # Try to close circuit
                    circuit_open = False
                    failure_count = 0
            
            try:
                result = func(*args, **kwargs)
                # Success - reset failure count
                failure_count = 0
                return result
                
            except expected_exception as e:
                failure_count += 1
                last_failure_time = current_time
                
                # Open circuit if threshold reached
                if failure_count >= failure_threshold:
                    circuit_open = True
                    logger = logging.getLogger(func.__module__)
                    logger.error(
                        f"Circuit breaker opened for {func.__name__} "
                        f"after {failure_count} failures"
                    )
                
                raise e
        
        return wrapper
    return decorator


def validate_numeric(value: Any, min_value: Optional[float] = None, 
                    max_value: Optional[float] = None) -> float:
    """
    Validate and convert a value to a numeric type.
    
    Args:
        value: Value to validate
        min_value: Minimum allowed value
        max_value: Maximum allowed value
        
    Returns:
        Validated numeric value
        
    Raises:
        ValueError: If value is invalid or out of range
    """
    try:
        numeric_value = float(value)
    except (TypeError, ValueError):
        raise ValueError(f"Value must be numeric: {value}")
    
    if min_value is not None and numeric_value < min_value:
        raise ValueError(f"Value {numeric_value} is below minimum {min_value}")
    
    if max_value is not None and numeric_value > max_value:
        raise ValueError(f"Value {numeric_value} is above maximum {max_value}")
    
    return numeric_value


def safe_divide(numerator: float, denominator: float, default: float = 0.0) -> float:
    """
    Safely divide two numbers, returning a default value if denominator is zero.
    
    Args:
        numerator: Numerator value
        denominator: Denominator value
        default: Default value to return if division by zero
        
    Returns:
        Division result or default value
    """
    try:
        if denominator == 0:
            return default
        return numerator / denominator
    except (TypeError, ValueError):
        return default


def format_timestamp(timestamp: Union[float, datetime, None] = None) -> str:
    """
    Format timestamp for logging and display.
    
    Args:
        timestamp: Timestamp to format (float for Unix timestamp, datetime object, or None for current time)
        
    Returns:
        Formatted timestamp string
    """
    if timestamp is None:
        dt = datetime.now(timezone.utc)
    elif isinstance(timestamp, (int, float)):
        dt = datetime.fromtimestamp(timestamp, timezone.utc)
    elif isinstance(timestamp, datetime):
        dt = timestamp
        if dt.tzinfo is None:
            dt = dt.replace(tzinfo=timezone.utc)
    else:
        dt = datetime.now(timezone.utc)
    
    return dt.strftime("%Y-%m-%d %H:%M:%S UTC")


def calculate_percentage_change(old_value: float, new_value: float) -> float:
    """
    Calculate percentage change between two values.
    
    Args:
        old_value: Old value
        new_value: New value
        
    Returns:
        Percentage change (positive for increase, negative for decrease)
    """
    if old_value == 0:
        return 0.0 if new_value == 0 else float('inf') if new_value > 0 else float('-inf')
    
    return ((new_value - old_value) / old_value) * 100


def is_market_open(exchange_timezone: str = "UTC") -> bool:
    """
    Check if the market is currently open.
    
    Args:
        exchange_timezone: Timezone of the exchange
        
    Returns:
        True if market is open, False otherwise
    """
    try:
        # This is a simplified implementation
        # In a real system, you would check exchange-specific trading hours
        from zoneinfo import ZoneInfo
        
        tz = ZoneInfo(exchange_timezone)
        now = datetime.now(tz)
        
        # Simple check: assume market is closed on weekends
        if now.weekday() >= 5:  # Saturday = 5, Sunday = 6
            return False
        
        # Assume market is open during business hours (9 AM - 5 PM)
        hour = now.hour
        if 9 <= hour < 17:
            return True
        
        return False
        
    except Exception:
        # If timezone handling fails, assume market is open
        return True


def log_performance_metrics(func_name: str, start_time: float, 
                           additional_metrics: Optional[Dict[str, Any]] = None):
    """
    Log performance metrics for a function execution.
    
    Args:
        func_name: Name of the function
        start_time: Start time from time.perf_counter()
        additional_metrics: Additional metrics to log
    """
    end_time = time.perf_counter()
    execution_time = end_time - start_time
    
    logger = logging.getLogger(__name__)
    
    metrics = {
        "function": func_name,
        "execution_time_ms": execution_time * 1000,
        "execution_time_s": execution_time
    }
    
    if additional_metrics:
        metrics.update(additional_metrics)
    
    logger.info(f"Performance metrics: {metrics}")

