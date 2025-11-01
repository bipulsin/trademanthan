"""
Conditions Module

This module provides functions to evaluate trading conditions, entry criteria,
and exit criteria based on indicator values and logic operators.
"""

import logging
from typing import Dict, List, Any, Optional, Tuple
import pandas as pd
import numpy as np


def evaluate_entry_exit(df: pd.DataFrame, 
                       trade_conditions: Dict[str, Any], 
                       logic_operator: str,
                       entry_criteria: Dict[str, Any], 
                       exit_criteria: Dict[str, Any]) -> Dict[str, Any]:
    """
    Evaluate entry and exit conditions based on current market data.
    
    Args:
        df: DataFrame with OHLCV data and computed indicators
        trade_conditions: Dictionary defining trade conditions
        logic_operator: "AND" or "OR" for combining conditions
        entry_criteria: Dictionary defining entry conditions
        exit_criteria: Dictionary defining exit conditions
        
    Returns:
        Dictionary with evaluation results:
        {
            "entry": bool,
            "exit": bool,
            "signals": Dict[str, Any],
            "confidence": float
        }
    """
    logger = logging.getLogger(__name__)
    
    if df.empty:
        logger.warning("Empty DataFrame provided for condition evaluation")
        return {"entry": False, "exit": False, "signals": {}, "confidence": 0.0}
    
    # Get the latest row for current evaluation
    current_row = df.iloc[-1]
    
    try:
        # Evaluate trade conditions
        trade_signal = evaluate_trade_conditions(current_row, trade_conditions, logic_operator)
        
        # Evaluate entry criteria
        entry_signal = evaluate_criteria(current_row, entry_criteria, logic_operator)
        
        # Evaluate exit criteria
        exit_signal = evaluate_criteria(current_row, exit_criteria, logic_operator)
        
        # Combine signals
        entry_decision = trade_signal and entry_signal
        exit_decision = trade_signal and exit_signal
        
        # Calculate confidence based on signal strength
        confidence = calculate_confidence(current_row, trade_conditions, entry_criteria, exit_criteria)
        
        result = {
            "entry": entry_decision,
            "exit": exit_decision,
            "signals": {
                "trade": trade_signal,
                "entry": entry_signal,
                "exit": exit_signal
            },
            "confidence": confidence
        }
        
        logger.debug("Condition evaluation result: %s", result)
        return result
        
    except Exception as e:
        logger.error("Error evaluating conditions: %s", e)
        return {"entry": False, "exit": False, "signals": {}, "confidence": 0.0}


def evaluate_trade_conditions(row: pd.Series, 
                            trade_conditions: Dict[str, Any], 
                            logic_operator: str) -> bool:
    """
    Evaluate trade conditions based on current market data.
    
    Args:
        row: Current market data row
        trade_conditions: Dictionary defining trade conditions
        logic_operator: "AND" or "OR" for combining conditions
        
    Returns:
        Boolean indicating if trade conditions are met
    """
    if not trade_conditions:
        return True  # No conditions means always trade
    
    conditions = []
    
    for indicator, conditions_dict in trade_conditions.items():
        if indicator not in row.index:
            continue
            
        indicator_value = row[indicator]
        if pd.isna(indicator_value):
            continue
            
        # Evaluate specific conditions for this indicator
        indicator_conditions = evaluate_indicator_conditions(
            indicator_value, conditions_dict
        )
        conditions.append(indicator_conditions)
    
    if not conditions:
        return True
    
    # Apply logic operator
    if logic_operator == "AND":
        return all(conditions)
    elif logic_operator == "OR":
        return any(conditions)
    else:
        return all(conditions)  # Default to AND


def evaluate_criteria(row: pd.Series, 
                    criteria: Dict[str, Any], 
                    logic_operator: str) -> bool:
    """
    Evaluate entry or exit criteria.
    
    Args:
        row: Current market data row
        criteria: Dictionary defining criteria
        logic_operator: "AND" or "OR" for combining conditions
        
    Returns:
        Boolean indicating if criteria are met
    """
    if not criteria:
        return False  # No criteria means never enter/exit
    
    conditions = []
    
    for condition_type, condition_config in criteria.items():
        if condition_type == "crossover":
            condition_result = evaluate_crossover_condition(row, condition_config)
            conditions.append(condition_result)
        elif condition_type == "threshold":
            condition_result = evaluate_threshold_condition(row, condition_config)
            conditions.append(condition_result)
        elif condition_type == "trend":
            condition_result = evaluate_trend_condition(row, condition_config)
            conditions.append(condition_result)
        elif condition_type == "custom":
            condition_result = evaluate_custom_condition(row, condition_config)
            conditions.append(condition_result)
    
    if not conditions:
        return False
    
    # Apply logic operator
    if logic_operator == "AND":
        return all(conditions)
    elif logic_operator == "OR":
        return any(conditions)
    else:
        return all(conditions)  # Default to AND


def evaluate_indicator_conditions(indicator_value: float, 
                                conditions_dict: Dict[str, Any]) -> bool:
    """
    Evaluate conditions for a specific indicator value.
    
    Args:
        indicator_value: Current value of the indicator
        conditions_dict: Dictionary defining conditions for this indicator
        
    Returns:
        Boolean indicating if conditions are met
    """
    if not conditions_dict:
        return True
    
    conditions = []
    
    for condition_type, condition_value in conditions_dict.items():
        if condition_type == "above":
            conditions.append(indicator_value > condition_value)
        elif condition_type == "below":
            conditions.append(indicator_value < condition_value)
        elif condition_type == "equals":
            conditions.append(indicator_value == condition_value)
        elif condition_type == "not_equals":
            conditions.append(indicator_value != condition_value)
        elif condition_type == "between":
            if isinstance(condition_value, (list, tuple)) and len(condition_value) == 2:
                min_val, max_val = condition_value
                conditions.append(min_val <= indicator_value <= max_val)
        elif condition_type == "outside":
            if isinstance(condition_value, (list, tuple)) and len(condition_value) == 2:
                min_val, max_val = condition_value
                conditions.append(indicator_value < min_val or indicator_value > max_val)
    
    return all(conditions) if conditions else True


def evaluate_crossover_condition(row: pd.Series, config: Dict[str, Any]) -> bool:
    """
    Evaluate crossover conditions (e.g., price crossing above/below indicator).
    
    Args:
        row: Current market data row
        config: Crossover configuration
        
    Returns:
        Boolean indicating if crossover condition is met
    """
    try:
        indicator1 = config.get("indicator1")
        indicator2 = config.get("indicator2")
        direction = config.get("direction", "above")  # above, below
        
        if not indicator1 or not indicator2:
            return False
        
        if indicator1 not in row.index or indicator2 not in row.index:
            return False
        
        value1 = row[indicator1]
        value2 = row[indicator2]
        
        if pd.isna(value1) or pd.isna(value2):
            return False
        
        if direction == "above":
            return value1 > value2
        elif direction == "below":
            return value1 < value2
        else:
            return False
            
    except Exception as e:
        logging.getLogger(__name__).error("Error evaluating crossover condition: %s", e)
        return False


def evaluate_threshold_condition(row: pd.Series, config: Dict[str, Any]) -> bool:
    """
    Evaluate threshold conditions (e.g., RSI above 70).
    
    Args:
        row: Current market data row
        config: Threshold configuration
        
    Returns:
        Boolean indicating if threshold condition is met
    """
    try:
        indicator = config.get("indicator")
        threshold = config.get("threshold")
        operator = config.get("operator", "above")  # above, below, equals
        
        if not indicator or threshold is None:
            return False
        
        if indicator not in row.index:
            return False
        
        value = row[indicator]
        if pd.isna(value):
            return False
        
        if operator == "above":
            return value > threshold
        elif operator == "below":
            return value < threshold
        elif operator == "equals":
            return value == threshold
        elif operator == "not_equals":
            return value != threshold
        else:
            return False
            
    except Exception as e:
        logging.getLogger(__name__).error("Error evaluating threshold condition: %s", e)
        return False


def evaluate_trend_condition(row: pd.Series, config: Dict[str, Any]) -> bool:
    """
    Evaluate trend conditions (e.g., trend direction).
    
    Args:
        row: Current market data row
        config: Trend configuration
        
    Returns:
        Boolean indicating if trend condition is met
    """
    try:
        indicator = config.get("indicator")
        expected_trend = config.get("trend")  # up, down, neutral
        
        if not indicator or not expected_trend:
            return False
        
        if indicator not in row.index:
            return False
        
        value = row[indicator]
        if pd.isna(value):
            return False
        
        if expected_trend == "up":
            return value > 0
        elif expected_trend == "down":
            return value < 0
        elif expected_trend == "neutral":
            return value == 0
        else:
            return False
            
    except Exception as e:
        logging.getLogger(__name__).error("Error evaluating trend condition: %s", e)
        return False


def evaluate_custom_condition(row: pd.Series, config: Dict[str, Any]) -> bool:
    """
    Evaluate custom conditions defined by the user.
    
    Args:
        row: Current market data row
        config: Custom condition configuration
        
    Returns:
        Boolean indicating if custom condition is met
    """
    try:
        # This is a placeholder for custom condition evaluation
        # Users can define their own logic here
        condition_type = config.get("type")
        
        if condition_type == "volume_spike":
            # Example: Volume spike condition
            volume = row.get("volume", 0)
            avg_volume = row.get("volume_sma", volume)
            threshold = config.get("threshold", 2.0)
            
            if avg_volume > 0:
                return volume > (avg_volume * threshold)
            return False
            
        elif condition_type == "price_momentum":
            # Example: Price momentum condition
            close = row.get("close", 0)
            sma = row.get("sma_20", close)
            
            if sma > 0:
                return close > sma
            return False
            
        else:
            return False
            
    except Exception as e:
        logging.getLogger(__name__).error("Error evaluating custom condition: %s", e)
        return False


def calculate_confidence(row: pd.Series, 
                        trade_conditions: Dict[str, Any],
                        entry_criteria: Dict[str, Any], 
                        exit_criteria: Dict[str, Any]) -> float:
    """
    Calculate confidence level based on signal strength.
    
    Args:
        row: Current market data row
        trade_conditions: Trade conditions configuration
        entry_criteria: Entry criteria configuration
        exit_criteria: Exit criteria configuration
        
    Returns:
        Confidence level between 0.0 and 1.0
    """
    try:
        confidence_factors = []
        
        # Factor 1: Indicator strength
        for indicator, conditions in trade_conditions.items():
            if indicator in row.index:
                value = row[indicator]
                if not pd.isna(value):
                    # Normalize indicator value to 0-1 range
                    if indicator.startswith("rsi"):
                        # RSI is already 0-100, normalize to 0-1
                        normalized = abs(value - 50) / 50
                        confidence_factors.append(normalized)
                    elif indicator.startswith("stoch"):
                        # Stochastic is already 0-100, normalize to 0-1
                        normalized = abs(value - 50) / 50
                        confidence_factors.append(normalized)
                    else:
                        # For other indicators, use a simple threshold
                        confidence_factors.append(0.5)
        
        # Factor 2: Volume confirmation
        if "volume" in row.index:
            volume = row["volume"]
            if not pd.isna(volume) and volume > 0:
                # Higher volume = higher confidence
                volume_confidence = min(volume / 1000000, 1.0)  # Normalize to reasonable range
                confidence_factors.append(volume_confidence)
        
        # Factor 3: Trend alignment
        trend_alignment = 0.5  # Default neutral
        if "st_direction" in row.index:
            direction = row["st_direction"]
            if direction == "UP":
                trend_alignment = 0.8
            elif direction == "DOWN":
                trend_alignment = 0.2
        confidence_factors.append(trend_alignment)
        
        if not confidence_factors:
            return 0.5
        
        # Calculate average confidence
        avg_confidence = sum(confidence_factors) / len(confidence_factors)
        
        # Ensure confidence is between 0 and 1
        return max(0.0, min(1.0, avg_confidence))
        
    except Exception as e:
        logging.getLogger(__name__).error("Error calculating confidence: %s", e)
        return 0.5


def get_signal_strength(row: pd.Series, indicator: str) -> float:
    """
    Get the strength of a signal for a specific indicator.
    
    Args:
        row: Current market data row
        indicator: Indicator name
        
    Returns:
        Signal strength between -1.0 and 1.0
    """
    if indicator not in row.index:
        return 0.0
    
    value = row[indicator]
    if pd.isna(value):
        return 0.0
    
    # Normalize different indicators to -1 to 1 range
    if indicator.startswith("rsi"):
        # RSI: 0-100 -> -1 to 1 (50 is neutral)
        return (value - 50) / 50
    elif indicator.startswith("stoch"):
        # Stochastic: 0-100 -> -1 to 1 (50 is neutral)
        return (value - 50) / 50
    elif indicator.startswith("macd"):
        # MACD: Use histogram for signal strength
        return np.tanh(value)  # Normalize to -1 to 1
    else:
        # For other indicators, return normalized value
        return np.tanh(value)

