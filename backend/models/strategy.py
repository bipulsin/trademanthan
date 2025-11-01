from sqlalchemy import Column, Integer, String, DateTime, Boolean, Float, Text, ForeignKey, JSON
from sqlalchemy.sql import func
from sqlalchemy.orm import relationship
from datetime import datetime

from .base import Base

class Strategy(Base):
    __tablename__ = "strategies"

    id = Column(Integer, primary_key=True, index=True)
    user_id = Column(Integer, ForeignKey("users.id"), nullable=False)
    broker_id = Column(Integer, ForeignKey("brokers.id"), nullable=True)
    
    # Basic Information
    name = Column(String(255), nullable=False)
    description = Column(Text, nullable=True)
    
    # Strategy Configuration
    product = Column(String(100), nullable=True)  # Trading product (e.g., NIFTY, BANKNIFTY)
    platform = Column(String(20), nullable=True)  # Platform: 'testnet' or 'live'
    product_id = Column(String(100), nullable=True)  # Product ID from Delta Exchange
    candle_duration = Column(String(20), nullable=True)  # Candle duration (e.g., 1m, 5m, 15m)
    indicators = Column(Text, nullable=False)  # List of selected indicators (text[] in DB)
    logic_operator = Column(String(10), nullable=False, default="AND")  # AND/OR logic
    
    # Indicator Parameters (stored as JSON for flexibility)
    parameters = Column(JSON, nullable=False)  # Indicator-specific parameters
    
    # Trading Criteria
    entry_criteria = Column(Text, nullable=False)  # Entry conditions
    exit_criteria = Column(Text, nullable=False)   # Exit conditions
    trade_conditions = Column(JSON, nullable=True)  # Trade conditions (jsonb in DB)
    
    # Risk Management
    stop_loss = Column(JSON, nullable=True)  # Stop loss configuration
    trailing_stop = Column(JSON, nullable=True)  # Trailing stop configuration
    
    # Strategy Status
    is_active = Column(Boolean, default=True)
    is_live = Column(Boolean, default=False)  # Whether strategy is currently executing
    is_backtested = Column(Boolean, default=False)  # Whether strategy has been backtested
    
    # Broker Connection Status
    broker_connected = Column(Boolean, default=False)
    broker_connection_date = Column(DateTime, nullable=True)
    
    # Execution Status
    execution_status = Column(String(20), default="STOPPED")  # STOPPED, RUNNING, PAUSED, ERROR
    last_execution = Column(DateTime, nullable=True)
    next_execution = Column(DateTime, nullable=True)
    
    # Performance Metrics
    total_pnl = Column(Float, default=0.0)
    last_trade_pnl = Column(Float, default=0.0)
    
    # Timestamps
    created_at = Column(DateTime, default=func.now())
    updated_at = Column(DateTime, default=func.now(), onupdate=func.now())
    
    # Relationships
    user = relationship("User", back_populates="strategies")
    broker = relationship("Broker", back_populates="strategies")
    trades = relationship("Trade", back_populates="strategy")
    backtests = relationship("Backtest", back_populates="strategy")
    execution_logs = relationship("StrategyExecutionLog", back_populates="strategy")

    def __repr__(self):
        return f"<Strategy(id={self.id}, name='{self.name}', user_id={self.user_id})>"

    def calculate_win_rate(self):
        """Calculate win rate based on winning vs total trades"""
        if self.total_trades > 0:
            self.win_rate = (self.winning_trades / self.total_trades) * 100
        return self.win_rate

    def update_performance(self, trade_pnl):
        """Update performance metrics after a trade"""
        self.total_trades += 1
        self.total_pnl += trade_pnl
        self.last_trade_pnl = trade_pnl
        self.last_trade_date = datetime.utcnow()
        
        if trade_pnl > 0:
            self.winning_trades += 1
        else:
            self.losing_trades += 1
        
        self.calculate_win_rate()

    def connect_broker(self, broker_id):
        """Connect strategy to a broker"""
        self.broker_id = broker_id
        self.broker_connected = True
        self.broker_connection_date = datetime.utcnow()
        self.updated_at = datetime.utcnow()

    def detach_broker(self):
        """Detach strategy from broker"""
        self.broker_id = None
        self.broker_connected = False
        self.broker_connection_date = None
        self.updated_at = datetime.utcnow()

    def start_execution(self):
        """Start strategy execution"""
        if not self.broker_connected:
            raise ValueError("Cannot start execution without broker connection")
        
        self.is_live = True
        self.execution_status = "RUNNING"
        self.last_execution = datetime.utcnow()
        self.updated_at = datetime.utcnow()

    def stop_execution(self):
        """Stop strategy execution"""
        self.is_live = False
        self.execution_status = "STOPPED"
        self.updated_at = datetime.utcnow()

    def pause_execution(self):
        """Pause strategy execution"""
        self.execution_status = "PAUSED"
        self.updated_at = datetime.utcnow()

class StrategyExecutionLog(Base):
    __tablename__ = "strategy_execution_logs"

    id = Column(Integer, primary_key=True, index=True)
    strategy_id = Column(Integer, ForeignKey("strategies.id"), nullable=False)
    user_id = Column(Integer, ForeignKey("users.id"), nullable=False)
    
    # Log Information
    log_level = Column(String(20), nullable=False)  # INFO, WARNING, ERROR, DEBUG
    message = Column(Text, nullable=False)
    details = Column(JSON, nullable=True)  # Additional context data
    
    # Execution Context
    execution_timestamp = Column(DateTime, default=func.now())
    market_data = Column(JSON, nullable=True)  # Market data at execution time
    signal_generated = Column(Boolean, default=False)
    order_placed = Column(Boolean, default=False)
    
    # Relationships
    strategy = relationship("Strategy", back_populates="execution_logs")
    user = relationship("User", back_populates="strategy_logs")

    def __repr__(self):
        return f"<StrategyExecutionLog(id={self.id}, strategy_id={self.strategy_id}, level={self.log_level})>"

class Trade(Base):
    __tablename__ = "trades"

    id = Column(Integer, primary_key=True, index=True)
    strategy_id = Column(Integer, ForeignKey("strategies.id"), nullable=False)
    user_id = Column(Integer, ForeignKey("users.id"), nullable=False)
    broker_id = Column(Integer, ForeignKey("brokers.id"), nullable=True)
    
    # Trade Information
    symbol = Column(String(50), nullable=False)
    side = Column(String(10), nullable=False)  # BUY/SELL
    quantity = Column(Float, nullable=False)
    entry_price = Column(Float, nullable=False)
    exit_price = Column(Float, nullable=True)
    
    # Trade Status
    status = Column(String(20), default="OPEN")  # OPEN, CLOSED, CANCELLED
    
    # P&L and Metrics
    pnl = Column(Float, default=0.0)
    pnl_percentage = Column(Float, default=0.0)
    
    # Timestamps
    entry_time = Column(DateTime, default=func.now())
    exit_time = Column(DateTime, nullable=True)
    
    # Trade Details
    entry_reason = Column(Text, nullable=True)  # Why the trade was entered
    exit_reason = Column(Text, nullable=True)   # Why the trade was exited
    
    # Strategy Execution Context
    execution_log_id = Column(Integer, ForeignKey("strategy_execution_logs.id"), nullable=True)
    
    # Relationships
    strategy = relationship("Strategy", back_populates="trades")
    user = relationship("User", back_populates="trades")
    broker = relationship("Broker", back_populates="trades")
    execution_log = relationship("StrategyExecutionLog")

    def __repr__(self):
        return f"<Trade(id={self.id}, symbol='{self.symbol}', side='{self.side}', pnl={self.pnl})>"

    def close_trade(self, exit_price, exit_reason=None):
        """Close the trade and calculate P&L"""
        self.exit_price = exit_price
        self.exit_time = datetime.utcnow()
        self.status = "CLOSED"
        self.exit_reason = exit_reason
        
        # Calculate P&L
        if self.side == "BUY":
            self.pnl = (exit_price - self.entry_price) * self.quantity
        else:
            self.pnl = (self.entry_price - exit_price) * self.quantity
        
        self.pnl_percentage = (self.pnl / (self.entry_price * self.quantity)) * 100

class Backtest(Base):
    __tablename__ = "backtests"

    id = Column(Integer, primary_key=True, index=True)
    strategy_id = Column(Integer, ForeignKey("strategies.id"), nullable=False)
    user_id = Column(Integer, ForeignKey("users.id"), nullable=False)
    
    # Backtest Configuration
    start_date = Column(DateTime, nullable=False)
    end_date = Column(DateTime, nullable=False)
    initial_capital = Column(Float, nullable=False)
    symbol = Column(String(50), nullable=False)
    timeframe = Column(String(20), nullable=False)  # 1m, 5m, 15m, 1h, 1d
    
    # Results
    final_capital = Column(Float, nullable=True)
    total_return = Column(Float, nullable=True)
    max_drawdown = Column(Float, nullable=True)
    sharpe_ratio = Column(Float, nullable=True)
    total_trades = Column(Integer, default=0)
    winning_trades = Column(Integer, default=0)
    losing_trades = Column(Integer, default=0)
    win_rate = Column(Float, default=0.0)
    
    # Status
    status = Column(String(20), default="RUNNING")  # RUNNING, COMPLETED, FAILED
    progress = Column(Float, default=0.0)  # 0.0 to 1.0
    
    # Timestamps
    created_at = Column(DateTime, default=func.now())
    completed_at = Column(DateTime, nullable=True)
    
    # Relationships
    strategy = relationship("Strategy", back_populates="backtests")
    user = relationship("User", back_populates="backtests")

    def __repr__(self):
        return f"<Backtest(id={self.id}, strategy_id={self.strategy_id}, status='{self.status}')>"

# Indicator Parameter Schemas (for validation)
INDICATOR_PARAMETERS = {
    "supertrend": {
        "atr_period": {"type": "int", "min": 1, "max": 100, "default": 14},
        "multiplier": {"type": "float", "min": 0.1, "max": 10.0, "default": 3.0}
    },
    "bb_squeeze": {
        "period": {"type": "int", "min": 5, "max": 200, "default": 20},
        "stddev_multiplier": {"type": "float", "min": 0.5, "max": 5.0, "default": 2.0},
        "squeeze_threshold": {"type": "float", "min": 0.1, "max": 2.0, "default": 0.5}
    },
    "rsi": {
        "period": {"type": "int", "min": 2, "max": 100, "default": 14},
        "overbought_level": {"type": "int", "min": 50, "max": 100, "default": 70},
        "oversold_level": {"type": "int", "min": 0, "max": 50, "default": 30}
    },
    "triple_ema": {
        "short_ema_period": {"type": "int", "min": 1, "max": 50, "default": 9},
        "medium_ema_period": {"type": "int", "min": 5, "max": 100, "default": 21},
        "long_ema_period": {"type": "int", "min": 20, "max": 200, "default": 50}
    }
}

# Available indicators
AVAILABLE_INDICATORS = [
    "supertrend",
    "bb_squeeze", 
    "rsi",
    "triple_ema"
]

# Logic operators
LOGIC_OPERATORS = ["AND", "OR"]

# Execution statuses
EXECUTION_STATUSES = ["STOPPED", "RUNNING", "PAUSED", "ERROR"]

# Log levels
LOG_LEVELS = ["INFO", "WARNING", "ERROR", "DEBUG"]
