from sqlalchemy import Column, Integer, String, DateTime, Boolean, Float, Text, ForeignKey
from sqlalchemy.sql import func
from sqlalchemy.orm import relationship
from .base import Base

class Broker(Base):
    __tablename__ = "brokers"
    
    id = Column(Integer, primary_key=True, index=True)
    user_id = Column(Integer, ForeignKey("users.id"), nullable=False)
    name = Column(String(255), nullable=False)
    type = Column(String(50), nullable=False)
    api_key = Column(String(500), nullable=True)
    api_secret = Column(String(500), nullable=True)
    access_token = Column(Text, nullable=True)
    api_url = Column(String(500), nullable=True)
    is_connected = Column(Boolean, default=False)
    connection_status = Column(String(50), default="disconnected")
    last_connection = Column(DateTime, nullable=True)
    test_mode = Column(Boolean, default=True)
    config = Column(Text, nullable=True)  # JSONB equivalent for SQLite
    created_at = Column(DateTime, default=func.now())
    updated_at = Column(DateTime, default=func.now(), onupdate=func.now())
    
    # Relationships - commented out temporarily to avoid import issues
    # user = relationship("User", back_populates="brokers")
    strategies = relationship("Strategy", back_populates="broker")
    trades = relationship("Trade", back_populates="broker")
    
    def __repr__(self):
        return f"<Broker(id={self.id}, name='{self.name}', user_id={self.user_id})>"


class IntradayStockOption(Base):
    """
    Stores intraday stock option alerts from Chartink webhooks
    Tracks the complete lifecycle from alert to trade execution
    """
    __tablename__ = "intraday_stock_options"
    
    id = Column(Integer, primary_key=True, index=True)
    created_date_time = Column(DateTime, default=func.now(), nullable=False, index=True)
    alert_time = Column(DateTime, nullable=False, index=True)
    alert_type = Column(String(20), nullable=False, index=True)  # 'Bullish' or 'Bearish'
    scan_name = Column(String(255), nullable=True)
    stock_name = Column(String(100), nullable=False, index=True)
    stock_ltp = Column(Float, nullable=True)
    stock_vwap = Column(Float, nullable=True)
    stock_vwap_previous_hour = Column(Float, nullable=True)  # VWAP from previous 1-hour candle
    stock_vwap_previous_hour_time = Column(DateTime, nullable=True)  # Time of previous hour VWAP
    # VWAP slope fields (calculated by cycle-based scheduler)
    vwap_slope_angle = Column(Float, nullable=True)  # VWAP slope angle in degrees
    vwap_slope_status = Column(String(20), nullable=True)  # 'Yes', 'No', 'Skipped'
    vwap_slope_direction = Column(String(20), nullable=True)  # 'upward', 'downward', 'flat'
    vwap_slope_time = Column(DateTime, nullable=True)  # Time when VWAP slope was calculated
    # Candle size fields (calculated when stock is received from webhook alert)
    candle_size_ratio = Column(Float, nullable=True)  # Ratio of current day candle size to previous day candle size
    candle_size_status = Column(String(20), nullable=True)  # 'Pass', 'Fail', 'Skipped'
    option_contract = Column(String(255), nullable=True)
    option_type = Column(String(10), nullable=True)  # 'CE' or 'PE'
    option_strike = Column(Float, nullable=True)
    option_ltp = Column(Float, nullable=True)
    option_vwap = Column(Float, nullable=True)
    # Option OHLC candles
    option_current_candle_open = Column(Float, nullable=True)
    option_current_candle_high = Column(Float, nullable=True)
    option_current_candle_low = Column(Float, nullable=True)
    option_current_candle_close = Column(Float, nullable=True)
    option_current_candle_time = Column(DateTime, nullable=True)
    option_previous_candle_open = Column(Float, nullable=True)
    option_previous_candle_high = Column(Float, nullable=True)
    option_previous_candle_low = Column(Float, nullable=True)
    option_previous_candle_close = Column(Float, nullable=True)
    option_previous_candle_time = Column(DateTime, nullable=True)
    
    # Trading execution fields (filled later)
    qty = Column(Integer, nullable=True)
    buy_time = Column(DateTime, nullable=True)
    buy_price = Column(Float, nullable=True)
    instrument_key = Column(String(255), nullable=True, index=True)  # Upstox instrument key (e.g., NSE_FO|104500) - stored at trade entry
    stop_loss = Column(Float, nullable=True)  # Stop loss price for risk management
    sell_time = Column(DateTime, nullable=True)
    sell_price = Column(Float, nullable=True)
    exit_reason = Column(String(50), nullable=True)  # 'profit_target', 'stop_loss', 'time_based', 'stock_vwap_cross', 'manual'
    pnl = Column(Float, nullable=True)
    status = Column(String(50), default='alert_received')  # alert_received, bought, sold, cancelled
    no_entry_reason = Column(String(255), nullable=True)  # Short description of why trade was not entered (e.g., "Index alignment", "VWAP slope", "Candle size", "Time >= 3PM", "Missing option data")
    
    # Metadata
    trade_date = Column(DateTime, nullable=False, index=True)  # Trading date (not created date)
    updated_at = Column(DateTime, default=func.now(), onupdate=func.now())
    
    def __repr__(self):
        return f"<IntradayStockOption(id={self.id}, stock='{self.stock_name}', type='{self.alert_type}', status='{self.status}')>"


class MasterStock(Base):
    """
    Master stock data from Dhan API
    Downloaded daily at 9 AM with filtered NSE options data
    """
    __tablename__ = "master_stock"
    
    id = Column(Integer, primary_key=True, index=True)
    security_id = Column(String(50), nullable=False, index=True)
    isin = Column(String(50), nullable=True)
    exch_id = Column(String(10), nullable=False, index=True)  # NSE, BSE, etc.
    segment = Column(String(10), nullable=True)
    instrument = Column(String(20), nullable=False, index=True)  # OPTSTK, FUTIDX, etc.
    underlying_security_id = Column(String(50), nullable=True)
    underlying_symbol = Column(String(100), nullable=True, index=True)
    symbol_name = Column(String(255), nullable=False, index=True)
    display_name = Column(String(255), nullable=True)
    instrument_type = Column(String(50), nullable=True)
    series = Column(String(10), nullable=True)
    lot_size = Column(Float, nullable=True)
    sm_expiry_date = Column(DateTime, nullable=True, index=True)
    strike_price = Column(Float, nullable=True, index=True)
    option_type = Column(String(10), nullable=True, index=True)  # CE, PE, XX
    tick_size = Column(Float, nullable=True)
    expiry_flag = Column(String(10), nullable=True, index=True)  # M for monthly
    
    # Metadata
    download_date = Column(DateTime, default=func.now(), nullable=False, index=True)
    created_at = Column(DateTime, default=func.now())
    
    def __repr__(self):
        return f"<MasterStock(id={self.id}, symbol='{self.symbol_name}', strike={self.strike_price}, type='{self.option_type}')>"


class UpstoxInstrument(Base):
    """
    Upstox instrument data from nse_instruments.json
    Stores all instrument details from Upstox API
    """
    __tablename__ = "upstox_instrument"
    
    id = Column(Integer, primary_key=True, index=True)
    instrument_key = Column(String(255), nullable=False, unique=True, index=True)  # Primary key from Upstox
    name = Column(String(255), nullable=True)
    trading_symbol = Column(String(255), nullable=True, index=True)
    exchange = Column(String(50), nullable=True, index=True)  # NSE, BSE, etc.
    segment = Column(String(50), nullable=True, index=True)  # NSE_EQ, NSE_FO, NCD_FO, etc.
    instrument_type = Column(String(50), nullable=True, index=True)  # CE, PE, EQ, FUT, etc.
    exchange_token = Column(String(50), nullable=True, index=True)
    isin = Column(String(50), nullable=True, index=True)
    
    # Asset/Symbol information
    asset_symbol = Column(String(100), nullable=True, index=True)
    asset_type = Column(String(50), nullable=True)  # CUR, EQ, etc.
    underlying_symbol = Column(String(100), nullable=True, index=True)
    underlying_type = Column(String(50), nullable=True)
    underlying_key = Column(String(255), nullable=True)
    asset_key = Column(String(255), nullable=True)
    
    # Option/Future specific fields
    strike_price = Column(Float, nullable=True, index=True)
    expiry = Column(DateTime, nullable=True, index=True)  # Expiry timestamp
    weekly = Column(Boolean, nullable=True, default=False)
    last_trading_date = Column(DateTime, nullable=True)
    
    # Trading parameters
    lot_size = Column(Integer, nullable=True)
    minimum_lot = Column(Integer, nullable=True)
    tick_size = Column(Float, nullable=True)
    qty_multiplier = Column(Float, nullable=True)
    freeze_quantity = Column(Float, nullable=True)
    price_quote_unit = Column(String(50), nullable=True)
    security_type = Column(String(50), nullable=True)  # NORMAL, etc.
    short_name = Column(String(255), nullable=True)
    
    # Metadata
    created_at = Column(DateTime, default=func.now(), nullable=False)
    updated_at = Column(DateTime, default=func.now(), onupdate=func.now())
    
    def __repr__(self):
        return f"<UpstoxInstrument(id={self.id}, instrument_key='{self.instrument_key}', trading_symbol='{self.trading_symbol}')>"


class HistoricalMarketData(Base):
    """
    Historical market data captured during hourly updates
    Stores VWAP, LTP, option prices, and VWAP slope for historical analysis
    """
    __tablename__ = "historical_market_data"
    
    id = Column(Integer, primary_key=True, index=True)
    
    # Stock information
    stock_name = Column(String(100), nullable=False, index=True)
    stock_vwap = Column(Float, nullable=True)  # Volume Weighted Average Price
    stock_ltp = Column(Float, nullable=True)  # Last Traded Price
    
    # VWAP slope fields (calculated by cycle-based scheduler)
    vwap_slope_angle = Column(Float, nullable=True)  # VWAP slope angle in degrees
    vwap_slope_status = Column(String(20), nullable=True)  # 'Yes', 'No', 'Skipped'
    vwap_slope_direction = Column(String(20), nullable=True)  # 'upward', 'downward', 'flat'
    vwap_slope_time = Column(DateTime, nullable=True)  # Time when VWAP slope was calculated
    
    # Option information
    option_contract = Column(String(255), nullable=True, index=True)  # Option contract name (e.g., RELIANCE-Nov2024-2500-CE)
    option_instrument_key = Column(String(255), nullable=True, index=True)  # Upstox instrument key (e.g., NSE_FO|104500)
    option_ltp = Column(Float, nullable=True)  # Option Last Traded Price
    
    # Timestamp
    scan_date = Column(DateTime, nullable=False, index=True)  # Date and time of the scan/update
    scan_time = Column(String(20), nullable=True)  # Human-readable time (e.g., "10:15 AM")
    
    # Metadata
    created_at = Column(DateTime, default=func.now(), nullable=False)
    
    def __repr__(self):
        return f"<HistoricalMarketData(id={self.id}, stock='{self.stock_name}', option='{self.option_contract}', time='{self.scan_date}')>"


