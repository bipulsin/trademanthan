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
    option_contract = Column(String(255), nullable=True)
    option_type = Column(String(10), nullable=True)  # 'CE' or 'PE'
    option_strike = Column(Float, nullable=True)
    option_ltp = Column(Float, nullable=True)
    option_vwap = Column(Float, nullable=True)
    
    # Trading execution fields (filled later)
    qty = Column(Integer, nullable=True)
    buy_time = Column(DateTime, nullable=True)
    buy_price = Column(Float, nullable=True)
    stop_loss = Column(Float, nullable=True)  # Stop loss price for risk management
    sell_time = Column(DateTime, nullable=True)
    sell_price = Column(Float, nullable=True)
    exit_reason = Column(String(50), nullable=True)  # 'profit_target', 'stop_loss', 'time_based', 'manual'
    pnl = Column(Float, nullable=True)
    status = Column(String(50), default='alert_received')  # alert_received, bought, sold, cancelled
    
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


