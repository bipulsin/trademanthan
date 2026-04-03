from sqlalchemy import Column, Integer, String, DateTime, Boolean, Text
from sqlalchemy.sql import func
from sqlalchemy.orm import relationship
from datetime import datetime
from .base import Base

class User(Base):
    __tablename__ = "users"
    
    id = Column(Integer, primary_key=True, index=True)
    google_id = Column(String(255), unique=True, index=True, nullable=True)
    email = Column(String(255), unique=True, index=True, nullable=False)
    username = Column(String(100), unique=True, index=True, nullable=True)
    full_name = Column(String(255), nullable=True)
    avatar_url = Column(Text, nullable=True)
    is_active = Column(Boolean, default=True)
    # DB column "isAdmin": "Yes" = system admin; blank otherwise
    is_admin = Column("isAdmin", String(10), nullable=True)
    # Optional comma-separated or JSON-ish page allowlist (up to 255 chars)
    page_permitted = Column(String(255), nullable=True)
    # User governance + activity tracking fields
    is_blocked = Column(Boolean, default=False, nullable=False)
    is_paid_user = Column(Boolean, default=False, nullable=False)
    last_login_at = Column(DateTime, nullable=True)
    last_login_ip = Column(String(64), nullable=True)
    last_page_visited = Column(String(255), nullable=True)
    last_page_visited_at = Column(DateTime, nullable=True)
    last_activity_ip = Column(String(64), nullable=True)
    created_at = Column(DateTime, default=func.now())
    updated_at = Column(DateTime, default=func.now(), onupdate=func.now())
    
    # Relationships - commented out temporarily to avoid import issues
    # brokers = relationship("Broker", back_populates="user")
    strategies = relationship("Strategy", back_populates="user")
    trades = relationship("Trade", back_populates="user")
    backtests = relationship("Backtest", back_populates="user")
    strategy_logs = relationship("StrategyExecutionLog", back_populates="user")
    
    def __repr__(self):
        return f"<User(id={self.id}, email='{self.email}', full_name='{self.full_name}')>"
