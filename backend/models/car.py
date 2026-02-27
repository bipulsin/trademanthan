"""
CAR GPT models - Stock list and configuration for Cumulative Average Return analysis
"""
from sqlalchemy import Column, Integer, String, DateTime, Numeric
from sqlalchemy.sql import func
from .base import Base


class CarStockList(Base):
    """
    Stores stock symbols for CAR GPT trailing analysis.
    Symbols are entered in carsetup page (comma-separated) and used for CAR analysis.
    """
    __tablename__ = "carstocklist"

    id = Column(Integer, primary_key=True, index=True)
    symbol = Column(String(50), nullable=False, index=True)
    userid = Column(Integer, nullable=False, default=4, server_default="4", index=True)
    buy_price = Column(Numeric(12, 2), nullable=False, default=0, server_default="0")
    created_at = Column(DateTime, default=func.now(), nullable=False)

    def __repr__(self):
        return f"<CarStockList(id={self.id}, symbol='{self.symbol}', userid={self.userid}, buy_price={self.buy_price}, created_at='{self.created_at}')>"
