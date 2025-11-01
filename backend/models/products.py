from sqlalchemy import Column, Integer, String, DateTime, Boolean, Numeric, Text
from sqlalchemy.sql import func
from .base import Base

class Product(Base):
    __tablename__ = "products"

    id = Column(Integer, primary_key=True, index=True)
    product_name = Column(String(255), nullable=False)
    product_id_testnet = Column(String(100), nullable=True)
    product_id_live = Column(String(100), nullable=True)
    symbol = Column(String(100), nullable=False, unique=True)
    base_asset = Column(String(50), nullable=True)
    quote_asset = Column(String(50), nullable=True)
    contract_type = Column(String(50), nullable=True)
    is_active = Column(Boolean, default=True)
    created_at = Column(DateTime, default=func.now())
    updated_at = Column(DateTime, default=func.now(), onupdate=func.now())

    def __repr__(self):
        return f"<Product(id={self.id}, symbol='{self.symbol}', name='{self.product_name}')>"
