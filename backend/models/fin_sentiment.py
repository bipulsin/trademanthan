"""Financial news sentiment snapshots (MarketAux + FinBERT)."""
from sqlalchemy import Column, String, Float, Integer, DateTime, func

from backend.models.base import Base


class StockFinSentiment(Base):
    """
    One row per arbitrage_master stock: latest API/NLP sentiment and rotated combined scores.
    """

    __tablename__ = "stock_fin_sentiment"

    stock = Column(String(64), primary_key=True, nullable=False, index=True)
    stock_instrument_key = Column(String(255), nullable=True)
    api_sentiment_avg = Column(Float, nullable=True)
    nlp_sentiment_avg = Column(Float, nullable=True)
    combined_sentiment_avg = Column(Float, nullable=True)
    last_combined_sentiment = Column(Float, nullable=True)
    current_combined_sentiment = Column(Float, nullable=True)
    news_count = Column(Integer, nullable=True)
    current_run_at = Column(DateTime(timezone=True), nullable=True)
    updated_at = Column(DateTime(timezone=True), server_default=func.now(), onupdate=func.now())


class FinSentimentJobState(Base):
    """Singleton watermark for MarketAux `published_after` (row id must be 1)."""

    __tablename__ = "fin_sentiment_job_state"

    id = Column(Integer, primary_key=True, nullable=False)
    watermark = Column(DateTime(timezone=True), nullable=False)
