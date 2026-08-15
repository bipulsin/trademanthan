"""Indian Equity Futures transaction cost calculator (NSE)."""

from __future__ import annotations

from dataclasses import dataclass, field

from rocket.config.constants import (
    DEFAULT_BROKERAGE_PER_ORDER,
    EXCHANGE_TURNOVER_RATE,
    GST_RATE,
    SEBI_PER_CRORE,
    STAMP_DUTY_BUY_RATE,
    STT_SELL_RATE,
)


@dataclass
class CostBreakdown:
    brokerage: float = 0.0
    stt: float = 0.0
    exchange: float = 0.0
    sebi: float = 0.0
    stamp_duty: float = 0.0
    gst: float = 0.0
    slippage: float = 0.0

    @property
    def total(self) -> float:
        return (
            self.brokerage
            + self.stt
            + self.exchange
            + self.sebi
            + self.stamp_duty
            + self.gst
            + self.slippage
        )

    def as_dict(self) -> dict:
        return {
            "brokerage": round(self.brokerage, 2),
            "stt": round(self.stt, 2),
            "exchange": round(self.exchange, 2),
            "sebi": round(self.sebi, 2),
            "stamp_duty": round(self.stamp_duty, 2),
            "gst": round(self.gst, 2),
            "slippage": round(self.slippage, 2),
            "total": round(self.total, 2),
        }


@dataclass
class CostAccumulator:
    brokerage: float = 0.0
    stt: float = 0.0
    exchange: float = 0.0
    sebi: float = 0.0
    stamp_duty: float = 0.0
    gst: float = 0.0
    slippage: float = 0.0

    def add(self, c: CostBreakdown) -> None:
        self.brokerage += c.brokerage
        self.stt += c.stt
        self.exchange += c.exchange
        self.sebi += c.sebi
        self.stamp_duty += c.stamp_duty
        self.gst += c.gst
        self.slippage += c.slippage

    @property
    def total(self) -> float:
        return (
            self.brokerage
            + self.stt
            + self.exchange
            + self.sebi
            + self.stamp_duty
            + self.gst
            + self.slippage
        )

    def as_dict(self) -> dict:
        return {
            "brokerage": round(self.brokerage, 2),
            "stt": round(self.stt, 2),
            "exchange": round(self.exchange, 2),
            "sebi": round(self.sebi, 2),
            "stamp_duty": round(self.stamp_duty, 2),
            "gst": round(self.gst, 2),
            "slippage": round(self.slippage, 2),
            "total": round(self.total, 2),
        }


class FuturesCostModel:
    def __init__(self, brokerage_per_order: float = DEFAULT_BROKERAGE_PER_ORDER):
        self.brokerage_per_order = float(brokerage_per_order)

    def compute(
        self,
        *,
        side: str,
        price: float,
        quantity: int,
        slippage_rupees: float = 0.0,
    ) -> CostBreakdown:
        """
        ``side``: BUY or SELL.
        ``quantity``: number of shares/units (lot_size × lots).
        Turnover = price × quantity.
        """
        side_u = side.upper()
        turnover = abs(float(price) * int(quantity))
        brokerage = self.brokerage_per_order
        exchange = turnover * EXCHANGE_TURNOVER_RATE
        sebi = (turnover / 1e7) * SEBI_PER_CRORE
        stt = turnover * STT_SELL_RATE if side_u == "SELL" else 0.0
        stamp = turnover * STAMP_DUTY_BUY_RATE if side_u == "BUY" else 0.0
        gst = GST_RATE * (brokerage + exchange + sebi)
        return CostBreakdown(
            brokerage=brokerage,
            stt=stt,
            exchange=exchange,
            sebi=sebi,
            stamp_duty=stamp,
            gst=gst,
            slippage=abs(float(slippage_rupees)),
        )
