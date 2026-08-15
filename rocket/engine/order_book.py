"""Order management and slippage matching for Rocket backtests."""

from __future__ import annotations

from dataclasses import dataclass, field
from datetime import datetime
from enum import Enum
from typing import List, Optional
from uuid import uuid4


class Side(str, Enum):
    BUY = "BUY"
    SELL = "SELL"


class OrderStatus(str, Enum):
    PENDING = "PENDING"
    FILLED = "FILLED"
    REJECTED = "REJECTED"
    CANCELLED = "CANCELLED"


@dataclass
class Order:
    symbol: str
    instrument_key: str
    side: Side
    quantity: int
    order_type: str = "MARKET"
    limit_price: Optional[float] = None
    stop_loss: Optional[float] = None
    take_profit: Optional[float] = None
    confidence: float = 0.0
    reason: str = ""
    order_id: str = field(default_factory=lambda: uuid4().hex[:12])
    status: OrderStatus = OrderStatus.PENDING
    fill_price: Optional[float] = None
    fill_time: Optional[datetime] = None
    reject_reason: str = ""


@dataclass
class Fill:
    order_id: str
    symbol: str
    instrument_key: str
    side: Side
    quantity: int
    price: float
    timestamp: datetime
    slippage_per_unit: float = 0.0


class OrderBook:
    """Simulated matching: market orders fill at mid ± bps slippage (tick-rounded)."""

    def __init__(
        self,
        default_slippage_ticks: float = 1.0,
        default_slippage_bps: float = 2.0,
    ):
        self.default_slippage_ticks = float(default_slippage_ticks)
        self.default_slippage_bps = float(default_slippage_bps)
        self.orders: List[Order] = []
        self.fills: List[Fill] = []

    def submit(self, order: Order) -> Order:
        self.orders.append(order)
        return order

    def match_market(
        self,
        order: Order,
        *,
        ref_price: float,
        tick_size: float,
        timestamp: datetime,
        slippage_ticks: Optional[float] = None,
        slippage_bps: Optional[float] = None,
    ) -> Optional[Fill]:
        if order.status != OrderStatus.PENDING:
            return None
        ticks = self.default_slippage_ticks if slippage_ticks is None else float(slippage_ticks)
        bps = self.default_slippage_bps if slippage_bps is None else float(slippage_bps)
        # Effective tick for rounding: treat values >= 1 as paise when they dwarf bps slip
        raw_tick = max(float(tick_size or 0.05), 0.01)
        bps_slip = abs(ref_price) * max(0.0, bps) / 10_000.0
        tick_slip = max(0.0, ticks) * raw_tick
        # Prefer bps when tick slip is unrealistically large vs price (>5 bps)
        if tick_slip > abs(ref_price) * 0.0005:
            slip = bps_slip
            round_tick = min(raw_tick, max(0.05, abs(ref_price) * 0.0001))
        else:
            slip = max(bps_slip, tick_slip)
            round_tick = raw_tick
        if order.side == Side.BUY:
            px = ref_price + slip
        else:
            px = max(0.05, ref_price - slip)
        if round_tick > 0:
            px = round(round(px / round_tick) * round_tick, 10)
        order.status = OrderStatus.FILLED
        order.fill_price = px
        order.fill_time = timestamp
        fill = Fill(
            order_id=order.order_id,
            symbol=order.symbol,
            instrument_key=order.instrument_key,
            side=order.side,
            quantity=order.quantity,
            price=px,
            timestamp=timestamp,
            slippage_per_unit=abs(px - ref_price),
        )
        self.fills.append(fill)
        return fill

    def reject(self, order: Order, reason: str) -> Order:
        order.status = OrderStatus.REJECTED
        order.reject_reason = reason
        return order
