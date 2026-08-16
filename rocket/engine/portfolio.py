"""Portfolio, margin proxy, MTM, and cash ledger for Rocket."""

from __future__ import annotations

from dataclasses import dataclass, field
from datetime import datetime
from typing import Dict, List, Optional

from rocket.config.constants import DEFAULT_INITIAL_MARGIN_PCT
from rocket.engine.costs import CostAccumulator, CostBreakdown, FuturesCostModel
from rocket.engine.order_book import Fill, Side


@dataclass
class Position:
    symbol: str
    instrument_key: str
    side: Side  # BUY = long, SELL = short
    quantity: int
    avg_price: float
    lot_size: int
    stop_loss: Optional[float] = None
    take_profit: Optional[float] = None
    opened_at: Optional[datetime] = None
    confidence: float = 0.0
    atr: Optional[float] = None
    trail_activated: bool = False

    @property
    def direction(self) -> int:
        return 1 if self.side == Side.BUY else -1

    def unrealized_pnl(self, mark: float) -> float:
        return (mark - self.avg_price) * self.quantity * self.direction

    def update_trailing_stop(
        self,
        *,
        high: float,
        low: float,
        activate_at_r: float = 1.0,
        trail_atr_mult: float = 2.0,
    ) -> None:
        """
        After +activate_at_r × ATR favorable move, ratchet stop by trail_atr_mult × ATR
        from the bar extreme (long: high − 2ATR; short: low + 2ATR).
        """
        if self.atr is None or self.atr <= 0:
            return
        atr = float(self.atr)
        if self.side == Side.BUY:
            if (high - self.avg_price) >= activate_at_r * atr:
                self.trail_activated = True
            if self.trail_activated:
                trail = high - trail_atr_mult * atr
                if self.stop_loss is None or trail > self.stop_loss:
                    self.stop_loss = trail
        else:
            if (self.avg_price - low) >= activate_at_r * atr:
                self.trail_activated = True
            if self.trail_activated:
                trail = low + trail_atr_mult * atr
                if self.stop_loss is None or trail < self.stop_loss:
                    self.stop_loss = trail


@dataclass
class ClosedTrade:
    symbol: str
    instrument_key: str
    side: str
    quantity: int
    entry_price: float
    exit_price: float
    entry_time: datetime
    exit_time: datetime
    pnl: float
    costs: float
    reason: str = ""


@dataclass
class Portfolio:
    initial_capital: float
    margin_pct: float = DEFAULT_INITIAL_MARGIN_PCT
    max_margin_utilization_pct: float = 0.85
    cost_model: FuturesCostModel = field(default_factory=FuturesCostModel)

    cash: float = field(init=False)
    positions: Dict[str, Position] = field(default_factory=dict)
    closed_trades: List[ClosedTrade] = field(default_factory=list)
    equity_curve: List[dict] = field(default_factory=list)
    costs: CostAccumulator = field(default_factory=CostAccumulator)
    marks: Dict[str, float] = field(default_factory=dict)

    def __post_init__(self) -> None:
        self.cash = float(self.initial_capital)

    def contract_value(self, price: float, quantity: int) -> float:
        return abs(float(price) * int(quantity))

    def margin_required(self, price: float, quantity: int) -> float:
        return self.contract_value(price, quantity) * self.margin_pct

    def used_margin(self) -> float:
        total = 0.0
        for pos in self.positions.values():
            mark = self.marks.get(pos.symbol, pos.avg_price)
            total += self.margin_required(mark, pos.quantity)
        return total

    def equity(self) -> float:
        mtm = 0.0
        for pos in self.positions.values():
            mark = self.marks.get(pos.symbol, pos.avg_price)
            mtm += pos.unrealized_pnl(mark)
        return self.cash + mtm

    def margin_utilization(self) -> float:
        eq = max(self.equity(), 1.0)
        return self.used_margin() / eq

    def can_open(self, price: float, quantity: int) -> bool:
        projected = self.used_margin() + self.margin_required(price, quantity)
        eq = max(self.equity(), 1.0)
        return (projected / eq) <= self.max_margin_utilization_pct

    def update_marks(self, snapshot: Dict[str, float], timestamp: datetime) -> None:
        self.marks.update(snapshot)
        self.equity_curve.append(
            {
                "timestamp": timestamp.isoformat(),
                "equity": round(self.equity(), 2),
                "cash": round(self.cash, 2),
                "used_margin": round(self.used_margin(), 2),
                "open_positions": len(self.positions),
            }
        )

    def apply_fill(
        self,
        fill: Fill,
        *,
        lot_size: int,
        stop_loss: Optional[float] = None,
        take_profit: Optional[float] = None,
        confidence: float = 0.0,
        reason: str = "",
        atr: Optional[float] = None,
    ) -> Optional[CostBreakdown]:
        slip_rupees = fill.slippage_per_unit * fill.quantity
        cost = self.cost_model.compute(
            side=fill.side.value,
            price=fill.price,
            quantity=fill.quantity,
            slippage_rupees=slip_rupees,
        )
        self.costs.add(cost)
        self.cash -= cost.total

        existing = self.positions.get(fill.symbol)
        # Opening or adding same direction
        if existing is None:
            if not self.can_open(fill.price, fill.quantity):
                # refund costs conceptually — reject after fill attempt shouldn't happen;
                # caller should check can_open first. Still record cost.
                return cost
            self.positions[fill.symbol] = Position(
                symbol=fill.symbol,
                instrument_key=fill.instrument_key,
                side=fill.side,
                quantity=fill.quantity,
                avg_price=fill.price,
                lot_size=lot_size,
                stop_loss=stop_loss,
                take_profit=take_profit,
                opened_at=fill.timestamp,
                confidence=confidence,
                atr=atr,
            )
            return cost

        # Closing / reducing opposite side
        if existing.side != fill.side:
            close_qty = min(existing.quantity, fill.quantity)
            pnl = (fill.price - existing.avg_price) * close_qty * existing.direction
            self.cash += pnl
            self.closed_trades.append(
                ClosedTrade(
                    symbol=fill.symbol,
                    instrument_key=fill.instrument_key,
                    side=existing.side.value,
                    quantity=close_qty,
                    entry_price=existing.avg_price,
                    exit_price=fill.price,
                    entry_time=existing.opened_at or fill.timestamp,
                    exit_time=fill.timestamp,
                    pnl=pnl - cost.total,
                    costs=cost.total,
                    reason=reason or "exit",
                )
            )
            remaining = existing.quantity - close_qty
            leftover = fill.quantity - close_qty
            if remaining <= 0:
                del self.positions[fill.symbol]
            else:
                existing.quantity = remaining
            if leftover > 0:
                # flip residual into new position
                self.positions[fill.symbol] = Position(
                    symbol=fill.symbol,
                    instrument_key=fill.instrument_key,
                    side=fill.side,
                    quantity=leftover,
                    avg_price=fill.price,
                    lot_size=lot_size,
                    stop_loss=stop_loss,
                    take_profit=take_profit,
                    opened_at=fill.timestamp,
                    confidence=confidence,
                    atr=atr,
                )
            return cost

        # Same side add
        total_qty = existing.quantity + fill.quantity
        existing.avg_price = (
            existing.avg_price * existing.quantity + fill.price * fill.quantity
        ) / total_qty
        existing.quantity = total_qty
        return cost

    def force_close_all(self, marks: Dict[str, float], timestamp: datetime, reason: str = "eod") -> None:
        for sym in list(self.positions.keys()):
            pos = self.positions[sym]
            px = marks.get(sym, pos.avg_price)
            exit_side = Side.SELL if pos.side == Side.BUY else Side.BUY
            fill = Fill(
                order_id=f"force-{sym}",
                symbol=sym,
                instrument_key=pos.instrument_key,
                side=exit_side,
                quantity=pos.quantity,
                price=px,
                timestamp=timestamp,
            )
            self.apply_fill(fill, lot_size=pos.lot_size, reason=reason)
