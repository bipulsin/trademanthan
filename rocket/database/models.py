"""SQLAlchemy model + query helpers for ``arbitrage_master`` current-month futures."""

from __future__ import annotations

import json
import logging
from dataclasses import dataclass
from pathlib import Path
from typing import Dict, List, Optional

from sqlalchemy import String, Text, select
from sqlalchemy.orm import DeclarativeBase, Mapped, Session, mapped_column

from rocket.config.constants import DEFAULT_LOT_SIZE, DEFAULT_TICK_SIZE
from rocket.config.settings import get_settings

logger = logging.getLogger(__name__)


class Base(DeclarativeBase):
    pass


class ArbitrageMaster(Base):
    """Subset of production ``arbitrage_master`` columns used by Rocket."""

    __tablename__ = "arbitrage_master"

    stock: Mapped[str] = mapped_column(Text, primary_key=True)
    stock_instrument_key: Mapped[Optional[str]] = mapped_column(Text, nullable=True)
    currmth_future_symbol: Mapped[Optional[str]] = mapped_column(Text, nullable=True)
    currmth_future_instrument_key: Mapped[Optional[str]] = mapped_column(String, nullable=True)
    nextmth_future_symbol: Mapped[Optional[str]] = mapped_column(Text, nullable=True)
    sector_index: Mapped[Optional[str]] = mapped_column(Text, nullable=True)


@dataclass(frozen=True)
class FuturesContract:
    symbol: str
    instrument_key: str
    futures_symbol: str
    lot_size: int
    tick_size: float
    sector: str = ""


def _lot_tick_lookup(instruments_path: Path) -> Dict[str, tuple[int, float]]:
    """Map instrument_key → (lot_size, tick_size) from nse_instruments.json."""
    out: Dict[str, tuple[int, float]] = {}
    if not instruments_path.exists():
        logger.warning("instruments file missing: %s", instruments_path)
        return out
    try:
        rows = json.loads(instruments_path.read_text(encoding="utf-8"))
    except Exception as exc:
        logger.error("failed loading instruments: %s", exc)
        return out
    for row in rows if isinstance(rows, list) else []:
        if not isinstance(row, dict):
            continue
        ik = str(row.get("instrument_key") or "").strip()
        if not ik:
            continue
        lot = int(float(row.get("lot_size") or row.get("quantity") or DEFAULT_LOT_SIZE))
        tick = float(row.get("tick_size") or row.get("tickSize") or DEFAULT_TICK_SIZE)
        out[ik] = (max(1, lot), tick if tick > 0 else DEFAULT_TICK_SIZE)
    return out


def load_active_current_month_contracts(
    session: Session,
    *,
    limit: int = 200,
    instruments_path: Optional[Path] = None,
) -> List[FuturesContract]:
    """
    Active current-month NSE stock futures from ``arbitrage_master``.

    ``lot_size`` / ``tick_size`` are not on the table — resolved from instruments JSON
    with standard defaults as fallback.
    """
    path = instruments_path or get_settings().rocket_instruments_path
    lookup = _lot_tick_lookup(path)
    stmt = (
        select(ArbitrageMaster)
        .where(ArbitrageMaster.currmth_future_instrument_key.is_not(None))
        .where(ArbitrageMaster.currmth_future_instrument_key != "")
        .order_by(ArbitrageMaster.stock)
        .limit(int(limit))
    )
    rows = session.execute(stmt).scalars().all()
    contracts: List[FuturesContract] = []
    seen: set[str] = set()
    for row in rows:
        sym = str(row.stock or "").strip().upper()
        ik = str(row.currmth_future_instrument_key or "").strip()
        if not sym or not ik or ik in seen:
            continue
        seen.add(ik)
        lot, tick = lookup.get(ik, (DEFAULT_LOT_SIZE, DEFAULT_TICK_SIZE))
        contracts.append(
            FuturesContract(
                symbol=sym,
                instrument_key=ik,
                futures_symbol=str(row.currmth_future_symbol or "").strip() or sym,
                lot_size=lot,
                tick_size=tick,
                sector=str(row.sector_index or "").strip(),
            )
        )
    logger.info("Loaded %s current-month futures contracts", len(contracts))
    return contracts
