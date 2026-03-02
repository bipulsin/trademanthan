import json
from pathlib import Path
from typing import Dict

from fastapi import APIRouter, HTTPException
from sqlalchemy import text

from backend.config import get_instruments_file_path
from backend.database import engine

from backend.services.arbitrage_daily_setup_scheduler import (
    arbitrage_daily_setup_scheduler,
    run_arbitrage_daily_setup_now,
)

router = APIRouter(prefix="/scan/arbitrage", tags=["arbitrage"])


def _ensure_arbitrage_order_table() -> None:
    with engine.begin() as conn:
        conn.execute(
            text(
                """
                CREATE TABLE IF NOT EXISTS arbiitrage_order (
                    id BIGSERIAL PRIMARY KEY,
                    stock TEXT NOT NULL,
                    stock_instrument_key TEXT NOT NULL,
                    currmth_future_symbol TEXT NOT NULL,
                    currmth_future_instrument_key TEXT NOT NULL,
                    buy_cost NUMERIC(16,4) NOT NULL,
                    buy_exit_cost NUMERIC(16,4),
                    current_future_state TEXT NOT NULL DEFAULT 'BUY',
                    nextmth_future_symbol TEXT NOT NULL,
                    nextmth_future_instrement_key TEXT NOT NULL,
                    sell_cost NUMERIC(16,4) NOT NULL,
                    sell_exit_cost NUMERIC(16,4),
                    nextmth_future_state TEXT NOT NULL DEFAULT 'SELL',
                    quantity INTEGER NOT NULL,
                    trade_status TEXT NOT NULL DEFAULT 'OPEN',
                    trade_entry_value NUMERIC(18,4) NOT NULL,
                    trade_entry_time TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP,
                    trade_exit_time TIMESTAMP,
                    trade_exit_value NUMERIC(18,4)
                )
                """
            )
        )
        conn.execute(
            text(
                """
                CREATE INDEX IF NOT EXISTS idx_arbiitrage_order_stock_trade_status
                ON arbiitrage_order (stock_instrument_key, trade_status)
                """
            )
        )
        conn.execute(
            text(
                """
                CREATE UNIQUE INDEX IF NOT EXISTS uq_arbiitrage_order_open_stock
                ON arbiitrage_order (stock_instrument_key)
                WHERE trade_status = 'OPEN'
                """
            )
        )


def _load_quantity_by_instrument_key() -> Dict[str, int]:
    instruments_file: Path = get_instruments_file_path()
    if not instruments_file.exists():
        raise FileNotFoundError(f"Instruments file not found: {instruments_file}")
    with instruments_file.open("r", encoding="utf-8") as f:
        instruments = json.load(f)
    quantity_map: Dict[str, int] = {}
    for inst in instruments if isinstance(instruments, list) else []:
        if not isinstance(inst, dict):
            continue
        key = inst.get("instrument_key")
        lot = inst.get("lot_size")
        if key and lot is not None:
            try:
                quantity_map[key] = int(float(lot))
            except (TypeError, ValueError):
                continue
    return quantity_map


@router.post("/daily-setup/run")
async def run_daily_setup_now():
    """
    On-demand run of arbitrage_dailySetup job.
    """
    try:
        result = run_arbitrage_daily_setup_now()
        return result
    except Exception as exc:
        raise HTTPException(status_code=500, detail=f"Failed to run arbitrage_dailySetup: {exc}")


@router.get("/daily-setup/status")
async def get_daily_setup_status():
    """
    Get scheduler status for arbitrage_dailySetup job.
    """
    try:
        status = arbitrage_daily_setup_scheduler.get_status()
        status["job_name"] = "arbitrage_dailySetup"
        status["schedule"] = "09:16 Asia/Kolkata (daily)"
        return status
    except Exception as exc:
        raise HTTPException(status_code=500, detail=f"Failed to get scheduler status: {exc}")


@router.get("/selection")
async def get_arbitrage_selection():
    """
    Fetch arbitrage selection rows with filter:
    - currmth_future_ltp < stock_ltp
    - nextmth_future_ltp <= currmth_future_ltp
    - nextmth_future_ltp within 3 points below currmth_future_ltp
    """
    try:
        _ensure_arbitrage_order_table()
        with engine.begin() as conn:
            rows = conn.execute(
                text(
                    """
                    SELECT
                        stock,
                        stock_instrument_key,
                        stock_ltp,
                        currmth_future_symbol,
                        currmth_future_instrument_key,
                        currmth_future_ltp,
                        nextmth_future_symbol,
                        nextmth_future_instrement_key,
                        nextmth_future_ltp,
                        EXISTS (
                            SELECT 1
                            FROM arbiitrage_order ao
                            WHERE ao.stock_instrument_key = arbitrage_master.stock_instrument_key
                              AND ao.trade_status = 'OPEN'
                        ) AS has_open_order
                    FROM arbitrage_master
                    WHERE
                        stock_ltp IS NOT NULL
                        AND currmth_future_ltp IS NOT NULL
                        AND nextmth_future_ltp IS NOT NULL
                        AND currmth_future_ltp < stock_ltp
                        AND nextmth_future_ltp <= currmth_future_ltp
                        AND nextmth_future_ltp >= (currmth_future_ltp - 3)
                    ORDER BY stock ASC
                    """
                )
            ).mappings().all()

        return {
            "success": True,
            "count": len(rows),
            "rows": [dict(row) for row in rows],
        }
    except Exception as exc:
        raise HTTPException(status_code=500, detail=f"Failed to fetch arbitrage selection: {exc}")


@router.post("/order")
async def place_arbitrage_order(payload: dict):
    """
    Insert arbitrage order entry in arbiitrage_order for a given stock_instrument_key.
    One OPEN order per stock_instrument_key is allowed.
    """
    stock_instrument_key = (payload or {}).get("stock_instrument_key")
    if not stock_instrument_key:
        raise HTTPException(status_code=400, detail="stock_instrument_key is required")

    try:
        _ensure_arbitrage_order_table()
        quantity_by_key = _load_quantity_by_instrument_key()
        with engine.begin() as conn:
            open_exists = conn.execute(
                text(
                    """
                    SELECT 1
                    FROM arbiitrage_order
                    WHERE stock_instrument_key = :stock_instrument_key
                      AND trade_status = 'OPEN'
                    LIMIT 1
                    """
                ),
                {"stock_instrument_key": stock_instrument_key},
            ).fetchone()
            if open_exists:
                raise HTTPException(status_code=409, detail="Open order already exists for this stock")

            master_row = conn.execute(
                text(
                    """
                    SELECT
                        stock,
                        stock_instrument_key,
                        currmth_future_symbol,
                        currmth_future_instrument_key,
                        currmth_future_ltp,
                        nextmth_future_symbol,
                        nextmth_future_instrement_key,
                        nextmth_future_ltp
                    FROM arbitrage_master
                    WHERE stock_instrument_key = :stock_instrument_key
                    LIMIT 1
                    """
                ),
                {"stock_instrument_key": stock_instrument_key},
            ).mappings().first()
            if not master_row:
                raise HTTPException(status_code=404, detail="No arbitrage master row found for stock key")

            required_fields = [
                "stock",
                "stock_instrument_key",
                "currmth_future_symbol",
                "currmth_future_instrument_key",
                "currmth_future_ltp",
                "nextmth_future_symbol",
                "nextmth_future_instrement_key",
                "nextmth_future_ltp",
            ]
            missing = [f for f in required_fields if master_row.get(f) in (None, "")]
            if missing:
                raise HTTPException(status_code=400, detail=f"Missing required arbitrage data: {', '.join(missing)}")

            quantity = quantity_by_key.get(master_row["currmth_future_instrument_key"])
            if not quantity or quantity <= 0:
                raise HTTPException(
                    status_code=400,
                    detail="Unable to determine quantity (lot_size) from instruments JSON for current month future key",
                )

            buy_cost = float(master_row["currmth_future_ltp"])
            sell_cost = float(master_row["nextmth_future_ltp"])
            trade_entry_value = (sell_cost - buy_cost) * quantity

            conn.execute(
                text(
                    """
                    INSERT INTO arbiitrage_order (
                        stock,
                        stock_instrument_key,
                        currmth_future_symbol,
                        currmth_future_instrument_key,
                        buy_cost,
                        buy_exit_cost,
                        current_future_state,
                        nextmth_future_symbol,
                        nextmth_future_instrement_key,
                        sell_cost,
                        sell_exit_cost,
                        nextmth_future_state,
                        quantity,
                        trade_status,
                        trade_entry_value,
                        trade_entry_time,
                        trade_exit_time,
                        trade_exit_value
                    ) VALUES (
                        :stock,
                        :stock_instrument_key,
                        :currmth_future_symbol,
                        :currmth_future_instrument_key,
                        :buy_cost,
                        NULL,
                        'BUY',
                        :nextmth_future_symbol,
                        :nextmth_future_instrement_key,
                        :sell_cost,
                        NULL,
                        'SELL',
                        :quantity,
                        'OPEN',
                        :trade_entry_value,
                        CURRENT_TIMESTAMP,
                        NULL,
                        NULL
                    )
                    """
                ),
                {
                    "stock": master_row["stock"],
                    "stock_instrument_key": master_row["stock_instrument_key"],
                    "currmth_future_symbol": master_row["currmth_future_symbol"],
                    "currmth_future_instrument_key": master_row["currmth_future_instrument_key"],
                    "buy_cost": buy_cost,
                    "nextmth_future_symbol": master_row["nextmth_future_symbol"],
                    "nextmth_future_instrement_key": master_row["nextmth_future_instrement_key"],
                    "sell_cost": sell_cost,
                    "quantity": quantity,
                    "trade_entry_value": trade_entry_value,
                },
            )

        return {
            "success": True,
            "message": f"Order placed for {master_row['stock']}",
            "stock_instrument_key": stock_instrument_key,
            "quantity": quantity,
            "trade_entry_value": round(trade_entry_value, 4),
        }
    except HTTPException:
        raise
    except Exception as exc:
        raise HTTPException(status_code=500, detail=f"Failed to place arbitrage order: {exc}")

