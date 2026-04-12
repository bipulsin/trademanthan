"""
Arbitrage Daily Setup Scheduler

Runs every 15 minutes on trading days (Asia/Kolkata):
- 09:15, 09:30, 09:45
- 10:00 through 15:45 (every 15 minutes)

Tasks:
1) Updates instrument/symbol fields in arbitrage_master from instruments JSON.
2) Updates LTP columns using Upstox API by instrument key.

Also supports on-demand execution via API.
"""

import json
import logging
from datetime import datetime
from pathlib import Path
from typing import Dict, List, Optional, Tuple

from apscheduler.schedulers.background import BackgroundScheduler
from apscheduler.triggers.cron import CronTrigger
from sqlalchemy import inspect, text

from backend.config import get_instruments_file_path, settings
from backend.database import engine
from backend.services.fno_sector_mapping_csv import load_fno_sector_index_map
from backend.services.sector_movers import equity_sector_index_instrument_key
from backend.services.upstox_service import UpstoxService

logger = logging.getLogger(__name__)


class ArbitrageDailySetupScheduler:
    def __init__(self) -> None:
        self.scheduler = BackgroundScheduler(timezone="Asia/Kolkata")
        self.is_running = False

    def start(self) -> None:
        if self.is_running:
            logger.info("Arbitrage daily setup scheduler already running")
            return

        self.scheduler.add_job(
            self.run_job,
            trigger=CronTrigger(day_of_week="mon-fri", hour=9, minute="15,30,45", timezone="Asia/Kolkata"),
            id="arbitrage_dailySetup",
            name="Arbitrage Daily Setup",
            replace_existing=True,
            max_instances=1,
            coalesce=True,
        )
        self.scheduler.add_job(
            self.run_job,
            trigger=CronTrigger(day_of_week="mon-fri", hour="10-15", minute="0,15,30,45", timezone="Asia/Kolkata"),
            id="arbitrage_dailySetup_intraday",
            name="Arbitrage Daily Setup Intraday",
            replace_existing=True,
            max_instances=1,
            coalesce=True,
        )
        self.scheduler.start()
        self.is_running = True
        logger.info("Arbitrage daily setup scheduler started (every 15 min, 09:15-15:45 Asia/Kolkata, Mon-Fri)")

    def stop(self) -> None:
        if not self.is_running:
            return
        try:
            self.scheduler.shutdown()
        finally:
            self.is_running = False
            logger.info("Arbitrage daily setup scheduler stopped")

    def run_now(self) -> Dict:
        return run_arbitrage_daily_setup()

    def run_job(self) -> None:
        try:
            result = run_arbitrage_daily_setup()
            logger.info(f"arbitrage_dailySetup completed: {result}")
        except Exception as exc:
            logger.error(f"arbitrage_dailySetup failed: {exc}", exc_info=True)

    def get_status(self) -> Dict:
        jobs = self.scheduler.get_jobs() if self.is_running else []
        return {
            "is_running": self.is_running and self.scheduler.running,
            "jobs_count": len(jobs),
            "jobs": [
                {
                    "id": job.id,
                    "name": job.name,
                    "next_run_time": str(job.next_run_time) if job.next_run_time else None,
                }
                for job in jobs
            ],
        }


def _ensure_arbitrage_sector_index_column() -> None:
    """Idempotent: add sector_index if missing (covers tables created before migration)."""
    try:
        insp = inspect(engine)
        if "arbitrage_master" not in insp.get_table_names():
            return
        cols = {c["name"] for c in insp.get_columns("arbitrage_master")}
        if "sector_index" in cols:
            return
        with engine.begin() as conn:
            conn.execute(text("ALTER TABLE arbitrage_master ADD COLUMN sector_index TEXT"))
        logger.info("arbitrage_master: added column sector_index")
    except Exception as e:
        logger.warning("arbitrage_master sector_index ensure failed: %s", e)


def _ensure_arbitrage_table() -> None:
    create_sql = text(
        """
        CREATE TABLE IF NOT EXISTS arbitrage_master (
            stock TEXT PRIMARY KEY,
            stock_instrument_key TEXT,
            stock_ltp NUMERIC,
            currmth_future_symbol TEXT,
            currmth_future_instrument_key TEXT,
            currmth_future_ltp NUMERIC,
            nextmth_future_symbol TEXT,
            nextmth_future_instrement_key TEXT,
            nextmth_future_ltp NUMERIC,
            sector_index TEXT
        )
        """
    )
    with engine.begin() as conn:
        conn.execute(create_sql)


def _load_instruments() -> List[Dict]:
    instruments_file: Path = get_instruments_file_path()
    if not instruments_file.exists():
        raise FileNotFoundError(f"Instruments file not found: {instruments_file}")
    with instruments_file.open("r", encoding="utf-8") as f:
        data = json.load(f)
    if not isinstance(data, list):
        raise ValueError("Instruments JSON format invalid: expected list")
    return data


def _build_mappings(instruments: List[Dict]) -> Tuple[Dict[str, str], Dict[str, List[Dict]]]:
    eq_map: Dict[str, str] = {}
    fut_map: Dict[str, List[Dict]] = {}

    for inst in instruments:
        if not isinstance(inst, dict):
            continue

        segment = inst.get("segment")
        inst_type = inst.get("instrument_type")
        trading_symbol = (inst.get("trading_symbol") or "").strip()

        if segment == "NSE_EQ" and trading_symbol:
            eq_map[trading_symbol] = inst.get("instrument_key")

        if segment == "NSE_FO" and inst_type == "FUT" and " FUT " in trading_symbol:
            symbol = trading_symbol.split(" FUT ", 1)[0].strip()
            fut_map.setdefault(symbol, []).append(inst)

    for symbol, contracts in fut_map.items():
        contracts.sort(key=lambda x: x.get("expiry") or 0)

    return eq_map, fut_map


def _pick_current_next_futures(contracts: List[Dict]) -> Tuple[Optional[Dict], Optional[Dict]]:
    if not contracts:
        return None, None

    now_ms = int(datetime.utcnow().timestamp() * 1000)
    upcoming = [c for c in contracts if (c.get("expiry") or 0) >= now_ms]
    source = upcoming if len(upcoming) >= 2 else contracts

    first = source[0] if len(source) >= 1 else None
    second = source[1] if len(source) >= 2 else None
    return first, second


def _get_close_price(upstox: UpstoxService, instrument_key: Optional[str], cache: Dict[str, Optional[float]]) -> Optional[float]:
    if not instrument_key:
        return None
    if instrument_key in cache:
        return cache[instrument_key]

    close_price: Optional[float] = None
    try:
        ohlc = upstox.get_ohlc_data(instrument_key) or {}
        close_val = ohlc.get("close")
        if close_val is not None:
            close_price = float(close_val)
    except Exception:
        logger.warning(f"OHLC fetch failed for {instrument_key}", exc_info=True)

    if close_price is None:
        try:
            quote = upstox.get_market_quote_by_key(instrument_key) or {}
            quote_close = quote.get("close")
            if quote_close is not None:
                close_price = float(quote_close)
            elif quote.get("last_price") is not None:
                close_price = float(quote.get("last_price"))
        except Exception:
            logger.warning(f"Quote fetch failed for {instrument_key}", exc_info=True)

    cache[instrument_key] = close_price
    return close_price


def run_arbitrage_daily_setup() -> Dict:
    _ensure_arbitrage_table()
    _ensure_arbitrage_sector_index_column()

    instruments = _load_instruments()
    eq_map, fut_map = _build_mappings(instruments)

    with engine.begin() as conn:
        stocks = [
            row[0]
            for row in conn.execute(
                text(
                    """
                    SELECT stock
                    FROM arbitrage_master
                    WHERE stock IS NOT NULL AND TRIM(stock) <> ''
                    ORDER BY stock
                    """
                )
            ).fetchall()
        ]

        fno_sector_map = load_fno_sector_index_map()
        metadata_updates: List[Dict] = []
        for stock in stocks:
            stock_key = eq_map.get(stock)
            current_fut, next_fut = _pick_current_next_futures(fut_map.get(stock, []))
            sym_u = str(stock or "").strip().upper()
            sector_idx = fno_sector_map.get(sym_u) or equity_sector_index_instrument_key(stock)
            metadata_updates.append(
                {
                    "stock": stock,
                    "stock_key": stock_key,
                    "cm_symbol": current_fut.get("trading_symbol") if current_fut else None,
                    "cm_key": current_fut.get("instrument_key") if current_fut else None,
                    "nm_symbol": next_fut.get("trading_symbol") if next_fut else None,
                    "nm_key": next_fut.get("instrument_key") if next_fut else None,
                    "sector_index": sector_idx,
                }
            )

        if metadata_updates:
            conn.execute(
                text(
                    """
                    UPDATE arbitrage_master
                    SET
                        stock_instrument_key = :stock_key,
                        currmth_future_symbol = :cm_symbol,
                        currmth_future_instrument_key = :cm_key,
                        nextmth_future_symbol = :nm_symbol,
                        nextmth_future_instrement_key = :nm_key,
                        sector_index = :sector_index
                    WHERE stock = :stock
                    """
                ),
                metadata_updates,
            )

        upstox = UpstoxService(settings.UPSTOX_API_KEY, settings.UPSTOX_API_SECRET)
        price_cache: Dict[str, Optional[float]] = {}
        price_updates: List[Dict] = []

        refreshed_rows = conn.execute(
            text(
                """
                SELECT
                    stock,
                    stock_instrument_key,
                    currmth_future_instrument_key,
                    nextmth_future_instrement_key
                FROM arbitrage_master
                ORDER BY stock
                """
            )
        ).fetchall()

        for stock, stock_key, cm_key, nm_key in refreshed_rows:
            price_updates.append(
                {
                    "stock": stock,
                    "stock_ltp": _get_close_price(upstox, stock_key, price_cache),
                    "cm_ltp": _get_close_price(upstox, cm_key, price_cache),
                    "nm_ltp": _get_close_price(upstox, nm_key, price_cache),
                }
            )

        if price_updates:
            conn.execute(
                text(
                    """
                    UPDATE arbitrage_master
                    SET
                        stock_ltp = :stock_ltp,
                        currmth_future_ltp = :cm_ltp,
                        nextmth_future_ltp = :nm_ltp
                    WHERE stock = :stock
                    """
                ),
                price_updates,
            )

        total_rows = conn.execute(text('SELECT COUNT(*) FROM arbitrage_master')).scalar() or 0
        populated_stock_key = conn.execute(
            text("SELECT COUNT(*) FROM arbitrage_master WHERE stock_instrument_key IS NOT NULL")
        ).scalar() or 0
        populated_curr_key = conn.execute(
            text("SELECT COUNT(*) FROM arbitrage_master WHERE currmth_future_instrument_key IS NOT NULL")
        ).scalar() or 0
        populated_next_key = conn.execute(
            text("SELECT COUNT(*) FROM arbitrage_master WHERE nextmth_future_instrement_key IS NOT NULL")
        ).scalar() or 0
        populated_stock_ltp = conn.execute(
            text("SELECT COUNT(*) FROM arbitrage_master WHERE stock_ltp IS NOT NULL")
        ).scalar() or 0
        populated_curr_ltp = conn.execute(
            text("SELECT COUNT(*) FROM arbitrage_master WHERE currmth_future_ltp IS NOT NULL")
        ).scalar() or 0
        populated_next_ltp = conn.execute(
            text("SELECT COUNT(*) FROM arbitrage_master WHERE nextmth_future_ltp IS NOT NULL")
        ).scalar() or 0
        populated_sector_index = conn.execute(
            text("SELECT COUNT(*) FROM arbitrage_master WHERE sector_index IS NOT NULL AND TRIM(sector_index) <> ''")
        ).scalar() or 0

    return {
        "success": True,
        "job_name": "arbitrage_dailySetup",
        "updated_at_ist": datetime.now().strftime("%Y-%m-%d %H:%M:%S"),
        "total_rows": int(total_rows),
        "populated": {
            "Stock_Instrument_key": int(populated_stock_key),
            "CurrMth_Future_Instrument_key": int(populated_curr_key),
            "NextMth_Future_Instrement_Key": int(populated_next_key),
            "Stock_LTP": int(populated_stock_ltp),
            "CurrMth_Future_LTP": int(populated_curr_ltp),
            "NextMth_Future_LTP": int(populated_next_ltp),
            "Sector_Index": int(populated_sector_index),
        },
    }


arbitrage_daily_setup_scheduler = ArbitrageDailySetupScheduler()


def start_arbitrage_daily_setup_scheduler() -> None:
    arbitrage_daily_setup_scheduler.start()


def stop_arbitrage_daily_setup_scheduler() -> None:
    arbitrage_daily_setup_scheduler.stop()


def run_arbitrage_daily_setup_now() -> Dict:
    return arbitrage_daily_setup_scheduler.run_now()

