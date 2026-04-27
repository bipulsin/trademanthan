"""
Arbitrage Daily Setup Scheduler

Scheduled (Asia/Kolkata, Mon–Fri, not NSE holidays / weekends):
- 09:10 — primary daily run. Updates instruments + LTPs.
- 09:20 — only if 09:10 did not complete successfully the same day (safety backstop).

On-demand: POST /scan/arbitrage/daily-setup/run (and helper scripts) — does not
set the 09:10 success flag, so 09:20 logic is unchanged.

Tasks:
1) Updates instrument/symbol fields in arbitrage_master from instruments JSON.
2) After IST calendar day > 21 and until the front contract's expiry in that month,
   currmth_* / nextmth_* use the 2nd and 3rd upcoming expiries (roll window);
   otherwise the 1st and 2nd. Missing contracts leave those columns NULL.
3) Updates LTP columns using Upstox API by instrument key.
"""

import json
import logging
import os
import tempfile
import time
from datetime import datetime
from pathlib import Path
from typing import Dict, List, Literal, Optional, Tuple

from apscheduler.schedulers.background import BackgroundScheduler
from apscheduler.triggers.cron import CronTrigger
from sqlalchemy import inspect, text

from backend.config import get_instruments_file_path, settings
from backend.database import engine
from backend.services.fno_sector_mapping_csv import load_fno_sector_index_map
from backend.services.sector_movers import (
    equity_sector_index_instrument_key,
    normalize_sector_instrument_key,
)
from backend.services.upstox_service import UpstoxService
from backend.services.market_holiday import IST, should_skip_scheduled_market_jobs_ist

logger = logging.getLogger(__name__)

# Persist whether 09:10 succeeded today so 09:20 can be skipped.
_PROJ_LOGS = Path(__file__).resolve().parents[2] / "logs"
MORNING_STATE_FILE = _PROJ_LOGS / "arbitrage_daily_setup_morning_state.json"

_Execution = Optional[Literal["morning_910", "morning_920"]]


def _morning_state_path() -> Path:
    return MORNING_STATE_FILE


def _read_morning_state() -> Optional[Dict]:
    path = _morning_state_path()
    if not path.exists():
        return None
    try:
        return json.loads(path.read_text(encoding="utf-8"))
    except (json.JSONDecodeError, OSError) as e:
        logger.warning("arbitrage daily setup: could not read morning state: %s", e)
        return None


def _write_morning_state(data: Dict) -> None:
    path = _morning_state_path()
    path.parent.mkdir(parents=True, exist_ok=True)
    payload = json.dumps(data, ensure_ascii=True, sort_keys=True)
    fd, tmp = tempfile.mkstemp(
        dir=str(path.parent), prefix="arbitrage_morning_", suffix=".json.tmp", text=True
    )
    try:
        with os.fdopen(fd, "w", encoding="utf-8") as f:
            f.write(payload)
        os.replace(tmp, path)
    except OSError as e:
        logger.warning("arbitrage daily setup: could not write morning state: %s", e)
        try:
            os.unlink(tmp)
        except OSError:
            pass


def _today_ist_str() -> str:
    return datetime.now(IST).strftime("%Y-%m-%d")


def _morning_910_flag(st: Optional[Dict]) -> Optional[bool]:
    if not st:
        return None
    for key in ("morning_910_ok", "daily_setup_ok", "slot_910_ok"):
        if key in st:
            return st.get(key) is True
    return None


def _morning_910_succeeded_today_ist() -> bool:
    """True only if today's 09:10 scheduled run completed successfully (IST date)."""
    st = _read_morning_state()
    if not st:
        return False
    if st.get("trading_date_ist") != _today_ist_str():
        return False
    return _morning_910_flag(st) is True


def _set_morning_910_state(ok: bool) -> None:
    _write_morning_state(
        {
            "trading_date_ist": _today_ist_str(),
            "morning_910_ok": ok,
            "recorded_at_ist": datetime.now(IST).strftime("%Y-%m-%d %H:%M:%S"),
        }
    )


class ArbitrageDailySetupScheduler:
    def __init__(self) -> None:
        self.scheduler = BackgroundScheduler(timezone="Asia/Kolkata")
        self.is_running = False

    def start(self) -> None:
        if self.is_running:
            logger.info("Arbitrage daily setup scheduler already running")
            return

        self.scheduler.add_job(
            self._run_morning_910,
            trigger=CronTrigger(day_of_week="mon-fri", hour=9, minute=10, timezone="Asia/Kolkata"),
            id="arbitrage_dailySetup_910",
            name="Arbitrage Daily Setup 09:10",
            replace_existing=True,
            max_instances=1,
            coalesce=True,
        )
        self.scheduler.add_job(
            self._run_morning_920,
            trigger=CronTrigger(day_of_week="mon-fri", hour=9, minute=20, timezone="Asia/Kolkata"),
            id="arbitrage_dailySetup_920",
            name="Arbitrage Daily Setup 09:20",
            replace_existing=True,
            max_instances=1,
            coalesce=True,
        )
        self.scheduler.start()
        self.is_running = True
        logger.info(
            "Arbitrage daily setup scheduler started (09:10 primary, 09:20 backstop, Asia/Kolkata, Mon–Fri)"
        )

    def stop(self) -> None:
        if not self.is_running:
            return
        try:
            self.scheduler.shutdown()
        finally:
            self.is_running = False
            logger.info("Arbitrage daily setup scheduler stopped")

    def run_now(self) -> Dict:
        return run_arbitrage_daily_setup(execution=None)

    def _run_morning_910(self) -> None:
        if should_skip_scheduled_market_jobs_ist():
            logger.debug("IST non-trading day — skip arbitrage daily setup 09:10")
            return
        try:
            out = run_arbitrage_daily_setup(execution="morning_910")
            if out.get("success"):
                _set_morning_910_state(True)
                logger.info("arbitrage daily setup 09:10 completed: %s", out)
            else:
                _set_morning_910_state(False)
                logger.error("arbitrage daily setup 09:10 reported failure: %s", out)
        except Exception as exc:
            _set_morning_910_state(False)
            logger.error("arbitrage daily setup 09:10 failed: %s", exc, exc_info=True)

    def _run_morning_920(self) -> None:
        if should_skip_scheduled_market_jobs_ist():
            logger.debug("IST non-trading day — skip arbitrage daily setup 09:20")
            return
        if _morning_910_succeeded_today_ist():
            logger.info("arbitrage daily setup 09:20 skipped (09:10 already succeeded today)")
            return
        try:
            out = run_arbitrage_daily_setup(execution="morning_920")
            if out.get("success"):
                logger.info("arbitrage daily setup 09:20 completed: %s", out)
            else:
                logger.error("arbitrage daily setup 09:20 reported failure: %s", out)
        except Exception as exc:
            logger.error("arbitrage daily setup 09:20 failed: %s", exc, exc_info=True)

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


def _futures_roll_window_ist(ist_now: datetime, first_upcoming: Optional[Dict]) -> bool:
    """
    After the 21st (IST) through the end of the front contract's listing month, and before
    that contract expires, use the 2nd/3rd serial expiries (skip the about-to-expire month).
    """
    if not first_upcoming:
        return False
    exp_ms = int(first_upcoming.get("expiry") or 0)
    if exp_ms <= 0:
        return False
    if ist_now.tzinfo is None:
        ist_now = IST.localize(ist_now)
    else:
        ist_now = ist_now.astimezone(IST)
    first_exp_ist = datetime.fromtimestamp(exp_ms / 1000.0, tz=IST)
    if ist_now.day <= 21:
        return False
    if (ist_now.year, ist_now.month) != (first_exp_ist.year, first_exp_ist.month):
        return False
    return ist_now < first_exp_ist


def _pick_current_next_futures(
    contracts: List[Dict], *, apply_roll_window: bool = True
) -> Tuple[Optional[Dict], Optional[Dict]]:
    """
    Select currmth / nextmth from not-yet-expired FUTs only. No fallback to fully expired
    chains — leave blank (None) if nothing qualifies.
    In the roll window (day > 21 IST, same month as front expiry, before expiry), cur/nm are
    the 2nd and 3rd upcoming; otherwise the 1st and 2nd.
    """
    if not contracts:
        return None, None

    now_ms = int(time.time() * 1000)
    sorted_c = sorted(contracts, key=lambda x: (x.get("expiry") or 0))
    upcoming = [c for c in sorted_c if (c.get("expiry") or 0) >= now_ms]
    if not upcoming:
        return None, None

    ist_now = datetime.now(IST)
    if ist_now.tzinfo is None:
        ist_now = IST.localize(ist_now)
    else:
        ist_now = ist_now.astimezone(IST)

    first = upcoming[0]
    if apply_roll_window and _futures_roll_window_ist(ist_now, first):
        if len(upcoming) < 2:
            return None, None
        cur = upcoming[1]
        nxt = upcoming[2] if len(upcoming) >= 3 else None
        return cur, nxt

    cur = upcoming[0]
    nxt = upcoming[1] if len(upcoming) >= 2 else None
    return cur, nxt


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


def run_arbitrage_daily_setup(execution: _Execution = None) -> Dict:
    """
    execution: None = on-demand / script; no morning state side effects here (state is set by scheduler).
    """
    try:
        return _run_arbitrage_daily_setup_impl(execution=execution)
    except Exception as e:
        logger.error("run_arbitrage_daily_setup failed: %s", e, exc_info=True)
        return {
            "success": False,
            "job_name": "arbitrage_dailySetup",
            "error": str(e),
            "execution": execution,
            "updated_at_ist": datetime.now(IST).strftime("%Y-%m-%d %H:%M:%S"),
        }


def _run_arbitrage_daily_setup_impl(execution: _Execution = None) -> Dict:
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
            # Rollover (skip-front after 21st until expiry) is intentionally morning-flow only.
            apply_roll_window = execution in ("morning_910", "morning_920")
            current_fut, next_fut = _pick_current_next_futures(
                fut_map.get(stock, []),
                apply_roll_window=apply_roll_window,
            )
            sym_u = str(stock or "").strip().upper()
            sector_idx = normalize_sector_instrument_key(
                fno_sector_map.get(sym_u) or equity_sector_index_instrument_key(stock)
            )
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
        "execution": execution,
        "updated_at_ist": datetime.now(IST).strftime("%Y-%m-%d %H:%M:%S"),
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


def get_morning_state_summary() -> Dict:
    st = _read_morning_state()
    return {
        "state_file": str(_morning_state_path()),
        "trading_date_ist": st.get("trading_date_ist") if st else None,
        "morning_910_ok_today": _morning_910_succeeded_today_ist(),
        "recorded_at_ist": st.get("recorded_at_ist") if st else None,
    }


arbitrage_daily_setup_scheduler = ArbitrageDailySetupScheduler()


def start_arbitrage_daily_setup_scheduler() -> None:
    arbitrage_daily_setup_scheduler.start()


def stop_arbitrage_daily_setup_scheduler() -> None:
    arbitrage_daily_setup_scheduler.stop()


def run_arbitrage_daily_setup_now() -> Dict:
    """On-demand run; does not participate in morning-state success tracking."""
    return run_arbitrage_daily_setup(execution=None)

