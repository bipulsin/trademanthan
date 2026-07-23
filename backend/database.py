import logging
import os
from contextlib import contextmanager
from typing import Any, Dict, Iterator, Optional

import backend.env_bootstrap  # noqa: F401 — load `<project_root>/.env` before os.getenv

from sqlalchemy import create_engine, inspect, text
from sqlalchemy.orm import Session, sessionmaker

# Database configuration - PostgreSQL production database (configurable via DATABASE_URL env var)
DATABASE_URL = os.getenv("DATABASE_URL", "postgresql://trademanthan:trademanthan123@localhost/trademanthan")
DB_POOL_SIZE = int(os.getenv("DB_POOL_SIZE", "20"))
DB_MAX_OVERFLOW = int(os.getenv("DB_MAX_OVERFLOW", "40"))
DB_POOL_TIMEOUT_SEC = int(os.getenv("DB_POOL_TIMEOUT_SEC", "60"))
DB_POOL_WARN_CHECKED_OUT = int(
    os.getenv("DB_POOL_WARN_CHECKED_OUT", str(max(1, int((DB_POOL_SIZE + DB_MAX_OVERFLOW) * 0.75))))
)

# Import Base from models.base to ensure all models use the same Base instance
from backend.models.base import Base

# Initialize engine and session factory
engine = None
SessionLocal = None

try:
    # Create engine with proper configuration to prevent rollbacks
    engine_kwargs = {
        "pool_pre_ping": True,  # Verify connections before use
        "pool_recycle": 3600,   # Recycle connections every hour
        "echo": False,           # Set to True for SQL query logging
    }
    
    # Only add isolation_level for PostgreSQL
    if "postgresql" in DATABASE_URL:
        engine_kwargs["isolation_level"] = "READ_COMMITTED"
        engine_kwargs["pool_size"] = max(1, DB_POOL_SIZE)
        engine_kwargs["max_overflow"] = max(0, DB_MAX_OVERFLOW)
        engine_kwargs["pool_timeout"] = max(1, DB_POOL_TIMEOUT_SEC)
    
    engine = create_engine(DATABASE_URL, **engine_kwargs)
    SessionLocal = sessionmaker(
        autocommit=False, 
        autoflush=False, 
        bind=engine,
        expire_on_commit=False  # Prevent objects from expiring after commit
    )
except Exception as e:
    print(f"Warning: Database connection failed during import: {e}")
    print("Database will be initialized when create_tables() is called")

def get_db_pool_stats() -> Dict[str, Any]:
    """Snapshot SQLAlchemy QueuePool usage (one engine per process)."""
    if engine is None:
        return {"available": False}
    pool = engine.pool
    max_capacity = DB_POOL_SIZE + DB_MAX_OVERFLOW
    checked_out = int(pool.checkedout())
    return {
        "available": True,
        "pool_size": int(pool.size()),
        "checked_out": checked_out,
        "checked_in": int(pool.checkedin()),
        "overflow": int(pool.overflow()),
        "max_capacity": max_capacity,
        "utilization_pct": round(100.0 * checked_out / max_capacity, 1) if max_capacity else 0.0,
        "warn_threshold": DB_POOL_WARN_CHECKED_OUT,
        "stressed": checked_out >= DB_POOL_WARN_CHECKED_OUT,
    }


def log_db_pool_pressure(logger: Optional[logging.Logger], tag: str = "") -> Dict[str, Any]:
    """Log when checked-out connections exceed DB_POOL_WARN_CHECKED_OUT."""
    stats = get_db_pool_stats()
    log = logger or logging.getLogger(__name__)
    if not stats.get("available"):
        return stats
    prefix = f"[db_pool{(' ' + tag) if tag else ''}]"
    if stats.get("stressed"):
        log.warning(
            "%s stressed checked_out=%s/%s overflow=%s utilization=%s%%",
            prefix,
            stats["checked_out"],
            stats["max_capacity"],
            stats["overflow"],
            stats["utilization_pct"],
        )
    else:
        log.debug(
            "%s ok checked_out=%s/%s overflow=%s",
            prefix,
            stats["checked_out"],
            stats["max_capacity"],
            stats["overflow"],
        )
    return stats


@contextmanager
def db_session() -> Iterator[Session]:
    """
    Short-lived Session for background jobs and broker I/O boundaries.
    Rolls back on error; always closes (returns connection to pool).
    """
    if SessionLocal is None:
        raise Exception("Database not initialized. Please call create_tables() first.")
    db = SessionLocal()
    try:
        yield db
    except Exception:
        try:
            db.rollback()
        except Exception:
            pass
        raise
    finally:
        try:
            db.close()
        except Exception as e:
            print(f"Warning: Error closing database session: {e}")


# Dependency to get database session
def get_db():
    if SessionLocal is None:
        raise Exception("Database not initialized. Please call create_tables() first.")
    
    db = SessionLocal()
    try:
        yield db
    finally:
        # Ensure proper cleanup
        try:
            db.close()
        except Exception as e:
            print(f"Warning: Error closing database session: {e}")

# Create all tables
def create_tables():
    global engine, SessionLocal
    
    if engine is None:
        try:
            # Create engine with proper configuration
            engine_kwargs = {
                "pool_pre_ping": True,
                "pool_recycle": 3600,
                "echo": False,
            }
            
            # Only add isolation_level for PostgreSQL
            if "postgresql" in DATABASE_URL:
                engine_kwargs["isolation_level"] = "READ_COMMITTED"
                engine_kwargs["pool_size"] = max(1, DB_POOL_SIZE)
                engine_kwargs["max_overflow"] = max(0, DB_MAX_OVERFLOW)
                engine_kwargs["pool_timeout"] = max(1, DB_POOL_TIMEOUT_SEC)
            
            engine = create_engine(DATABASE_URL, **engine_kwargs)
            SessionLocal = sessionmaker(
                autocommit=False, 
                autoflush=False, 
                bind=engine,
                expire_on_commit=False
            )
        except Exception as e:
            print(f"Error creating database engine: {e}")
            raise
    
    try:
        Base.metadata.create_all(bind=engine)
        _run_startup_schema_migrations(engine)
        print("Database tables created successfully")
    except Exception as e:
        print(f"Error creating tables: {e}")
        raise


def _run_startup_schema_migrations(db_engine):
    """
    Apply lightweight runtime schema migrations that are safe/idempotent.
    This keeps production in sync without requiring a separate migration runner.
    """
    try:
        inspector = inspect(db_engine)
        table_names = inspector.get_table_names()
        with db_engine.begin() as conn:
            if "carstocklist" in table_names:
                column_names = {col["name"] for col in inspector.get_columns("carstocklist")}
                if "userid" not in column_names:
                    conn.execute(text("ALTER TABLE carstocklist ADD COLUMN userid INTEGER DEFAULT 4"))
                    print("Applied migration: added carstocklist.userid column")
                if "buy_price" not in column_names:
                    conn.execute(text("ALTER TABLE carstocklist ADD COLUMN buy_price NUMERIC(12,2) DEFAULT 0"))
                    print("Applied migration: added carstocklist.buy_price column")

                conn.execute(text("UPDATE carstocklist SET userid = 4 WHERE userid IS NULL"))
                conn.execute(text("UPDATE carstocklist SET buy_price = 0 WHERE buy_price IS NULL"))

                # PostgreSQL supports setting NOT NULL/DEFAULT after column creation.
                if db_engine.dialect.name == "postgresql":
                    conn.execute(text("ALTER TABLE carstocklist ALTER COLUMN userid SET DEFAULT 4"))
                    conn.execute(text("ALTER TABLE carstocklist ALTER COLUMN userid SET NOT NULL"))
                    conn.execute(text("ALTER TABLE carstocklist ALTER COLUMN buy_price SET DEFAULT 0"))
                    conn.execute(text("ALTER TABLE carstocklist ALTER COLUMN buy_price SET NOT NULL"))
                    # Dedupe then unique (userid, symbol) so CSV upload can use ON CONFLICT upsert.
                    conn.execute(
                        text(
                            """
                            DELETE FROM carstocklist a
                            USING carstocklist b
                            WHERE a.id > b.id AND a.userid = b.userid AND a.symbol = b.symbol
                            """
                        )
                    )
                    conn.execute(
                        text(
                            """
                            CREATE UNIQUE INDEX IF NOT EXISTS uq_carstocklist_user_symbol
                            ON carstocklist (userid, symbol)
                            """
                        )
                    )

            # Rename legacy typo table to the correct table name if required.
            conn.execute(
                text(
                    """
                    DO $$
                    BEGIN
                        IF to_regclass('public.arbiitrage_order') IS NOT NULL
                           AND to_regclass('public.arbitrage_order') IS NULL THEN
                            ALTER TABLE arbiitrage_order RENAME TO arbitrage_order;
                        END IF;
                    END
                    $$;
                    """
                )
            )

            # Arbitrage order book table.
            conn.execute(
                text(
                    """
                    CREATE TABLE IF NOT EXISTS arbitrage_order (
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
                    CREATE INDEX IF NOT EXISTS idx_arbitrage_order_stock_trade_status
                    ON arbitrage_order (stock_instrument_key, trade_status)
                    """
                )
            )
            if db_engine.dialect.name == "postgresql":
                conn.execute(
                    text(
                        """
                        CREATE UNIQUE INDEX IF NOT EXISTS uq_arbitrage_order_open_stock
                        ON arbitrage_order (stock_instrument_key)
                        WHERE trade_status = 'OPEN'
                        """
                    )
                )

            # CAR NIFTY200 table: CAR analysis cache for stocks from arbitrage_master
            conn.execute(
                text(
                    """
                    CREATE TABLE IF NOT EXISTS car_nifty200 (
                        stock TEXT PRIMARY KEY,
                        stock_instrument_key TEXT,
                        stock_ltp NUMERIC(16,4),
                        date_52weekhigh DATE,
                        last10daycummavg TEXT,
                        signal TEXT,
                        last_updated_date DATE,
                        dma50 NUMERIC(16,4),
                        dma100 NUMERIC(16,4),
                        dma200 NUMERIC(16,4)
                    )
                    """
                )
            )
            # Add DMA columns if table already existed without them (PostgreSQL)
            if db_engine.dialect.name == "postgresql":
                for col in ("dma50", "dma100", "dma200"):
                    try:
                        conn.execute(text(f"ALTER TABLE car_nifty200 ADD COLUMN IF NOT EXISTS {col} NUMERIC(16,4)"))
                    except Exception:
                        pass
            # One-time seed: copy from arbitrage_master (only if car_nifty200 is empty)
            row_count = conn.execute(text("SELECT COUNT(*) FROM car_nifty200")).scalar() or 0
            if row_count == 0:
                if db_engine.dialect.name == "postgresql":
                    conn.execute(
                        text(
                            """
                            INSERT INTO car_nifty200 (stock, stock_instrument_key, stock_ltp)
                            SELECT stock, stock_instrument_key, stock_ltp
                            FROM arbitrage_master
                            WHERE stock IS NOT NULL AND TRIM(stock) <> ''
                              AND stock_instrument_key IS NOT NULL
                            ON CONFLICT (stock) DO NOTHING
                            """
                        )
                    )
                else:
                    conn.execute(
                        text(
                            """
                            INSERT INTO car_nifty200 (stock, stock_instrument_key, stock_ltp)
                            SELECT stock, stock_instrument_key, stock_ltp
                            FROM arbitrage_master
                            WHERE stock IS NOT NULL AND TRIM(stock) <> ''
                              AND stock_instrument_key IS NOT NULL
                            """
                        )
                    )
                try:
                    seed_count = conn.execute(text("SELECT COUNT(*) FROM car_nifty200")).scalar() or 0
                    print(f"car_nifty200 one-time seed: {seed_count} rows from arbitrage_master")
                except Exception:
                    pass

            # MarketAux + FinBERT sentiment job tables
            conn.execute(
                text(
                    """
                    CREATE TABLE IF NOT EXISTS stock_fin_sentiment (
                        stock VARCHAR(64) PRIMARY KEY,
                        stock_instrument_key TEXT,
                        api_sentiment_avg DOUBLE PRECISION,
                        nlp_sentiment_avg DOUBLE PRECISION,
                        combined_sentiment_avg DOUBLE PRECISION,
                        last_combined_sentiment DOUBLE PRECISION,
                        current_combined_sentiment DOUBLE PRECISION,
                        current_combined_sentiment_reason TEXT,
                        news_count INTEGER,
                        current_run_at TIMESTAMPTZ,
                        updated_at TIMESTAMPTZ DEFAULT CURRENT_TIMESTAMP
                    )
                    """
                )
            )
            conn.execute(
                text(
                    """
                    CREATE TABLE IF NOT EXISTS fin_sentiment_job_state (
                        id INTEGER PRIMARY KEY,
                        watermark TIMESTAMPTZ NOT NULL
                    )
                    """
                )
            )
            if db_engine.dialect.name == "postgresql":
                conn.execute(
                    text(
                        """
                        INSERT INTO fin_sentiment_job_state (id, watermark)
                        SELECT 1, (CURRENT_TIMESTAMP AT TIME ZONE 'UTC' - interval '90 minutes')
                        WHERE NOT EXISTS (SELECT 1 FROM fin_sentiment_job_state WHERE id = 1)
                        """
                    )
                )
            else:
                conn.execute(
                    text(
                        """
                        INSERT INTO fin_sentiment_job_state (id, watermark)
                        SELECT 1, datetime('now', '-90 minutes')
                        WHERE NOT EXISTS (SELECT 1 FROM fin_sentiment_job_state WHERE id = 1)
                        """
                    )
                )

            # Use a fresh inspector: table may have been created above and was not in initial table_names.
            _insp_sfs = inspect(db_engine)
            if "stock_fin_sentiment" in _insp_sfs.get_table_names():
                sfs_cols = {col["name"] for col in _insp_sfs.get_columns("stock_fin_sentiment")}
                if "current_combined_sentiment_reason" not in sfs_cols:
                    conn.execute(
                        text(
                            "ALTER TABLE stock_fin_sentiment ADD COLUMN current_combined_sentiment_reason TEXT"
                        )
                    )
                    print(
                        "Applied migration: added stock_fin_sentiment.current_combined_sentiment_reason"
                    )
                    _insp_sfs = inspect(db_engine)
                for col in _insp_sfs.get_columns("stock_fin_sentiment"):
                    if col["name"] != "current_combined_sentiment_reason":
                        continue
                    t = col["type"]
                    if db_engine.dialect.name == "postgresql" and getattr(t, "length", None) is not None:
                        conn.execute(
                            text(
                                "ALTER TABLE stock_fin_sentiment "
                                "ALTER COLUMN current_combined_sentiment_reason TYPE TEXT "
                                "USING current_combined_sentiment_reason::text"
                            )
                        )
                        print(
                            "Applied migration: widened stock_fin_sentiment.current_combined_sentiment_reason to TEXT"
                        )
                    break

            if "intraday_stock_options" in table_names:
                iso_columns = {col["name"] for col in inspector.get_columns("intraday_stock_options")}
                if "entry_slip_checks" not in iso_columns:
                    conn.execute(
                        text("ALTER TABLE intraday_stock_options ADD COLUMN entry_slip_checks INTEGER DEFAULT 0")
                    )
                    print("Applied migration: added intraday_stock_options.entry_slip_checks")

            if "users" in table_names:
                user_columns = {col["name"] for col in inspector.get_columns("users")}
                if "is_blocked" not in user_columns:
                    conn.execute(text("ALTER TABLE users ADD COLUMN is_blocked BOOLEAN DEFAULT FALSE"))
                    print("Applied migration: added users.is_blocked")
                if "is_paid_user" not in user_columns:
                    conn.execute(text("ALTER TABLE users ADD COLUMN is_paid_user BOOLEAN DEFAULT FALSE"))
                    print("Applied migration: added users.is_paid_user")
                if "last_login_at" not in user_columns:
                    conn.execute(text("ALTER TABLE users ADD COLUMN last_login_at TIMESTAMP"))
                    print("Applied migration: added users.last_login_at")
                if "last_login_ip" not in user_columns:
                    conn.execute(text("ALTER TABLE users ADD COLUMN last_login_ip VARCHAR(64)"))
                    print("Applied migration: added users.last_login_ip")
                if "last_page_visited" not in user_columns:
                    conn.execute(text("ALTER TABLE users ADD COLUMN last_page_visited VARCHAR(255)"))
                    print("Applied migration: added users.last_page_visited")
                if "last_page_visited_at" not in user_columns:
                    conn.execute(text("ALTER TABLE users ADD COLUMN last_page_visited_at TIMESTAMP"))
                    print("Applied migration: added users.last_page_visited_at")
                if "last_activity_ip" not in user_columns:
                    conn.execute(text("ALTER TABLE users ADD COLUMN last_activity_ip VARCHAR(64)"))
                    print("Applied migration: added users.last_activity_ip")
                conn.execute(text("UPDATE users SET is_blocked = FALSE WHERE is_blocked IS NULL"))
                conn.execute(text("UPDATE users SET is_paid_user = FALSE WHERE is_paid_user IS NULL"))

            # arbitrage_master: Upstox Nifty sector index instrument_key per equity (BOD JSON, NSE_INDEX segment)
            if "arbitrage_master" in table_names:
                _am_cols = {c["name"] for c in inspector.get_columns("arbitrage_master")}
                if "sector_index" not in _am_cols:
                    conn.execute(text("ALTER TABLE arbitrage_master ADD COLUMN sector_index TEXT"))
                    print("Applied migration: added arbitrage_master.sector_index")
                try:
                    from backend.services.market_data.schema import ensure_market_data_columns

                    ensure_market_data_columns()
                except Exception as _md_mig_err:
                    print(f"arbitrage_master market_data columns migration skipped: {_md_mig_err}")

            # Smart Futures daily picks (CMS picker job → smart_futures_daily)
            if "smart_futures_daily" not in table_names:
                if db_engine.dialect.name == "postgresql":
                    conn.execute(
                        text(
                            """
                            CREATE TABLE smart_futures_daily (
                                id BIGSERIAL PRIMARY KEY,
                                session_date DATE NOT NULL,
                                stock TEXT NOT NULL,
                                fut_symbol TEXT,
                                fut_instrument_key TEXT NOT NULL,
                                side TEXT NOT NULL,
                                obv_slope DOUBLE PRECISION,
                                volume_surge DOUBLE PRECISION,
                                adx_14 DOUBLE PRECISION,
                                atr_14 DOUBLE PRECISION,
                                renko_momentum DOUBLE PRECISION,
                                ha_trend DOUBLE PRECISION,
                                macd_div DOUBLE PRECISION,
                                rsi_div DOUBLE PRECISION,
                                stoch_div DOUBLE PRECISION,
                                cms DOUBLE PRECISION,
                                final_cms DOUBLE PRECISION,
                                sector_score DOUBLE PRECISION,
                                combined_sentiment DOUBLE PRECISION,
                                entry_price DOUBLE PRECISION,
                                sl_price DOUBLE PRECISION,
                                target_price DOUBLE PRECISION,
                                hold_type TEXT,
                                entry_at TIMESTAMP WITH TIME ZONE NOT NULL DEFAULT NOW(),
                                trend_continuation TEXT,
                                scan_trigger TEXT,
                                vix_at_scan DOUBLE PRECISION,
                                order_status TEXT,
                                buy_price DOUBLE PRECISION,
                                created_at TIMESTAMP WITH TIME ZONE DEFAULT NOW(),
                                updated_at TIMESTAMP WITH TIME ZONE DEFAULT NOW(),
                                CONSTRAINT uq_sfd_session_fut UNIQUE (session_date, fut_instrument_key)
                            )
                            """
                        )
                    )
                    conn.execute(
                        text(
                            "CREATE INDEX IF NOT EXISTS idx_sfd_session_date ON smart_futures_daily (session_date DESC)"
                        )
                    )
                    print("Applied migration: created smart_futures_daily (PostgreSQL)")

            if "smart_futures_daily" in table_names:
                _sfd_cols = {c["name"] for c in inspector.get_columns("smart_futures_daily")}
                if "order_status" not in _sfd_cols:
                    conn.execute(text("ALTER TABLE smart_futures_daily ADD COLUMN order_status TEXT"))
                    print("Applied migration: added smart_futures_daily.order_status")
                if "buy_price" not in _sfd_cols:
                    conn.execute(text("ALTER TABLE smart_futures_daily ADD COLUMN buy_price DOUBLE PRECISION"))
                    print("Applied migration: added smart_futures_daily.buy_price")
                if "atr5_14_ratio" not in _sfd_cols:
                    conn.execute(
                        text("ALTER TABLE smart_futures_daily ADD COLUMN atr5_14_ratio DOUBLE PRECISION")
                    )
                    print("Applied migration: added smart_futures_daily.atr5_14_ratio")
                if "sell_price" not in _sfd_cols:
                    conn.execute(text("ALTER TABLE smart_futures_daily ADD COLUMN sell_price DOUBLE PRECISION"))
                    print("Applied migration: added smart_futures_daily.sell_price")
                if "sell_time" not in _sfd_cols:
                    conn.execute(
                        text(
                            "ALTER TABLE smart_futures_daily ADD COLUMN sell_time TIMESTAMP WITH TIME ZONE"
                        )
                    )
                    print("Applied migration: added smart_futures_daily.sell_time")

                _sfd_new = [
                    ("signal_tier", "TEXT"),
                    ("tier_multiplier", "DOUBLE PRECISION"),
                    ("sizing_tier_mult", "DOUBLE PRECISION"),
                    ("calculated_lots", "INTEGER"),
                    ("stop_loss_price", "DOUBLE PRECISION"),
                    ("stop_stage", "TEXT"),
                    ("current_stop_price", "DOUBLE PRECISION"),
                    ("time_filter_passed", "BOOLEAN"),
                    ("regime_filter_passed", "BOOLEAN"),
                    ("regime_filter_reason", "TEXT"),
                    ("oi_value", "INTEGER"),
                    ("oi_change", "INTEGER"),
                    ("oi_signal", "TEXT"),
                    ("oi_gate_passed", "BOOLEAN"),
                    ("oi_gate_reason", "TEXT"),
                    ("ema_slope_norm", "DOUBLE PRECISION"),
                    ("cms_score_raw", "DOUBLE PRECISION"),
                    ("cms_final", "DOUBLE PRECISION"),
                    ("reentry_consumed", "BOOLEAN DEFAULT FALSE"),
                ]
                for colname, coltype in _sfd_new:
                    if colname not in _sfd_cols:
                        conn.execute(text(f"ALTER TABLE smart_futures_daily ADD COLUMN {colname} {coltype}"))
                        print(f"Applied migration: added smart_futures_daily.{colname}")
                        _sfd_cols.add(colname)
                if "premkt_rank" not in _sfd_cols:
                    conn.execute(text("ALTER TABLE smart_futures_daily ADD COLUMN premkt_rank INTEGER"))
                    print("Applied migration: added smart_futures_daily.premkt_rank")
                    _sfd_cols.add("premkt_rank")
                if "oi_heat_rank" not in _sfd_cols:
                    conn.execute(text("ALTER TABLE smart_futures_daily ADD COLUMN oi_heat_rank INTEGER"))
                    print("Applied migration: added smart_futures_daily.oi_heat_rank")
                    _sfd_cols.add("oi_heat_rank")

                # Entry-gate / reclaim-score persistence (non-breaking ADD COLUMN set).
                _sfd_entry_gate_cols = [
                    ("reclaim_score_last", "DOUBLE PRECISION"),
                    ("reclaim_score_prev", "DOUBLE PRECISION"),
                    ("reclaim_score_updated_at", "TIMESTAMP WITH TIME ZONE"),
                    ("manual_exit_reason", "VARCHAR(32)"),
                    ("manual_exit_at", "TIMESTAMP WITH TIME ZONE"),
                    ("exit_journal_note", "TEXT"),
                ]
                for _cname, _ctype in _sfd_entry_gate_cols:
                    if _cname not in _sfd_cols:
                        conn.execute(
                            text(f"ALTER TABLE smart_futures_daily ADD COLUMN {_cname} {_ctype}")
                        )
                        print(f"Applied migration: added smart_futures_daily.{_cname}")
                        _sfd_cols.add(_cname)

                _sfd_signal_cols = [
                    ("signal_status", "TEXT"),
                    ("scan_bar_high", "DOUBLE PRECISION"),
                    ("scan_bar_low", "DOUBLE PRECISION"),
                    ("m15_vwap_at_scan", "DOUBLE PRECISION"),
                ]
                for _cname, _ctype in _sfd_signal_cols:
                    if _cname not in _sfd_cols:
                        conn.execute(
                            text(f"ALTER TABLE smart_futures_daily ADD COLUMN {_cname} {_ctype}")
                        )
                        print(f"Applied migration: added smart_futures_daily.{_cname}")
                        _sfd_cols.add(_cname)

            # Smart Futures carry-forward watchlist: late-session picks that passed score+VWAP.
            _insp_wl = inspect(db_engine)
            if "smart_futures_watchlist" not in _insp_wl.get_table_names():
                if db_engine.dialect.name == "postgresql":
                    conn.execute(
                        text(
                            """
                            CREATE TABLE smart_futures_watchlist (
                                id BIGSERIAL PRIMARY KEY,
                                trigger_date DATE NOT NULL,
                                daily_id BIGINT,
                                symbol TEXT NOT NULL,
                                fut_symbol TEXT,
                                fut_instrument_key TEXT NOT NULL,
                                side TEXT NOT NULL,
                                trigger_score DOUBLE PRECISION,
                                trigger_price DOUBLE PRECISION,
                                vwap_at_trigger DOUBLE PRECISION,
                                trigger_at TIMESTAMP WITH TIME ZONE,
                                added_at TIMESTAMP WITH TIME ZONE NOT NULL DEFAULT NOW(),
                                cleared_at TIMESTAMP WITH TIME ZONE,
                                CONSTRAINT uq_sf_watchlist_date_ikey UNIQUE (trigger_date, fut_instrument_key)
                            )
                            """
                        )
                    )
                    conn.execute(
                        text(
                            "CREATE INDEX IF NOT EXISTS idx_sf_watchlist_trigger_date "
                            "ON smart_futures_watchlist (trigger_date DESC)"
                        )
                    )
                    print("Applied migration: created smart_futures_watchlist (PostgreSQL)")

            # Smart Futures backtest results (separate from live smart_futures_daily)
            if "backtest_smart_future" not in table_names:
                if db_engine.dialect.name == "postgresql":
                    conn.execute(
                        text(
                            """
                            CREATE TABLE backtest_smart_future (
                                id BIGSERIAL PRIMARY KEY,
                                session_date DATE NOT NULL,
                                simulated_asof TIMESTAMPTZ NOT NULL,
                                scan_time_label TEXT NOT NULL,
                                stock TEXT NOT NULL,
                                fut_symbol TEXT,
                                fut_instrument_key TEXT NOT NULL,
                                side TEXT NOT NULL,
                                obv_slope DOUBLE PRECISION,
                                volume_surge DOUBLE PRECISION,
                                adx_14 DOUBLE PRECISION,
                                atr_14 DOUBLE PRECISION,
                                atr5_14_ratio DOUBLE PRECISION,
                                renko_momentum DOUBLE PRECISION,
                                ha_trend DOUBLE PRECISION,
                                macd_div DOUBLE PRECISION,
                                rsi_div DOUBLE PRECISION,
                                stoch_div DOUBLE PRECISION,
                                cms DOUBLE PRECISION,
                                final_cms DOUBLE PRECISION,
                                sector_score DOUBLE PRECISION,
                                combined_sentiment DOUBLE PRECISION,
                                entry_price DOUBLE PRECISION,
                                sl_price DOUBLE PRECISION,
                                target_price DOUBLE PRECISION,
                                hold_type TEXT,
                                trend_continuation TEXT,
                                scan_trigger TEXT,
                                vix_at_scan DOUBLE PRECISION,
                                sentiment_source TEXT,
                                sentiment_run_at_match_count INTEGER,
                                created_at TIMESTAMPTZ DEFAULT NOW()
                            )
                            """
                        )
                    )
                    conn.execute(
                        text(
                            "CREATE INDEX IF NOT EXISTS idx_btsf_sim ON backtest_smart_future (simulated_asof DESC)"
                        )
                    )
                    conn.execute(
                        text(
                            "CREATE INDEX IF NOT EXISTS idx_btsf_session ON backtest_smart_future (session_date DESC)"
                        )
                    )
                    print("Applied migration: created backtest_smart_future (PostgreSQL)")

            # Pre-market F&O Top 10 watchlist (OBV + gap + range; job weekdays ~9:10 IST)
            if "premarket_watchlist" not in table_names:
                if db_engine.dialect.name == "postgresql":
                    conn.execute(
                        text(
                            """
                            CREATE TABLE premarket_watchlist (
                                id BIGSERIAL PRIMARY KEY,
                                session_date DATE NOT NULL,
                                rank SMALLINT NOT NULL,
                                stock TEXT NOT NULL,
                                instrument_key TEXT,
                                obv_slope DOUBLE PRECISION,
                                gap_strength DOUBLE PRECISION,
                                gap_pct_signed DOUBLE PRECISION,
                                range_position DOUBLE PRECISION,
                                momentum DOUBLE PRECISION,
                                composite_score DOUBLE PRECISION,
                                ltp DOUBLE PRECISION,
                                computed_at TIMESTAMPTZ NOT NULL,
                                CONSTRAINT uq_premarket_session_rank UNIQUE (session_date, rank)
                            )
                            """
                        )
                    )
                    conn.execute(
                        text(
                            "CREATE INDEX IF NOT EXISTS idx_premarket_session ON premarket_watchlist (session_date DESC)"
                        )
                    )
                    print("Applied migration: created premarket_watchlist (PostgreSQL)")

            _insp_pm = inspect(db_engine)
            if "premarket_watchlist" in _insp_pm.get_table_names():
                _pm_cols = {c["name"] for c in _insp_pm.get_columns("premarket_watchlist")}
                if "momentum" not in _pm_cols:
                    conn.execute(
                        text("ALTER TABLE premarket_watchlist ADD COLUMN momentum DOUBLE PRECISION")
                    )
                    print("Applied migration: added premarket_watchlist.momentum")

            # Relative Strength Scanner snapshot (Top-5 bullish/bearish vs NIFTY; 5-min job)
            if "relative_strength_snapshot" not in table_names:
                if db_engine.dialect.name == "postgresql":
                    conn.execute(
                        text(
                            """
                            CREATE TABLE relative_strength_snapshot (
                                id BIGSERIAL PRIMARY KEY,
                                scan_time TIMESTAMPTZ NOT NULL,
                                symbol TEXT NOT NULL,
                                current_price DOUBLE PRECISION,
                                previous_close DOUBLE PRECISION,
                                stock_percent DOUBLE PRECISION,
                                nifty_percent DOUBLE PRECISION,
                                relative_strength DOUBLE PRECISION,
                                ema5 DOUBLE PRECISION,
                                ema9 DOUBLE PRECISION,
                                ema10 DOUBLE PRECISION,
                                vwap DOUBLE PRECISION,
                                supertrend DOUBLE PRECISION,
                                macd DOUBLE PRECISION,
                                macd_signal DOUBLE PRECISION,
                                macd_histogram DOUBLE PRECISION,
                                adx DOUBLE PRECISION,
                                volume DOUBLE PRECISION,
                                avg_volume DOUBLE PRECISION,
                                volume_ratio DOUBLE PRECISION,
                                kavach_state TEXT,
                                kavach_strength INTEGER,
                                trade_score DOUBLE PRECISION,
                                ranking_type TEXT NOT NULL,
                                rank_position INTEGER NOT NULL,
                                created_at TIMESTAMPTZ NOT NULL DEFAULT NOW()
                            )
                            """
                        )
                    )
                    conn.execute(
                        text(
                            "CREATE INDEX IF NOT EXISTS idx_rss_scan_time "
                            "ON relative_strength_snapshot (scan_time DESC)"
                        )
                    )
                    conn.execute(
                        text(
                            "CREATE INDEX IF NOT EXISTS idx_rss_symbol "
                            "ON relative_strength_snapshot (symbol)"
                        )
                    )
                    conn.execute(
                        text(
                            "CREATE INDEX IF NOT EXISTS idx_rss_ranking_type "
                            "ON relative_strength_snapshot (ranking_type)"
                        )
                    )
                    conn.execute(
                        text(
                            "CREATE INDEX IF NOT EXISTS idx_rss_trade_score "
                            "ON relative_strength_snapshot (trade_score DESC)"
                        )
                    )
                    print("Applied migration: created relative_strength_snapshot (PostgreSQL)")

            # Daily RS Trade Checklist (per-stock pre-trade entry checklist)
            if "daily_checklist" not in table_names:
                if db_engine.dialect.name == "postgresql":
                    conn.execute(
                        text(
                            """
                            CREATE TABLE daily_checklist (
                                id BIGSERIAL PRIMARY KEY,
                                session_date DATE NOT NULL,
                                symbol TEXT NOT NULL,
                                direction TEXT NOT NULL,
                                rs_pct DOUBLE PRECISION,
                                dashboard_score INTEGER,
                                dashboard_kavach TEXT,
                                vol_multiplier DOUBLE PRECISION,
                                news_clean BOOLEAN,
                                adx_935 DOUBLE PRECISION,
                                adx_935_status TEXT,
                                nifty_open_direction TEXT,
                                entry_time TEXT,
                                time_ok BOOLEAN,
                                kavach_score_entry INTEGER,
                                score_ok BOOLEAN,
                                confidence TEXT,
                                confidence_ok BOOLEAN,
                                trading_state TEXT,
                                state_ok BOOLEAN,
                                ema_vs_vwap TEXT,
                                ema_ok BOOLEAN,
                                supertrend TEXT,
                                st_ok BOOLEAN,
                                macd TEXT,
                                macd_ok BOOLEAN,
                                adx_entry DOUBLE PRECISION,
                                di_alignment TEXT,
                                adx_ok BOOLEAN,
                                volume TEXT,
                                volume_ok BOOLEAN,
                                counter_rs BOOLEAN DEFAULT FALSE,
                                gate_score INTEGER,
                                decision TEXT,
                                section TEXT,
                                notes TEXT,
                                created_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
                                updated_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
                                UNIQUE (session_date, symbol)
                            )
                            """
                        )
                    )
                    conn.execute(
                        text(
                            "CREATE INDEX IF NOT EXISTS idx_daily_checklist_date "
                            "ON daily_checklist (session_date DESC)"
                        )
                    )
                    print("Applied migration: created daily_checklist (PostgreSQL)")

            # RS Scanner move-maturity daily history (one row per symbol per session)
            if "rs_scanner_history" not in table_names:
                if db_engine.dialect.name == "postgresql":
                    conn.execute(
                        text(
                            """
                            CREATE TABLE rs_scanner_history (
                                id BIGSERIAL PRIMARY KEY,
                                date DATE NOT NULL,
                                symbol TEXT NOT NULL,
                                direction TEXT NOT NULL,
                                rs_pct DOUBLE PRECISION,
                                daily_range_pct DOUBLE PRECISION,
                                atr14_pct DOUBLE PRECISION,
                                range_vs_atr_ratio DOUBLE PRECISION,
                                consecutive_days_on_list INTEGER NOT NULL DEFAULT 1,
                                maturity_tag TEXT NOT NULL DEFAULT 'FRESH',
                                created_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
                                UNIQUE (date, symbol)
                            )
                            """
                        )
                    )
                    conn.execute(
                        text(
                            "CREATE INDEX IF NOT EXISTS idx_rs_hist_date "
                            "ON rs_scanner_history (date DESC)"
                        )
                    )
                    conn.execute(
                        text(
                            "CREATE INDEX IF NOT EXISTS idx_rs_hist_symbol_date "
                            "ON rs_scanner_history (symbol, date DESC)"
                        )
                    )
                    print("Applied migration: created rs_scanner_history (PostgreSQL)")

            # daily_checklist: move-maturity fields from RS scanner history
            if "daily_checklist" in table_names and db_engine.dialect.name == "postgresql":
                _dc_cols = {c["name"] for c in inspect(db_engine).get_columns("daily_checklist")}
                for col, typ in (
                    ("maturity_tag", "TEXT"),
                    ("consecutive_days_on_list", "INTEGER"),
                    ("range_vs_atr_ratio", "DOUBLE PRECISION"),
                    ("eligibility_note", "TEXT"),
                ):
                    if col not in _dc_cols:
                        conn.execute(
                            text(f"ALTER TABLE daily_checklist ADD COLUMN {col} {typ}")
                        )
                        print(f"Applied migration: added daily_checklist.{col}")

            # relative_strength_snapshot: Kavach volume/confidence columns
            if "relative_strength_snapshot" in table_names and db_engine.dialect.name == "postgresql":
                _rss_cols = {c["name"] for c in inspect(db_engine).get_columns("relative_strength_snapshot")}
                for col, typ in (
                    ("volume_tod_ratio", "DOUBLE PRECISION"),
                    ("volume_label", "TEXT"),
                    ("vwap_purity_pct", "DOUBLE PRECISION"),
                    ("market_regime", "TEXT"),
                    ("confidence_grade", "TEXT"),
                ):
                    if col not in _rss_cols:
                        conn.execute(
                            text(f"ALTER TABLE relative_strength_snapshot ADD COLUMN {col} {typ}")
                        )
                        print(f"Applied migration: added relative_strength_snapshot.{col}")

            # RS anchor snapshots at fixed IST decision times
            if "rs_anchor_snapshot" not in table_names:
                if db_engine.dialect.name == "postgresql":
                    conn.execute(
                        text(
                            """
                            CREATE TABLE rs_anchor_snapshot (
                                id BIGSERIAL PRIMARY KEY,
                                session_date DATE NOT NULL,
                                capture_label TEXT NOT NULL,
                                capture_time TIMESTAMPTZ NOT NULL,
                                rank_position INTEGER NOT NULL,
                                symbol TEXT NOT NULL,
                                direction TEXT NOT NULL,
                                current_price DOUBLE PRECISION,
                                relative_strength DOUBLE PRECISION,
                                trade_score DOUBLE PRECISION,
                                confidence_grade TEXT,
                                market_regime TEXT,
                                adx DOUBLE PRECISION,
                                volume_label TEXT,
                                volume_ratio DOUBLE PRECISION,
                                vwap_purity_pct DOUBLE PRECISION,
                                supertrend DOUBLE PRECISION,
                                macd DOUBLE PRECISION,
                                macd_signal DOUBLE PRECISION,
                                ema5 DOUBLE PRECISION,
                                vwap DOUBLE PRECISION,
                                maturity_tag TEXT,
                                sector TEXT,
                                created_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
                                UNIQUE (session_date, capture_label, symbol, direction)
                            )
                            """
                        )
                    )
                    conn.execute(
                        text(
                            "CREATE INDEX IF NOT EXISTS idx_rs_anchor_date_label "
                            "ON rs_anchor_snapshot (session_date DESC, capture_label)"
                        )
                    )
                    conn.execute(
                        text(
                            "CREATE INDEX IF NOT EXISTS idx_rs_anchor_symbol "
                            "ON rs_anchor_snapshot (symbol, session_date DESC)"
                        )
                    )
                    print("Applied migration: created rs_anchor_snapshot (PostgreSQL)")

            # daily_checklist: Kavach quality + rotation + live RS fields
            if "daily_checklist" in table_names and db_engine.dialect.name == "postgresql":
                _dc_cols2 = {c["name"] for c in inspect(db_engine).get_columns("daily_checklist")}
                for col, typ in (
                    ("vwap_purity_pct", "DOUBLE PRECISION"),
                    ("market_regime", "TEXT"),
                    ("quality_display", "TEXT"),
                    ("live_rs_direction", "TEXT"),
                    ("live_rs_updated_at", "TIMESTAMPTZ"),
                    ("rotation_day_type", "TEXT"),
                    ("carryover_warning", "BOOLEAN DEFAULT FALSE"),
                    ("sector_badge", "TEXT"),
                    ("data_refreshed_at", "TIMESTAMPTZ"),
                    ("fii_dii_flow", "TEXT"),
                ):
                    if col not in _dc_cols2:
                        conn.execute(
                            text(f"ALTER TABLE daily_checklist ADD COLUMN {col} {typ}")
                        )
                        print(f"Applied migration: added daily_checklist.{col}")

            # daily_checklist: Kavach live recompute + GO timing
            if "daily_checklist" in table_names and db_engine.dialect.name == "postgresql":
                _dc_cols3 = {c["name"] for c in inspect(db_engine).get_columns("daily_checklist")}
                for col, typ in (
                    ("go_enter_first_at", "TIMESTAMPTZ"),
                    ("go_sticky_until", "TIMESTAMPTZ"),
                    ("indicator_as_of", "TIMESTAMPTZ"),
                    ("indicator_source", "TEXT"),
                    ("indicator_stale", "BOOLEAN DEFAULT FALSE"),
                    ("chart_reversed", "BOOLEAN DEFAULT FALSE"),
                ):
                    if col not in _dc_cols3:
                        conn.execute(
                            text(f"ALTER TABLE daily_checklist ADD COLUMN {col} {typ}")
                        )
                        print(f"Applied migration: added daily_checklist.{col}")

            if "rs_fast_watch" not in table_names:
                if db_engine.dialect.name == "postgresql":
                    conn.execute(
                        text(
                            """
                            CREATE TABLE rs_fast_watch (
                                id BIGSERIAL PRIMARY KEY,
                                session_date DATE NOT NULL,
                                symbol TEXT NOT NULL,
                                direction TEXT NOT NULL,
                                first_flip_at TIMESTAMPTZ NOT NULL,
                                kavach_state TEXT,
                                trade_score DOUBLE PRECISION,
                                confidence_grade TEXT,
                                UNIQUE (session_date, symbol, direction)
                            )
                            """
                        )
                    )
                    conn.execute(
                        text(
                            "CREATE INDEX IF NOT EXISTS idx_rs_fast_watch_date "
                            "ON rs_fast_watch (session_date DESC)"
                        )
                    )
                    print("Applied migration: created rs_fast_watch (PostgreSQL)")

            if "rs_fast_watch" in table_names:
                _fw_cols = {col["name"] for col in inspector.get_columns("rs_fast_watch")}
                for col, typ in (
                    ("is_reversal", "BOOLEAN DEFAULT FALSE"),
                    ("lock_direction", "TEXT"),
                    ("prev_kavach_state", "TEXT"),
                    ("flip_price", "DOUBLE PRECISION"),
                ):
                    if col not in _fw_cols and db_engine.dialect.name == "postgresql":
                        conn.execute(text(f"ALTER TABLE rs_fast_watch ADD COLUMN {col} {typ}"))
                        print(f"Applied migration: rs_fast_watch.{col}")

            if "rs_live_kavach_audit" not in table_names:
                if db_engine.dialect.name == "postgresql":
                    conn.execute(
                        text(
                            """
                            CREATE TABLE rs_live_kavach_audit (
                                id BIGSERIAL PRIMARY KEY,
                                session_date DATE NOT NULL,
                                computed_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
                                symbol TEXT NOT NULL,
                                lock_direction TEXT,
                                bar_evaluated_at TIMESTAMPTZ NOT NULL,
                                kavach_state TEXT,
                                prev_kavach_state TEXT,
                                trade_score DOUBLE PRECISION,
                                confidence_grade TEXT,
                                volume_label TEXT,
                                vwap_purity_pct DOUBLE PRECISION,
                                market_regime TEXT,
                                adx DOUBLE PRECISION,
                                ema5 DOUBLE PRECISION,
                                ema10 DOUBLE PRECISION,
                                vwap DOUBLE PRECISION,
                                price DOUBLE PRECISION,
                                timeframe TEXT DEFAULT '10m'
                            )
                            """
                        )
                    )
                    conn.execute(
                        text(
                            "CREATE INDEX IF NOT EXISTS idx_rs_live_kavach_audit_sym_date "
                            "ON rs_live_kavach_audit (session_date DESC, symbol, bar_evaluated_at)"
                        )
                    )
                    print("Applied migration: created rs_live_kavach_audit (PostgreSQL)")

            if "trade_log" not in table_names:
                if db_engine.dialect.name == "postgresql":
                    conn.execute(
                        text(
                            """
                            CREATE TABLE IF NOT EXISTS trade_log (
                                id BIGSERIAL PRIMARY KEY,
                                session_date DATE NOT NULL,
                                symbol TEXT NOT NULL,
                                contract TEXT,
                                direction TEXT NOT NULL,
                                qty INTEGER,
                                entry_time TIME NOT NULL,
                                entry_price DOUBLE PRECISION NOT NULL,
                                exit_time TIME,
                                exit_price DOUBLE PRECISION,
                                exit_price_intended DOUBLE PRECISION,
                                slippage_pts DOUBLE PRECISION,
                                points_captured DOUBLE PRECISION,
                                ema10_at_entry DOUBLE PRECISION,
                                ema5_at_entry DOUBLE PRECISION,
                                vwap_at_entry DOUBLE PRECISION,
                                entry_to_ema10_buffer_pct DOUBLE PRECISION,
                                planned_risk_pts DOUBLE PRECISION,
                                planned_risk_inr DOUBLE PRECISION,
                                confidence_at_entry TEXT,
                                trade_score_at_entry DOUBLE PRECISION,
                                adx_at_entry DOUBLE PRECISION,
                                confidence_at_exit TEXT,
                                trade_score_at_exit DOUBLE PRECISION,
                                mfe_r DOUBLE PRECISION,
                                mae_r DOUBLE PRECISION,
                                r_realized DOUBLE PRECISION,
                                bars_held_10m INTEGER,
                                exit_trigger TEXT,
                                notes TEXT,
                                source TEXT DEFAULT 'manual',
                                created_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
                                updated_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
                                UNIQUE (session_date, symbol, direction, entry_time)
                            )
                            """
                        )
                    )
                    conn.execute(
                        text(
                            "CREATE INDEX IF NOT EXISTS idx_trade_log_session "
                            "ON trade_log (session_date DESC, symbol)"
                        )
                    )
                    print("Applied migration: created trade_log (Rule 27 journal)")

            # Shadow-only: |entry−EMA10|/entry×100 — never used to gate entries.
            if db_engine.dialect.name == "postgresql":
                try:
                    conn.execute(
                        text(
                            "ALTER TABLE trade_log "
                            "ADD COLUMN IF NOT EXISTS entry_to_ema10_buffer_pct DOUBLE PRECISION"
                        )
                    )
                except Exception:
                    pass

            # Shadow-only VWAP touch-reject candle log (research; no live gate).
            if db_engine.dialect.name == "postgresql":
                try:
                    conn.execute(
                        text(
                            """
                            CREATE TABLE IF NOT EXISTS kavach_vwap_touch_reject_log (
                                id BIGSERIAL PRIMARY KEY,
                                session_date DATE NOT NULL,
                                symbol TEXT NOT NULL,
                                lock_direction TEXT NOT NULL,
                                bar_evaluated_at TIMESTAMPTZ NOT NULL,
                                bar_open DOUBLE PRECISION,
                                bar_high DOUBLE PRECISION,
                                bar_low DOUBLE PRECISION,
                                bar_close DOUBLE PRECISION,
                                vwap DOUBLE PRECISION,
                                vwap_touch_reject BOOLEAN NOT NULL DEFAULT FALSE,
                                vwap_wick_through_pts DOUBLE PRECISION,
                                source TEXT DEFAULT 'live',
                                logged_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
                                UNIQUE (session_date, symbol, lock_direction, bar_evaluated_at)
                            )
                            """
                        )
                    )
                    conn.execute(
                        text(
                            "CREATE INDEX IF NOT EXISTS idx_kavach_vwap_touch_reject_session "
                            "ON kavach_vwap_touch_reject_log (session_date DESC, symbol)"
                        )
                    )
                except Exception:
                    pass

            # Shadow-only READY VWAP close-confirmation episodes (research; no live gate).
            if db_engine.dialect.name == "postgresql":
                try:
                    conn.execute(
                        text(
                            """
                            CREATE TABLE IF NOT EXISTS kavach_vwap_close_confirm_shadow (
                                id BIGSERIAL PRIMARY KEY,
                                session_date DATE NOT NULL,
                                symbol TEXT NOT NULL,
                                direction TEXT NOT NULL,
                                ts_ready_first_flagged TIMESTAMPTZ NOT NULL,
                                price_at_ready DOUBLE PRECISION,
                                vwap_at_ready DOUBLE PRECISION,
                                vwap_close_confirmed BOOLEAN NOT NULL DEFAULT FALSE,
                                ts_vwap_close_confirmed TIMESTAMPTZ,
                                price_at_vwap_confirm DOUBLE PRECISION,
                                candles_to_confirm INTEGER,
                                bars_since_ready_at_eod_or_expiry INTEGER,
                                episode_ended_at TIMESTAMPTZ,
                                episode_end_reason TEXT,
                                source TEXT DEFAULT 'live',
                                updated_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
                                UNIQUE (session_date, symbol, ts_ready_first_flagged)
                            )
                            """
                        )
                    )
                    conn.execute(
                        text(
                            "CREATE INDEX IF NOT EXISTS idx_kavach_vwap_close_confirm_session "
                            "ON kavach_vwap_close_confirm_shadow (session_date DESC, symbol)"
                        )
                    )
                except Exception:
                    pass

            if "rs_go_board_shadow_log" not in table_names:
                if db_engine.dialect.name == "postgresql":
                    conn.execute(
                        text(
                            """
                            CREATE TABLE rs_go_board_shadow_log (
                                id BIGSERIAL PRIMARY KEY,
                                session_date DATE NOT NULL,
                                evaluated_at TIMESTAMPTZ NOT NULL,
                                symbol TEXT NOT NULL,
                                side TEXT NOT NULL,
                                outcome TEXT NOT NULL,
                                filter_reason TEXT,
                                is_reversal BOOLEAN DEFAULT FALSE,
                                confidence_grade TEXT,
                                kavach_state TEXT,
                                price DOUBLE PRECISION,
                                freshness_pct DOUBLE PRECISION,
                                stop_pct DOUBLE PRECISION,
                                stop_inr_1lot DOUBLE PRECISION,
                                vwap_slope DOUBLE PRECISION,
                                adx DOUBLE PRECISION,
                                regime TEXT,
                                window_label TEXT,
                                detail_json TEXT
                            )
                            """
                        )
                    )
                    conn.execute(
                        text(
                            "CREATE INDEX IF NOT EXISTS idx_rs_go_board_shadow_date "
                            "ON rs_go_board_shadow_log (session_date DESC, evaluated_at)"
                        )
                    )
                    print("Applied migration: created rs_go_board_shadow_log (PostgreSQL)")

            if "rs_go_board_backtest_log" not in table_names:
                if db_engine.dialect.name == "postgresql":
                    conn.execute(
                        text(
                            """
                            CREATE TABLE rs_go_board_backtest_log (
                                id BIGSERIAL PRIMARY KEY,
                                session_date DATE NOT NULL,
                                flip_at TIMESTAMPTZ NOT NULL,
                                symbol TEXT NOT NULL,
                                direction TEXT NOT NULL,
                                is_reversal BOOLEAN DEFAULT FALSE,
                                lock_direction TEXT,
                                prev_kavach TEXT,
                                new_kavach TEXT,
                                flip_price DOUBLE PRECISION,
                                adx DOUBLE PRECISION,
                                regime TEXT,
                                volume_label TEXT,
                                purity_pct DOUBLE PRECISION,
                                confidence_grade TEXT,
                                vwap_slope DOUBLE PRECISION,
                                freshness_pct DOUBLE PRECISION,
                                stop_pct DOUBLE PRECISION,
                                stop_level DOUBLE PRECISION,
                                window_label TEXT,
                                gate_regime_pass BOOLEAN,
                                gate_adx_pass BOOLEAN,
                                gate_slope_pass BOOLEAN,
                                gate_freshness_pass BOOLEAN,
                                gate_stop_pass BOOLEAN,
                                gate_grade_pass BOOLEAN,
                                combined_pass BOOLEAN,
                                mfe_pct DOUBLE PRECISION,
                                mae_pct DOUBLE PRECISION,
                                hit_1p5_before_stop BOOLEAN,
                                hit_2p0_before_stop BOOLEAN,
                                stopped_out BOOLEAN,
                                session_end_pct DOUBLE PRECISION,
                                detail_json TEXT,
                                created_at TIMESTAMPTZ DEFAULT NOW()
                            )
                            """
                        )
                    )
                    conn.execute(
                        text(
                            "CREATE INDEX IF NOT EXISTS idx_rs_go_board_bt_date "
                            "ON rs_go_board_backtest_log (session_date DESC, flip_at)"
                        )
                    )
                    conn.execute(
                        text(
                            "CREATE UNIQUE INDEX IF NOT EXISTS idx_rs_go_board_bt_uniq "
                            "ON rs_go_board_backtest_log (session_date, symbol, flip_at)"
                        )
                    )
                    print("Applied migration: created rs_go_board_backtest_log (PostgreSQL)")

            # Daily checklist morning snapshot lock (Top 5+5 at/after 09:25 IST)
            if "daily_snapshot" not in table_names:
                if db_engine.dialect.name == "postgresql":
                    conn.execute(
                        text(
                            """
                            CREATE TABLE daily_snapshot (
                                id BIGSERIAL PRIMARY KEY,
                                snapshot_date DATE NOT NULL,
                                symbol TEXT NOT NULL,
                                direction TEXT NOT NULL CHECK (direction IN ('BULL', 'BEAR')),
                                rank INTEGER NOT NULL,
                                rs_score DOUBLE PRECISION,
                                locked_at TIMESTAMPTZ DEFAULT NOW(),
                                UNIQUE (snapshot_date, symbol, direction)
                            )
                            """
                        )
                    )
                    conn.execute(
                        text(
                            "CREATE INDEX IF NOT EXISTS idx_daily_snapshot_date "
                            "ON daily_snapshot (snapshot_date DESC)"
                        )
                    )
                    print("Applied migration: created daily_snapshot (PostgreSQL)")

            if "snapshot_lock" not in table_names:
                if db_engine.dialect.name == "postgresql":
                    conn.execute(
                        text(
                            """
                            CREATE TABLE snapshot_lock (
                                lock_date DATE PRIMARY KEY,
                                locked_at TIMESTAMPTZ,
                                locked_by TEXT DEFAULT 'auto'
                            )
                            """
                        )
                    )
                    print("Applied migration: created snapshot_lock (PostgreSQL)")

            if "rs_lock_membership_audit" not in table_names:
                if db_engine.dialect.name == "postgresql":
                    conn.execute(
                        text(
                            """
                            CREATE TABLE rs_lock_membership_audit (
                                id BIGSERIAL PRIMARY KEY,
                                session_date DATE NOT NULL,
                                symbol TEXT NOT NULL,
                                direction TEXT NOT NULL,
                                event_type TEXT NOT NULL,
                                rule TEXT NOT NULL,
                                rank INTEGER,
                                persistence_top5_frac DOUBLE PRECISION,
                                persistence_clean_bars INTEGER,
                                detail JSONB,
                                event_at TIMESTAMPTZ DEFAULT NOW()
                            )
                            """
                        )
                    )
                    conn.execute(
                        text(
                            "CREATE INDEX IF NOT EXISTS idx_rs_lock_membership_audit_day "
                            "ON rs_lock_membership_audit (session_date DESC, symbol, event_at)"
                        )
                    )
                    print("Applied migration: created rs_lock_membership_audit (PostgreSQL)")

            # RS Conviction Score board + Setup Radar
            if "rs_conviction_config" not in table_names:
                if db_engine.dialect.name == "postgresql":
                    conn.execute(
                        text(
                            """
                            CREATE TABLE rs_conviction_config (
                                key TEXT PRIMARY KEY,
                                value TEXT NOT NULL,
                                updated_at TIMESTAMPTZ DEFAULT NOW()
                            )
                            """
                        )
                    )
                    print("Applied migration: created rs_conviction_config (PostgreSQL)")

            if "rs_conviction_state" not in table_names:
                if db_engine.dialect.name == "postgresql":
                    conn.execute(
                        text(
                            """
                            CREATE TABLE rs_conviction_state (
                                id BIGSERIAL PRIMARY KEY,
                                session_date DATE NOT NULL,
                                symbol TEXT NOT NULL,
                                side TEXT NOT NULL CHECK (side IN ('BULL', 'BEAR')),
                                persistence_credit DOUBLE PRECISION DEFAULT 0,
                                opening_anchor DOUBLE PRECISION DEFAULT 0,
                                rs_component DOUBLE PRECISION DEFAULT 0,
                                slope_component DOUBLE PRECISION DEFAULT 0,
                                accum_component DOUBLE PRECISION DEFAULT 0,
                                whip_penalty DOUBLE PRECISION DEFAULT 0,
                                conviction_score DOUBLE PRECISION DEFAULT 0,
                                whipsaw_cross_count INTEGER DEFAULT 0,
                                accum_active BOOLEAN DEFAULT FALSE,
                                updated_at TIMESTAMPTZ DEFAULT NOW(),
                                UNIQUE (session_date, symbol, side)
                            )
                            """
                        )
                    )
                    conn.execute(
                        text(
                            "CREATE INDEX IF NOT EXISTS idx_rs_conviction_state_date "
                            "ON rs_conviction_state (session_date DESC)"
                        )
                    )
                    print("Applied migration: created rs_conviction_state (PostgreSQL)")

            if "rs_conviction_board" not in table_names:
                if db_engine.dialect.name == "postgresql":
                    conn.execute(
                        text(
                            """
                            CREATE TABLE rs_conviction_board (
                                id BIGSERIAL PRIMARY KEY,
                                session_date DATE NOT NULL,
                                side TEXT NOT NULL CHECK (side IN ('BULL', 'BEAR')),
                                rank INTEGER NOT NULL,
                                symbol TEXT NOT NULL,
                                conviction_score DOUBLE PRECISION,
                                promoted_at TIMESTAMPTZ,
                                UNIQUE (session_date, side, rank),
                                UNIQUE (session_date, side, symbol)
                            )
                            """
                        )
                    )
                    print("Applied migration: created rs_conviction_board (PostgreSQL)")

            if "rs_conviction_challenger" not in table_names:
                if db_engine.dialect.name == "postgresql":
                    conn.execute(
                        text(
                            """
                            CREATE TABLE rs_conviction_challenger (
                                session_date DATE NOT NULL,
                                side TEXT NOT NULL,
                                challenger_symbol TEXT NOT NULL,
                                displaced_symbol TEXT NOT NULL,
                                cycles_won INTEGER DEFAULT 1,
                                PRIMARY KEY (session_date, side)
                            )
                            """
                        )
                    )
                    print("Applied migration: created rs_conviction_challenger (PostgreSQL)")

            if "rs_conviction_promotion_log" not in table_names:
                if db_engine.dialect.name == "postgresql":
                    conn.execute(
                        text(
                            """
                            CREATE TABLE rs_conviction_promotion_log (
                                id BIGSERIAL PRIMARY KEY,
                                session_date DATE NOT NULL,
                                event_time TIMESTAMPTZ NOT NULL,
                                side TEXT NOT NULL,
                                event_type TEXT NOT NULL,
                                symbol TEXT NOT NULL,
                                replaced_symbol TEXT,
                                detail_json TEXT
                            )
                            """
                        )
                    )
                    print("Applied migration: created rs_conviction_promotion_log (PostgreSQL)")

            if "rs_conviction_scoring_log" not in table_names:
                if db_engine.dialect.name == "postgresql":
                    conn.execute(
                        text(
                            """
                            CREATE TABLE rs_conviction_scoring_log (
                                id BIGSERIAL PRIMARY KEY,
                                session_date DATE NOT NULL,
                                cycle_time TIMESTAMPTZ NOT NULL,
                                symbol TEXT NOT NULL,
                                side TEXT NOT NULL,
                                rs_component DOUBLE PRECISION,
                                opening_anchor DOUBLE PRECISION,
                                persistence_credit DOUBLE PRECISION,
                                slope_component DOUBLE PRECISION,
                                accum_component DOUBLE PRECISION,
                                whip_penalty DOUBLE PRECISION,
                                conviction_score DOUBLE PRECISION,
                                in_raw_top5 BOOLEAN
                            )
                            """
                        )
                    )
                    print("Applied migration: created rs_conviction_scoring_log (PostgreSQL)")

            if "rs_setup_radar" not in table_names:
                if db_engine.dialect.name == "postgresql":
                    conn.execute(
                        text(
                            """
                            CREATE TABLE rs_setup_radar (
                                id BIGSERIAL PRIMARY KEY,
                                session_date DATE NOT NULL,
                                symbol TEXT NOT NULL,
                                side TEXT NOT NULL,
                                setup_state TEXT DEFAULT 'NEUTRAL',
                                display_state TEXT DEFAULT 'NEUTRAL',
                                gap_atr DOUBLE PRECISION,
                                sl_rupees DOUBLE PRECISION,
                                sl_pct DOUBLE PRECISION,
                                ema5 DOUBLE PRECISION,
                                vwap DOUBLE PRECISION,
                                price DOUBLE PRECISION,
                                state_since TIMESTAMPTZ,
                                updated_at TIMESTAMPTZ DEFAULT NOW(),
                                UNIQUE (session_date, symbol)
                            )
                            """
                        )
                    )
                    print("Applied migration: created rs_setup_radar (PostgreSQL)")

            if "rs_setup_radar_log" not in table_names:
                if db_engine.dialect.name == "postgresql":
                    conn.execute(
                        text(
                            """
                            CREATE TABLE rs_setup_radar_log (
                                id BIGSERIAL PRIMARY KEY,
                                session_date DATE NOT NULL,
                                event_time TIMESTAMPTZ NOT NULL,
                                symbol TEXT NOT NULL,
                                side TEXT NOT NULL,
                                state_from TEXT,
                                state_to TEXT,
                                gap_atr DOUBLE PRECISION,
                                sl_pct DOUBLE PRECISION,
                                whipsaw_count INTEGER
                            )
                            """
                        )
                    )
                    print("Applied migration: created rs_setup_radar_log (PostgreSQL)")

                    print("Applied migration: created rs_setup_radar_log (PostgreSQL)")

            if "rs_conviction_state" in table_names:
                rs_state_cols = {col["name"] for col in inspector.get_columns("rs_conviction_state")}
                if "ema10_10m" not in rs_state_cols and db_engine.dialect.name == "postgresql":
                    conn.execute(text("ALTER TABLE rs_conviction_state ADD COLUMN ema10_10m DOUBLE PRECISION"))
                    print("Applied migration: rs_conviction_state.ema10_10m")

            if "rs_setup_radar" in table_names:
                radar_cols = {col["name"] for col in inspector.get_columns("rs_setup_radar")}
                if "gap_prev1" not in radar_cols and db_engine.dialect.name == "postgresql":
                    conn.execute(text("ALTER TABLE rs_setup_radar ADD COLUMN gap_prev1 DOUBLE PRECISION"))
                    print("Applied migration: rs_setup_radar.gap_prev1")

            # rs_scanner_history: CLIMACTIC maturity tag support (no schema change needed — TEXT tag)
            if "oi_heatmap_latest" not in table_names:
                if db_engine.dialect.name == "postgresql":
                    conn.execute(
                        text(
                            """
                            CREATE TABLE oi_heatmap_latest (
                                id BIGSERIAL PRIMARY KEY,
                                rank INTEGER NOT NULL,
                                instrument_key TEXT NOT NULL,
                                underlying_symbol TEXT,
                                trading_symbol TEXT,
                                expiry TEXT,
                                ltp DOUBLE PRECISION,
                                chg_pct DOUBLE PRECISION,
                                oi BIGINT,
                                oi_chg BIGINT,
                                oi_chg_pct DOUBLE PRECISION,
                                oi_signal TEXT,
                                prev_oi_signal TEXT,
                                volume BIGINT,
                                score DOUBLE PRECISION,
                                updated_at TIMESTAMPTZ NOT NULL
                            )
                            """
                        )
                    )
                    conn.execute(
                        text(
                            "CREATE INDEX IF NOT EXISTS idx_oi_heatmap_updated ON oi_heatmap_latest (updated_at DESC)"
                        )
                    )
                    print("Applied migration: created oi_heatmap_latest (PostgreSQL)")
            _insp_oi = inspect(db_engine)
            if "oi_heatmap_latest" in _insp_oi.get_table_names():
                _oi_cols = {c["name"] for c in _insp_oi.get_columns("oi_heatmap_latest")}
                if "prev_oi_signal" not in _oi_cols and db_engine.dialect.name == "postgresql":
                    conn.execute(text("ALTER TABLE oi_heatmap_latest ADD COLUMN prev_oi_signal TEXT"))
                    print("Applied migration: added oi_heatmap_latest.prev_oi_signal")

            # WebSocket-derived intraday 1m OHLC+OI candles (for today's backtest replay).
            if "upstox_ws_intraday_1m" not in table_names and db_engine.dialect.name == "postgresql":
                conn.execute(
                    text(
                        """
                        CREATE TABLE upstox_ws_intraday_1m (
                            instrument_key TEXT NOT NULL,
                            candle_time TIMESTAMPTZ NOT NULL,
                            open DOUBLE PRECISION NOT NULL,
                            high DOUBLE PRECISION NOT NULL,
                            low DOUBLE PRECISION NOT NULL,
                            close DOUBLE PRECISION NOT NULL,
                            oi_open BIGINT NOT NULL,
                            oi_high BIGINT NOT NULL,
                            oi_low BIGINT NOT NULL,
                            oi_close BIGINT NOT NULL,
                            updated_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
                            CONSTRAINT pk_upstox_ws_intraday_1m PRIMARY KEY (instrument_key, candle_time)
                        )
                        """
                    )
                )
                conn.execute(
                    text(
                        "CREATE INDEX IF NOT EXISTS idx_upstox_ws_intraday_1m_time "
                        "ON upstox_ws_intraday_1m (candle_time DESC)"
                    )
                )
                print("Applied migration: created upstox_ws_intraday_1m (PostgreSQL)")

                print("Applied migration: created upstox_ws_intraday_1m (PostgreSQL)")

            if "upstox_ws_intraday_1m" in inspect(db_engine).get_table_names():
                _ws1m_cols = {c["name"] for c in inspect(db_engine).get_columns("upstox_ws_intraday_1m")}
                for col, typ in (
                    ("volume", "BIGINT DEFAULT 0"),
                    ("bid_depth_qty", "BIGINT DEFAULT 0"),
                    ("ask_depth_qty", "BIGINT DEFAULT 0"),
                    ("tbq", "BIGINT DEFAULT 0"),
                    ("tsq", "BIGINT DEFAULT 0"),
                    ("candle_source", "TEXT DEFAULT 'ltp_tick'"),
                ):
                    if col not in _ws1m_cols and db_engine.dialect.name == "postgresql":
                        conn.execute(text(f"ALTER TABLE upstox_ws_intraday_1m ADD COLUMN {col} {typ}"))
                        print(f"Applied migration: upstox_ws_intraday_1m.{col}")

            if "upstox_ws_orderflow_latest" not in table_names and db_engine.dialect.name == "postgresql":
                conn.execute(
                    text(
                        """
                        CREATE TABLE upstox_ws_orderflow_latest (
                            instrument_key TEXT PRIMARY KEY,
                            bid_depth_qty BIGINT DEFAULT 0,
                            ask_depth_qty BIGINT DEFAULT 0,
                            depth_imbalance_ratio DOUBLE PRECISION,
                            tbq BIGINT DEFAULT 0,
                            tsq BIGINT DEFAULT 0,
                            pressure_ratio DOUBLE PRECISION,
                            oi BIGINT,
                            ltp DOUBLE PRECISION,
                            oi_change INTEGER DEFAULT 0,
                            updated_at TIMESTAMPTZ NOT NULL DEFAULT NOW()
                        )
                        """
                    )
                )
                print("Applied migration: created upstox_ws_orderflow_latest")

            if "upstox_ws_orderflow_1m" not in table_names and db_engine.dialect.name == "postgresql":
                conn.execute(
                    text(
                        """
                        CREATE TABLE upstox_ws_orderflow_1m (
                            instrument_key TEXT NOT NULL,
                            bucket_time TIMESTAMPTZ NOT NULL,
                            oi BIGINT,
                            oi_change INTEGER DEFAULT 0,
                            bid_depth_qty BIGINT DEFAULT 0,
                            ask_depth_qty BIGINT DEFAULT 0,
                            depth_imbalance_ratio DOUBLE PRECISION,
                            tbq BIGINT DEFAULT 0,
                            tsq BIGINT DEFAULT 0,
                            pressure_ratio DOUBLE PRECISION,
                            ltp DOUBLE PRECISION,
                            updated_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
                            CONSTRAINT pk_upstox_ws_orderflow_1m PRIMARY KEY (instrument_key, bucket_time)
                        )
                        """
                    )
                )
                conn.execute(
                    text(
                        "CREATE INDEX IF NOT EXISTS idx_upstox_ws_orderflow_1m_time "
                        "ON upstox_ws_orderflow_1m (bucket_time DESC)"
                    )
                )
                print("Applied migration: created upstox_ws_orderflow_1m")

            if "rs_silent_accumulation_log" not in table_names and db_engine.dialect.name == "postgresql":
                conn.execute(
                    text(
                        """
                        CREATE TABLE rs_silent_accumulation_log (
                            id BIGSERIAL PRIMARY KEY,
                            session_date DATE NOT NULL,
                            computed_at TIMESTAMPTZ NOT NULL,
                            symbol TEXT NOT NULL,
                            side TEXT NOT NULL,
                            accum_score DOUBLE PRECISION,
                            active BOOLEAN DEFAULT FALSE,
                            detail_json TEXT,
                            created_at TIMESTAMPTZ DEFAULT NOW()
                        )
                        """
                    )
                )
                conn.execute(
                    text(
                        "CREATE INDEX IF NOT EXISTS idx_rs_silent_accumulation_log "
                        "ON rs_silent_accumulation_log (session_date DESC, symbol, computed_at DESC)"
                    )
                )
                print("Applied migration: created rs_silent_accumulation_log")

            if "rs_momentum_ignition_log" not in table_names and db_engine.dialect.name == "postgresql":
                conn.execute(
                    text(
                        """
                        CREATE TABLE rs_momentum_ignition_log (
                            id BIGSERIAL PRIMARY KEY,
                            session_date DATE NOT NULL,
                            computed_at TIMESTAMPTZ NOT NULL,
                            symbol TEXT NOT NULL,
                            side TEXT NOT NULL,
                            ignition_score DOUBLE PRECISION,
                            ignition_building BOOLEAN DEFAULT FALSE,
                            components_json TEXT,
                            created_at TIMESTAMPTZ DEFAULT NOW()
                        )
                        """
                    )
                )
                conn.execute(
                    text(
                        "CREATE INDEX IF NOT EXISTS idx_rs_momentum_ignition_log "
                        "ON rs_momentum_ignition_log (session_date DESC, symbol, computed_at DESC)"
                    )
                )
                print("Applied migration: created rs_momentum_ignition_log")

            if "rs_universe_kavach_archive" not in table_names and db_engine.dialect.name == "postgresql":
                conn.execute(
                    text(
                        """
                        CREATE TABLE rs_universe_kavach_archive (
                            session_date DATE NOT NULL,
                            symbol TEXT NOT NULL,
                            instrument_key TEXT NOT NULL DEFAULT '',
                            future_symbol TEXT,
                            archive_time TIMESTAMPTZ NOT NULL,
                            kavach_state TEXT,
                            kavach_strength TEXT,
                            relative_strength DOUBLE PRECISION,
                            stock_percent DOUBLE PRECISION,
                            nifty_percent DOUBLE PRECISION,
                            volume_ratio DOUBLE PRECISION,
                            volume_tod_ratio DOUBLE PRECISION,
                            volume_label TEXT,
                            adx DOUBLE PRECISION,
                            trade_score DOUBLE PRECISION,
                            confidence_grade TEXT,
                            ranking_side TEXT,
                            would_be_rank_bull INTEGER,
                            would_be_rank_bear INTEGER,
                            universe_size INTEGER,
                            PRIMARY KEY (session_date, symbol)
                        )
                        """
                    )
                )
                conn.execute(
                    text(
                        "CREATE INDEX IF NOT EXISTS idx_rs_univ_kavach_archive_date "
                        "ON rs_universe_kavach_archive (session_date DESC)"
                    )
                )
                print("Applied migration: created rs_universe_kavach_archive")

            if "rs_universe_kavach_archive_run" not in table_names and db_engine.dialect.name == "postgresql":
                conn.execute(
                    text(
                        """
                        CREATE TABLE rs_universe_kavach_archive_run (
                            session_date DATE PRIMARY KEY,
                            archive_time TIMESTAMPTZ NOT NULL,
                            universe_size INTEGER,
                            symbols_archived INTEGER,
                            directional_bull INTEGER,
                            directional_bear INTEGER,
                            contract_month_hint TEXT,
                            instrument_key_sample TEXT,
                            prev_session_instrument_key_sample TEXT,
                            rollover_detected BOOLEAN DEFAULT FALSE
                        )
                        """
                    )
                )
                print("Applied migration: created rs_universe_kavach_archive_run")

            if "rs_shadow_selection_log" not in table_names and db_engine.dialect.name == "postgresql":
                conn.execute(
                    text(
                        """
                        CREATE TABLE rs_shadow_selection_log (
                            id BIGSERIAL PRIMARY KEY,
                            session_date DATE NOT NULL,
                            checkpoint_label TEXT NOT NULL,
                            checkpoint_time TIMESTAMPTZ NOT NULL,
                            side TEXT NOT NULL,
                            selection_method TEXT NOT NULL,
                            rank_position INTEGER NOT NULL,
                            symbol TEXT NOT NULL,
                            relative_strength DOUBLE PRECISION,
                            volume_ratio DOUBLE PRECISION,
                            vw_score DOUBLE PRECISION,
                            trade_score DOUBLE PRECISION,
                            kavach_state TEXT,
                            instrument_key TEXT,
                            scan_time TIMESTAMPTZ,
                            UNIQUE (session_date, checkpoint_label, side, selection_method, rank_position)
                        )
                        """
                    )
                )
                conn.execute(
                    text(
                        "CREATE INDEX IF NOT EXISTS idx_rs_shadow_sel_log_date "
                        "ON rs_shadow_selection_log (session_date DESC, checkpoint_label)"
                    )
                )
                print("Applied migration: created rs_shadow_selection_log")

            if "rs_shadow_tardy_addendum" not in table_names and db_engine.dialect.name == "postgresql":
                conn.execute(
                    text(
                        """
                        CREATE TABLE rs_shadow_tardy_addendum (
                            session_date DATE NOT NULL,
                            checkpoint_label TEXT NOT NULL DEFAULT '10:15',
                            symbol TEXT NOT NULL,
                            side TEXT NOT NULL,
                            relative_strength DOUBLE PRECISION,
                            volume_ratio DOUBLE PRECISION,
                            trade_score DOUBLE PRECISION,
                            kavach_state TEXT,
                            instrument_key TEXT,
                            on_morning_lock BOOLEAN DEFAULT FALSE,
                            logged_at TIMESTAMPTZ NOT NULL,
                            PRIMARY KEY (session_date, symbol, side)
                        )
                        """
                    )
                )
                print("Applied migration: created rs_shadow_tardy_addendum")

            # NSE (India) closed dates — IST calendar; scheduled market-data jobs skip these days.
            _insp_h = inspect(db_engine)
            _tables_h = _insp_h.get_table_names()
            if "holiday" not in _tables_h:
                if db_engine.dialect.name == "postgresql":
                    conn.execute(
                        text(
                            """
                            CREATE TABLE holiday (
                                id BIGSERIAL PRIMARY KEY,
                                holiday_date DATE NOT NULL,
                                description TEXT,
                                created_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
                                CONSTRAINT uq_holiday_date UNIQUE (holiday_date)
                            )
                            """
                        )
                    )
                    conn.execute(
                        text("CREATE INDEX IF NOT EXISTS idx_holiday_date ON holiday (holiday_date)")
                    )
                    print("Applied migration: created holiday (PostgreSQL)")
            if "holiday" in inspect(db_engine).get_table_names() and db_engine.dialect.name == "postgresql":
                conn.execute(
                    text(
                        """
                        INSERT INTO holiday (holiday_date, description) VALUES
                        ('2026-01-15', 'Municipal Corporation Election - Maharashtra'),
                        ('2026-01-26', 'Republic Day'),
                        ('2026-03-03', 'Holi'),
                        ('2026-03-26', 'Shri Ram Navami'),
                        ('2026-03-31', 'Shri Mahavir Jayanti'),
                        ('2026-04-03', 'Good Friday'),
                        ('2026-04-14', 'Dr. Baba Saheb Ambedkar Jayanti'),
                        ('2026-05-01', 'Maharashtra Day'),
                        ('2026-05-28', 'Bakri Id'),
                        ('2026-06-26', 'Muharram'),
                        ('2026-09-14', 'Ganesh Chaturthi'),
                        ('2026-10-02', 'Mahatma Gandhi Jayanti'),
                        ('2026-10-20', 'Dussehra'),
                        ('2026-11-10', 'Diwali-Balipratipada'),
                        ('2026-11-24', 'Prakash Gurpurb Sri Guru Nanak Dev'),
                        ('2026-12-25', 'Christmas')
                        ON CONFLICT (holiday_date) DO NOTHING
                        """
                    )
                )

            # Vajra futures rating (TWCTO trade qualification — curr-month FUT from arbitrage_master)
            if "vajra_futures_rating" not in table_names and db_engine.dialect.name == "postgresql":
                conn.execute(
                    text(
                        """
                        CREATE TABLE vajra_futures_rating (
                            id BIGSERIAL PRIMARY KEY,
                            session_date DATE NOT NULL,
                            stock TEXT NOT NULL,
                            future_symbol TEXT,
                            instrument_key TEXT NOT NULL,
                            trade_type TEXT NOT NULL,
                            confidence DOUBLE PRECISION NOT NULL,
                            bull_score DOUBLE PRECISION,
                            bear_score DOUBLE PRECISION,
                            structure_pass BOOLEAN NOT NULL DEFAULT FALSE,
                            momentum_pass BOOLEAN NOT NULL DEFAULT FALSE,
                            trend_pass BOOLEAN NOT NULL DEFAULT FALSE,
                            volume_pass BOOLEAN NOT NULL DEFAULT FALSE,
                            obv_label TEXT,
                            market_phase TEXT,
                            reversal_risk TEXT,
                            computed_at TIMESTAMPTZ NOT NULL,
                            CONSTRAINT uq_vajra_session_instrument UNIQUE (session_date, instrument_key)
                        )
                        """
                    )
                )
                conn.execute(
                    text(
                        "CREATE INDEX IF NOT EXISTS idx_vajra_session_conf "
                        "ON vajra_futures_rating (session_date, trade_type, confidence DESC)"
                    )
                )
                print("Applied migration: created vajra_futures_rating (PostgreSQL)")

            # BTST stock-options backtest (CSV-fed, read-only analysis)
            if "btst_backtest_runs" not in table_names and db_engine.dialect.name == "postgresql":
                conn.execute(
                    text(
                        """
                        CREATE TABLE btst_backtest_runs (
                            id SERIAL PRIMARY KEY,
                            run_date TIMESTAMP DEFAULT NOW(),
                            csv_filename TEXT,
                            notes TEXT
                        )
                        """
                    )
                )
                conn.execute(
                    text(
                        """
                        CREATE TABLE btst_strategy_config (
                            key TEXT PRIMARY KEY,
                            value TEXT NOT NULL,
                            updated_at TIMESTAMP DEFAULT NOW()
                        )
                        """
                    )
                )
                conn.execute(
                    text(
                        """
                        CREATE TABLE btst_backtest_results (
                            id SERIAL PRIMARY KEY,
                            run_id INT REFERENCES btst_backtest_runs(id),
                            trade_date DATE NOT NULL,
                            stock_symbol TEXT NOT NULL,
                            sector TEXT,
                            change_pct NUMERIC,
                            reference_price NUMERIC,
                            atm_strike NUMERIC,
                            direction TEXT,
                            option_symbol TEXT,
                            numeric_instrument_key TEXT,
                            data_mode TEXT,
                            supertrend_pass BOOLEAN,
                            hull_pass BOOLEAN,
                            eligible_final BOOLEAN,
                            entry_premium NUMERIC,
                            exit_a_premium NUMERIC,
                            exit_b_premium NUMERIC,
                            lot_size INT,
                            buy_cost NUMERIC,
                            exit_a_pnl NUMERIC,
                            exit_b_pnl NUMERIC,
                            no_data_reason TEXT
                        )
                        """
                    )
                )
                conn.execute(
                    text(
                        "CREATE INDEX IF NOT EXISTS idx_btst_results_run_date "
                        "ON btst_backtest_results (run_id, trade_date)"
                    )
                )
                conn.execute(
                    text(
                        "CREATE UNIQUE INDEX IF NOT EXISTS uq_btst_trade_date_symbol "
                        "ON btst_backtest_results (trade_date, stock_symbol)"
                    )
                )
                print("Applied migration: created btst_backtest tables (PostgreSQL)")

            if "btst_backtest_results" in table_names and db_engine.dialect.name == "postgresql":
                btst_cols = {c["name"] for c in inspect(db_engine).get_columns("btst_backtest_results")}
                run_cols = (
                    {c["name"] for c in inspect(db_engine).get_columns("btst_backtest_runs")}
                    if "btst_backtest_runs" in table_names
                    else set()
                )
                if "csv_filename" not in run_cols and "btst_backtest_runs" in table_names:
                    conn.execute(text("ALTER TABLE btst_backtest_runs ADD COLUMN csv_filename TEXT"))
                    print("Applied migration: btst_backtest_runs.csv_filename")
                for col, typ in (
                    ("sector", "TEXT"),
                    ("reference_price", "NUMERIC"),
                    ("numeric_instrument_key", "TEXT"),
                    ("change_pct", "NUMERIC"),
                    ("no_data_reason", "TEXT"),
                ):
                    if col not in btst_cols:
                        conn.execute(text(f"ALTER TABLE btst_backtest_results ADD COLUMN {col} {typ}"))
                        print(f"Applied migration: btst_backtest_results.{col}")
                if "change_pct_at_1445" in btst_cols:
                    conn.execute(
                        text(
                            """
                            UPDATE btst_backtest_results
                            SET change_pct = change_pct_at_1445
                            WHERE change_pct IS NULL AND change_pct_at_1445 IS NOT NULL
                            """
                        )
                    )
                if "no_eligible_reason" in btst_cols:
                    conn.execute(
                        text(
                            """
                            UPDATE btst_backtest_results
                            SET no_data_reason = no_eligible_reason
                            WHERE no_data_reason IS NULL AND no_eligible_reason IS NOT NULL
                            """
                        )
                    )
                if "direction" in btst_cols:
                    conn.execute(
                        text(
                            """
                            UPDATE btst_backtest_results
                            SET direction = CASE
                                WHEN direction ILIKE 'bullish' THEN 'CE'
                                WHEN direction ILIKE 'bearish' THEN 'PE'
                                ELSE direction
                            END
                            WHERE direction IS NOT NULL
                            """
                        )
                    )
                if "spot_price_1445" in btst_cols:
                    conn.execute(
                        text(
                            """
                            UPDATE btst_backtest_results
                            SET reference_price = spot_price_1445
                            WHERE reference_price IS NULL AND spot_price_1445 IS NOT NULL
                            """
                        )
                    )
                conn.execute(text("DROP INDEX IF EXISTS uq_btst_trade_date_side"))
                conn.execute(text("DROP INDEX IF EXISTS uq_btst_trade_date_symbol"))
                conn.execute(
                    text(
                        """
                        CREATE UNIQUE INDEX IF NOT EXISTS uq_btst_trade_date_symbol_dir
                        ON btst_backtest_results (trade_date, stock_symbol, direction)
                        WHERE stock_symbol IS NOT NULL AND TRIM(stock_symbol) <> ''
                          AND direction IS NOT NULL
                        """
                    )
                )

            # Legacy Smart Futures DB tables removed (screener rebuild); drop if still present.
            _sf_tables = (
                "smart_futures_order_audit",
                "smart_futures_position",
                "smart_futures_candidate",
                "smart_futures_config",
            )
            _names_now = set(inspect(db_engine).get_table_names())
            for _tbl in _sf_tables:
                if _tbl in _names_now:
                    if db_engine.dialect.name == "postgresql":
                        conn.execute(text(f'DROP TABLE IF EXISTS "{_tbl}" CASCADE'))
                    else:
                        conn.execute(text(f"DROP TABLE IF EXISTS {_tbl}"))
                    print(f"Applied migration: dropped legacy table {_tbl}")
                    _names_now.discard(_tbl)
    except Exception as migration_error:
        print(f"Warning: startup schema migration failed: {migration_error}")
