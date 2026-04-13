import os

import backend.env_bootstrap  # noqa: F401 — load `<project_root>/.env` before os.getenv

from sqlalchemy import create_engine, inspect, text
from sqlalchemy.orm import sessionmaker

# Database configuration - PostgreSQL production database (configurable via DATABASE_URL env var)
DATABASE_URL = os.getenv("DATABASE_URL", "postgresql://trademanthan:trademanthan123@localhost/trademanthan")

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
