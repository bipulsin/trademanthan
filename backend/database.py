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

            if "stock_fin_sentiment" in table_names:
                sfs_cols = {col["name"] for col in inspect(db_engine).get_columns("stock_fin_sentiment")}
                if "current_combined_sentiment_reason" not in sfs_cols:
                    conn.execute(
                        text(
                            "ALTER TABLE stock_fin_sentiment ADD COLUMN current_combined_sentiment_reason TEXT"
                        )
                    )
                    print(
                        "Applied migration: added stock_fin_sentiment.current_combined_sentiment_reason"
                    )

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

            # Smart Futures (NSE F&O Renko intraday engine)
            conn.execute(
                text(
                    """
                    CREATE TABLE IF NOT EXISTS smart_futures_config (
                        id INTEGER PRIMARY KEY DEFAULT 1,
                        live_enabled BOOLEAN NOT NULL DEFAULT FALSE,
                        position_size SMALLINT NOT NULL DEFAULT 1,
                        partial_exit_enabled BOOLEAN NOT NULL DEFAULT FALSE,
                        brick_atr_period INTEGER NOT NULL DEFAULT 10,
                        brick_atr_override NUMERIC(18, 8),
                        updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
                        CONSTRAINT chk_sf_pos_size CHECK (position_size >= 1 AND position_size <= 3),
                        CONSTRAINT chk_sf_brick_atr_period CHECK (brick_atr_period >= 2 AND brick_atr_period <= 99),
                        CONSTRAINT chk_sf_config_singleton CHECK (id = 1)
                    )
                    """
                )
            )
            if "smart_futures_config" in table_names:
                _sfc_cols = {c["name"] for c in inspect(db_engine).get_columns("smart_futures_config")}
                if "brick_atr_period" not in _sfc_cols:
                    conn.execute(
                        text(
                            "ALTER TABLE smart_futures_config ADD COLUMN brick_atr_period INTEGER DEFAULT 10"
                        )
                    )
                    conn.execute(text("UPDATE smart_futures_config SET brick_atr_period = 10 WHERE brick_atr_period IS NULL"))
                    print("Applied migration: added smart_futures_config.brick_atr_period")
                _sfc_cols = {c["name"] for c in inspect(db_engine).get_columns("smart_futures_config")}
                if "brick_atr_override" not in _sfc_cols:
                    conn.execute(text("ALTER TABLE smart_futures_config ADD COLUMN brick_atr_override NUMERIC(18, 8)"))
                    print("Applied migration: added smart_futures_config.brick_atr_override")
            conn.execute(
                text(
                    """
                    INSERT INTO smart_futures_config (id, live_enabled, position_size, partial_exit_enabled)
                    SELECT 1, FALSE, 1, FALSE
                    WHERE NOT EXISTS (SELECT 1 FROM smart_futures_config WHERE id = 1)
                    """
                )
            )
            conn.execute(
                text(
                    """
                    CREATE TABLE IF NOT EXISTS smart_futures_candidate (
                        id BIGSERIAL PRIMARY KEY,
                        session_date DATE NOT NULL,
                        symbol TEXT NOT NULL,
                        instrument_key TEXT NOT NULL,
                        score SMALLINT NOT NULL DEFAULT 0,
                        direction TEXT NOT NULL DEFAULT 'NONE',
                        last_brick_color TEXT,
                        entry_signal BOOLEAN NOT NULL DEFAULT FALSE,
                        exit_ready BOOLEAN NOT NULL DEFAULT FALSE,
                        main_brick_size NUMERIC(18, 8),
                        ltp NUMERIC(18, 4),
                        prefilter_pass BOOLEAN NOT NULL DEFAULT FALSE,
                        structure_pass BOOLEAN NOT NULL DEFAULT FALSE,
                        updated_at TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP,
                        CONSTRAINT uq_sfc_session_symbol UNIQUE (session_date, symbol)
                    )
                    """
                )
            )
            conn.execute(
                text(
                    """
                    CREATE INDEX IF NOT EXISTS idx_sfc_session_score
                    ON smart_futures_candidate (session_date, score DESC)
                    """
                )
            )
            conn.execute(
                text(
                    """
                    CREATE TABLE IF NOT EXISTS smart_futures_position (
                        id BIGSERIAL PRIMARY KEY,
                        session_date DATE NOT NULL,
                        user_id INTEGER,
                        symbol TEXT NOT NULL,
                        instrument_key TEXT NOT NULL,
                        direction TEXT NOT NULL,
                        lots_open INTEGER NOT NULL DEFAULT 1,
                        lots_total INTEGER NOT NULL DEFAULT 1,
                        entry_price NUMERIC(18, 4),
                        main_brick_size NUMERIC(18, 8),
                        half_brick_size NUMERIC(18, 8),
                        entry_order_id TEXT,
                        status TEXT NOT NULL DEFAULT 'OPEN',
                        created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
                        closed_at TIMESTAMP,
                        CONSTRAINT chk_sf_pos_lots CHECK (lots_open >= 0 AND lots_total >= 1)
                    )
                    """
                )
            )
            conn.execute(
                text(
                    """
                    CREATE INDEX IF NOT EXISTS idx_sfp_open
                    ON smart_futures_position (session_date, status)
                    """
                )
            )
            conn.execute(
                text(
                    """
                    CREATE TABLE IF NOT EXISTS smart_futures_order_audit (
                        id BIGSERIAL PRIMARY KEY,
                        user_id INTEGER,
                        position_id BIGINT,
                        side TEXT NOT NULL,
                        order_id TEXT,
                        quantity INTEGER,
                        created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
                    )
                    """
                )
            )
    except Exception as migration_error:
        print(f"Warning: startup schema migration failed: {migration_error}")
