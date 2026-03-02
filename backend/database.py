from sqlalchemy import create_engine, inspect, text
from sqlalchemy.orm import sessionmaker
import os
from dotenv import load_dotenv

load_dotenv()

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
    except Exception as migration_error:
        print(f"Warning: startup schema migration failed: {migration_error}")
