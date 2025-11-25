from sqlalchemy import create_engine
from sqlalchemy.orm import sessionmaker
import os
from dotenv import load_dotenv

load_dotenv()

# Database configuration - PostgreSQL production database (configurable via DATABASE_URL env var)
DATABASE_URL = os.getenv("DATABASE_URL", "postgresql://trademanthan:trademanthan123@localhost/trademanthan")

# Import Base from models.base to ensure all models use the same Base instance
try:
    from backend.models.base import Base
except ImportError:
    from models.base import Base

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
        print("Database tables created successfully")
    except Exception as e:
        print(f"Error creating tables: {e}")
        raise
