"""PostgreSQL connection pool for Rocket (SQLAlchemy 2.0)."""

from __future__ import annotations

from contextlib import contextmanager
from typing import Generator, Optional

from sqlalchemy import create_engine
from sqlalchemy.engine import Engine
from sqlalchemy.orm import Session, sessionmaker

from rocket.config.settings import get_settings

_engine: Optional[Engine] = None
_SessionLocal: Optional[sessionmaker] = None


def get_engine(database_url: Optional[str] = None) -> Engine:
    global _engine, _SessionLocal
    url = database_url or get_settings().DATABASE_URL
    if _engine is None:
        _engine = create_engine(
            url,
            pool_pre_ping=True,
            pool_size=5,
            max_overflow=10,
            pool_recycle=1800,
        )
        _SessionLocal = sessionmaker(autocommit=False, autoflush=False, bind=_engine)
    return _engine


def get_session_factory(database_url: Optional[str] = None) -> sessionmaker:
    get_engine(database_url)
    assert _SessionLocal is not None
    return _SessionLocal


@contextmanager
def session_scope(database_url: Optional[str] = None) -> Generator[Session, None, None]:
    factory = get_session_factory(database_url)
    session = factory()
    try:
        yield session
        session.commit()
    except Exception:
        session.rollback()
        raise
    finally:
        session.close()
