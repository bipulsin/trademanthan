"""Postgres advisory locks so two app instances never double-run a Tarang job."""
from __future__ import annotations

import hashlib
import logging
from contextlib import contextmanager
from typing import Iterator

from sqlalchemy import text

from backend.database import engine

logger = logging.getLogger(__name__)


def _lock_key(name: str) -> int:
    h = hashlib.sha256(f"tarang:{name}".encode()).digest()
    return int.from_bytes(h[:8], "big") % (2**31 - 1)


@contextmanager
def try_job_lock(name: str) -> Iterator[bool]:
    """Hold a session-level advisory lock on one connection for the job body."""
    key = _lock_key(name)
    conn = engine.connect()
    held = False
    try:
        row = conn.execute(text("SELECT pg_try_advisory_lock(:k) AS got"), {"k": key}).mappings().first()
        held = bool(row and row.get("got"))
        yield held
    except Exception:
        logger.exception("advisory lock %s failed", name)
        yield False
    finally:
        if held:
            try:
                conn.execute(text("SELECT pg_advisory_unlock(:k)"), {"k": key})
            except Exception:
                logger.exception("advisory unlock %s failed", name)
        conn.close()
