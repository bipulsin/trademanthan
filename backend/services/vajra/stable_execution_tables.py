"""Per-user stable execution mode state (IST session)."""
from __future__ import annotations

import json
from typing import Any, Dict, List, Optional

from sqlalchemy import text
from sqlalchemy.orm import Session


def ensure_vajra_stable_execution_table(db: Session) -> None:
    db.execute(
        text(
            """
            CREATE TABLE IF NOT EXISTS vajra_stable_execution_state (
                user_id INTEGER NOT NULL,
                session_date DATE NOT NULL,
                stable_mode_enabled BOOLEAN NOT NULL DEFAULT TRUE,
                focus_mode_enabled BOOLEAN NOT NULL DEFAULT FALSE,
                sticky_persist_minutes INTEGER NOT NULL DEFAULT 30,
                frozen_focus_stocks JSONB NOT NULL DEFAULT '[]',
                watchlist_frozen_at TIMESTAMPTZ,
                sticky_slots JSONB NOT NULL DEFAULT '[]',
                updated_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
                PRIMARY KEY (user_id, session_date)
            )
            """
        )
    )
