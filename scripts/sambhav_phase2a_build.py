#!/usr/bin/env python3
"""Build Sambhav Phase 2A feature/target research dataset (no model training)."""

from __future__ import annotations

import json
import sys
from pathlib import Path

ROOT = Path(__file__).resolve().parents[1]
if str(ROOT) not in sys.path:
    sys.path.insert(0, str(ROOT))

from backend.database import SessionLocal  # noqa: E402
from backend.services.sambhav.phase2a import run_phase2a  # noqa: E402
from backend.services.sambhav.tables import ensure_sambhav_tables  # noqa: E402


def main() -> int:
    ensure_sambhav_tables()
    db = SessionLocal()
    try:
        research = run_phase2a(db, persist=True)
        print(json.dumps(research, indent=2, default=str))
        return 0 if research.get("status") == "PASS" else 1
    finally:
        db.close()


if __name__ == "__main__":
    raise SystemExit(main())
