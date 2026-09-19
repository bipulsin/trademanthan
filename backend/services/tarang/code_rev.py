"""Resolve the running Tarang code commit for immutable snapshots."""
from __future__ import annotations

import os
import subprocess
from functools import lru_cache
from pathlib import Path


@lru_cache(maxsize=1)
def code_commit() -> str:
    for key in ("TRADEMANTHAN_REF", "APP_SRC_REV", "GIT_SHA", "SOURCE_VERSION"):
        v = (os.getenv(key) or "").strip()
        if v:
            return v[:64]
    root = Path(__file__).resolve().parents[3]
    try:
        out = subprocess.check_output(
            ["git", "rev-parse", "HEAD"],
            cwd=str(root),
            stderr=subprocess.DEVNULL,
            timeout=3,
        )
        return out.decode().strip()[:64]
    except Exception:
        return "unknown"


def rule_set_version() -> int:
    from backend.services.tarang.config import get_risk

    try:
        return int(get_risk().get("version") or 1)
    except (TypeError, ValueError):
        return 1
