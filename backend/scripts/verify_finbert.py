#!/usr/bin/env python3
"""Verify FinBERT install: load ProsusAI/finbert and run one inference."""
from __future__ import annotations

import sys
from pathlib import Path

ROOT = Path(__file__).resolve().parent.parent.parent
sys.path.insert(0, str(ROOT))

from backend.services.finbert_service import is_finbert_available, preload, predict_sentiment


def main() -> int:
    if not is_finbert_available():
        print("Missing deps: install torch (CPU wheel) and requirements-ml.txt")
        return 1
    preload()
    out = predict_sentiment(
        "Operating margin improved year over year despite higher rates."
    )
    print("OK:", out)
    return 0


if __name__ == "__main__":
    sys.exit(main())
