"""Smart Futures picker: CMS-based scans from arbitrage_master → smart_futures_daily."""

from __future__ import annotations

from typing import Any

__all__ = ["run_smart_futures_picker_job"]


def __getattr__(name: str) -> Any:
    if name == "run_smart_futures_picker_job":
        from backend.services.smart_futures_picker.job import run_smart_futures_picker_job as fn

        return fn
    raise AttributeError(f"module {__name__!r} has no attribute {name!r}")
