"""Smart Futures picker: CMS-based scans from arbitrage_master → smart_futures_daily."""

from backend.services.smart_futures_picker.job import run_smart_futures_picker_job

__all__ = ["run_smart_futures_picker_job"]
