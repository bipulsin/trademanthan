#!/usr/bin/env python3
"""
One-time (or --force) backfill of stock_fin_sentiment.current_combined_sentiment_reason using OpenAI.

1) Parse latest [fin_sentiment][S09] UPSERT lines from scan_st1_algo.log for sample titles.
2) For each DB row, merge log title with NSE attchmntText excerpt (latest row for symbol, 2-day window).
3) Call gpt-4o-mini (same as scheduler) with title + excerpt + stored nlp/combined scores.

  PYTHONPATH=. python backend/scripts/backfill_fin_sentiment_reason_openai.py
  PYTHONPATH=. python backend/scripts/backfill_fin_sentiment_reason_openai.py --force
"""
from __future__ import annotations

import argparse
import logging
import sys
from pathlib import Path

ROOT = Path(__file__).resolve().parents[2]
LOG_DEFAULT = ROOT / "logs" / "scan_st1_algo.log"


def _setup_logging() -> None:
    logging.basicConfig(
        level=logging.INFO,
        format="%(asctime)s - %(levelname)s - %(name)s - %(message)s",
        stream=sys.stdout,
    )


def main() -> int:
    import backend.env_bootstrap  # noqa: F401 — load .env before Settings

    _setup_logging()
    log = logging.getLogger("backfill_fin_sentiment_reason")

    parser = argparse.ArgumentParser()
    parser.add_argument(
        "--log-path",
        default=str(LOG_DEFAULT),
        help="Path to scan_st1_algo.log (S09 UPSERT lines)",
    )
    parser.add_argument(
        "--force",
        action="store_true",
        help="Overwrite rows that already have a non-empty reason",
    )
    args = parser.parse_args()

    from backend.config import settings

    if not (settings.OPENAI_API_KEY or "").strip():
        log.error("OPENAI_API_KEY is not set; aborting.")
        return 2

    from backend.database import SessionLocal
    from backend.models.fin_sentiment import StockFinSentiment
    from backend.services.fin_sentiment_reason_openai import (
        derive_sentiment_reason_openai,
        fetch_latest_nse_announcement_for_symbol,
        load_latest_s09_samples_from_log,
    )

    samples = load_latest_s09_samples_from_log(args.log_path)
    log.info("Loaded %s symbols from log samples", len(samples))

    db = SessionLocal()
    try:
        rows = db.query(StockFinSentiment).order_by(StockFinSentiment.stock).all()
        updated = 0
        skipped = 0
        for row in rows:
            sym = row.stock
            if row.current_combined_sentiment_reason and str(row.current_combined_sentiment_reason).strip():
                if not args.force:
                    skipped += 1
                    continue

            title_log = samples.get(sym)
            nse_pkg = fetch_latest_nse_announcement_for_symbol(sym, lookback_calendar_days=2)
            title_nse = (nse_pkg or {}).get("title") or ""
            detail = (nse_pkg or {}).get("detail_excerpt") or ""

            title = (title_log or title_nse or "").strip()
            if not title and not detail:
                log.warning("%s: no log sample and no NSE text; skip", sym)
                skipped += 1
                continue

            if not title:
                title = title_nse or sym

            announcements = [{"title": title[:512], "detail_excerpt": detail[:6000]}]
            reason = derive_sentiment_reason_openai(
                symbol=sym,
                announcements=announcements,
                nlp_sentiment_avg=row.nlp_sentiment_avg,
                combined_sentiment_avg=row.current_combined_sentiment or row.combined_sentiment_avg,
            )
            if not reason:
                log.warning("%s: OpenAI returned no reason; skip", sym)
                skipped += 1
                continue

            row.current_combined_sentiment_reason = reason
            updated += 1
            log.info("%s: updated reason len=%s", sym, len(reason))

        db.commit()
        log.info("Done updated=%s skipped=%s", updated, skipped)
        return 0
    except Exception as e:
        log.exception("backfill failed: %s", e)
        db.rollback()
        return 1
    finally:
        db.close()


if __name__ == "__main__":
    raise SystemExit(main())
