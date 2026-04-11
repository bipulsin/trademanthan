"""
Scheduled job: NSE corporate announcements for arbitrage_master stocks,
FinBERT on announcement text, combined score persisted with last/current rotation.

Replaces MarketAux: NSE JSON has no entity sentiment; api_sentiment_avg is left null
and combined_sentiment_avg follows FinBERT when available.
"""
from __future__ import annotations

import logging
from collections import defaultdict
from dataclasses import dataclass
from datetime import datetime, timedelta
from typing import Any, Dict, List, Optional, Tuple

import pytz
from sqlalchemy import text
from sqlalchemy.orm import Session

from backend.database import SessionLocal
from backend.models.fin_sentiment import FinSentimentJobState, StockFinSentiment
from backend.services.nse_corporate_client import NseCorporateAnnouncementsClient

logger = logging.getLogger(__name__)

IST = pytz.timezone("Asia/Kolkata")
MAX_ITEMS_PER_STOCK = 5


@dataclass
class CorpHit:
    seq_id: str
    published_utc: datetime
    title: str


def _parse_row_time_utc(row: Dict[str, Any]) -> Optional[datetime]:
    from dateutil import parser as date_parser

    for key in ("sort_date", "an_dt", "anDt"):
        v = row.get(key)
        if not v:
            continue
        try:
            dt = date_parser.parse(str(v))
            if dt.tzinfo is None:
                dt = IST.localize(dt)
            return dt.astimezone(pytz.UTC)
        except Exception:
            continue
    return None


def _row_seq_id(row: Dict[str, Any]) -> str:
    for k in ("seq_id", "seqId", "SeqId"):
        v = row.get(k)
        if v is not None and str(v).strip():
            return str(v).strip()
    sym = (row.get("symbol") or "").strip()
    dt = (row.get("dt") or row.get("sort_date") or "").strip()
    return f"{sym}|{dt}"


def _row_title(row: Dict[str, Any]) -> str:
    parts = [
        (row.get("sm_name") or "").strip(),
        (row.get("desc") or "").strip(),
    ]
    return " — ".join(p for p in parts if p) or "(no title)"


def _finbert_numeric_from_scores(scores: Dict[str, Any]) -> float:
    def _g(*keys: str) -> float:
        for k in keys:
            for cand in (k, k.lower(), k.upper()):
                if cand in scores:
                    try:
                        return float(scores[cand])
                    except (TypeError, ValueError):
                        pass
        return 0.0

    pos = _g("positive", "Positive", "POSITIVE")
    neg = _g("negative", "Negative", "NEGATIVE")
    return max(-1.0, min(1.0, pos - neg))


def _get_watermark(db: Session) -> datetime:
    row = db.get(FinSentimentJobState, 1)
    if row and row.watermark:
        w = row.watermark
        return w if w.tzinfo else pytz.UTC.localize(w)
    return datetime.now(pytz.UTC) - timedelta(hours=48)


def _set_watermark(db: Session, ts: datetime) -> None:
    row = db.get(FinSentimentJobState, 1)
    if not row:
        row = FinSentimentJobState(id=1, watermark=ts)
        db.add(row)
    else:
        row.watermark = ts


def _load_arbitrage_rows(db: Session) -> List[Tuple[str, Optional[str]]]:
    rows = db.execute(
        text(
            """
            SELECT stock, stock_instrument_key
            FROM arbitrage_master
            WHERE stock IS NOT NULL AND TRIM(stock) <> ''
            ORDER BY stock
            """
        )
    ).fetchall()
    out: List[Tuple[str, Optional[str]]] = []
    for r in rows:
        if not r or not r[0]:
            continue
        out.append((str(r[0]).strip().upper(), (str(r[1]).strip() if r[1] else None)))
    return out


def run_fin_sentiment_job() -> Dict[str, Any]:
    """
    Entry point for APScheduler. One NSE API pull per run; filters since watermark.
    """
    summary: Dict[str, Any] = {
        "ok": False,
        "stocks_considered": 0,
        "stocks_with_news": 0,
        "rows_upserted": 0,
        "nse_announcements_total": 0,
        "skipped": None,
    }

    now_utc = datetime.now(pytz.UTC)
    now_ist = now_utc.astimezone(IST)

    db = SessionLocal()
    try:
        watermark = _get_watermark(db)
        arb = _load_arbitrage_rows(db)
        summary["stocks_considered"] = len(arb)
        if not arb:
            summary["skipped"] = "arbitrage_master empty"
            _set_watermark(db, now_utc)
            db.commit()
            summary["ok"] = True
            return summary

        arb_set = {s for s, _ in arb}
        client = NseCorporateAnnouncementsClient(lookback_calendar_days=2)
        nse_ok, raw = client.fetch_equity_announcements()
        if not nse_ok:
            summary["skipped"] = "nse_corporate_announcements_unavailable"
            summary["ok"] = False
            logger.warning("Fin sentiment job skipped: %s", summary["skipped"])
            return summary

        summary["nse_announcements_total"] = len(raw)

        bucket: Dict[str, List[CorpHit]] = defaultdict(list)
        seen: Dict[str, set] = defaultdict(set)

        for row in raw:
            sym = (row.get("symbol") or "").strip().upper()
            if not sym or sym not in arb_set:
                continue
            pub = _parse_row_time_utc(row)
            if pub is None or pub <= watermark:
                continue
            sid = _row_seq_id(row)
            if sid in seen[sym]:
                continue
            seen[sym].add(sid)
            bucket[sym].append(CorpHit(seq_id=sid, published_utc=pub, title=_row_title(row)))

        finbert_ok = False
        try:
            from backend.services.finbert_service import is_finbert_available, predict_sentiment

            finbert_ok = is_finbert_available()
        except Exception:
            finbert_ok = False

        for sym, ikey in arb:
            hits = bucket.get(sym) or []
            if not hits:
                continue
            summary["stocks_with_news"] += 1
            hits.sort(key=lambda h: h.published_utc.timestamp(), reverse=True)
            hits = hits[:MAX_ITEMS_PER_STOCK]
            api_avg: Optional[float] = None

            titles = [h.title[:512] for h in hits]
            nlp_avg: Optional[float] = None
            if finbert_ok and titles:
                try:
                    fb = predict_sentiment(titles)
                    nums = [_finbert_numeric_from_scores(x.get("scores") or {}) for x in fb]
                    nlp_avg = sum(nums) / len(nums) if nums else None
                except Exception as e:
                    logger.warning("FinBERT batch failed for %s: %s", sym, e)
                    nlp_avg = None

            if api_avg is not None and nlp_avg is not None:
                combined = (float(api_avg) + float(nlp_avg)) / 2.0
            elif api_avg is not None:
                combined = float(api_avg)
            elif nlp_avg is not None:
                combined = float(nlp_avg)
            else:
                combined = None

            if combined is None:
                continue

            row = db.get(StockFinSentiment, sym)
            prev_current = float(row.current_combined_sentiment) if row and row.current_combined_sentiment is not None else None

            if row is None:
                row = StockFinSentiment(
                    stock=sym,
                    stock_instrument_key=ikey,
                    api_sentiment_avg=api_avg,
                    nlp_sentiment_avg=nlp_avg,
                    combined_sentiment_avg=combined,
                    last_combined_sentiment=None,
                    current_combined_sentiment=combined,
                    news_count=len(hits),
                    current_run_at=now_ist,
                )
                db.add(row)
            else:
                row.stock_instrument_key = ikey or row.stock_instrument_key
                row.api_sentiment_avg = api_avg
                row.nlp_sentiment_avg = nlp_avg
                row.combined_sentiment_avg = combined
                row.last_combined_sentiment = prev_current
                row.current_combined_sentiment = combined
                row.news_count = len(hits)
                row.current_run_at = now_ist

            summary["rows_upserted"] += 1

        _set_watermark(db, now_utc)
        db.commit()
        summary["ok"] = True
        logger.info(
            "Fin sentiment job OK (NSE corporate): stocks=%s with_news=%s rows=%s nse_rows=%s",
            summary["stocks_considered"],
            summary["stocks_with_news"],
            summary["rows_upserted"],
            summary["nse_announcements_total"],
        )
        return summary
    except Exception as e:
        logger.error("Fin sentiment job failed: %s", e, exc_info=True)
        db.rollback()
        summary["error"] = str(e)
        return summary
    finally:
        db.close()
