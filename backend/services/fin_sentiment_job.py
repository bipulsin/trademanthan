"""
Scheduled job: MarketAux news + entity sentiment for arbitrage_master stocks,
FinBERT on titles, combined score persisted with last/current rotation.
"""
from __future__ import annotations

import logging
import time
from collections import defaultdict
from dataclasses import dataclass
from datetime import datetime, timedelta
from typing import Any, Dict, List, Optional, Tuple

import pytz
from sqlalchemy import text
from sqlalchemy.orm import Session

from backend.config import settings
from backend.database import SessionLocal
from backend.models.fin_sentiment import FinSentimentJobState, StockFinSentiment
from backend.services.marketaux_client import MarketauxClient

logger = logging.getLogger(__name__)

IST = pytz.timezone("Asia/Kolkata")
SYMBOL_CHUNK = 12  # keep URLs small; tune if your MarketAux plan allows more
MAX_NEWS_PER_STOCK = 5
BATCH_SLEEP_SEC = 0.12


@dataclass
class ArticleHit:
    uuid: str
    published_at: str
    title: str
    entity_sentiment: float


def _parse_ts(s: str) -> float:
    if not s:
        return 0.0
    s = str(s).replace("Z", "+00:00")
    try:
        from dateutil import parser as date_parser

        return date_parser.parse(s).timestamp()
    except Exception:
        return 0.0


def _entity_sentiment_for_symbol(article: Dict[str, Any], symbol_upper: str) -> Optional[float]:
    entities = article.get("entities") or article.get("entity") or []
    if isinstance(entities, dict):
        entities = [entities]
    if not isinstance(entities, list):
        return None
    best: Optional[float] = None
    for ent in entities:
        if not isinstance(ent, dict):
            continue
        sym = (ent.get("symbol") or ent.get("ticker") or "").strip().upper()
        if sym != symbol_upper:
            continue
        for key in ("sentiment_score", "sentiment", "overall_sentiment_score"):
            v = ent.get(key)
            if v is None:
                continue
            try:
                f = float(v)
                if f == f:  # not NaN
                    best = f if best is None else (best + f) / 2
            except (TypeError, ValueError):
                continue
    return best


def _finbert_numeric_from_scores(scores: Dict[str, Any]) -> float:
    """Map FinBERT class probs to [-1, 1] similar to MarketAux entity score."""
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
        return row.watermark if row.watermark.tzinfo else pytz.UTC.localize(row.watermark)
    return datetime.now(pytz.UTC) - timedelta(minutes=90)


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
    Entry point for APScheduler. Idempotent per watermark; commits once at end.
    """
    summary: Dict[str, Any] = {
        "ok": False,
        "stocks_considered": 0,
        "stocks_with_news": 0,
        "rows_upserted": 0,
        "batches": 0,
        "skipped": None,
    }
    token = (settings.MARKETAUX_API_TOKEN or "").strip()
    if not token:
        summary["skipped"] = "MARKETAUX_API_TOKEN missing"
        logger.info("Fin sentiment job skipped: %s", summary["skipped"])
        return summary

    client = MarketauxClient(token)
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

        symbols = [s for s, _ in arb]

        # symbol -> list ArticleHit (dedupe uuid across all batches)
        bucket: Dict[str, List[ArticleHit]] = defaultdict(list)
        seen_uuid: Dict[str, set] = defaultdict(set)

        batches = 0
        for i in range(0, len(symbols), SYMBOL_CHUNK):
            chunk = symbols[i : i + SYMBOL_CHUNK]
            articles = client.fetch_news_for_symbols(chunk, watermark, limit=100)
            batches += 1
            for art in articles:
                uid = str(art.get("uuid") or art.get("id") or "").strip()
                title = (art.get("title") or art.get("snippet") or "").strip() or "(no title)"
                pub = str(art.get("published_at") or art.get("date") or "")
                for sym in chunk:
                    sc = _entity_sentiment_for_symbol(art, sym)
                    if sc is None:
                        continue
                    if uid and uid in seen_uuid[sym]:
                        continue
                    if uid:
                        seen_uuid[sym].add(uid)
                    bucket[sym].append(ArticleHit(uuid=uid, published_at=pub, title=title, entity_sentiment=float(sc)))
            time.sleep(BATCH_SLEEP_SEC)

        summary["batches"] = batches

        # Per stock: sort by time desc, keep 5, compute API avg
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
            hits.sort(key=lambda h: _parse_ts(h.published_at), reverse=True)
            hits = hits[:MAX_NEWS_PER_STOCK]
            api_scores = [h.entity_sentiment for h in hits]
            api_avg = sum(api_scores) / len(api_scores) if api_scores else None

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
            "Fin sentiment job OK: stocks=%s with_news=%s rows=%s batches=%s",
            summary["stocks_considered"],
            summary["stocks_with_news"],
            summary["rows_upserted"],
            summary["batches"],
        )
        return summary
    except Exception as e:
        logger.error("Fin sentiment job failed: %s", e, exc_info=True)
        db.rollback()
        summary["error"] = str(e)
        return summary
    finally:
        db.close()
