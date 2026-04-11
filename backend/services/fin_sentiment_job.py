"""
Scheduled job: NSE corporate announcements for arbitrage_master stocks,
FinBERT on announcement text, combined score persisted with last/current rotation.

Replaces MarketAux: NSE JSON has no entity sentiment; api_sentiment_avg is left null
and combined_sentiment_avg follows FinBERT when available.

Logs use prefix [fin_sentiment][Sxx] for stage analysis in scan_st1_algo.log.
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
from backend.services.fin_sentiment_reason_openai import (
    derive_sentiment_reason_openai,
    nse_attachment_excerpt,
)
from backend.services.nse_corporate_client import NseCorporateAnnouncementsClient

logger = logging.getLogger(__name__)

IST = pytz.timezone("Asia/Kolkata")
MAX_ITEMS_PER_STOCK = 5
# NSE from_date/to_date: IST calendar span (0 = today only).
DEFAULT_NSE_LOOKBACK_CALENDAR_DAYS = 0
REASON_MAX_LEN = 2000


def _build_current_combined_sentiment_reason(
    *,
    n_hits: int,
    api_avg: Optional[float],
    nlp_avg: Optional[float],
) -> str:
    """
    Short text stored on each update: what current_combined_sentiment represents this run.
    """
    core = (
        "Same as combined_sentiment_avg for this run: ~−1 bearish to +1 bullish. "
    )
    if api_avg is not None and nlp_avg is not None:
        src = (
            f"Average of third-party entity sentiment and FinBERT over {n_hits} "
            f"announcement(s) since the job watermark."
        )
    elif nlp_avg is not None:
        src = (
            f"Mean FinBERT score on up to {n_hits} latest NSE equity corporate announcement(s) "
            f"for this symbol since the job watermark; NSE filings carry no vendor sentiment score."
        )
    elif api_avg is not None:
        src = (
            f"Mean third-party entity sentiment on {n_hits} item(s); FinBERT was not applied or failed."
        )
    else:
        src = "No scored inputs; row should not normally persist with a combined value."
    text = (core + src).strip()
    if len(text) > REASON_MAX_LEN:
        return text[: REASON_MAX_LEN - 1] + "…"
    return text


@dataclass
class CorpHit:
    seq_id: str
    published_utc: datetime
    title: str
    detail_excerpt: str


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
    joined = " — ".join(p for p in parts if p)
    return joined if joined else "(no title)"


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


def run_fin_sentiment_job(
    *,
    nse_lookback_calendar_days: int = DEFAULT_NSE_LOOKBACK_CALENDAR_DAYS,
) -> Dict[str, Any]:
    """
    Entry point for APScheduler. One NSE API pull per run; filters since watermark.

    nse_lookback_calendar_days: 0 = single IST calendar day (today); 1 = today+yesterday, etc.
    """
    summary: Dict[str, Any] = {
        "ok": False,
        "stocks_considered": 0,
        "stocks_with_news": 0,
        "rows_upserted": 0,
        "nse_announcements_total": 0,
        "skipped": None,
        "filter_raw_rows": 0,
        "filter_symbol_in_arb": 0,
        "filter_symbol_not_arb": 0,
        "filter_no_publish_time": 0,
        "filter_at_or_before_watermark": 0,
        "filter_duplicate_seq": 0,
        "filter_accepted_hits": 0,
        "openai_reason_ok": 0,
        "openai_reason_fallback": 0,
    }

    now_utc = datetime.now(pytz.UTC)
    now_ist = now_utc.astimezone(IST)
    run_tag = now_ist.strftime("%Y-%m-%d %H:%M:%S %Z")

    logger.info("[fin_sentiment][S01] start run_tag=%s nse_lookback_calendar_days=%s", run_tag, nse_lookback_calendar_days)

    db = SessionLocal()
    try:
        watermark = _get_watermark(db)
        logger.info("[fin_sentiment][S02] watermark_utc=%s", watermark.isoformat())

        arb = _load_arbitrage_rows(db)
        summary["stocks_considered"] = len(arb)
        logger.info("[fin_sentiment][S03] arbitrage_master rows=%s", len(arb))

        if not arb:
            summary["skipped"] = "arbitrage_master empty"
            logger.info("[fin_sentiment][S04] skip reason=%s advance_watermark=True", summary["skipped"])
            _set_watermark(db, now_utc)
            db.commit()
            summary["ok"] = True
            logger.info(
                "[fin_sentiment][S99] end ok=%s openai_ok=%s openai_fallback=%s summary=%s",
                summary["ok"],
                summary.get("openai_reason_ok"),
                summary.get("openai_reason_fallback"),
                summary,
            )
            return summary

        arb_set = {s for s, _ in arb}
        client = NseCorporateAnnouncementsClient(lookback_calendar_days=nse_lookback_calendar_days)
        logger.info("[fin_sentiment][S05] nse client created; fetching corporate-announcements")

        nse_ok, raw = client.fetch_equity_announcements()
        if not nse_ok:
            summary["skipped"] = "nse_corporate_announcements_unavailable"
            summary["ok"] = False
            logger.warning(
                "[fin_sentiment][S06] FAIL nse_fetch ok=False skipped=%s — watermark NOT advanced",
                summary["skipped"],
            )
            logger.info(
                "[fin_sentiment][S99] end ok=%s openai_ok=%s openai_fallback=%s summary=%s",
                summary["ok"],
                summary.get("openai_reason_ok"),
                summary.get("openai_reason_fallback"),
                summary,
            )
            return summary

        summary["nse_announcements_total"] = len(raw)
        summary["filter_raw_rows"] = len(raw)
        logger.info("[fin_sentiment][S06] nse_fetch ok=True raw_rows=%s", len(raw))

        bucket: Dict[str, List[CorpHit]] = defaultdict(list)
        seen: Dict[str, set] = defaultdict(set)

        for row in raw:
            sym = (row.get("symbol") or "").strip().upper()
            if not sym:
                continue
            if sym not in arb_set:
                summary["filter_symbol_not_arb"] += 1
                continue
            summary["filter_symbol_in_arb"] += 1
            pub = _parse_row_time_utc(row)
            if pub is None:
                summary["filter_no_publish_time"] += 1
                continue
            if pub <= watermark:
                summary["filter_at_or_before_watermark"] += 1
                continue
            sid = _row_seq_id(row)
            if sid in seen[sym]:
                summary["filter_duplicate_seq"] += 1
                continue
            seen[sym].add(sid)
            bucket[sym].append(
                CorpHit(
                    seq_id=sid,
                    published_utc=pub,
                    title=_row_title(row),
                    detail_excerpt=nse_attachment_excerpt(row),
                )
            )
            summary["filter_accepted_hits"] += 1

        symbols_with_hits = [s for s, hs in bucket.items() if hs]
        logger.info(
            "[fin_sentiment][S07] after_filter symbol_hits_in_arb=%s symbols_with_hits=%s "
            "not_arb=%s no_time=%s lte_watermark=%s dup=%s accepted=%s",
            summary["filter_symbol_in_arb"],
            len(symbols_with_hits),
            summary["filter_symbol_not_arb"],
            summary["filter_no_publish_time"],
            summary["filter_at_or_before_watermark"],
            summary["filter_duplicate_seq"],
            summary["filter_accepted_hits"],
        )

        finbert_ok = False
        try:
            from backend.services.finbert_service import is_finbert_available, predict_sentiment

            finbert_ok = is_finbert_available()
        except Exception as e:
            logger.info("[fin_sentiment][S08] finbert import/check failed: %s", e)
            finbert_ok = False

        logger.info("[fin_sentiment][S08] finbert_available=%s", finbert_ok)

        detail_lines = 0
        max_detail = 25

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
                    logger.warning("[fin_sentiment][S09] finbert_failed symbol=%s err=%s", sym, e)
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
                logger.info(
                    "[fin_sentiment][S09] symbol=%s hits=%s SKIP no_combined (nlp=%s)",
                    sym,
                    len(hits),
                    nlp_avg,
                )
                continue

            announcements = [
                {"title": h.title[:512], "detail_excerpt": (h.detail_excerpt or "")[:6000]}
                for h in hits
            ]
            reason_llm = derive_sentiment_reason_openai(
                symbol=sym,
                announcements=announcements,
                nlp_sentiment_avg=nlp_avg,
                combined_sentiment_avg=combined,
            )
            if reason_llm:
                reason = reason_llm
                summary["openai_reason_ok"] += 1
                logger.info("[fin_sentiment][S09b] symbol=%s openai_reason=len=%s", sym, len(reason))
            else:
                reason = _build_current_combined_sentiment_reason(
                    n_hits=len(hits), api_avg=api_avg, nlp_avg=nlp_avg
                )
                summary["openai_reason_fallback"] += 1
                logger.info("[fin_sentiment][S09b] symbol=%s openai_reason=fallback", sym)

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
                    current_combined_sentiment_reason=reason,
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
                row.current_combined_sentiment_reason = reason
                row.news_count = len(hits)
                row.current_run_at = now_ist

            summary["rows_upserted"] += 1
            if detail_lines < max_detail:
                sample = (titles[0][:120] + "…") if titles and len(titles[0]) > 120 else (titles[0] if titles else "")
                logger.info(
                    "[fin_sentiment][S09] symbol=%s UPSERT hits=%s nlp_avg=%.4f combined=%.4f prev_current=%s sample=%r",
                    sym,
                    len(hits),
                    float(nlp_avg) if nlp_avg is not None else float("nan"),
                    float(combined),
                    prev_current,
                    sample,
                )
                detail_lines += 1

        if summary["rows_upserted"] > detail_lines:
            logger.info(
                "[fin_sentiment][S09] … upsert detail truncated (logged=%s total_upserts=%s)",
                detail_lines,
                summary["rows_upserted"],
            )

        _set_watermark(db, now_utc)
        db.commit()
        summary["ok"] = True
        logger.info(
            "[fin_sentiment][S10] commit watermark_utc=%s stocks_with_news=%s rows_upserted=%s",
            now_utc.isoformat(),
            summary["stocks_with_news"],
            summary["rows_upserted"],
        )
        logger.info(
            "[fin_sentiment][S99] end ok=%s openai_ok=%s openai_fallback=%s summary=%s",
            summary["ok"],
            summary.get("openai_reason_ok"),
            summary.get("openai_reason_fallback"),
            summary,
        )
        return summary
    except Exception as e:
        logger.error("[fin_sentiment][ERR] %s", e, exc_info=True)
        db.rollback()
        summary["error"] = str(e)
        logger.info(
            "[fin_sentiment][S99] end ok=%s openai_ok=%s openai_fallback=%s summary=%s",
            summary["ok"],
            summary.get("openai_reason_ok"),
            summary.get("openai_reason_fallback"),
            summary,
        )
        return summary
    finally:
        db.close()
