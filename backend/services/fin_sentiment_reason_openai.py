"""
OpenAI-derived explanation for stock_fin_sentiment.current_combined_sentiment_reason.

Uses announcement title + first lines of NSE body text (attchmntText), plus FinBERT nlp avg,
after nlp_sentiment_avg is known. Model: gpt-4o-mini, JSON output.
"""
from __future__ import annotations

import ast
import json
import logging
import re
from pathlib import Path
from typing import Any, Dict, List, Optional

from backend.config import settings

logger = logging.getLogger(__name__)

MODEL = "gpt-4o-mini"


def nse_attachment_excerpt(row: Dict[str, Any], max_lines: int = 10, max_chars: int = 6000) -> str:
    """First lines of NSE corporate announcement body (attchmntText), if present."""
    for k in ("attchmntText", "attchmnt_Text", "attachmentText", "attchmnt_text"):
        v = row.get(k)
        if v and str(v).strip():
            text = str(v).strip().replace("\r\n", "\n")
            lines = text.split("\n")
            chunk = "\n".join(lines[:max_lines])
            return chunk[:max_chars]
    return ""


# Display labels (model returns JSON-safe slugs; MA shown as M&A in output)
SLUG_TO_BRACKET = {
    "ORDER": "ORDER",
    "EARNINGS": "EARNINGS",
    "MA": "M&A",
    "EXPANSION": "EXPANSION",
    "REGULATION": "REGULATION",
    "NEGATIVE": "NEGATIVE",
    "OTHER": "OTHER",
}


def _normalize_category(raw: Optional[str]) -> str:
    if not raw:
        return "OTHER"
    s = str(raw).strip().upper().replace(" ", "_")
    aliases = {
        "M&A": "MA",
        "M_AND_A": "MA",
        "MERGER": "MA",
        "ACQUISITION": "MA",
        "NEGATIVE_NEWS": "NEGATIVE",
    }
    s = aliases.get(s, s)
    if s in SLUG_TO_BRACKET:
        return s
    return "OTHER"


def derive_sentiment_reason_openai(
    *,
    symbol: str,
    announcements: List[Dict[str, str]],
    nlp_sentiment_avg: Optional[float],
    combined_sentiment_avg: Optional[float],
    max_output_chars: int = 8000,
) -> Optional[str]:
    """
    announcements: each dict has keys 'title', 'detail_excerpt' (may be empty).

    Returns bracketed reason like "[EARNINGS] ..." or None if skipped / failed.
    """
    key = (settings.OPENAI_API_KEY or "").strip()
    if not key:
        logger.info("[fin_sentiment][openai] skip: OPENAI_API_KEY missing")
        return None
    if not announcements:
        return None

    try:
        from openai import OpenAI
    except ImportError:
        logger.warning("[fin_sentiment][openai] openai package not installed")
        return None

    client = OpenAI(api_key=key, timeout=60.0, max_retries=1)
    body = []
    for i, a in enumerate(announcements, start=1):
        title = (a.get("title") or "").strip() or "(no title)"
        det = (a.get("detail_excerpt") or "").strip()
        body.append(f"--- Announcement {i} ---\nTitle: {title}\nDetail (excerpt):\n{det or '(none)'}\n")
    joined = "\n".join(body)

    user_prompt = (
        f"Symbol: {symbol}\n"
        f"Pipeline FinBERT mean (nlp_sentiment_avg): {nlp_sentiment_avg!r} "
        f"(approx -1 bearish to +1 bullish).\n"
        f"Pipeline combined_sentiment_avg for this run: {combined_sentiment_avg!r}.\n\n"
        f"Corporate filing(s):\n{joined}\n"
        "Task: (1) Pick exactly one category slug from this list: "
        "ORDER, EARNINGS, MA, EXPANSION, REGULATION, NEGATIVE, OTHER. "
        "MA means mergers, acquisitions, demergers, schemes of arrangement. "
        "ORDER means large contracts/wins/orders. EARNINGS means results, guidance, profit warnings. "
        "EXPANSION means capacity, new plants, geography, partnerships. "
        "REGULATION means exchange/SEBI compliance, filings, board changes without deal economics. "
        "NEGATIVE means clearly adverse events (fraud, default, severe penalty) if evident from text. "
        "OTHER if none fit.\n"
        "(2) Write reason: 2–4 sentences for a trader on why the combined score direction makes sense "
        "given the filing tone and the numeric nlp average.\n"
        'Return JSON only: {"category":"<slug>","reason":"<text>"}'
    )

    try:
        resp = client.chat.completions.create(
            model=MODEL,
            messages=[
                {
                    "role": "system",
                    "content": "You output compact JSON only. No markdown.",
                },
                {"role": "user", "content": user_prompt},
            ],
            response_format={"type": "json_object"},
            temperature=0.25,
        )
        raw = (resp.choices[0].message.content or "").strip()
        data = json.loads(raw)
    except Exception as e:
        logger.warning("[fin_sentiment][openai] symbol=%s call failed: %s", symbol, e)
        return None

    cat = _normalize_category(data.get("category"))
    bracket = SLUG_TO_BRACKET.get(cat, "OTHER")
    reason = (data.get("reason") or "").strip()
    if not reason:
        reason = "No explanation returned by model."
    reason = re.sub(r"\s+", " ", reason)
    out = f"[{bracket}] {reason}"
    if len(out) > max_output_chars:
        out = out[: max_output_chars - 1] + "…"
    logger.info("[fin_sentiment][openai] symbol=%s category=%s len=%s", symbol, bracket, len(out))
    return out


def parse_s09_upsert_log_line(line: str) -> Optional[tuple[str, str]]:
    """Return (symbol, sample_title) from [fin_sentiment][S09] ... UPSERT ... sample=%r line."""
    if "[fin_sentiment][S09]" not in line or "UPSERT" not in line or "sample=" not in line:
        return None
    ms = re.search(r"\bsymbol=([^\s]+).*?\bUPSERT\b", line)
    if not ms:
        return None
    mt = re.search(r"\bsample=(.+)$", line)
    if not mt:
        return None
    try:
        title = str(ast.literal_eval(mt.group(1).strip()))
    except (SyntaxError, ValueError):
        return None
    return ms.group(1), title


def nse_row_to_title_and_detail(r: Dict[str, Any]) -> Dict[str, str]:
    title_parts = [(r.get("sm_name") or "").strip(), (r.get("desc") or "").strip()]
    title = " — ".join(p for p in title_parts if p) or "(no title)"
    return {"title": title, "detail_excerpt": nse_attachment_excerpt(r)}


def fetch_latest_nse_announcement_for_symbol(
    symbol: str, *, lookback_calendar_days: int = 2
) -> Optional[Dict[str, str]]:
    """
    Most recent NSE corporate row for symbol (by sort_date/an_dt string desc).
    Returns {"title", "detail_excerpt"} or None.
    """
    from backend.services.nse_corporate_client import NseCorporateAnnouncementsClient

    client = NseCorporateAnnouncementsClient(lookback_calendar_days=lookback_calendar_days)
    ok, rows = client.fetch_equity_announcements()
    if not ok:
        return None
    symu = symbol.strip().upper()
    idx = index_latest_nse_row_by_symbol([x for x in rows if isinstance(x, dict)])
    r = idx.get(symu)
    if not r:
        return None
    return nse_row_to_title_and_detail(r)


def announcement_from_index(
    symbol: str, index: Dict[str, Dict[str, Any]]
) -> Optional[Dict[str, str]]:
    r = index.get(symbol.strip().upper())
    if not r:
        return None
    return nse_row_to_title_and_detail(r)


def load_latest_s09_samples_from_log(log_path: str) -> Dict[str, str]:
    """Last occurrence per symbol wins (single file)."""
    p = Path(log_path)
    if not p.is_file():
        return {}
    return _merge_s09_samples_from_lines(p.read_text(encoding="utf-8", errors="replace").splitlines())


def _merge_s09_samples_from_lines(lines: List[str]) -> Dict[str, str]:
    out: Dict[str, str] = {}
    for line in lines:
        parsed = parse_s09_upsert_log_line(line)
        if parsed:
            sym, title = parsed
            out[sym] = title
    return out


def load_latest_s09_samples_from_log_dir(log_dir: str) -> Dict[str, str]:
    """
    Merge S09 UPSERT samples from smart_future_algo.log (and legacy scan_st1_algo.log*) files.
    Files processed oldest → newest so the newest line per symbol wins.
    """
    d = Path(log_dir)
    if not d.is_dir():
        return {}
    paths: List[Path] = []
    for _pat in ("smart_future_algo.log*", "scan_st1_algo.log*"):
        paths.extend(d.glob(_pat))
    paths = sorted(paths, key=lambda p: p.stat().st_mtime)
    out: Dict[str, str] = {}
    for p in paths:
        if not p.is_file():
            continue
        try:
            lines = p.read_text(encoding="utf-8", errors="replace").splitlines()
        except OSError:
            continue
        out.update(_merge_s09_samples_from_lines(lines))
    return out


def index_latest_nse_row_by_symbol(rows: List[Dict[str, Any]]) -> Dict[str, Dict[str, Any]]:
    """Pick most recent raw NSE row per symbol from a corporate-announcements payload."""
    by_sym: Dict[str, List[Dict[str, Any]]] = {}
    for r in rows:
        if not isinstance(r, dict):
            continue
        sym = (r.get("symbol") or "").strip().upper()
        if not sym:
            continue
        by_sym.setdefault(sym, []).append(r)

    def sort_key(r: Dict[str, Any]) -> str:
        return str(r.get("sort_date") or r.get("an_dt") or "")

    out: Dict[str, Dict[str, Any]] = {}
    for sym, cand in by_sym.items():
        cand.sort(key=sort_key, reverse=True)
        out[sym] = cand[0]
    return out
