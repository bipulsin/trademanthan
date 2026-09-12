"""JSON month files: data/divtest/{instrument}/{timeframe}/{YYYY-MM}.json"""
from __future__ import annotations

import json
import re
from pathlib import Path
from typing import Any, Dict, List, Optional

from backend.services.divtest.config import DATA_DIR, TIMEFRAMES
from backend.services.divtest.strategy import analyze_trades


def _safe(name: str) -> str:
    return re.sub(r"[^A-Z0-9._-]+", "_", str(name or "UNKNOWN").upper())


def _tf(tf: str) -> str:
    return re.sub(r"[^a-z0-9._-]+", "_", str(tf or "15min").lower())


def month_path(instrument: str, timeframe: str, yyyy_mm: str) -> Path:
    return DATA_DIR / _safe(instrument) / _tf(timeframe) / f"{yyyy_mm}.json"


def month_exists(instrument: str, timeframe: str, yyyy_mm: str) -> bool:
    return month_path(instrument, timeframe, yyyy_mm).exists()


def read_month(instrument: str, timeframe: str, yyyy_mm: str) -> Optional[Dict[str, Any]]:
    p = month_path(instrument, timeframe, yyyy_mm)
    if not p.exists():
        return None
    try:
        return json.loads(p.read_text(encoding="utf-8"))
    except Exception:
        return None


def write_month(instrument: str, timeframe: str, yyyy_mm: str, payload: Dict[str, Any]) -> Dict[str, Any]:
    p = month_path(instrument, timeframe, yyyy_mm)
    p.parent.mkdir(parents=True, exist_ok=True)
    from datetime import datetime, timezone

    doc = {
        "instrument": _safe(instrument),
        "timeframe": _tf(timeframe),
        "month": yyyy_mm,
        "updated_at": datetime.now(timezone.utc).isoformat(),
        "source": payload.get("source"),
        "instrument_key": payload.get("instrument_key"),
        "candles": payload.get("candles") or 0,
        "notes": payload.get("notes") or [],
        "trades": payload.get("trades") or [],
        "meta": payload.get("meta") or {},
    }
    p.write_text(json.dumps(doc, indent=2), encoding="utf-8")
    return doc


def append_month_trades(
    instrument: str, timeframe: str, yyyy_mm: str, trades: List[Dict[str, Any]], extra: Optional[Dict[str, Any]] = None
) -> Dict[str, Any]:
    existing = read_month(instrument, timeframe, yyyy_mm) or {"trades": [], "notes": [], "candles": 0}
    extra = extra or {}

    def key_of(t: Dict[str, Any]) -> str:
        return f"{t.get('side')}|{t.get('entry_datetime')}|{t.get('exit_datetime')}|{t.get('entry_price')}|{t.get('exit_price')}"

    seen = {key_of(t) for t in (existing.get("trades") or [])}
    merged = list(existing.get("trades") or [])
    for t in trades or []:
        k = key_of(t)
        if k not in seen:
            seen.add(k)
            merged.append(t)
    notes = list({*(existing.get("notes") or []), *(extra.get("notes") or [])})
    return write_month(
        instrument,
        timeframe,
        yyyy_mm,
        {
            **existing,
            **extra,
            "trades": merged,
            "notes": notes,
        },
    )


def list_instruments() -> List[str]:
    if not DATA_DIR.exists():
        return []
    return sorted([p.name for p in DATA_DIR.iterdir() if p.is_dir()])


def load_all_trades(instrument: Optional[str] = None, timeframe: Optional[str] = None) -> Dict[str, Any]:
    instruments = [_safe(instrument)] if instrument and instrument != "All" else list_instruments()
    tfs = [_tf(timeframe)] if timeframe and timeframe != "All" else [t["id"] for t in TIMEFRAMES]
    trades: List[Dict[str, Any]] = []
    files = 0
    for inst in instruments:
        for tf in tfs:
            d = DATA_DIR / inst / tf
            if not d.exists():
                continue
            for f in d.glob("*.json"):
                files += 1
                try:
                    doc = json.loads(f.read_text(encoding="utf-8"))
                except Exception:
                    continue
                for t in doc.get("trades") or []:
                    row = dict(t)
                    row.setdefault("instrument", inst)
                    row.setdefault("timeframe", tf)
                    row["_month"] = doc.get("month")
                    trades.append(row)
    trades.sort(key=lambda t: str(t.get("entry_datetime") or ""))
    return {"trades": trades, "files": files}


def get_results_summary(instrument: Optional[str] = None, timeframe: Optional[str] = None) -> Dict[str, Any]:
    loaded = load_all_trades(instrument=instrument, timeframe=timeframe)
    trades = loaded["trades"]
    analysis = analyze_trades(trades)
    by_tf = {}
    for tf in [t["id"] for t in TIMEFRAMES]:
        subset = [t for t in trades if t.get("timeframe") == tf]
        by_tf[tf] = analyze_trades(subset)
    return {
        "filters": {"instrument": instrument or "All", "timeframe": timeframe or "All"},
        "instruments": list_instruments(),
        "trade_count": len(trades),
        "trades": trades,
        "analysis": analysis,
        "by_timeframe": by_tf,
    }
