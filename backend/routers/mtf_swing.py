"""Read-only MTF Swing results. Rows come from the append-only trades CSVs."""
from __future__ import annotations

import csv
import io
import re
from pathlib import Path
from typing import Any, Dict, List, Optional

from fastapi import APIRouter, Depends, HTTPException, Query
from fastapi.responses import PlainTextResponse
from sqlalchemy.orm import Session

from backend.database import get_db
from backend.models.user import User
from backend.routers.auth import get_user_from_token, oauth2_scheme
from backend.services.mtf_swing.config import ABLATION_VARIANTS, LEAVE_ONE_OUT_VARIANTS, month_windows
from backend.services.mtf_swing.csv_store import read_rows
from backend.services.mtf_swing.data import (
    data_dir_v2,
    discards_path,
    discards_path_v2,
    regime_log_path,
    trades_path,
    trades_path_v2,
    variant_summary_path,
)
from backend.services.mtf_swing.metrics import (
    ablation_table,
    build_dashboard,
    build_v2_dashboard,
    compare_v1_equivalent,
    months_present,
    pick_default_variant,
    variant_coverage,
)

router = APIRouter(prefix="/mtf-swing", tags=["mtf-swing"])

_MONTH_RE = re.compile(r"^(all|full|2025-1[1-2]|2026-0[1-9]|2026-10)$")
_VARIANT_ANY = "any"


def _auth_user(token: str = Depends(oauth2_scheme), db: Session = Depends(get_db)) -> User:
    return get_user_from_token(token, db)


def _candidate_csvs() -> tuple:
    root = Path(__file__).resolve().parents[2]
    name = "trades.csv"
    return (
        Path("/home/ubuntu/trademanthan/data/mtf_swing") / name,
        root / "data" / "mtf_swing" / name,
        trades_path(),
    )


def _candidate_v2_csvs() -> tuple:
    root = Path(__file__).resolve().parents[2]
    name = "trades.csv"
    return (
        Path("/home/ubuntu/trademanthan/data/mtf_swing_v2") / name,
        root / "data" / "mtf_swing_v2" / name,
        trades_path_v2(),
    )


def find_trades_csv() -> Optional[Path]:
    seen = set()
    for path in _candidate_csvs():
        key = str(path)
        if key in seen:
            continue
        seen.add(key)
        try:
            if path.is_file():
                return path
        except OSError:
            continue
    return None


def find_trades_csv_v2() -> Optional[Path]:
    seen = set()
    for path in _candidate_v2_csvs():
        key = str(path)
        if key in seen:
            continue
        seen.add(key)
        try:
            if path.is_file():
                return path
        except OSError:
            continue
    return None


def load_trades() -> List[Dict[str, str]]:
    path = find_trades_csv()
    if path is None:
        return []
    return read_rows(path)


def load_trades_v2() -> List[Dict[str, str]]:
    path = find_trades_csv_v2()
    if path is None:
        return []
    return read_rows(path)


def _find_discards_v2() -> Optional[Path]:
    candidates = (
        Path("/home/ubuntu/trademanthan/data/mtf_swing_v2/discards.csv"),
        Path(__file__).resolve().parents[2] / "data" / "mtf_swing_v2" / "discards.csv",
        discards_path_v2(),
    )
    for path in candidates:
        try:
            if path.is_file():
                return path
        except OSError:
            continue
    return None


def _normalize_month(month: str) -> str:
    m = (month or "all").strip().lower()
    if m == "full":
        return "all"
    return m


@router.get("/health")
def health(_: User = Depends(_auth_user)) -> Dict[str, Any]:
    path = find_trades_csv()
    rows = read_rows(path) if path else []
    v2 = find_trades_csv_v2()
    v2_rows = read_rows(v2) if v2 else []
    return {
        "ok": True,
        "csv": str(path or ""),
        "rows": len(rows),
        "discards_csv": str(discards_path()),
        "v2_csv": str(v2 or ""),
        "v2_rows": len(v2_rows),
        "v2_data_dir": str(data_dir_v2()),
    }


@router.get("/results")
def results(
    timeframe: str = Query("60m", pattern="^(60m|15m|all)$"),
    _: User = Depends(_auth_user),
) -> Dict[str, Any]:
    path = find_trades_csv()
    rows = read_rows(path) if path else []
    dash = build_dashboard(rows, timeframe=timeframe)
    if timeframe != "all":
        view = [r for r in rows if r.get("timeframe") == timeframe]
    else:
        view = rows
    return {
        "ok": True,
        "csv": str(path or ""),
        "timeframe": timeframe,
        "trades": view,
        "summary": dash["summary"],
        "months": dash["months"],
        "charts": dash["charts"],
        "open_trades": dash["open_trades"],
    }


@router.get("/v2/results")
def v2_results(
    timeframe: str = Query("all", pattern="^(60m|15m|all)$"),
    month: str = Query("all"),
    variant: str = Query("auto"),
    _: User = Depends(_auth_user),
) -> Dict[str, Any]:
    month_n = _normalize_month(month)
    if not _MONTH_RE.match(month_n):
        raise HTTPException(status_code=422, detail=f"invalid month {month!r}")
    path = find_trades_csv_v2()
    rows = read_rows(path) if path else []
    disc_path = _find_discards_v2()
    discards = read_rows(disc_path) if disc_path else []
    v1_path = find_trades_csv()
    v1_rows = read_rows(v1_path) if v1_path else []
    cov = variant_coverage(rows, timeframe=timeframe)
    default_variant = pick_default_variant(rows, timeframe=timeframe)
    raw_variant = (variant or "auto").strip().lower()
    if raw_variant in ("auto", "default", ""):
        variant_n = default_variant
    else:
        variant_n = raw_variant
    dash = build_v2_dashboard(
        rows,
        discards,
        timeframe=timeframe,
        month=month_n,
        variant=variant_n,
        v1_trades=v1_rows,
    )
    ablation = ablation_table(rows)
    summary_path = variant_summary_path()
    summary_rows = read_rows(summary_path) if summary_path.is_file() else []
    # Filter on-disk summary to selected TF / month when not Full.
    if summary_rows:
        filtered = summary_rows
        if timeframe != "all":
            filtered = [r for r in filtered if r.get("timeframe") == timeframe]
        if month_n != "all":
            filtered = [r for r in filtered if r.get("month") == month_n]
        ablation = filtered or ablation
    regime_path = regime_log_path()
    # Month dropdown counts for the *selected* variant (zeros = no rows yet).
    selected_months = {
        m.get("month"): int(m.get("trades") or 0) for m in (dash.get("months") or [])
    }
    present = months_present(rows, timeframe=timeframe, variant=variant_n)
    full_n = sum(selected_months.get(lab, 0) for _, _, lab in month_windows())
    month_options = [
        {
            "value": "all",
            "label": f"Full ({full_n} trades · {len(present)} mo)"
            if present
            else "Full",
        }
    ] + [
        {
            "value": lab,
            "label": f"{lab} ({selected_months.get(lab, 0)})",
        }
        for _, _, lab in month_windows()
    ]
    def _variant_label(tag: str) -> str:
        info = cov.get(tag) or {}
        n = int(info.get("trades") or 0)
        mc = int(info.get("month_count") or 0)
        # Human-readable names so Experiment tab never looks like raw CSV tags.
        pretty = {
            "v1_equivalent": "validation (v1 flags)",
            "all": "v2 experiment (all filters)",
            "regime_only": "regime only",
            "sl_only": "pivot SL only",
            "tp_only": "TP 1.5R / no time exit",
            "rr_only": "min RR 1.3 only",
            "all_except_regime": "all except regime",
            "all_except_sl": "all except pivot SL",
            "all_except_tp": "all except TP rule",
            "all_except_rr": "all except min RR",
        }.get(tag, tag)
        if not n:
            return pretty
        if tag == "all" and info.get("full_period"):
            return f"{pretty} ({n} · full period)"
        if tag == "all" and mc and mc < 12:
            return f"{pretty} ({n} · pilot {mc} mo only)"
        if tag == "v1_equivalent" and info.get("full_period"):
            return f"{pretty} ({n} · full period)"
        if info.get("full_period"):
            return f"{pretty} ({n} · full)"
        return f"{pretty} ({n} · {mc} mo)"

    variant_options = (
        [{"value": "any", "label": "Any variant"}]
        + [{"value": v, "label": _variant_label(v)} for v in ABLATION_VARIANTS]
        + [{"value": v, "label": _variant_label(v)} for v in LEAVE_ONE_OUT_VARIANTS]
    )
    return {
        "ok": True,
        "csv": str(path or ""),
        "timeframe": timeframe,
        "month": month_n,
        "variant": variant_n,
        "default_variant": default_variant,
        "variant_coverage": cov,
        "scope": dash.get("scope"),
        "scope_months": dash.get("scope_months") or [],
        "trades": dash["trades"],
        "summary": dash["summary"],
        "months": dash["months"],
        "month_compare": dash.get("month_compare") or [],
        "charts": dash["charts"],
        "open_trades": dash["open_trades"],
        "open_mtm": dash.get("open_mtm") or {},
        "funnel": dash.get("funnel") or {},
        "comparison": dash.get("comparison"),
        "robustness": dash.get("robustness") or {},
        "ablation": dash.get("ablation_vs_baseline") or summary_rows or ablation,
        "ablation_detail": ablation,
        "regime_log_csv": str(regime_path if regime_path.is_file() else ""),
        "month_options": month_options,
        "variant_options": variant_options,
    }


@router.get("/v2/validate")
def v2_validate(
    timeframe: str = Query("all", pattern="^(60m|15m|all)$"),
    _: User = Depends(_auth_user),
) -> Dict[str, Any]:
    """Compare v1_equivalent rows to v1 for the like-for-like validation gate."""
    v1 = load_trades()
    v2 = load_trades_v2()
    if timeframe == "all":
        reports = {
            "60m": compare_v1_equivalent(v1, v2, timeframe="60m"),
            "15m": compare_v1_equivalent(v1, v2, timeframe="15m"),
            "all": compare_v1_equivalent(v1, v2, timeframe="all"),
        }
        return {"ok": all(r.get("ok") for r in reports.values()), "reports": reports}
    report = compare_v1_equivalent(v1, v2, timeframe=timeframe)
    return {"ok": bool(report.get("ok")), "reports": {timeframe: report}}


@router.get("/trades.csv")
def download_trades(
    timeframe: str = Query("all", pattern="^(60m|15m|all)$"),
    _: User = Depends(_auth_user),
) -> PlainTextResponse:
    path = find_trades_csv()
    if path is None:
        raise HTTPException(status_code=404, detail="MTF Swing trades CSV not written yet")
    if timeframe == "all":
        text = path.read_text(encoding="utf-8")
    else:
        rows = [r for r in read_rows(path) if r.get("timeframe") == timeframe]
        buf = io.StringIO()
        if rows:
            writer = csv.DictWriter(buf, fieldnames=list(rows[0].keys()))
            writer.writeheader()
            writer.writerows(rows)
        elif path.is_file():
            header = path.read_text(encoding="utf-8").splitlines()[:1]
            buf.write("\n".join(header) + ("\n" if header else ""))
        text = buf.getvalue()
    return PlainTextResponse(text, media_type="text/csv")


@router.get("/v2/trades.csv")
def download_trades_v2(
    timeframe: str = Query("all", pattern="^(60m|15m|all)$"),
    month: str = Query("all"),
    variant: str = Query("auto"),
    _: User = Depends(_auth_user),
) -> PlainTextResponse:
    month_n = _normalize_month(month)
    if not _MONTH_RE.match(month_n):
        raise HTTPException(status_code=422, detail=f"invalid month {month!r}")
    path = find_trades_csv_v2()
    if path is None:
        raise HTTPException(status_code=404, detail="MTF Swing v2 trades CSV not written yet")
    rows = read_rows(path)
    raw_variant = (variant or "auto").strip().lower()
    if raw_variant in ("auto", "default", ""):
        variant_n = pick_default_variant(rows, timeframe=timeframe)
    else:
        variant_n = raw_variant
    if timeframe != "all":
        rows = [r for r in rows if r.get("timeframe") == timeframe]
    if month_n != "all":
        rows = [r for r in rows if r.get("month") == month_n]
    if variant_n and variant_n not in (_VARIANT_ANY, "*", "all_variants"):
        rows = [r for r in rows if r.get("variant") == variant_n]
    buf = io.StringIO()
    if rows:
        writer = csv.DictWriter(buf, fieldnames=list(rows[0].keys()))
        writer.writeheader()
        writer.writerows(rows)
    elif path.is_file():
        header = path.read_text(encoding="utf-8").splitlines()[:1]
        buf.write("\n".join(header) + ("\n" if header else ""))
    return PlainTextResponse(buf.getvalue(), media_type="text/csv")
