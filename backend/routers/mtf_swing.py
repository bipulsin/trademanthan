"""Read-only MTF Swing results. Rows come from the append-only trades CSVs."""
from __future__ import annotations

import csv
import io
from pathlib import Path
from typing import Any, Dict, List, Optional

from fastapi import APIRouter, Depends, HTTPException, Query
from fastapi.responses import PlainTextResponse
from sqlalchemy.orm import Session

from backend.database import get_db
from backend.models.user import User
from backend.routers.auth import get_user_from_token, oauth2_scheme
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
from backend.services.mtf_swing.metrics import ablation_table, build_dashboard, build_v2_dashboard

router = APIRouter(prefix="/mtf-swing", tags=["mtf-swing"])


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
    month: str = Query("all", pattern="^(all|2026-06|2026-10)$"),
    variant: str = Query("all"),
    _: User = Depends(_auth_user),
) -> Dict[str, Any]:
    path = find_trades_csv_v2()
    rows = read_rows(path) if path else []
    disc_path = _find_discards_v2()
    discards = read_rows(disc_path) if disc_path else []
    v1_path = find_trades_csv()
    v1_rows = read_rows(v1_path) if v1_path else []
    # Restrict v1 comparison to the same months/timeframes when filtering.
    dash = build_v2_dashboard(
        rows,
        discards,
        timeframe=timeframe,
        month=month,
        variant=variant,
        v1_trades=v1_rows,
    )
    ablation = ablation_table(rows) if any(r.get("variant") and r.get("variant") != "all" for r in rows) else []
    # Prefer on-disk variant_summary when present.
    summary_path = variant_summary_path()
    summary_rows = read_rows(summary_path) if summary_path.is_file() else []
    regime_path = regime_log_path()
    return {
        "ok": True,
        "csv": str(path or ""),
        "timeframe": timeframe,
        "month": month,
        "variant": variant,
        "trades": dash["trades"],
        "summary": dash["summary"],
        "months": dash["months"],
        "charts": dash["charts"],
        "open_trades": dash["open_trades"],
        "open_mtm": dash.get("open_mtm") or {},
        "funnel": dash.get("funnel") or {},
        "comparison": dash.get("comparison"),
        "ablation": summary_rows or ablation,
        "regime_log_csv": str(regime_path if regime_path.is_file() else ""),
    }


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
    month: str = Query("all", pattern="^(all|2026-06|2026-10)$"),
    variant: str = Query("all"),
    _: User = Depends(_auth_user),
) -> PlainTextResponse:
    path = find_trades_csv_v2()
    if path is None:
        raise HTTPException(status_code=404, detail="MTF Swing v2 trades CSV not written yet")
    rows = read_rows(path)
    if timeframe != "all":
        rows = [r for r in rows if r.get("timeframe") == timeframe]
    if month != "all":
        rows = [r for r in rows if r.get("month") == month]
    if variant != "all":
        rows = [r for r in rows if r.get("variant") == variant]
    buf = io.StringIO()
    if rows:
        writer = csv.DictWriter(buf, fieldnames=list(rows[0].keys()))
        writer.writeheader()
        writer.writerows(rows)
    elif path.is_file():
        header = path.read_text(encoding="utf-8").splitlines()[:1]
        buf.write("\n".join(header) + ("\n" if header else ""))
    return PlainTextResponse(buf.getvalue(), media_type="text/csv")
