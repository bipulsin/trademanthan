"""Read-only MTF Swing results. Rows come from the append-only trades CSV."""
from __future__ import annotations

from pathlib import Path
from typing import Any, Dict, List, Optional

from fastapi import APIRouter, Depends, HTTPException, Query
from fastapi.responses import PlainTextResponse
from sqlalchemy.orm import Session

from backend.database import get_db
from backend.models.user import User
from backend.routers.auth import get_user_from_token, oauth2_scheme
from backend.services.mtf_swing.csv_store import read_rows
from backend.services.mtf_swing.data import discards_path, trades_path
from backend.services.mtf_swing.metrics import build_dashboard

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


def load_trades() -> List[Dict[str, str]]:
    path = find_trades_csv()
    if path is None:
        return []
    return read_rows(path)


@router.get("/health")
def health(_: User = Depends(_auth_user)) -> Dict[str, Any]:
    path = find_trades_csv()
    rows = read_rows(path) if path else []
    return {
        "ok": True,
        "csv": str(path or ""),
        "rows": len(rows),
        "discards_csv": str(discards_path()),
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
        import csv
        import io

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
