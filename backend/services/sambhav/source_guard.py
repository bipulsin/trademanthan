"""Guards so feature/ML code does not mutate Sambhav source OHLC tables."""

from __future__ import annotations

import ast
from pathlib import Path
from typing import Iterable, List

# Canonical market-data source — OHLC must remain unchanged by ML pipelines.
SOURCE_TABLES = frozenset(
    {
        "sambhav_10m_candles",
        "sambhav_raw_candles",
    }
)

# Modules allowed to write source candles (import / upsert only).
ALLOWED_SOURCE_WRITERS = frozenset(
    {
        "backend/services/sambhav/importer.py",
        "backend/services/sambhav/candles.py",
        "backend/services/sambhav/historical.py",
    }
)

FEATURE_ML_MODULES = (
    "backend/services/sambhav/features.py",
    "backend/services/sambhav/targets.py",
    "backend/services/sambhav/train.py",
    "backend/services/sambhav/baselines.py",
    "backend/services/sambhav/walk_forward.py",
    "backend/services/sambhav/predict.py",
    "backend/services/sambhav/metrics.py",
    "backend/services/sambhav/calibration.py",
)


def _sql_writes_source(sql: str) -> List[str]:
    low = sql.lower()
    hits: List[str] = []
    for table in SOURCE_TABLES:
        if table in low and any(
            op in low for op in ("update ", "delete ", "truncate ", "drop ", "alter ")
        ):
            # INSERT into source is also forbidden in ML modules
            hits.append(table)
        elif table in low and "insert " in low:
            hits.append(table)
    return hits


def scan_module_for_source_writes(path: Path) -> List[str]:
    """Static scan: flag SQL that would mutate source tables."""
    text = path.read_text(encoding="utf-8")
    findings: List[str] = []
    # AST string literals that look like SQL
    try:
        tree = ast.parse(text)
    except SyntaxError:
        return [f"syntax_error:{path}"]
    for node in ast.walk(tree):
        if isinstance(node, ast.Constant) and isinstance(node.value, str):
            hits = _sql_writes_source(node.value)
            for h in hits:
                findings.append(f"{path.as_posix()}:{h}")
    return findings


def assert_feature_modules_do_not_mutate_source(
    repo_root: Path,
    modules: Iterable[str] = FEATURE_ML_MODULES,
) -> None:
    bad: List[str] = []
    for rel in modules:
        p = repo_root / rel
        if not p.exists():
            continue
        bad.extend(scan_module_for_source_writes(p))
    if bad:
        raise AssertionError(
            "Feature/ML modules must not mutate Sambhav source OHLC tables: " + "; ".join(bad)
        )
