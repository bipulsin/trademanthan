#!/usr/bin/env python3
"""Create/ensure trade_log and import Rule 27 Excel + Jul-21 enriched rows."""
from __future__ import annotations

import json
import sys
from pathlib import Path

ROOT = Path(__file__).resolve().parents[1]
if str(ROOT) not in sys.path:
    sys.path.insert(0, str(ROOT))

DEFAULT_XLSX = ROOT / "docs/diagnostics/trade_log_13Jul_17Jul2026.xlsx"


def main() -> None:
    from backend.services.rule27_trade_log import import_excel_and_enriched

    xlsx = Path(sys.argv[1]) if len(sys.argv) > 1 else DEFAULT_XLSX
    result = import_excel_and_enriched(str(xlsx))
    print(json.dumps(result, indent=2, default=str))


if __name__ == "__main__":
    main()
