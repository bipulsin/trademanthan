#!/usr/bin/env python3
"""Build data/mtf_swing/v1_fetch_holes.json from v1 backtest logs.

Used by v2 ``v1_equivalent`` so continuous carry replays the same candle-gap
months that produced the frozen v1 trades.csv.
"""
from __future__ import annotations

import argparse
import json
import sys
from pathlib import Path

ROOT = Path(__file__).resolve().parents[1]
if str(ROOT) not in sys.path:
    sys.path.insert(0, str(ROOT))

from backend.services.mtf_swing.parity import (  # noqa: E402
    build_holes_from_v1_logs,
    default_v1_log_specs,
    holes_path,
    save_holes,
)


def main() -> int:
    parser = argparse.ArgumentParser()
    parser.add_argument("--out", type=Path, default=None)
    args = parser.parse_args()
    specs = default_v1_log_specs()
    holes = build_holes_from_v1_logs(specs)
    out = save_holes(
        holes,
        args.out,
        meta={
            "sources": [{"path": str(p), "default_tf": tf} for p, tf in specs],
        },
    )
    by_tf: dict = {}
    for tf, _sym, month in holes:
        by_tf.setdefault(tf, {})
        by_tf[tf][month] = by_tf[tf].get(month, 0) + 1
    print(json.dumps({"path": str(out), "count": len(holes), "by_tf_month": by_tf}, indent=2))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
