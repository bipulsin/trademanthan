#!/usr/bin/env python3
"""Run Kavach 22-Aug BT-1..4 research checkpoint over trade_log.

Research only — does not alter live gates. Writes bt_checkpoint_* tables and
optional JSON/CSV under docs/diagnostics and Downloads.
"""
from __future__ import annotations

import argparse
import json
import sys
from datetime import date
from pathlib import Path

ROOT = Path(__file__).resolve().parents[1]
if str(ROOT) not in sys.path:
    sys.path.insert(0, str(ROOT))

from backend.services.kavach_bt_checkpoint.config import DATE_FROM, DATE_TO
from backend.services.kavach_bt_checkpoint.export import detail_csv, summary_csv
from backend.services.kavach_bt_checkpoint.runner import run_checkpoint


def main() -> int:
    p = argparse.ArgumentParser(description="Kavach BT checkpoint 22-Aug")
    p.add_argument("--from", dest="date_from", default=DATE_FROM.isoformat())
    p.add_argument("--to", dest="date_to", default=DATE_TO.isoformat())
    p.add_argument("--run-id", default=None)
    p.add_argument("--no-fo-sample", action="store_true")
    p.add_argument("--skip-db-write-exports-only", action="store_true", help=argparse.SUPPRESS)
    args = p.parse_args()

    out = run_checkpoint(
        date_from=date.fromisoformat(args.date_from),
        date_to=date.fromisoformat(args.date_to),
        run_id=args.run_id,
        fo_sample=not args.no_fo_sample,
    )

    diag = ROOT / "docs" / "diagnostics"
    diag.mkdir(parents=True, exist_ok=True)
    rid = out["run_id"]
    json_path = diag / f"KAVACH_BT_CHECKPOINT_22AUG2026_{rid}.json"
    # strip bulky details for markdown companion; keep full in json
    slim = {k: v for k, v in out.items() if k != "details"}
    slim["detail_symbols"] = sorted({d["symbol"] for d in out.get("details") or []})
    json_path.write_text(json.dumps(slim, indent=2, default=str), encoding="utf-8")

    md_path = diag / "KAVACH_BT_CHECKPOINT_22AUG2026.md"
    lines = [
        "# Kavach BT Checkpoint — 22-Aug-2026",
        "",
        f"**run_id:** `{rid}`",
        f"**window:** {args.date_from} → {args.date_to}",
        f"**trades loaded:** {out.get('n_trades_loaded')} | **enriched:** {out.get('n_details')}",
        "",
        "## Recommendations",
        "",
    ]
    for s in out.get("summaries") or []:
        if s.get("cohort_type") == "recommendation" and s.get("recommendation_text"):
            lines.append(f"- **{s['cohort_key']}:** {s['recommendation_text']}")
    lines += ["", "## Cohorts", ""]
    for s in out.get("summaries") or []:
        if s.get("cohort_type") == "recommendation":
            continue
        lines.append(
            f"- `{s['cohort_type']}` / `{s['cohort_key']}`: n={s.get('n')} "
            f"win%={s.get('win_rate')} avgR={s.get('avg_r')} pnl={s.get('total_pnl')}"
        )
    lines += [
        "",
        "## Notes",
        "",
        "- Research only: resistance + Garuda are warning/shadow; PB≥5 hard-block is display/research.",
        "- Rule 15 is entry-only (never logged as an exit trigger).",
        "- Live Take Trade / READY gates unchanged.",
        "",
    ]
    md_path.write_text("\n".join(lines), encoding="utf-8")

    downloads = Path.home() / "Downloads"
    if downloads.is_dir():
        (downloads / f"kavach_bt_checkpoint_detail_{rid}.csv").write_text(
            detail_csv(out.get("details") or []), encoding="utf-8"
        )
        (downloads / f"kavach_bt_checkpoint_summary_{rid}.csv").write_text(
            summary_csv(out.get("summaries") or []), encoding="utf-8"
        )

    print(json.dumps(slim, indent=2, default=str))
    print(f"Wrote {json_path}")
    print(f"Wrote {md_path}")
    return 0 if out.get("ok") else 1


if __name__ == "__main__":
    raise SystemExit(main())
