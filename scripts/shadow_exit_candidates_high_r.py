#!/usr/bin/env python3
"""Shadow-replay the three full-exit candidates on high-R give-back fixtures.

Shadow-only research for the 22-Jul checkpoint. Does NOT flip live gates
(EXIT_CANDIDATE_LIVE stays off). Writes JSON + markdown under docs/diagnostics.

Usage:
  PYTHONPATH=. python3 scripts/shadow_exit_candidates_high_r.py
  PYTHONPATH=. python3 scripts/shadow_exit_candidates_high_r.py --json-only
"""
from __future__ import annotations

import argparse
import json
import sys
from datetime import datetime, timezone
from pathlib import Path
from typing import Any, Dict, List

_ROOT = Path(__file__).resolve().parent.parent
if str(_ROOT) not in sys.path:
    sys.path.insert(0, str(_ROOT))

from backend.services.kavach_exit_candidate_fixtures import all_fixtures  # noqa: E402
from backend.services.kavach_exit_candidate_shadow import (  # noqa: E402
    CANDIDATE_IDS,
    HIGH_R_ARM,
    RATCHET_FLOOR_R,
    RATCHET_GIVEBACK_R,
    SPIKE_RETAIN_R,
    evaluate_candidates_on_bars,
    exit_candidate_live_enabled,
    replay_result_to_dict,
)

OUT_DIR = _ROOT / "docs/diagnostics"


def _now_iso() -> str:
    try:
        import pytz

        return datetime.now(pytz.timezone("Asia/Kolkata")).isoformat()
    except Exception:
        return datetime.now(timezone.utc).isoformat()


def _delta(result: Dict[str, Any], cid: str) -> Dict[str, Any]:
    base = result.get("baseline") or {}
    cand = (result.get("candidates") or {}).get(cid)
    if not cand or not base:
        return {
            "fired": bool(cand),
            "bars_earlier": None,
            "exit_r_delta": None,
            "saved_vs_baseline_r": None,
        }
    bars_earlier = int(base["bar_index"]) - int(cand["bar_index"])
    exit_r_delta = round(float(cand["exit_r"]) - float(base["exit_r"]), 4)
    return {
        "fired": True,
        "bars_earlier": bars_earlier,
        "exit_r_delta": exit_r_delta,
        "saved_vs_baseline_r": exit_r_delta,
        "candidate_exit_r": cand["exit_r"],
        "baseline_exit_r": base["exit_r"],
        "candidate_bar_index": cand["bar_index"],
        "baseline_bar_index": base["bar_index"],
    }


def run_all() -> Dict[str, Any]:
    cases: List[Dict[str, Any]] = []
    for name, (trade, bars) in all_fixtures().items():
        result = evaluate_candidates_on_bars(bars, trade)
        payload = replay_result_to_dict(result)
        payload["case_id"] = name
        payload["n_bars"] = len(bars)
        payload["vs_baseline"] = {cid: _delta(payload, cid) for cid in CANDIDATE_IDS}
        cases.append(payload)

    # Cohort rollups (give-back cases only — exclude TATAELXSI control)
    giveback_cases = [c for c in cases if not c["case_id"].startswith("TATAELXSI")]
    control = [c for c in cases if c["case_id"].startswith("TATAELXSI")]
    rollup: Dict[str, Any] = {}
    for cid in CANDIDATE_IDS:
        fired = 0
        saved: List[float] = []
        earlier: List[int] = []
        for c in giveback_cases:
            vs = c["vs_baseline"][cid]
            if vs["fired"]:
                fired += 1
                if vs["exit_r_delta"] is not None:
                    saved.append(float(vs["exit_r_delta"]))
                if vs["bars_earlier"] is not None:
                    earlier.append(int(vs["bars_earlier"]))
        rollup[cid] = {
            "fired_on_giveback_cases": fired,
            "giveback_case_count": len(giveback_cases),
            "mean_saved_r": round(sum(saved) / len(saved), 4) if saved else None,
            "mean_bars_earlier": round(sum(earlier) / len(earlier), 2) if earlier else None,
            "control_silent": all(
                not (c["vs_baseline"][cid]["fired"])
                or c["peak_r"] >= HIGH_R_ARM  # only silent if unarmed
                for c in control
            ),
        }
        # Control must have peak < arm and no fires
        for c in control:
            rollup[cid]["control_peak_r"] = c["peak_r"]
            rollup[cid]["control_any_fire"] = any(
                c["candidates"][x] is not None for x in CANDIDATE_IDS
            )

    return {
        "meta": {
            "generated_at": _now_iso(),
            "shadow_only": True,
            "live_enabled": exit_candidate_live_enabled(),
            "constraint": "single-lot binary full-exit only — no partial booking",
            "tunables": {
                "HIGH_R_ARM": HIGH_R_ARM,
                "RATCHET_GIVEBACK_R": RATCHET_GIVEBACK_R,
                "RATCHET_FLOOR_R": RATCHET_FLOOR_R,
                "SPIKE_RETAIN_R": SPIKE_RETAIN_R,
            },
            "note": (
                "FEDERALBNK bars from 20-Jul case log; POLICYBZR/ADANIGREEN/CHOLAFIN "
                "are shape-preserving reconstructions for candidate comparison. "
                "Re-fetch Upstox 5m→10m on paperclip before treating R deltas as final."
            ),
        },
        "rollup": rollup,
        "cases": cases,
    }


def render_markdown(report: Dict[str, Any]) -> str:
    lines: List[str] = []
    lines.append("# Exit-candidate shadow — high-R give-back (2026-07-20)")
    lines.append("")
    lines.append(
        "Shadow-log / research only. **No live exit gating changed.** "
        "`EXIT_CANDIDATE_LIVE` remains off until the 22-Jul checkpoint."
    )
    lines.append("")
    lines.append("## Constraint")
    lines.append("")
    lines.append(
        "Trading is single-lot, binary exit only — candidates are full-exit rule "
        "changes (faster ratchet / spike-reverse / intrabar EMA5 fast-confirm), "
        "not partial profit booking."
    )
    lines.append("")
    lines.append("## Tunables (shadow defaults)")
    lines.append("")
    for k, v in report["meta"]["tunables"].items():
        lines.append(f"- `{k}` = {v}")
    lines.append("")
    lines.append("## Rollup (give-back cohort)")
    lines.append("")
    lines.append("| Candidate | Fired | Mean ΔR vs baseline | Mean bars earlier |")
    lines.append("|-----------|------:|--------------------:|------------------:|")
    for cid, r in report["rollup"].items():
        lines.append(
            f"| `{cid}` | {r['fired_on_giveback_cases']}/{r['giveback_case_count']} | "
            f"{r['mean_saved_r']} | {r['mean_bars_earlier']} |"
        )
    lines.append("")
    lines.append("## Cases")
    lines.append("")
    for c in report["cases"]:
        t = c["trade"]
        lines.append(f"### {c['case_id']} — {t['symbol']} {t['direction']}")
        lines.append("")
        lines.append(
            f"- Entry `{t['entry']}`, risk `{t['risk_pts']}` pts, "
            f"peak **{c['peak_r']}R**, session `{t.get('session_date')}`"
        )
        if t.get("notes"):
            lines.append(f"- Notes: {t['notes']}")
        base = c.get("baseline")
        if base:
            lines.append(
                f"- Baseline: `{base['reason']}` @ bar {base['bar_index']} "
                f"px={base['exit_price']} → **{base['exit_r']:.3f}R** "
                f"(give-back {c.get('giveback_r')}R from peak)"
            )
        else:
            lines.append("- Baseline: no exit in fixture window")
        for cid in CANDIDATE_IDS:
            ev = c["candidates"].get(cid)
            vs = c["vs_baseline"][cid]
            if not ev:
                lines.append(f"- `{cid}`: did not fire")
                continue
            lines.append(
                f"- `{cid}`: bar {ev['bar_index']} px={ev['exit_price']} → "
                f"**{ev['exit_r']:.3f}R** "
                f"(ΔR {vs['exit_r_delta']:+}, {vs['bars_earlier']} bar(s) earlier) "
                f"— {ev['reason']}"
            )
        lines.append("")

    lines.append("## 22-Jul checkpoint flags (no code)")
    lines.append("")
    lines.append(
        "1. **FEDERALBNK**: C1/C2/C3 all fire on the spike candle (close 350.80 ≈ 1.4R) "
        "vs baseline EMA5 exit one bar later (~0.6R close / ₹0 fill). Spike-reverse (C2) "
        "is the most direct narrative match."
    )
    lines.append(
        "2. **TATAELXSI control**: peak &lt; 2R → all candidates silent on the stop-out "
        "window (avoids holding-through-pullback bias from the same-day continuation)."
    )
    lines.append(
        "3. **Live wiring**: `kavach_exit_candidate_shadow_log` + snapshot hook in "
        "`evaluate_open_trades` log would-fire only; state machine unchanged."
    )
    lines.append(
        "4. **Before any live flip**: re-run with Upstox-fetched 10m bars for "
        "FEDERALBNK/POLICYBZR/ADANIGREEN; confirm C1 ratchet floor does not "
        "chop winners that grind &gt;2R without a spike fade."
    )
    lines.append("")
    lines.append(f"_Generated {report['meta']['generated_at']}_")
    lines.append("")
    return "\n".join(lines)


def main() -> int:
    ap = argparse.ArgumentParser()
    ap.add_argument("--json-only", action="store_true")
    ap.add_argument(
        "--out-dir",
        type=Path,
        default=OUT_DIR,
        help="Diagnostics output directory",
    )
    args = ap.parse_args()
    report = run_all()
    out_dir = args.out_dir
    out_dir.mkdir(parents=True, exist_ok=True)
    json_path = out_dir / "EXIT_CANDIDATE_SHADOW_2026-07-20.json"
    md_path = out_dir / "EXIT_CANDIDATE_SHADOW_2026-07-20.md"
    json_path.write_text(json.dumps(report, indent=2) + "\n", encoding="utf-8")
    if not args.json_only:
        md_path.write_text(render_markdown(report), encoding="utf-8")
    print(json.dumps({"json": str(json_path), "md": str(md_path), "rollup": report["rollup"]}, indent=2))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
