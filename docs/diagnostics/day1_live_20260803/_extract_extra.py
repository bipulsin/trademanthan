#!/usr/bin/env python3
import json
import re
from datetime import date, datetime, timedelta, timezone
from pathlib import Path

from sqlalchemy import text

from backend.database import engine

DAY = date(2026, 8, 3)
out = {}

with engine.connect() as c:
    low = c.execute(
        text(
            """
        SELECT scan_time AT TIME ZONE 'Asia/Kolkata' AS scan_ist,
               count(*) AS n,
               count(*) FILTER (WHERE exclusion_reason IS NULL) AS scored
        FROM rs_universe_score_snapshot WHERE session_date=:d
        GROUP BY scan_time
        ORDER BY scan_time
        """
        ),
        {"d": DAY},
    ).mappings().all()
    out["cycles_full"] = [
        {
            "scan_ist": r["scan_ist"].isoformat(),
            "n": r["n"],
            "scored": r["scored"],
            "pct": round(100.0 * r["scored"] / r["n"], 1) if r["n"] else 0,
        }
        for r in low
    ]
    out["low_cov"] = [x for x in out["cycles_full"] if x["scored"] < 150]

# Candle deny from algo log (timestamps are UTC in this file)
textlog = Path("/app/logs/smart_future_algo.log").read_text(errors="ignore")
deny_rows = []
for line in textlog.splitlines():
    if "2026-08-03" not in line or "candle_deny_pct" not in line:
        continue
    m = re.search(
        r"(2026-08-03 \d{2}:\d{2}:\d{2}).*'execution': '([^']+)'.*"
        r"'candle_deny_pct': ([0-9.]+).*'elapsed_sec': ([0-9.]+).*"
        r"'candle_keys_requested': (\d+)",
        line,
    )
    if not m:
        m = re.search(
            r"(2026-08-03 \d{2}:\d{2}:\d{2}).*'execution': '([^']+)'.*"
            r"'candle_deny_pct': ([0-9.]+).*'elapsed_sec': ([0-9.]+)",
            line,
        )
    if m:
        g = list(m.groups())
        # convert UTC -> IST for readability
        utc = datetime.strptime(g[0], "%Y-%m-%d %H:%M:%S").replace(tzinfo=timezone.utc)
        ist = utc.astimezone(timezone(timedelta(hours=5, minutes=30)))
        deny_rows.append(
            {
                "log_utc": g[0],
                "ist": ist.strftime("%Y-%m-%d %H:%M:%S"),
                "execution": g[1],
                "deny_pct": float(g[2]),
                "elapsed_sec": float(g[3]),
                "requested": int(g[4]) if len(g) > 4 else None,
            }
        )
out["candle_deny"] = deny_rows

# RS scan duration lines
rs_dur = []
for line in textlog.splitlines():
    if "2026-08-03" not in line:
        continue
    if "duration_sec" in line and (
        "rs_" in line.lower()
        or "relative_strength" in line.lower()
        or "score cycle" in line.lower()
        or "Universe" in line
        or "scored" in line
    ):
        m = re.search(r"(2026-08-03 \d{2}:\d{2}:\d{2}).*duration_sec.: ([0-9.]+)", line)
        if m:
            rs_dur.append({"utc": m.group(1), "duration_sec": float(m.group(2)), "snip": line[-240:]})
# also common pattern from scanner
for line in textlog.splitlines():
    if "2026-08-03" not in line:
        continue
    m = re.search(
        r"(2026-08-03 \d{2}:\d{2}:\d{2}).*(?:RS scan|Relative Strength|universe score).*?([0-9.]+)s",
        line,
        re.I,
    )
    if m:
        rs_dur.append({"utc": m.group(1), "duration_sec": float(m.group(2)), "snip": line[-200:]})

out["rs_dur_raw_n"] = len(rs_dur)
out["rs_dur_sample"] = rs_dur[:20]

# EW: look for early ew_event bullish with ema_reliable in SQ breakdowns already have
# Also check consistency for ew
with engine.connect() as c:
    ew = c.execute(
        text(
            """
        SELECT logged_at AT TIME ZONE 'Asia/Kolkata' AS ts,
               symbol,
               inputs->'structural_quality'->>'ew_event' AS ew_event,
               inputs->'structural_quality'->>'EW' AS ew,
               inputs->'structural_quality'->>'ema_reliable' AS ema_reliable,
               inputs->'structural_quality'->>'bar_hhmm' AS bar_hhmm
        FROM kavach_ready_consistency_log
        WHERE session_date=:d
          AND inputs->'structural_quality'->>'ew_event' IS NOT NULL
        ORDER BY logged_at
        LIMIT 30
        """
        ),
        {"d": DAY},
    ).mappings().all()
    out["ew_events"] = [{**dict(r), "ts": r["ts"].isoformat()} for r in ew]

    # SQ score_breakdown already has FORTIS ew_event bullish at 10:25

Path("/tmp/day1_extra.json").write_text(json.dumps(out, indent=2, default=str))
print(
    "low",
    len(out["low_cov"]),
    "deny",
    len(deny_rows),
    "rs_dur",
    len(rs_dur),
    "ew",
    len(out["ew_events"]),
)
if deny_rows:
    hourly = [d for d in deny_rows if "hourly" in d["execution"] or "stock_next" in d["execution"]]
    ten = [d for d in deny_rows if d["execution"] == "scheduled_10m"]
    print(
        "deny10m",
        [d["deny_pct"] for d in ten],
        "hourly",
        [d["deny_pct"] for d in hourly],
    )
print("low_cov", out["low_cov"][:8])
