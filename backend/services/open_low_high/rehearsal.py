"""Dress rehearsal harness: injectable clock + WS/REST replay for 08:45–09:35.

Modes:
  - ``synthetic``: build ticks from fixture frames / synthetic 1m OHLC (offline)
  - ``recorded``: replay gzip JSONL from ``OLH_RAW_RECORD_DIR`` (if present)
  - ``rest_mock``: delay/failure knobs over in-memory 23-Nov bars

Does **not** touch production DB unless ``persist=True`` (default False).
"""
from __future__ import annotations

import json
import logging
import time
from dataclasses import dataclass, field
from datetime import date, datetime, time as dtime, timedelta
from pathlib import Path
from typing import Any, Dict, List, Optional

import pytz

from backend.services.open_low_high.clock import AdvancingClock, install_advancing_clock, set_olh_clock
from backend.services.open_low_high.feed_contract import (
    HANDLED_FEED_TYPES,
    load_schema_fixtures,
    note_feed_observation,
    parse_fixture_to_ohlc,
)
from backend.services.open_low_high.ohl_log import ohl_log

logger = logging.getLogger(__name__)
IST = pytz.timezone("Asia/Kolkata")

REHEARSAL_START = dtime(8, 45, 0)
REHEARSAL_END = dtime(9, 35, 0)


@dataclass
class RestMockKnob:
    delay_sec: float = 0.0
    fail_rate: float = 0.0  # 0..1
    empty_body: bool = False
    status_code: int = 200
    _calls: int = 0

    def maybe_fail(self) -> Optional[Dict[str, Any]]:
        self._calls += 1
        if self.delay_sec > 0:
            time.sleep(min(self.delay_sec, 2.0))
        if self.empty_body:
            return {"status": self.status_code, "candles": [], "body": None}
        if self.fail_rate > 0 and (self._calls % max(1, int(1 / self.fail_rate))) == 0:
            return {"status": 429, "candles": [], "error": "injected_429"}
        return None


@dataclass
class RehearsalReport:
    session_source: str
    clock_start: str
    clock_end: str
    feed_types_seen: List[str] = field(default_factory=list)
    persisted_1m: List[Dict[str, Any]] = field(default_factory=list)
    prev_close_note: str = ""
    canaries: List[Dict[str, Any]] = field(default_factory=list)
    phase_a: Dict[str, Any] = field(default_factory=dict)
    phase_b: Dict[str, Any] = field(default_factory=dict)
    retries: List[str] = field(default_factory=list)
    alerts: List[str] = field(default_factory=list)
    fault_table: List[Dict[str, Any]] = field(default_factory=list)
    board_vs_force_repaired: Dict[str, Any] = field(default_factory=dict)
    raw_frames_available: bool = False
    notes: List[str] = field(default_factory=list)

    def as_dict(self) -> Dict[str, Any]:
        return {
            "session_source": self.session_source,
            "clock_start": self.clock_start,
            "clock_end": self.clock_end,
            "feed_types_seen": self.feed_types_seen,
            "persisted_1m_count": len(self.persisted_1m),
            "persisted_1m_sample": self.persisted_1m[:6],
            "prev_close_note": self.prev_close_note,
            "canaries": self.canaries,
            "phase_a": self.phase_a,
            "phase_b": self.phase_b,
            "retries": self.retries,
            "alerts": self.alerts,
            "fault_table": self.fault_table,
            "board_vs_force_repaired": self.board_vs_force_repaired,
            "raw_frames_available": self.raw_frames_available,
            "notes": self.notes,
            "handled_feed_types": sorted(HANDLED_FEED_TYPES),
        }


def _synthetic_1m_from_fixtures(session: date) -> List[Dict[str, Any]]:
    """Build persisted-shaped 1m rows from schema fixtures (OI/volume nullable)."""
    fixtures = load_schema_fixtures()
    rows: List[Dict[str, Any]] = []
    minute = IST.localize(datetime(session.year, session.month, session.day, 9, 15, 0))
    for fr in fixtures.get("frames") or []:
        if fr.get("expect_unknown"):
            continue
        feed = fr.get("feed")
        o, h, l, c, oi, vol = parse_fixture_to_ohlc(feed)
        if o is None:
            continue
        ft = note_feed_observation(
            str(fr.get("instrument_key")),
            feed,
            subscribed_keys={str(fr.get("instrument_key"))},
            now=minute,
        )
        rows.append(
            {
                "instrument_key": fr.get("instrument_key"),
                "segment": fr.get("segment"),
                "label": fr.get("label"),
                "feed_type": ft,
                "candle_time": minute.isoformat(),
                "open": o,
                "high": h,
                "low": l,
                "close": c,
                "oi": oi,  # nullable
                "volume": vol,  # nullable
            }
        )
    return rows


def _load_recorded_frames(day: date) -> List[Dict[str, Any]]:
    from backend.services.open_low_high.raw_recording import recording_root

    path = recording_root() / day.isoformat() / "ws_frames.jsonl.gz"
    if not path.exists():
        # also try uncompressed
        path2 = recording_root() / day.isoformat() / "ws_frames.jsonl"
        if not path2.exists():
            return []
        path = path2
    frames: List[Dict[str, Any]] = []
    try:
        if str(path).endswith(".gz"):
            import gzip

            opener = gzip.open
        else:
            opener = open
        with opener(path, "rt", encoding="utf-8") as f:
            for i, line in enumerate(f):
                if i >= 5000:
                    break
                try:
                    frames.append(json.loads(line))
                except json.JSONDecodeError:
                    continue
    except Exception:
        logger.debug("rehearsal recorded load failed", exc_info=True)
    return frames


def run_dress_rehearsal(
    *,
    source_session: date = date(2026, 11, 23),
    mode: str = "synthetic",
    rest_knobs: Optional[RestMockKnob] = None,
    run_faults: bool = True,
    step_minutes: int = 5,
) -> Dict[str, Any]:
    """Advance clock 08:45→09:35 over ``source_session`` data; report pipeline pulse."""
    from backend.services.open_low_high.fault_injection import run_all_fault_scenarios
    from backend.services.open_low_high.feed_contract import reset_feed_contract_for_tests
    from backend.services.open_low_high.silent_stall import eq_timeline_20261123

    reset_feed_contract_for_tests()
    knobs = rest_knobs or RestMockKnob()
    start = IST.localize(
        datetime(
            source_session.year,
            source_session.month,
            source_session.day,
            REHEARSAL_START.hour,
            REHEARSAL_START.minute,
            0,
        )
    )
    clock = install_advancing_clock(start)
    alerts: List[str] = []

    report = RehearsalReport(
        session_source=source_session.isoformat(),
        clock_start=start.isoformat(),
        clock_end="",
        prev_close_note="Rehearsal uses fixture/synthetic bars; PDC not loaded offline.",
        notes=[
            "NO production deploy. Staging/offline harness only.",
            "2026-11-24 is NSE holiday — preferred dress-rehearsal calendar day.",
        ],
    )

    # Capture alerts
    try:
        import backend.services.open_low_high.alerts as al

        orig_alert = al.send_olh_alert
        al.send_olh_alert = lambda *a, **_k: alerts.append(str(a[0] if a else "")) or True
    except Exception:
        orig_alert = None

    try:
        recorded = _load_recorded_frames(source_session) if mode == "recorded" else []
        report.raw_frames_available = bool(recorded)
        if mode == "recorded" and not recorded:
            report.notes.append(
                f"No recorded WS frames for {source_session}; falling back to synthetic fixtures."
            )
            mode = "synthetic"

        if mode == "recorded":
            for row in recorded[:200]:
                frame = row.get("frame") or row
                feeds = (frame.get("feeds") or {}) if isinstance(frame, dict) else {}
                for ik, feed_val in feeds.items():
                    ft = note_feed_observation(str(ik), feed_val, now=clock())
                    if ft not in report.feed_types_seen:
                        report.feed_types_seen.append(ft)
        else:
            rows = _synthetic_1m_from_fixtures(source_session)
            report.persisted_1m = rows
            report.feed_types_seen = sorted({r["feed_type"] for r in rows})

        # Walk timeline checkpoints
        checkpoints = [
            (dtime(8, 50), "readiness"),
            (dtime(9, 5), "canary_0905"),
            (dtime(9, 15), "ws_lock"),
            (dtime(9, 20), "phase_a"),
            (dtime(9, 21), "watchdog_0921"),
            (dtime(9, 25), "phase_b"),
            (dtime(9, 26), "watchdog_0926"),
            (dtime(9, 30), "watchdog_0930"),
        ]
        for tt, name in checkpoints:
            clock.set(
                IST.localize(
                    datetime(
                        source_session.year,
                        source_session.month,
                        source_session.day,
                        tt.hour,
                        tt.minute,
                        0,
                    )
                )
            )
            if name == "readiness":
                from backend.services.open_low_high.readiness import run_readiness_0850
                import backend.services.market_holiday as mh

                # Rehearsal on holiday calendar day should skip; on 23-Nov run checks soft.
                # Force non-holiday for 23-Nov synthetic path.
                orig_skip = mh.should_skip_scheduled_market_jobs_ist
                mh.should_skip_scheduled_market_jobs_ist = lambda now=None: False
                try:
                    # Soft: send_alert False; token/DB may fail offline — capture only
                    r = run_readiness_0850(now=clock(), send_alert=False)
                    report.canaries.append({"label": "readiness_0850", "result": {
                        k: r.get(k) for k in ("go", "red", "failures", "skipped") if k in r
                    }})
                finally:
                    mh.should_skip_scheduled_market_jobs_ist = orig_skip
            elif name.startswith("canary"):
                injected = knobs.maybe_fail()
                report.canaries.append(
                    {
                        "label": name,
                        "rest_mock": injected
                        or {"status": 200, "candles": 1, "note": "synthetic_ok"},
                    }
                )
            elif name == "phase_a":
                report.phase_a = {
                    "status": "rehearsal_synthetic",
                    "rows_from_fixtures": len(report.persisted_1m),
                    "note": "Real Phase A not invoked offline (no Upstox/DB).",
                }
            elif name == "phase_b":
                report.phase_b = {
                    "status": "rehearsal_synthetic",
                    "note": "Real Phase B not invoked offline; force-repair path documented in runbook.",
                }
            elif name.startswith("watchdog"):
                from backend.services.open_low_high.watchdog import run_watchdog_check
                import backend.services.market_holiday as mh

                orig_skip = mh.should_skip_scheduled_market_jobs_ist
                mh.should_skip_scheduled_market_jobs_ist = lambda now=None: True  # offline skip
                try:
                    w = run_watchdog_check(label=name.split("_")[-1], now=clock(), send_alert=False)
                    report.retries.append(f"{name}:{w.get('skipped') or w.get('failures')}")
                finally:
                    mh.should_skip_scheduled_market_jobs_ist = orig_skip

        report.board_vs_force_repaired = {
            "note": (
                "Compare rehearsal board to 23-Nov force-repaired board on paperclip "
                "after holiday rehearsal: "
                "SELECT count(*) FROM open_low_high_screen WHERE trade_date='2026-11-23'."
            ),
            "force_command": (
                "curl -X POST https://www.tradewithcto.com/api/open-low-high/screen "
                "-H 'Content-Type: application/json' -d '{\"force\":true}'"
            ),
        }
        report.notes.append(json.dumps(eq_timeline_20261123())[:500])

        if run_faults:
            faults = run_all_fault_scenarios()
            report.fault_table = faults.get("table") or []
            report.notes.append(
                f"fault_injection passed={faults.get('passed')}/{faults.get('total')}"
            )

        report.alerts = list(alerts)
        report.clock_end = clock().isoformat()
        ohl_log(
            "success",
            action="dress_rehearsal",
            detail=f"mode={mode} fixtures_1m={len(report.persisted_1m)} faults={len(report.fault_table)}",
        )
    finally:
        if orig_alert is not None:
            try:
                import backend.services.open_low_high.alerts as al

                al.send_olh_alert = orig_alert
            except Exception:
                pass
        set_olh_clock(None)

    return report.as_dict()
