"""API resolve path must not block on full live pipeline when DB has rows."""
from unittest.mock import patch

from backend.services.vajra.job import resolve_vajra_ratings_for_api


def test_stale_db_returns_snapshot_without_live_pipeline():
    db_rows = [{"stock": "RELIANCE", "evs_score": None}] * 10
    with patch("backend.services.vajra.job.fetch_vajra_ratings_for_session", return_value=db_rows):
        with patch("backend.services.vajra.job.fetch_vajra_ratings_updated_at", return_value=None):
            with patch("backend.services.vajra.job.is_vajra_ratings_stale", return_value=(True, "missing_evs")):
                with patch("backend.services.vajra.job._run_transition_pipeline_live") as live:
                    rows, source, reason = resolve_vajra_ratings_for_api(use_cache=False)
    live.assert_not_called()
    assert len(rows) == 10
    assert source == "db_stale"
    assert reason == "missing_evs"
