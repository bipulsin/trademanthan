-- trade_log: entry tagging for win-rate / R-realized correlation (journal only).
-- Applied also via ensure_trade_log_table + database.py startup (IF NOT EXISTS).
-- No backfill; no index; nullable; no DB CHECK (app-level enum like exit_trigger_type).
-- Allowed entry_trigger_type values: pullback_entry, ignition_leg, discretionary, re_entry

ALTER TABLE trade_log
    ADD COLUMN IF NOT EXISTS entry_trigger_type TEXT;

ALTER TABLE trade_log
    ADD COLUMN IF NOT EXISTS pullback_number_at_entry INTEGER;
