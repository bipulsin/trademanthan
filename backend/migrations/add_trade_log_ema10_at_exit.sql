-- trade_log: EMA10 at exit for stop-ref drift queries (journal only).
-- Applied also via ensure_trade_log_table + database.py startup (IF NOT EXISTS).

ALTER TABLE trade_log
    ADD COLUMN IF NOT EXISTS ema10_at_exit DOUBLE PRECISION;
