-- trade_log: execution-quality slippage in INR (journal only).
-- intended exit already exists as exit_price_intended; pts as slippage_pts.
-- Applied also via ensure_trade_log_table + database.py startup (IF NOT EXISTS).

ALTER TABLE trade_log
    ADD COLUMN IF NOT EXISTS slippage_inr DOUBLE PRECISION;
