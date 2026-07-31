-- trade_log: peak unrealized P&L and peak-to-exit giveback in R (journal only).
-- Applied also via ensure_trade_log_table + database.py startup (IF NOT EXISTS).
-- Future closed trades should auto-fill from the hold-period unrealized P&L series.

ALTER TABLE trade_log
  ADD COLUMN IF NOT EXISTS peak_unrealized_pnl DOUBLE PRECISION;

ALTER TABLE trade_log
  ADD COLUMN IF NOT EXISTS peak_to_exit_giveback_r DOUBLE PRECISION;
