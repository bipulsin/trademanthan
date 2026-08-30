-- period_label for monthly history rows (e.g. 2026-05).
ALTER TABLE breakfast_strategy_trades
    ADD COLUMN IF NOT EXISTS period_label TEXT;

CREATE INDEX IF NOT EXISTS ix_breakfast_trades_period
    ON breakfast_strategy_trades (period_label, mode);
