-- Anchor-based TP/SL and pre-exit extreme (MFE) for Breakfast Strategy trades.
ALTER TABLE breakfast_strategy_trades
    ADD COLUMN IF NOT EXISTS anchor_price DOUBLE PRECISION;

ALTER TABLE breakfast_strategy_trades
    ADD COLUMN IF NOT EXISTS pre_exit_extreme DOUBLE PRECISION;
