-- Spot-proxy OOS runs: price_source tagging + backtest_oos_spot mode.
ALTER TABLE breakfast_strategy_trades
    ADD COLUMN IF NOT EXISTS price_source TEXT;

ALTER TABLE breakfast_strategy_trades
    DROP CONSTRAINT IF EXISTS breakfast_strategy_trades_mode_check;

ALTER TABLE breakfast_strategy_trades
    ADD CONSTRAINT breakfast_strategy_trades_mode_check
    CHECK (mode IN ('backtest', 'forward', 'backtest_oos', 'backtest_oos_spot'));
