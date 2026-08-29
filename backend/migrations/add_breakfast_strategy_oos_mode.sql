-- Allow out-of-sample validation runs distinct from primary backtest.
ALTER TABLE breakfast_strategy_trades
    DROP CONSTRAINT IF EXISTS breakfast_strategy_trades_mode_check;

ALTER TABLE breakfast_strategy_trades
    ADD CONSTRAINT breakfast_strategy_trades_mode_check
    CHECK (mode IN ('backtest', 'forward', 'backtest_oos'));
