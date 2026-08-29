-- Allow pnl_cap exit trigger on breakfast_strategy_trades.
ALTER TABLE breakfast_strategy_trades
    DROP CONSTRAINT IF EXISTS breakfast_strategy_trades_exit_trigger_type_check;

ALTER TABLE breakfast_strategy_trades
    ADD CONSTRAINT breakfast_strategy_trades_exit_trigger_type_check
    CHECK (exit_trigger_type IS NULL OR exit_trigger_type IN (
        'target_hit', 'sl_hit', 'time_exit', 'data_gap', 'pnl_cap', 'vwap_breach'
    ));
