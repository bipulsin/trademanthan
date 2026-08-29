ALTER TABLE breakfast_strategy_trades
    ADD COLUMN IF NOT EXISTS instrument_label TEXT;

ALTER TABLE breakfast_strategy_trades
    ADD COLUMN IF NOT EXISTS underlying_symbol TEXT;
