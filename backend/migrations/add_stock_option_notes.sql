-- User trade notes on Stock Options signals (Edit & Exit modal).
ALTER TABLE stock_option_signals
    ADD COLUMN IF NOT EXISTS notes TEXT;

COMMENT ON COLUMN stock_option_signals.notes IS
    'Optional user trade notes from Edit & Exit modal (distinct from system remarks).';
