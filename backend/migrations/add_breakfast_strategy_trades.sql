-- Breakfast Strategy paper/backtest trade log (standalone — not trade_log).
CREATE TABLE IF NOT EXISTS breakfast_strategy_trades (
    id                      BIGSERIAL PRIMARY KEY,
    session_date            DATE NOT NULL,
    symbol                  TEXT NOT NULL,
    direction               TEXT NOT NULL CHECK (direction IN ('long', 'short')),
    UNIQUE (session_date, symbol, direction),

    mode                    TEXT NOT NULL DEFAULT 'backtest'
                            CHECK (mode IN ('backtest', 'forward')),
    strategy_status         TEXT NOT NULL DEFAULT 'shadow'
                            CHECK (strategy_status IN ('shadow', 'promoted')),

    sector                  TEXT,
    sector_index            TEXT,
    sector_rank             SMALLINT,
    stock_rank              SMALLINT,

    nifty_bias              TEXT NOT NULL CHECK (nifty_bias IN ('positive', 'negative')),
    nifty_bias_pct          DOUBLE PRECISION,
    nifty_open_5m           DOUBLE PRECISION,
    nifty_close_5m          DOUBLE PRECISION,

    stock_move_pct_at_entry DOUBLE PRECISION,
    setup_open_5m           DOUBLE PRECISION,
    setup_high_5m           DOUBLE PRECISION,
    setup_low_5m            DOUBLE PRECISION,
    setup_close_5m          DOUBLE PRECISION,
    setup_volume_5m         DOUBLE PRECISION,

    instrument_key          TEXT,
    lot_size                INTEGER NOT NULL,

    entry_time              TIMESTAMPTZ NOT NULL,
    entry_price             DOUBLE PRECISION NOT NULL,
    sl_price                DOUBLE PRECISION NOT NULL,
    tp_price                DOUBLE PRECISION NOT NULL,

    exit_time               TIMESTAMPTZ,
    exit_price              DOUBLE PRECISION,
    exit_trigger_type       TEXT CHECK (exit_trigger_type IN (
                                'target_hit', 'sl_hit', 'time_exit', 'data_gap'
                            )),

    pnl_inr                 DOUBLE PRECISION,
    pnl_points              DOUBLE PRECISION,

    notes                   TEXT,
    created_at              TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    updated_at              TIMESTAMPTZ NOT NULL DEFAULT NOW()
);

CREATE INDEX IF NOT EXISTS ix_breakfast_trades_session
    ON breakfast_strategy_trades (session_date DESC);

CREATE INDEX IF NOT EXISTS ix_breakfast_trades_mode_session
    ON breakfast_strategy_trades (mode, session_date DESC);
