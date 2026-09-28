CREATE TABLE IF NOT EXISTS premium_futures_divergence_pick (
    trade_date DATE NOT NULL,
    underlying TEXT NOT NULL,
    side TEXT NOT NULL CHECK (side IN ('bullish', 'bearish')),
    fut_symbol TEXT,
    instrument_key TEXT,
    contracts INTEGER,
    active BOOLEAN NOT NULL DEFAULT TRUE,
    enter_enabled BOOLEAN NOT NULL DEFAULT FALSE,
    ticker_eligible BOOLEAN NOT NULL DEFAULT FALSE,
    screening_id INTEGER,
    action TEXT,
    div_at TIMESTAMP WITHOUT TIME ZONE,
    updated_at TIMESTAMP WITHOUT TIME ZONE NOT NULL,
    PRIMARY KEY (trade_date, underlying, side)
);

CREATE TABLE IF NOT EXISTS premium_futures_divergence_could (
    id BIGSERIAL PRIMARY KEY,
    trade_date DATE NOT NULL,
    underlying TEXT NOT NULL,
    side TEXT NOT NULL CHECK (side IN ('bullish', 'bearish')),
    fut_symbol TEXT,
    instrument_key TEXT,
    contracts INTEGER,
    direction_type TEXT NOT NULL,
    entry_ltp DOUBLE PRECISION,
    entry_at TIMESTAMP WITHOUT TIME ZONE,
    exit_ltp DOUBLE PRECISION,
    exit_at TIMESTAMP WITHOUT TIME ZONE,
    first_scan_at TIMESTAMP WITHOUT TIME ZONE,
    ltp_missing_entry BOOLEAN NOT NULL DEFAULT FALSE,
    ltp_missing_exit BOOLEAN NOT NULL DEFAULT FALSE
);

CREATE UNIQUE INDEX IF NOT EXISTS ux_pf_divergence_could_open
    ON premium_futures_divergence_could (trade_date, underlying, side)
    WHERE exit_at IS NULL;
