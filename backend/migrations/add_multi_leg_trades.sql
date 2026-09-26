-- Manual multi-leg options journal (Straddle / Iron Fly / Iron Condor).
-- Record-keeping and live mark-to-market only. No broker orders.

CREATE TABLE IF NOT EXISTS multi_leg_trades (
    id UUID PRIMARY KEY,
    trade_type TEXT NOT NULL
        CHECK (trade_type IN ('STRADDLE', 'IRON_FLY', 'IRON_CONDOR', 'UNCLASSIFIED')),
    instrument TEXT NOT NULL,
    spot_price_entry NUMERIC(18, 4) NOT NULL,
    entry_date DATE NOT NULL,
    expiry_date DATE NOT NULL,
    status TEXT NOT NULL
        CHECK (status IN ('ACTIVE', 'CLOSED')),
    exit_date TIMESTAMPTZ,
    total_pnl NUMERIC(18, 4),
    created_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    updated_at TIMESTAMPTZ NOT NULL DEFAULT NOW()
);

CREATE INDEX IF NOT EXISTS ix_multi_leg_trades_status_entry
    ON multi_leg_trades (status, entry_date DESC);

CREATE TABLE IF NOT EXISTS multi_leg_trade_legs (
    id UUID PRIMARY KEY,
    trade_id UUID REFERENCES multi_leg_trades(id) ON DELETE CASCADE,
    side TEXT NOT NULL CHECK (side IN ('BUY', 'SELL')),
    option_type TEXT NOT NULL CHECK (option_type IN ('CE', 'PE')),
    strike_price NUMERIC(18, 4) NOT NULL,
    leg_expiry_date DATE NOT NULL,
    entry_price NUMERIC(18, 4) NOT NULL,
    entry_time TIMESTAMPTZ NOT NULL,
    exit_price NUMERIC(18, 4),
    exit_time TIMESTAMPTZ,
    ltp NUMERIC(18, 4),
    delta DOUBLE PRECISION,
    lot_size INTEGER NOT NULL CHECK (lot_size > 0),
    instrument_key TEXT,
    leg_pnl NUMERIC(18, 4),
    upstox_order_id TEXT,
    sort_order INTEGER NOT NULL DEFAULT 0,
    created_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    updated_at TIMESTAMPTZ NOT NULL DEFAULT NOW()
);

CREATE INDEX IF NOT EXISTS ix_multi_leg_trade_legs_trade
    ON multi_leg_trade_legs (trade_id, sort_order);

-- upstox_order_id unique index is created in ensure_multi_leg_tables after the
-- additive column exists on databases created from an older copy of this file.
