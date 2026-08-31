-- 3a: Permanent sector/index registry + daily prev-close slot
-- 17 standing rows: 16 Breakfast sectors + NIFTY50

CREATE TABLE IF NOT EXISTS nifty_benchmark_reference (
    instrument_key              TEXT PRIMARY KEY,
    display_label               TEXT NOT NULL,
    benchmark_kind              TEXT NOT NULL
                                CHECK (benchmark_kind IN ('sector', 'broad')),
    breakfast_sort_order        SMALLINT,
    prev_session_close          DOUBLE PRECISION,
    prev_session_close_for_date DATE,
    prev_session_close_source   TEXT,
    created_at                  TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    updated_at                  TIMESTAMPTZ NOT NULL DEFAULT NOW()
);

CREATE UNIQUE INDEX IF NOT EXISTS uq_nbr_display_label
    ON nifty_benchmark_reference (display_label);

CREATE INDEX IF NOT EXISTS ix_nbr_prev_close_for_date
    ON nifty_benchmark_reference (prev_session_close_for_date DESC);

COMMENT ON TABLE nifty_benchmark_reference IS
    'Standing registry of Breakfast sector indices (16) + NIFTY50. prev_session_close* updated daily by breakfast prev-close job.';

INSERT INTO nifty_benchmark_reference
    (instrument_key, display_label, benchmark_kind, breakfast_sort_order)
VALUES
    ('NSE_INDEX|Nifty Pvt Bank',        'Nifty Private Bank',         'sector',  1),
    ('NSE_INDEX|Nifty IT',              'Nifty IT',                   'sector',  2),
    ('NSE_INDEX|Nifty Auto',            'Nifty Auto',                 'sector',  3),
    ('NSE_INDEX|Nifty FMCG',            'Nifty FMCG',                 'sector',  4),
    ('NSE_INDEX|Nifty Metal',           'Nifty Metal',                'sector',  5),
    ('NSE_INDEX|Nifty Realty',          'Nifty Realty',               'sector',  6),
    ('NSE_INDEX|Nifty Energy',          'Nifty Energy',               'sector',  7),
    ('NSE_INDEX|Nifty Infra',           'Nifty Infra',                'sector',  8),
    ('NSE_INDEX|Nifty PSU Bank',        'Nifty PSU Bank',             'sector',  9),
    ('NSE_INDEX|NIFTY HEALTHCARE',      'Nifty Healthcare',           'sector', 10),
    ('NSE_INDEX|NIFTY CONSR DURBL',     'Nifty Consumer Durables',    'sector', 11),
    ('NSE_INDEX|NIFTY OIL AND GAS',     'Nifty Oil & Gas',            'sector', 12),
    ('NSE_INDEX|Nifty Fin Service',     'Nifty Financial Services',   'sector', 13),
    ('NSE_INDEX|Nifty Chemicals',       'Nifty Chemicals',            'sector', 14),
    ('NSE_INDEX|Nifty Serv Sector',     'Nifty Services',             'sector', 15),
    ('NSE_INDEX|Nifty MS IT Telcm',     'Nifty Telecom',              'sector', 16),
    ('NSE_INDEX|Nifty 50',              'NIFTY50',                    'broad',  NULL)
ON CONFLICT (instrument_key) DO UPDATE SET
    display_label        = EXCLUDED.display_label,
    benchmark_kind       = EXCLUDED.benchmark_kind,
    breakfast_sort_order = EXCLUDED.breakfast_sort_order,
    updated_at           = NOW();
