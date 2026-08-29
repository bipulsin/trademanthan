-- Breakfast / NSE heatmap alignment: arbitrage_master sector_index fixes.
-- Idempotent updates; safe to re-run.

-- Pharma F&O names → Healthcare index only (Nifty Pharma index not used for stock tagging).
UPDATE arbitrage_master
SET sector_index = 'NSE_INDEX|NIFTY HEALTHCARE',
    sector = 'HEALTHCARE'
WHERE UPPER(TRIM(stock)) IN (
    'AUROPHARMA', 'DIVISLAB', 'DRREDDY', 'GLENMARK', 'LAURUSLABS', 'LUPIN',
    'SUNPHARMA', 'TORNTPHARM', 'ZYDUSLIFE', 'APOLLOHOSP', 'MAXHEALTH'
);

-- NSE Metal includes ADANIENT (was tagged Energy).
UPDATE arbitrage_master
SET sector_index = 'NSE_INDEX|Nifty Metal',
    sector = 'METAL'
WHERE UPPER(TRIM(stock)) = 'ADANIENT';

-- NSE FMCG includes RADICO (sector_index was NULL).
UPDATE arbitrage_master
SET sector_index = 'NSE_INDEX|Nifty FMCG',
    sector = 'FMCG'
WHERE UPPER(TRIM(stock)) = 'RADICO';

-- Nifty Chemicals F&O constituents.
UPDATE arbitrage_master
SET sector_index = 'NSE_INDEX|Nifty Chemicals',
    sector = 'COMMODITIES'
WHERE UPPER(TRIM(stock)) IN ('SOLARINDS', 'UPL');

-- Nifty IT: WIPRO (F&O; was missing from arbitrage_master).
UPDATE arbitrage_master
SET sector_index = 'NSE_INDEX|Nifty IT',
    sector = 'IT'
WHERE UPPER(TRIM(stock)) = 'WIPRO';

-- Clear orphan Nifty Pharma index tags (index not in Breakfast universe).
UPDATE arbitrage_master
SET sector_index = 'NSE_INDEX|NIFTY HEALTHCARE',
    sector = 'HEALTHCARE'
WHERE sector_index = 'NSE_INDEX|Nifty Pharma';

-- Nifty MS IT Telcm: telecom only (IDEA, INDUSTOWER). Not BHARTIARTL.
UPDATE arbitrage_master
SET sector_index = 'NSE_INDEX|Nifty MS IT Telcm',
    sector = 'SERVICES'
WHERE UPPER(TRIM(stock)) IN ('IDEA', 'INDUSTOWER');

-- Nifty Services Sector (thematic): BHARTIARTL, INDIGO, INDHOTEL, SWIGGY — not MS IT Telcm.
UPDATE arbitrage_master
SET sector_index = 'NSE_INDEX|Nifty Serv Sector',
    sector = 'SERVICES'
WHERE UPPER(TRIM(stock)) IN ('BHARTIARTL', 'INDIGO', 'INDHOTEL', 'SWIGGY');

-- Brokers / AMC → Nifty Financial Services (Breakfast universe).
UPDATE arbitrage_master
SET sector_index = 'NSE_INDEX|Nifty Fin Service',
    sector = 'FINANCIALS'
WHERE UPPER(TRIM(stock)) IN ('ANGELONE', 'MOTILALOFS', 'NAM-INDIA');

-- GVT&D → Nifty Energy (NSE constituent; no Nifty Engineering index on Upstox).
UPDATE arbitrage_master
SET sector_index = 'NSE_INDEX|Nifty Energy',
    sector = 'ENERGY'
WHERE UPPER(TRIM(stock)) = 'GVT&D';
