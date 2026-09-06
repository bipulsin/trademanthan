-- Align arbitrage_master.sector with sector_instrument_key / sector_index.
-- Short label = drop NSE_INDEX| and Nifty/NIFTY prefix (Fin Service exact).
-- Idempotent. Breakfast ranking uses sector_index keys, not this string.

UPDATE arbitrage_master
SET sector = CASE COALESCE(NULLIF(TRIM(sector_instrument_key), ''), TRIM(sector_index))
    WHEN 'NSE_INDEX|Nifty Auto' THEN 'Auto'
    WHEN 'NSE_INDEX|Nifty Bank' THEN 'Bank'
    WHEN 'NSE_INDEX|Nifty Chemicals' THEN 'Chemicals'
    WHEN 'NSE_INDEX|NIFTY CONSR DURBL' THEN 'CONSR DURBL'
    WHEN 'NSE_INDEX|Nifty Consumer Durables' THEN 'CONSR DURBL'
    WHEN 'NSE_INDEX|Nifty Energy' THEN 'Energy'
    WHEN 'NSE_INDEX|Nifty Fin Service' THEN 'Fin Service'
    WHEN 'NSE_INDEX|Nifty Financial Services' THEN 'Fin Service'
    WHEN 'NSE_INDEX|Nifty FMCG' THEN 'FMCG'
    WHEN 'NSE_INDEX|NIFTY HEALTHCARE' THEN 'HEALTHCARE'
    WHEN 'NSE_INDEX|Nifty Infra' THEN 'Infra'
    WHEN 'NSE_INDEX|Nifty IT' THEN 'IT'
    WHEN 'NSE_INDEX|Nifty Media' THEN 'Media'
    WHEN 'NSE_INDEX|Nifty Metal' THEN 'Metal'
    WHEN 'NSE_INDEX|Nifty MS IT Telcm' THEN 'MS IT Telcm'
    WHEN 'NSE_INDEX|Nifty Telecom' THEN 'MS IT Telcm'
    WHEN 'NSE_INDEX|NIFTY OIL AND GAS' THEN 'OIL AND GAS'
    WHEN 'NSE_INDEX|Nifty Pharma' THEN 'Pharma'
    WHEN 'NSE_INDEX|Nifty PSU Bank' THEN 'PSU Bank'
    WHEN 'NSE_INDEX|Nifty Pvt Bank' THEN 'Pvt Bank'
    WHEN 'NSE_INDEX|Nifty Private Bank' THEN 'Pvt Bank'
    WHEN 'NSE_INDEX|Nifty Realty' THEN 'Realty'
    WHEN 'NSE_INDEX|Nifty Serv Sector' THEN 'Serv Sector'
    WHEN 'NSE_INDEX|Nifty Services' THEN 'Serv Sector'
    WHEN 'NSE_INDEX|Nifty Trans Logis' THEN 'Trans Logis'
    WHEN 'NSE_INDEX|Nifty Logistics' THEN 'Trans Logis'
    ELSE NULLIF(
        regexp_replace(
            regexp_replace(
                COALESCE(NULLIF(TRIM(sector_instrument_key), ''), TRIM(sector_index)),
                '^NSE_INDEX\|',
                ''
            ),
            '^(Nifty|NIFTY)[[:space:]_]*',
            ''
        ),
        ''
    )
END
WHERE COALESCE(NULLIF(TRIM(sector_instrument_key), ''), TRIM(sector_index)) IS NOT NULL
  AND TRIM(COALESCE(NULLIF(TRIM(sector_instrument_key), ''), TRIM(sector_index))) <> '';
