
-- dq_check.sql
SELECT 'prices_recent_30d' AS check, COUNT(*) AS cnt
FROM read_parquet('G:/AI/datahub/silver/alpha/prices/yyyymm=*/**/*.parquet')
WHERE date >= date_trunc('day', now()) - INTERVAL 30 DAY;

SELECT 'prices_dupe_keys' AS check, date, symbol, COUNT(*) AS c
FROM read_parquet('G:/AI/datahub/silver/alpha/prices/yyyymm=*/**/*.parquet')
GROUP BY 2,3
HAVING c > 1
LIMIT 50;

SELECT 'prices_null_required' AS check, COUNT(*) AS cnt
FROM read_parquet('G:/AI/datahub/silver/alpha/prices/yyyymm=*/**/*.parquet')
WHERE date IS NULL OR symbol IS NULL OR close IS NULL;
