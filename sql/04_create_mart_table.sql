CREATE TABLE IF NOT EXISTS mart.mart_airkorea_region_summary (
    sido_name TEXT PRIMARY KEY,
    avg_pm10 NUMERIC,
    avg_pm25 NUMERIC,
    avg_o3 NUMERIC,
    avg_no2 NUMERIC,
    avg_co NUMERIC,
    avg_so2 NUMERIC,
    record_count INT,
    last_updated TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);