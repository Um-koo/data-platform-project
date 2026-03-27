CREATE TABLE IF NOT EXISTS staging.staging_airkorea (
    id SERIAL PRIMARY KEY,
    station_name TEXT,
    sido_name TEXT,
    data_time TIMESTAMP,
    pm10_value NUMERIC,
    pm25_value NUMERIC,
    o3_value NUMERIC,
    no2_value NUMERIC,
    co_value NUMERIC,
    so2_value NUMERIC,
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);

DO $$
BEGIN
    IF NOT EXISTS (
        SELECT 1
        FROM pg_constraint
        WHERE conname = 'unique_airkorea_row'
    ) THEN
        ALTER TABLE staging.staging_airkorea
        ADD CONSTRAINT unique_airkorea_row
        UNIQUE (station_name, sido_name, data_time);
    END IF;
END $$;