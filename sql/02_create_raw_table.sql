CREATE TABLE IF NOT EXISTS raw.raw_airkorea (
    id SERIAL PRIMARY KEY,
    file_name TEXT,
    region TEXT,
    collected_at TIMESTAMP,
    data JSONB,
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);