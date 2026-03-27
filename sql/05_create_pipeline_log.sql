CREATE TABLE IF NOT EXISTS mart.pipeline_log (
    id SERIAL PRIMARY KEY,
    pipeline_name VARCHAR(100),
    status VARCHAR(20),
    start_time TIMESTAMP,
    end_time TIMESTAMP,
    message TEXT
);