CREATE OR REPLACE VIEW mart.v_airkorea_summary AS
SELECT *
FROM mart.mart_airkorea_region_summary;

CREATE OR REPLACE VIEW mart.v_airkorea_top3 AS
SELECT *
FROM mart.mart_airkorea_region_summary
ORDER BY avg_pm10 DESC
LIMIT 3;