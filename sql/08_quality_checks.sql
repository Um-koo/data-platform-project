-- PM10 결측값 확인
SELECT *
FROM staging.staging_airkorea
WHERE pm10_value IS NULL;

-- PM2.5 결측값 확인
SELECT *
FROM staging.staging_airkorea
WHERE pm25_value IS NULL;

-- data_time 결측값 확인
SELECT *
FROM staging.staging_airkorea
WHERE data_time IS NULL;

-- PM10 이상값 확인
SELECT *
FROM staging.staging_airkorea
WHERE pm10_value > 80
ORDER BY pm10_value DESC;

-- 중복 여부 확인
SELECT
    station_name,
    sido_name,
    data_time,
    COUNT(*) AS cnt
FROM staging.staging_airkorea
GROUP BY station_name, sido_name, data_time
HAVING COUNT(*) > 1;