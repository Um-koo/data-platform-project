-- 지역별 평균 미세먼지 및 초미세먼지
SELECT
    sido_name,
    AVG(pm10_value) AS avg_pm10,
    AVG(pm25_value) AS avg_pm25,
    COUNT(*) AS record_count
FROM staging.staging_airkorea
GROUP BY sido_name
ORDER BY avg_pm10 DESC;

-- TOP 오염 지역
SELECT *
FROM mart.mart_airkorea_region_summary
ORDER BY avg_pm10 DESC
LIMIT 3;

-- 이상값 탐지
SELECT *
FROM staging.staging_airkorea
WHERE pm10_value > 80
ORDER BY pm10_value DESC;

-- 측정소별 평균 PM10
SELECT
    station_name,
    sido_name,
    AVG(pm10_value) AS avg_pm10
FROM staging.staging_airkorea
GROUP BY station_name, sido_name
ORDER BY avg_pm10 DESC;

-- 지역별 평균 가스 농도
SELECT
    sido_name,
    AVG(o3_value) AS avg_o3,
    AVG(no2_value) AS avg_no2,
    AVG(co_value) AS avg_co,
    AVG(so2_value) AS avg_so2
FROM staging.staging_airkorea
GROUP BY sido_name
ORDER BY avg_o3 DESC;