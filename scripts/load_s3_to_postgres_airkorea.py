import json
import boto3
import pandas as pd
import psycopg
from io import BytesIO
from datetime import datetime

# AWS S3 인증 정보
# 주의: Git 업로드 전에는 실제 값을 placeholder로 교체해야 함
AWS_ACCESS_KEY_ID = "YOUR_AWS_ACCESS_KEY_ID"
AWS_SECRET_ACCESS_KEY = "YOUR_AWS_SECRET_ACCESS_KEY"
REGION_NAME = "ap-northeast-2"
BUCKET_NAME = "data-platform-raw-umkoo"

# PostgreSQL 연결 정보
DB_CONFIG = {
    "host": "localhost",
    "port": 5433,
    "dbname": "airflow",
    "user": "airflow",
    "password": "airflow"
}

# S3에서 조회할 AirKorea 파일 prefix
PREFIX = "raw/airkorea/date=2026-03-26/"


def get_s3_client():
    """
    S3 접근을 위한 boto3 client를 생성한다.
    """
    return boto3.client(
        "s3",
        aws_access_key_id=AWS_ACCESS_KEY_ID,
        aws_secret_access_key=AWS_SECRET_ACCESS_KEY,
        region_name=REGION_NAME,
    )


def get_db_connection():
    """
    PostgreSQL 연결 객체를 생성한다.
    """
    return psycopg.connect(**DB_CONFIG)


def list_csv_keys():
    """
    지정한 S3 prefix 아래의 CSV 파일 목록을 조회한다.
    """
    s3 = get_s3_client()
    response = s3.list_objects_v2(Bucket=BUCKET_NAME, Prefix=PREFIX)

    if "Contents" not in response:
        return []

    return [obj["Key"] for obj in response["Contents"] if obj["Key"].endswith(".csv")]


def insert_into_raw(file_name: str, region: str, collected_at: datetime, df: pd.DataFrame):
    """
    S3에서 읽은 CSV 데이터를 row 단위 JSON으로 변환하여
    raw.raw_airkorea 테이블에 적재한다.

    raw 계층은 원본 데이터를 최대한 보존하는 레이어이므로,
    파일명 / 지역 / 수집시각 / JSON 데이터를 함께 저장한다.
    """
    conn = get_db_connection()
    cur = conn.cursor()

    for _, row in df.iterrows():
        row_dict = {
            "stationName": row.get("stationName"),
            "sidoName": row.get("sidoName"),
            "dataTime": str(row.get("dataTime")) if pd.notna(row.get("dataTime")) else None,
            "pm10Value": None if pd.isna(row.get("pm10Value")) else float(row.get("pm10Value")),
            "pm25Value": None if pd.isna(row.get("pm25Value")) else float(row.get("pm25Value")),
            "o3Value": None if pd.isna(row.get("o3Value")) else float(row.get("o3Value")),
            "no2Value": None if pd.isna(row.get("no2Value")) else float(row.get("no2Value")),
            "coValue": None if pd.isna(row.get("coValue")) else float(row.get("coValue")),
            "so2Value": None if pd.isna(row.get("so2Value")) else float(row.get("so2Value")),
        }

        cur.execute(
            """
            INSERT INTO raw.raw_airkorea (file_name, region, collected_at, data)
            VALUES (%s, %s, %s, %s)
            """,
            (file_name, region, collected_at, json.dumps(row_dict))
        )

    conn.commit()
    cur.close()
    conn.close()


def raw_to_staging():
    """
    raw.raw_airkorea에 저장된 JSON 데이터를
    분석 가능한 컬럼 구조로 변환하여 staging.staging_airkorea에 적재한다.

    ON CONFLICT를 사용하여 중복 데이터가 발생하지 않도록 처리한다.
    """
    conn = get_db_connection()
    cur = conn.cursor()

    cur.execute(
        """
        INSERT INTO staging.staging_airkorea (
            station_name, sido_name, data_time,
            pm10_value, pm25_value, o3_value,
            no2_value, co_value, so2_value
        )
        SELECT
            data ->> 'stationName' AS station_name,
            data ->> 'sidoName' AS sido_name,
            (data ->> 'dataTime')::timestamp AS data_time,
            NULLIF(data ->> 'pm10Value', '')::numeric AS pm10_value,
            NULLIF(data ->> 'pm25Value', '')::numeric AS pm25_value,
            NULLIF(data ->> 'o3Value', '')::numeric AS o3_value,
            NULLIF(data ->> 'no2Value', '')::numeric AS no2_value,
            NULLIF(data ->> 'coValue', '')::numeric AS co_value,
            NULLIF(data ->> 'so2Value', '')::numeric AS so2_value
        FROM raw.raw_airkorea
        ON CONFLICT (station_name, sido_name, data_time) DO NOTHING
        """
    )

    conn.commit()
    cur.close()
    conn.close()


def staging_to_mart():
    """
    staging.staging_airkorea 데이터를 기반으로
    지역별 평균 대기질 지표를 mart.mart_airkorea_region_summary에 집계한다.

    ON CONFLICT DO UPDATE를 사용하여
    같은 지역(sido_name)이 이미 존재할 경우 최신 집계값으로 갱신한다.
    """
    conn = get_db_connection()
    cur = conn.cursor()

    cur.execute(
        """
        INSERT INTO mart.mart_airkorea_region_summary (
            sido_name, avg_pm10, avg_pm25, avg_o3, avg_no2, avg_co, avg_so2, record_count
        )
        SELECT
            sido_name,
            AVG(pm10_value),
            AVG(pm25_value),
            AVG(o3_value),
            AVG(no2_value),
            AVG(co_value),
            AVG(so2_value),
            COUNT(*)
        FROM staging.staging_airkorea
        GROUP BY sido_name
        ON CONFLICT (sido_name) DO UPDATE
        SET
            avg_pm10 = EXCLUDED.avg_pm10,
            avg_pm25 = EXCLUDED.avg_pm25,
            avg_o3 = EXCLUDED.avg_o3,
            avg_no2 = EXCLUDED.avg_no2,
            avg_co = EXCLUDED.avg_co,
            avg_so2 = EXCLUDED.avg_so2,
            record_count = EXCLUDED.record_count,
            last_updated = CURRENT_TIMESTAMP
        """
    )

    conn.commit()
    cur.close()
    conn.close()


def main():
    """
    S3의 AirKorea CSV 파일을 읽어
    raw → staging → mart 계층으로 적재하는 전체 흐름을 수행한다.
    """
    s3 = get_s3_client()
    csv_keys = list_csv_keys()

    print(f"CSV 파일 수: {len(csv_keys)}")

    for key in csv_keys:
        print(f"[처리 중] {key}")

        # S3에서 CSV 파일 읽기
        obj = s3.get_object(Bucket=BUCKET_NAME, Key=key)
        df = pd.read_csv(BytesIO(obj["Body"].read()), encoding="utf-8-sig")

        # S3 경로에서 지역 정보 추출
        region = key.split("region=")[1].split("/")[0]

        # 파일명 추출
        file_name = key.split("/")[-1]

        # 파일명에서 수집 시각 추출
        ts_part = file_name.replace(".csv", "").split("_")[-2:]
        collected_at = datetime.strptime("_".join(ts_part), "%Y%m%d_%H%M%S")

        # dataTime 컬럼을 datetime 타입으로 변환
        df["dataTime"] = pd.to_datetime(df["dataTime"], errors="coerce")

        # raw 테이블 적재
        insert_into_raw(file_name, region, collected_at, df)

    print("[완료] S3 → raw")

    # raw → staging
    raw_to_staging()
    print("[완료] raw → staging")

    # staging → mart
    staging_to_mart()
    print("[완료] staging → mart")

    print("전체 파이프라인 완료")


if __name__ == "__main__":
    main()