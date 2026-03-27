import subprocess
import psycopg
from datetime import datetime

# PostgreSQL 연결 정보
# pipeline_log 테이블에 실행 이력을 남기기 위해 사용
DB_CONFIG = {
    "host": "localhost",
    "port": 5433,
    "dbname": "airflow",
    "user": "airflow",
    "password": "airflow"
}


def get_db_connection():
    """
    PostgreSQL 연결 객체를 생성한다.
    pipeline_log 기록에 사용된다.
    """
    return psycopg.connect(**DB_CONFIG)


def run_script(script_name):
    """
    지정한 Python 스크립트를 순차적으로 실행한다.

    예:
    - fetch_airkorea_to_s3.py
    - load_s3_to_postgres_airkorea.py

    실행 중 오류가 발생하면 예외를 발생시켜
    전체 파이프라인을 실패 상태로 처리한다.
    """
    print(f"\n[실행] {script_name}")

    result = subprocess.run(["python", f"scripts/{script_name}"])

    if result.returncode != 0:
        raise Exception(f"{script_name} 실행 실패")


def insert_log(pipeline_name, status, start_time, end_time, message):
    """
    파이프라인 실행 결과를 mart.pipeline_log 테이블에 기록한다.

    기록 항목:
    - pipeline_name: 파이프라인 이름
    - status: SUCCESS / FAILED
    - start_time: 실행 시작 시각
    - end_time: 실행 종료 시각
    - message: 실행 결과 메시지
    """
    conn = get_db_connection()
    cur = conn.cursor()

    cur.execute(
        """
        INSERT INTO mart.pipeline_log
        (pipeline_name, status, start_time, end_time, message)
        VALUES (%s, %s, %s, %s, %s)
        """,
        (pipeline_name, status, start_time, end_time, message)
    )

    conn.commit()
    cur.close()
    conn.close()


def main():
    """
    전체 AirKorea 데이터 파이프라인을 실행한다.

    실행 순서:
    1. AirKorea API 호출 및 S3 저장
    2. S3 → raw → staging → mart 적재
    3. pipeline_log 실행 로그 기록
    """
    pipeline_name = "airkorea_pipeline"
    start_time = datetime.now()

    try:
        print("=== AirKorea Pipeline 실행 시작 ===")
        print("실행 시각:", start_time)

        # 1단계: API 호출 후 S3에 Raw 데이터 저장
        run_script("fetch_airkorea_to_s3.py")

        # 2단계: S3 데이터를 PostgreSQL raw/staging/mart 계층으로 적재
        run_script("load_s3_to_postgres_airkorea.py")

        end_time = datetime.now()

        # 정상 실행 로그 기록
        insert_log(
            pipeline_name,
            "SUCCESS",
            start_time,
            end_time,
            "정상 실행 완료"
        )

        print("=== Pipeline 실행 완료 ===")

    except Exception as e:
        end_time = datetime.now()

        # 실패 로그 기록
        insert_log(
            pipeline_name,
            "FAILED",
            start_time,
            end_time,
            str(e)
        )

        print("❌ 오류 발생:", e)


if __name__ == "__main__":
    main()