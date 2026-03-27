import psycopg

conn = psycopg.connect(
    host="localhost",
    port=5433,
    dbname="airflow",   # ← 이걸로 바꿔야 함
    user="airflow",
    password="airflow"
)

print("DB 연결 성공")

conn.close()