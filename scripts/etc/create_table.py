import psycopg

conn = psycopg.connect(
    host="localhost",
    port=5433,
    dbname="airflow",
    user="airflow",
    password="airflow"
)

cur = conn.cursor()

cur.execute("""
CREATE TABLE IF NOT EXISTS raw_weather (
    id SERIAL PRIMARY KEY,
    data JSONB,
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);
""")

conn.commit()
cur.close()
conn.close()

print("테이블 생성 완료")