import boto3
import psycopg
import json

# 🔥 여기 추가
s3 = boto3.client(
    's3',
    aws_access_key_id='AKIA23A62ZK6YMUVY7PP',
    aws_secret_access_key='ZmMySuxIVUetK7Mc39lmtOLSsP4d3ZUXlz2qXXe5'
)

bucket = "data-platform-raw-umkoo"
key = "raw/weather/2026-03-23/sample.json"

# S3에서 파일 읽기
response = s3.get_object(Bucket=bucket, Key=key)
data = json.loads(response['Body'].read())

# DB 연결
conn = psycopg.connect(
    host="localhost",
    port=5433,
    dbname="airflow",
    user="airflow",
    password="airflow"
)

cur = conn.cursor()

cur.execute(
    "INSERT INTO raw_weather (data) VALUES (%s)",
    (json.dumps(data),)
)

conn.commit()
cur.close()
conn.close()

print("S3 → PostgreSQL 적재 완료")