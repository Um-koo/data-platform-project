import psycopg

conn = psycopg.connect(
    host="localhost",
    port=5433,
    dbname="airflow",
    user="airflow",
    password="airflow"
)

cur = conn.cursor()

cur.execute("SELECT id, data FROM raw_weather LIMIT 5;")

rows = cur.fetchall()

for row in rows:
    print(row)

cur.close()
conn.close()