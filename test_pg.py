import psycopg2

conn = psycopg2.connect(
    host="localhost",
    dbname="prueba_dbt",
    user="postgres",
    password="postgres"
)

cur = conn.cursor()
cur.execute("select 1;")
print(cur.fetchone())

cur.close()
conn.close()
