import psycopg2

try:
    conn = psycopg2.connect(
        host="localhost",
        database="test",
        user="airflow",
        password="airflow",
        port="5432"
    )
    print("Conexão bem-sucedida ao banco de dados PostgreSQL")
    cursor = conn.cursor()
    cursor.execute("DROP SCHEMA IF EXISTS public CASCADE;")
    cursor.execute("CREATE SCHEMA public;")
    conn.commit()
    conn.close()
except Exception as e:
    print(f"Erro ao conectar ao banco de dados PostgreSQL: {e}")