import subprocess
import json
import psycopg2
from datetime import datetime

# Datos conexión a Neon (modifica según tus datos)
DB_HOST = 'ep-aged-flower-acuqk2rl.sa-east-1.aws.neon.tech'
DB_PORT = 5432
DB_NAME = 'neondb'
DB_USER = 'neondb_owner'
DB_PASS = 'npg_cXzbEB42GQIA'  # Cambia aquí

# Función para ejecutar dbt test y obtener JSON de salida
def run_dbt_tests():
    print("Ejecutando dbt test...")
    result = subprocess.run(
        ['dbt', 'test', '--output', 'json'],
        capture_output=True,
        text=True
    )
    if result.returncode != 0:
        print("Error ejecutando dbt test")
        print(result.stderr)
        return None
    
    # La salida JSON puede estar en stdout o en un archivo, aquí asumimos stdout
    return result.stdout

# Función para insertar logs en la tabla
def insert_logs(conn, test_results):
    cursor = conn.cursor()

    # Parseamos la salida JSON (puede ser una lista de resultados)
    try:
        results_json = json.loads(test_results)
    except json.JSONDecodeError as e:
        print("Error decodificando JSON:", e)
        return
    
    now = datetime.utcnow()

    # Recorremos resultados e insertamos en tabla
    for test in results_json:
        test_name = test.get('node', {}).get('name', 'unknown')
        status = test.get('status', 'unknown')
        message = test.get('message', '')
        
        insert_sql = """
        INSERT INTO dbt_test_logs (test_name, status, message, run_at)
        VALUES (%s, %s, %s, %s)
        """
        cursor.execute(insert_sql, (test_name, status, message, now))
    
    conn.commit()
    cursor.close()
    print("Logs insertados en la tabla.")

def main():
    test_results = run_dbt_tests()
    if test_results is None:
        return
    
    # Conectar a la base de datos
    try:
        conn = psycopg2.connect(
            host=DB_HOST,
            port=DB_PORT,
            dbname=DB_NAME,
            user=DB_USER,
            password=DB_PASS,
            sslmode='require'
        )
    except Exception as e:
        print("Error conectando a la base:", e)
        return
    
    insert_logs(conn, test_results)
    conn.close()

if __name__ == '__main__':
    main()
