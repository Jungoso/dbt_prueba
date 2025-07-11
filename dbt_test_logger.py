import subprocess
import psycopg2
from datetime import datetime, timezone
import json
import os

# Agregar ruta a dbt.exe al PATH (ajustar si dbt está en otra ruta)
os.environ['PATH'] += r";C:\Users\josed\AppData\Local\Packages\PythonSoftwareFoundation.Python.3.12_qbz5n2kfra8p0\LocalCache\local-packages\Python312\Scripts"

# Configuración base de datos
DB_HOST = 'ep-aged-flower-acuqk2rl.sa-east-1.aws.neon.tech'
DB_PORT = 5432
DB_NAME = 'neondb'
DB_USER = 'neondb_owner'
DB_PASS = 'npg_cXzbEB42GQIA'

def run_dbt_tests():
    print("▶️ Ejecutando dbt test...")
    result = subprocess.run(
        ["dbt", "test", "--store-failures"],
        capture_output=True,
        text=True
    )
    print("STDOUT:\n", result.stdout)
    print("STDERR:\n", result.stderr)
    if result.returncode != 0:
        print(f"❌ Error al ejecutar dbt test (código {result.returncode})")
    else:
        print("✅ dbt test ejecutado correctamente")
    return result.returncode == 0

def parse_and_log_results(conn):
    run_results_path = os.path.join("target", "run_results.json")
    if not os.path.exists(run_results_path):
        print("⚠️ No se encontró el archivo run_results.json. ¿Se ejecutó dbt test?")
        return

    with open(run_results_path, "r", encoding="utf-8") as f:
        data = json.load(f)

    results = data.get("results", [])
    if not results:
        print("ℹ️ No se encontraron resultados en run_results.json")
        return

    now = datetime.now(timezone.utc)
    cursor = conn.cursor()

    insert_sql = """
        INSERT INTO dbt_test_logs (test_name, model_name, status, error_message, execution_time)
        VALUES (%s, %s, %s, %s, %s)
    """

    inserted_count = 0
    for result in results:
        # No filtramos por resource_type porque no existe en el JSON
        test_name = result.get("unique_id", "")
        # Extraer modelo desde unique_id (ajustar si el formato cambia)
        model_name = test_name.split(".")[2] if len(test_name.split(".")) > 2 else ""

        status = result.get("status", "unknown")
        error_message = result.get("message", None)

        cursor.execute(insert_sql, (test_name, model_name, status, error_message, now))
        inserted_count += 1

    conn.commit()
    cursor.close()

    print(f"✅ Se insertaron {inserted_count} registros en dbt_test_logs.")

def main():
    success = run_dbt_tests()

    try:
        conn = psycopg2.connect(
            host=DB_HOST,
            port=DB_PORT,
            dbname=DB_NAME,
            user=DB_USER,
            password=DB_PASS,
            sslmode='require'
        )
        parse_and_log_results(conn)
        conn.close()
    except Exception as e:
        print("❌ Error conectando a la base de datos:", e)

    if not success:
        print("❌ El comando dbt test terminó con error, pero los resultados fueron registrados.")

if __name__ == "__main__":
    main()
