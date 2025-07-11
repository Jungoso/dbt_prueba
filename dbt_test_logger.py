import subprocess
import psycopg2
from datetime import datetime
import os

# Agregar ruta a dbt.exe al PATH para que se encuentre el ejecutable
os.environ['PATH'] += r";C:\Users\josed\AppData\Local\Packages\PythonSoftwareFoundation.Python.3.12_qbz5n2kfra8p0\LocalCache\local-packages\Python312\Scripts"

# Datos de conexión a Neon
DB_HOST = 'ep-aged-flower-acuqk2rl.sa-east-1.aws.neon.tech'
DB_PORT = 5432
DB_NAME = 'neondb'
DB_USER = 'neondb_owner'
DB_PASS = 'npg_cXzbEB42GQIA'  # ⚠️ NO lo compartas en entornos públicos

# Ejecutar dbt test y procesar los resultados
def run_dbt_tests_and_log():
    print("Ejecutando dbt test...")
    
    result = subprocess.run(
        [r"C:\Users\josed\AppData\Local\Packages\PythonSoftwareFoundation.Python.3.12_qbz5n2kfra8p0\LocalCache\local-packages\Python312\Scripts\dbt.exe", 'test'],
        capture_output=True,
        text=True
    )

    stdout_lines = result.stdout.strip().split('\n')
    print("STDOUT:")
    print(result.stdout)
    print("STDERR:")
    print(result.stderr)

    # Crear conexión a PostgreSQL
    try:
        conn = psycopg2.connect(
            host=DB_HOST,
            port=DB_PORT,
            dbname=DB_NAME,
            user=DB_USER,
            password=DB_PASS,
            sslmode='require'
        )
        cursor = conn.cursor()
        now = datetime.utcnow()
        execution_id = now.strftime("%Y%m%d%H%M%S")

        insert_sql = """
        INSERT INTO dbt_test_logs (execution_id, test_name, model_name, status, error_message, execution_time)
        VALUES (%s, %s, %s, %s, %s, %s)
        """

        for line in stdout_lines:
            if line.startswith("PASS") or line.startswith("FAIL"):
                parts = line.split()
                if len(parts) < 4:
                    continue  # línea incompleta

                status = parts[0]  # PASS o FAIL
                test_full_name = parts[1]  # Ej: test_not_null_users_user_id
                execution_time_raw = parts[-1].strip("[]")  # Ej: 0.23s

                # Intentar extraer test_name y model_name
                try:
                    test_parts = test_full_name.split('_')
                    test_name = test_parts[1]  # not_null
                    model_name = "_".join(test_parts[2:-1])  # users
                except Exception:
                    test_name = test_full_name
                    model_name = "desconocido"

                error_message = line if status == "FAIL" else None
                execution_time = float(execution_time_raw.replace("s", ""))

                cursor.execute(insert_sql, (
                    execution_id,
                    test_name,
                    model_name,
                    status,
                    error_message,
                    execution_time
                ))

        conn.commit()
        cursor.close()
        conn.close()
        print("Resultados de dbt test insertados en dbt_test_logs.")

    except Exception as e:
        print("Error conectando o insertando en la base:", e)

def main():
    run_dbt_tests_and_log()

if __name__ == '__main__':
    main()
