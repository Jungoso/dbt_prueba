from airflow import DAG
from airflow.operators.bash import BashOperator
from airflow.operators.python import PythonOperator
from airflow.providers.google.cloud.operators.bigquery import BigQueryInsertJobOperator
from airflow.providers.google.cloud.hooks.bigquery import BigQueryHook

from datetime import datetime
import json
import os

# Configuración básica -> Esto se tiene que configurar con los campos de gcp final
PROJECT_ID = 'tu-proyecto-gcp'
DATASET_ID = 'dbt_logs'
TABLE_ID = 'dbt_test_logs'
RUN_RESULTS_PATH = '/usr/app/dbt/target/run_results.json'  # Ruta absoluta en el contenedor

default_args = {
    'owner': 'Jose',
    'start_date': datetime(2024, 1, 1),
    'retries': 1,
}

def parse_and_insert_to_bq(**context):
    hook = BigQueryHook(gcp_conn_id='google_cloud_default', use_legacy_sql=False)

    with open(RUN_RESULTS_PATH, 'r', encoding='utf-8') as f:
        data = json.load(f)

    rows = []
    now = datetime.utcnow().isoformat()

    for result in data.get("results", []):
        rows.append({
            "test_name": result.get("unique_id", ""),
            "status": result.get("status", ""),
            "error_message": result.get("message", ""),
            "execution_time": now
        })

    if not rows:
        print("⚠️ No hay resultados para insertar.")
        return

    # Crea tabla si no existe
    schema = [
        {"name": "test_name", "type": "STRING", "mode": "REQUIRED"},
        {"name": "status", "type": "STRING", "mode": "REQUIRED"},
        {"name": "error_message", "type": "STRING", "mode": "NULLABLE"},
        {"name": "execution_time", "type": "TIMESTAMP", "mode": "REQUIRED"}
    ]

    hook.insert_all(
        project_id=PROJECT_ID,
        dataset_id=DATASET_ID,
        table_id=TABLE_ID,
        rows=[{"json": row} for row in rows],
        ignore_unknown_values=True,
        fail_on_invalid_rows=False
    )

with DAG(
    dag_id="dbt_test_logs_to_bigquery",
    default_args=default_args,
    schedule_interval="@daily",
    catchup=False,
    description="Corre dbt test, lee run_results.json y sube logs a BigQuery"
) as dag:

    # Tarea 1: Ejecutar dbt test
    run_dbt_test = BashOperator(
        task_id='run_dbt_test',
        bash_command='cd /usr/app/dbt && dbt test --store-failures'
    )

    # Tarea 2: Cargar run_results a BigQuery
    insert_logs_to_bq = PythonOperator(
        task_id='insert_logs_to_bq',
        python_callable=parse_and_insert_to_bq,
        provide_context=True
    )

    run_dbt_test >> insert_logs_to_bq
