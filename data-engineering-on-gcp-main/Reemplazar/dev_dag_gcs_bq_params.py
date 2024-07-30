from google.cloud import bigquery
from google.cloud import storage
import pyarrow.parquet as pq
from airflow import DAG
from airflow.operators.python_operator import PythonOperator
from datetime import datetime, timedelta
from config import fetch_and_store_values, load_parquet_to_bigquery, move_file

default_dag_args = {
    'owner': 'airflow',
    'depends_on_past': False,
    'email_on_failure': 'xt9088@rimac.com.pe',
    'email_on_retry': False,
    'retries': 0,
    'start_date': datetime(2023, 1, 1),
}

with DAG(
    'dag_compass_gcs_to_bq',
    catchup=False,
    default_args=default_dag_args,
    description='Import data de siniestros de GCS a BigQuery',
    schedule_interval=None,
    max_active_runs=50,
    concurrency=1
) as dag:

    task_fetch_bq_params = PythonOperator(
        task_id='obtener_parametros',
        python_callable=fetch_and_store_values,
        op_kwargs={
            'table': 'DATA_FLOW_CONFIG',
        },
        provide_context=True,
    )

    task_load_parquet_to_bq = PythonOperator(
        task_id='importar_gcs_to_bq',
        python_callable=load_parquet_to_bigquery,
        provide_context=True,
    )
    
    task_move_file = PythonOperator(
        task_id='mover_archivo',
        python_callable=move_file,
        provide_context=True,
    )

    task_fetch_bq_params >> task_load_parquet_to_bq >> task_move_file