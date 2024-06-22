from airflow import DAG
from airflow.providers.google.cloud.sensors.gcs import GCSObjectUpdateSensor
from airflow.operators.trigger_dagrun import TriggerDagRunOperator
from datetime import datetime, timedelta

default_args = {
    'start_date': datetime(2023, 1, 1),
    'owner': 'airflow',
    'depends_on_past': False,
    'email_on_failure': False,
    'email_on_retry': False,
    'retries': 1,
    'retry_delay': timedelta(minutes=5),
}

def trigger_main_dag(execution_date, **context):
    return {
        'trigger_dag_id': 'dev_dag_siniestros_parquet_GCS_BQ_Script',
        'execution_date': execution_date,
        'conf': context['dag_run'].conf
    }

with DAG(
    'monitor_gcs_file_update',
    default_args=default_args,
    description='Monitor GCS for file updates and trigger main DAG',
    schedule_interval=timedelta(minutes=60),
    catchup=False,
) as dag:

    detect_parquet_file = GCSObjectUpdateSensor(
        task_id='detect_parquet_file',
        bucket='us-east4-dev-airflow-data-9879bdea-bucket',
        object='data/data-ipress/ipress_clinicas/internacional/nombre_archivo.parquet',
        google_cloud_conn_id='google_cloud_storage_default',
        timeout=1200,  # 20 minutes
        poke_interval=60,  # Check every 60 seconds
        mode='poke',
        dag=dag,
    )

    trigger_main_dag_task = TriggerDagRunOperator(
        task_id='trigger_main_dag',
        trigger_dag_id='dev_dag_siniestros_parquet_GCS_BQ_Script',
        reset_dag_run=True,
        wait_for_completion=True,
    )

    detect_parquet_file >> trigger_main_dag_task
