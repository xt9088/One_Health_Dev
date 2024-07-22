from datetime import datetime, timedelta
from airflow import DAG
from airflow.operators.python_operator import PythonOperator
import pyarrow.dataset as ds

default_args = {
    'owner': 'airflow',
    'depends_on_past': False,
    'start_date': datetime(2023, 1, 1),
    'email_on_failure': False,
    'email_on_retry': False,
}

def merge_parquet_files(**kwargs):
    input_path = "gs://he-dev-data-historicos/Merge/"
    output_path = "gs://he-dev-data-historicos/MergedOutput/"

    data = ds.dataset(input_path, format="parquet")

    ds.write_dataset(
        data, 
        output_path, 
        format="parquet",
        min_rows_per_group=1000000,
        max_rows_per_file=3000000
    )

with DAG(
    'merge_parquet_files_dag_3',
    default_args=default_args,
    description='A simple DAG to merge Parquet files',
    schedule_interval=timedelta(days=1),
    catchup=False,
) as dag:

    merge_files_task = PythonOperator(
        task_id='merge_files',
        python_callable=merge_parquet_files,
        provide_context=True,
    )

    merge_files_task

