from airflow import DAG
from airflow.operators.python import PythonOperator
from google.cloud import bigquery, storage
from datetime import datetime, timedelta
import pandas as pd
import pyarrow as pa
import pyarrow.parquet as pq
import tempfile

default_dag_args = {
    'start_date': datetime(2023, 1, 1),
    'owner': 'airflow',
    'depends_on_past': False,
    'email_on_failure': False,
    'email_on_retry': False,
    'retries': 1,
    'retry_delay': timedelta(minutes=5),
}

def extract_data_from_bigquery(project_id, dataset_id, table_id):
    client = bigquery.Client(project=project_id)
    query = f"""
    SELECT * FROM `{project_id}.{dataset_id}.{table_id}`
    LIMIT 100
    """
    query_job = client.query(query)
    results = query_job.result()
    rows = [dict(row) for row in results]

    df = pd.DataFrame(rows)
    
    temp_file = tempfile.NamedTemporaryFile(delete=False, suffix=".parquet")
    table = pa.Table.from_pandas(df)
    pq.write_table(table, temp_file.name)
    
    return temp_file.name

def upload_to_gcs(parquet_file_path, destination_bucket, destination_path, destination_file_name):
    storage_client = storage.Client()
    bucket = storage_client.bucket(destination_bucket)
    blob = bucket.blob(f"{destination_path}/{destination_file_name}.parquet")
    blob.upload_from_filename(parquet_file_path)
    print(f'Uploaded {parquet_file_path} to {destination_bucket}/{destination_path}/{destination_file_name}.parquet')

def extract_and_upload(**kwargs):
    # Configuration values
    project_id = 'he-dev-data'
    dataset_id = 'dev_data_landing'
    table_id = 'iafas_mdm_siniestro_dif'
    destination_bucket = 'he-dev-data'
    destination_path = 'he-dev-data-ipress/ipress_clinicas/internacional/'
    destination_file_name = 'data'
    
    # Extract data from BigQuery
    parquet_file_path = extract_data_from_bigquery(project_id, dataset_id, table_id)
    
    # Upload Parquet file to GCS
    upload_to_gcs(parquet_file_path, destination_bucket, destination_path, destination_file_name)

with DAG(
    'bigquery_to_parquet_gcs',
    catchup=False,
    default_args=default_dag_args,
    description='Extract data from BigQuery, convert to Parquet, and upload to GCS',
    schedule_interval=timedelta(days=1),
) as dag:

    task_extract_and_upload = PythonOperator(
        task_id='extract_and_upload',
        python_callable=extract_and_upload,
    )
