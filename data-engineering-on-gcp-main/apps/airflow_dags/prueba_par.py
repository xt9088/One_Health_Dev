import datetime
import pandas as pd
from airflow import DAG
from airflow.operators.python_operator import PythonOperator
from google.cloud import storage, bigquery

# Set the default arguments for the DAG
yesterday = datetime.datetime.combine(datetime.datetime.today() - datetime.timedelta(1), datetime.datetime.min.time())

default_dag_args = {
    'start_date': yesterday,
    'owner': 'airflow',
    'depends_on_past': False,
    'email_on_failure': False,
    'email_on_retry': False,
    'retries': 1,
    'retry_delay': datetime.timedelta(minutes=5),
}

# Function to fetch and store values from BigQuery
def fetch_and_store_values(table, id_config_table, **kwargs):
    client = bigquery.Client()
    query = f"""
        SELECT * FROM {table} WHERE Id = '{id_config_table}'
    """
    query_job = client.query(query)
    result = query_job.result()
    row = [dict(row) for row in result][0]  # Fetch the first row and convert it to a dictionary
    
    # Push the parameters to XCom
    kwargs['ti'].xcom_push(key='file', value=row['File_name'])
    kwargs['ti'].xcom_push(key='source_bucket', value=row['Source_bucket'])
    kwargs['ti'].xcom_push(key='source_path', value=row['Source_path'])
    kwargs['ti'].xcom_push(key='project_id', value=row['Project_id'])
    kwargs['ti'].xcom_push(key='dataset_id', value=row['Dataset_id'])
    kwargs['ti'].xcom_push(key='table_id', value=row['Table_id'])
    kwargs['ti'].xcom_push(key='local_path', value=row['Local_path'])

def write_to_tmp(storage_client, bucket_name, source_path, local_path):
    source_bucket = storage_client.get_bucket(bucket_name)
    blob = source_bucket.blob(source_path)
    blob.download_to_filename(local_path)

def load_parquet_to_bigquery(**kwargs):
    # Retrieve parameters from XCom
    ti = kwargs['ti']
    file = ti.xcom_pull(key='file', task_ids='fetch_bq_params')
    source_bucket = ti.xcom_pull(key='source_bucket', task_ids='fetch_bq_params')
    source_path = ti.xcom_pull(key='source_path', task_ids='fetch_bq_params')
    project_id = ti.xcom_pull(key='project_id', task_ids='fetch_bq_params')
    dataset_id = ti.xcom_pull(key='dataset_id', task_ids='fetch_bq_params')
    table_id = ti.xcom_pull(key='table_id', task_ids='fetch_bq_params')
    local_path = ti.xcom_pull(key='local_path', task_ids='fetch_bq_params')
    source_path_2 = f'{source_path}{file}.parquet'
    local_path_2 = f'{local_path}{file}.parquet'

    # Initialize Google Cloud Storage and BigQuery clients
    storage_client = storage.Client()
    bigquery_client = bigquery.Client()
    
    write_to_tmp(storage_client, source_bucket, source_path_2, local_path_2)        

    # Read parquet file into a pandas DataFrame
    df = pd.read_parquet(local_path_2, engine='pyarrow')

    # Load DataFrame into BigQuery table
    job_config = bigquery.LoadJobConfig(write_disposition="WRITE_APPEND")
    dataset_ref = bigquery_client.dataset(dataset_id)
    table_ref = dataset_ref.table(table_id)
    
    job = bigquery_client.load_table_from_dataframe(df, table_ref, job_config=job_config)
    job.result()  # Wait for the job to complete

# Define the DAG
with DAG(
    'prueba_params',
    catchup=False,
    default_args=default_dag_args,
    description='Import data de siniestros de GCS a BigQuery',
    schedule_interval=datetime.timedelta(days=1),
) as dag:
    
    # Define the task to fetch parameters from BigQuery
    task_fetch_bq_params = PythonOperator(
        task_id='fetch_bq_params',
        python_callable=fetch_and_store_values,
        op_kwargs={
            'table': 'he-dev-data.dev_data_landing.iafas_rimac_m_parametros_onehealth',
            'id_config_table': 'Siniestro_One_Health'
        },
    )
    
    # Define the task to load Parquet to BigQuery
    task_load_parquet_to_bq = PythonOperator(
        task_id='importar_gcs_to_bq',
        python_callable=load_parquet_to_bigquery,
        provide_context=True,
    )

    # Set the task sequence
    task_fetch_bq_params >> task_load_parquet_to_bq
