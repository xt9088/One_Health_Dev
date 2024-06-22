from airflow import DAG
import pandas as pd
from airflow.operators.python_operator import PythonOperator
from airflow.providers.mysql.hooks.mysql import MySqlHook
from datetime import datetime, timedelta
from google.cloud import storage, bigquery

yesterday = datetime.combine(datetime.today() - timedelta(1), datetime.min.time())

default_dag_args = {
    'start_date': yesterday,
    'owner': 'airflow',
    'depends_on_past': False,
    'email_on_failure': False,
    'email_on_retry': False,
    'retries': 1,
    'retry_delay': timedelta(minutes=5),
}

def fetch_and_store_values(table, id_config_table, **kwargs):
    # Create a MySQL hook
    mysql_hook = MySqlHook(mysql_conn_id='Cloud_sql_MySQL')

    # Define the query
    query = f"""
        SELECT * FROM {table} WHERE ID = '{id_config_table}'
    """
    # Execute the query
    connection = mysql_hook.get_conn()
    cursor = connection.cursor(dictionary=True)
    cursor.execute(query)
    result = cursor.fetchone()  # Fetch the first row as a dictionary
    
    # Push the parameters to XCom
    kwargs['ti'].xcom_push(key='DESTINATION_FILE_NAME', value=result['File_name'])
    kwargs['ti'].xcom_push(key='DESTINATION_BUCKET', value=result['Source_bucket'])
    kwargs['ti'].xcom_push(key='DESTINATION_DIRECTORY', value=result['Source_path'])
    kwargs['ti'].xcom_push(key='PROJECT_ID', value=result['Project_id'])
    kwargs['ti'].xcom_push(key='DESTINATION_DSET_LANDING', value=result['Dataset_id'])
    kwargs['ti'].xcom_push(key='DESTINATION_TABLE_LANDING', value=result['Table_id'])
    kwargs['ti'].xcom_push(key='local_path', value=result['Local_path'])

def write_to_tmp(storage_client, bucket_name, source_path, local_path):
    source_bucket = storage_client.bucket(bucket_name)
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
    bigquery_client = bigquery.Client(project=project_id)
    
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
    schedule_interval=timedelta(days=1),
) as dag:
    
    # Define the task to fetch parameters from MySQL
    task_fetch_bq_params = PythonOperator(
        task_id='fetch_bq_params',
        python_callable=fetch_and_store_values,
        op_kwargs={
            'table': 'DATA_FLOW_CONFIG',
            'id_config_table': '5050dccc-d92a-42b9-9fb2-fa9b69505bab'
        },
        provide_context=True,
    )
    
    # Define the task to load Parquet to BigQuery
    task_load_parquet_to_bq = PythonOperator(
        task_id='importar_gcs_to_bq',
        python_callable=load_parquet_to_bigquery,
        provide_context=True,
    )

    # Set the task sequence
    task_fetch_bq_params >> task_load_parquet_to_bq
