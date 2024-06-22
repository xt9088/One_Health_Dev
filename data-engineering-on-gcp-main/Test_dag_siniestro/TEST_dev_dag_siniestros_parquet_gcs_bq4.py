from airflow import DAG
import pandas as pd
from airflow.operators.python_operator import PythonOperator
from airflow.providers.mysql.hooks.mysql import MySqlHook
from datetime import datetime, timedelta
from google.cloud import storage, bigquery
import os

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

def fetch_all_values(table, **kwargs):
    # Create a MySQL hook
    mysql_hook = MySqlHook(mysql_conn_id='Cloud_SQL_db_compass')

    # Define the query
    query = f"SELECT * FROM {table}"

    # Execute the query
    connection = mysql_hook.get_conn()
    cursor = connection.cursor()
    cursor.execute(query)
    results = cursor.fetchall()
    
    # Get column names
    column_names = [desc[0] for desc in cursor.description]
    
    # Convert results to a list of dictionaries
    rows = [dict(zip(column_names, row)) for row in results]

    # Print rows to the logs for debugging
    for row in rows:
        print(f"Row fetched: {row}")
    
    # Push the list of rows to XCom
    kwargs['ti'].xcom_push(key='rows', value=rows)

def process_parquet_file(row, **kwargs):
    file = row['DESTINATION_FILE_NAME']
    source_bucket = row['DESTINATION_BUCKET']
    source_path = row['DESTINATION_DIRECTORY']
    project_id = row['PROJECT_ID']
    dataset_id = row['DESTINATION_DSET_LANDING']
    table_id = row['DESTINATION_TABLE_LANDING']
    sql_script = row['SQL_SCRIPT']
    
    local_path = '/tmp/'
    source_path_2 = f'{source_path}{file}.parquet'
    local_path_2 = f'{local_path}{file}.parquet'

    # Initialize Google Cloud Storage and BigQuery clients
    storage_client = storage.Client()
    bigquery_client = bigquery.Client(project=project_id)

    # Download the file from GCS
    print(f"Downloading file from gs://{source_bucket}/{source_path_2} to {local_path_2}")
    try:
        bucket = storage_client.bucket(source_bucket)
        blob = bucket.blob(source_path_2)
        blob.download_to_filename(local_path_2)
    except Exception as e:
        print(f"Error downloading file: {e}")
        raise

    # Check if the file exists locally
    if not os.path.exists(local_path_2):
        raise FileNotFoundError(f"File {local_path_2} not found.")

    # Read parquet file into a pandas DataFrame
    try:
        df = pd.read_parquet(local_path_2, engine='pyarrow')
        print(f"DataFrame loaded with {len(df)} rows.")
    except Exception as e:
        print(f"Error reading parquet file: {e}")
        raise
    
    # Print the SQL script for debugging
    print(f"SQL script: {sql_script}")

    # Create the table if it does not exist
    try:
        bigquery_client.query(sql_script).result()
        print(f"Table creation query executed successfully.")
    except Exception as e:
        print(f"Error executing table creation query: {e}")
        raise

    # Load DataFrame into BigQuery table
    job_config = bigquery.LoadJobConfig(write_disposition="WRITE_APPEND")
    dataset_ref = bigquery_client.dataset(dataset_id)
    table_ref = dataset_ref.table(table_id)
    
    try:
        job = bigquery_client.load_table_from_dataframe(df, table_ref, job_config=job_config)
        job.result()  # Wait for the job to complete
        print(f"Data loaded successfully into {dataset_id}.{table_id}")
    except Exception as e:
        print(f"Error loading data into BigQuery: {e}")
        raise

# Define the DAG
with DAG(
    'TEST_4.py',
    catchup=False,
    default_args=default_dag_args,
    description='Import data de siniestros de GCS a BigQuery',
    schedule_interval=timedelta(days=1),
) as dag:

    # Define the task to fetch all rows from MySQL
    task_fetch_all_values = PythonOperator(
        task_id='fetch_all_values',
        python_callable=fetch_all_values,
        op_kwargs={
            'table': 'DATA_FLOW_CONFIG',
        },
        provide_context=True,
    )

    def create_dynamic_tasks(**kwargs):
        ti = kwargs['ti']
        rows = ti.xcom_pull(key='rows', task_ids='fetch_all_values')
        
        if not rows:
            raise ValueError("No rows fetched from MySQL.")
        
        print(f"Rows fetched: {rows}")

        for row in rows:
            task_id = f"process_parquet_file_{row['ID']}"
            process_task = PythonOperator(
                task_id=task_id,
                python_callable=process_parquet_file,
                op_kwargs={'row': row},
                provide_context=True,
                dag=dag,
            )
            print(f"Task created: {task_id}")
            process_task.set_upstream(task_fetch_all_values)

    # Task to create dynamic tasks for each row
    create_tasks = PythonOperator(
        task_id='create_dynamic_tasks',
        python_callable=create_dynamic_tasks,
        provide_context=True,
    )

    # Set the task sequence
    task_fetch_all_values >> create_tasks
