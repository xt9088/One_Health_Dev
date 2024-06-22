from google.cloud import bigquery
from google.cloud import storage
from airflow import DAG
from airflow.operators.python_operator import PythonOperator
from airflow.providers.mysql.hooks.mysql import MySqlHook
from datetime import datetime, timedelta

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
    mysql_hook = MySqlHook(mysql_conn_id='Cloud_SQL_db_compass')

    # Define the query
    query = f"""
        SELECT * FROM {table} WHERE Id = '{id_config_table}'
    """
    # Execute the query
    connection = mysql_hook.get_conn()
    cursor = connection.cursor()
    cursor.execute(query)
    result = cursor.fetchall()
    
    # Get column names
    column_names = [desc[0] for desc in cursor.description]

    if result:
        # Convert the first row to a dictionary
        result_dict = dict(zip(column_names, result[0]))

        # Push the parameters to XCom
        kwargs['ti'].xcom_push(key='file', value=result_dict['DESTINATION_FILE_NAME'])
        kwargs['ti'].xcom_push(key='source_bucket', value=result_dict['DESTINATION_BUCKET'])
        kwargs['ti'].xcom_push(key='source_path', value=result_dict['DESTINATION_DIRECTORY'])
        kwargs['ti'].xcom_push(key='project_id', value=result_dict['PROJECT_ID'])
        kwargs['ti'].xcom_push(key='dataset_id', value=result_dict['DESTINATION_DSET_LANDING'])
        kwargs['ti'].xcom_push(key='table_id', value=result_dict['DESTINATION_TABLE_LANDING'])
    else:
        raise ValueError("No results found for the query.")

def load_parquet_to_bigquery(**kwargs):
    # Retrieve parameters from XCom
    ti = kwargs['ti']
    file = ti.xcom_pull(key='file', task_ids='obtener_parametros')
    source_bucket = ti.xcom_pull(key='source_bucket', task_ids='obtener_parametros')
    source_path = ti.xcom_pull(key='source_path', task_ids='obtener_parametros')
    project_id = ti.xcom_pull(key='project_id', task_ids='obtener_parametros')
    dataset_id = ti.xcom_pull(key='dataset_id', task_ids='obtener_parametros')
    table_id = ti.xcom_pull(key='table_id', task_ids='obtener_parametros')
    source_path_2 = f'{source_path}{file}.parquet'
    gcs_uri = f'gs://{source_bucket}/{source_path_2}'

    # Initialize BigQuery client
    client = bigquery.Client(project=project_id)

    # Define the job configuration
    job_config = bigquery.LoadJobConfig(
        source_format=bigquery.SourceFormat.PARQUET,
        write_disposition=bigquery.WriteDisposition.WRITE_APPEND,
    )

    # Define the table reference
    table_ref = client.dataset(dataset_id).table(table_id)

    # Create a load job
    load_job = client.load_table_from_uri(
        gcs_uri,
        table_ref,
        job_config=job_config
    )

    # Wait for the job to complete
    load_job.result()

    print(f'Loaded {load_job.output_rows} rows into {dataset_id}:{table_id}.')

# Define the DAG
with DAG(
    'dev_dag_siniestros_parquet_GCS_BQ',
    catchup=False,
    default_args=default_dag_args,
    description='Import data de siniestros de GCS a BigQuery',
    schedule_interval=timedelta(days=1),
) as dag:

    # Define the task to fetch parameters from MySQL
    task_fetch_bq_params = PythonOperator(
        task_id='obtener_parametros',
        python_callable=fetch_and_store_values,
        op_kwargs={
            'table': 'DATA_FLOW_CONFIG',
            'id_config_table': 'd10532fa-3222-47c9-a5b4-faf62b842a11'
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