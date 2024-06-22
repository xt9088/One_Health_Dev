from airflow import DAG
from airflow.providers.google.cloud.operators.bigquery import BigQueryInsertJobOperator
from airflow.providers.mysql.hooks.mysql import MySqlHook
from airflow.operators.python_operator import PythonOperator
from airflow.utils.dates import days_ago

default_args = {
    'owner': 'airflow',
    'start_date': days_ago(1),
}

dag = DAG(
    'bigquery_load_with_params',
    default_args=default_args,
    description='Load and transform BigQuery table with parameters from MySQL',
    schedule_interval=None,
)

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
    file = ti.xcom_pull(key='file', task_ids='fetch_params')
    source_bucket = ti.xcom_pull(key='source_bucket', task_ids='fetch_params')
    source_path = ti.xcom_pull(key='source_path', task_ids='fetch_params')
    project_id = ti.xcom_pull(key='project_id', task_ids='fetch_params')
    dataset_id = ti.xcom_pull(key='dataset_id', task_ids='fetch_params')
    table_id = ti.xcom_pull(key='table_id', task_ids='fetch_params')
    source_path_2 = f'{source_path}{file}.parquet'
    gcs_uri = f'gs://{source_bucket}/{source_path_2}'

    # Define BigQuery load job configuration
    load_job_config = {
        "load": {
            "sourceUris": [gcs_uri],
            "destinationTable": {
                "projectId": project_id,
                "datasetId": dataset_id,
                "tableId": table_id,
            },
            "writeDisposition": "WRITE_APPEND",
            "sourceFormat": "PARQUET"
        }
    }

    # Execute BigQuery load job
    load_task = BigQueryInsertJobOperator(
        task_id='load_parquet_to_bigquery',
        configuration=load_job_config,
        dag=kwargs['dag']
    )

    load_task.execute(kwargs)

# Task to fetch parameters from MySQL
fetch_params = PythonOperator(
    task_id='fetch_params',
    python_callable=fetch_and_store_values,
    op_kwargs={
        'table': 'DATA_FLOW_CONFIG',
        'id_config_table': 'd10532fa-3222-47c9-a5b4-faf62b842a11'
    },
    provide_context=True,
    dag=dag,
)

# SQL query for the initial load and transformation
initial_load_sql = """
CREATE OR REPLACE TABLE your_project.silver.table1 AS
SELECT 
  column1,
  column2,
  -- your transformations here
FROM 
  your_project.bronze.table1
"""

# Task for initial load and transformation in BigQuery
initial_load = BigQueryInsertJobOperator(
    task_id='initial_load',
    configuration={
        "query": {
            "query": initial_load_sql,
            "useLegacySql": False,
        }
    },
    dag=dag,
)

# Task to load Parquet data to BigQuery
load_parquet = PythonOperator(
    task_id='load_parquet_to_bigquery',
    python_callable=load_parquet_to_bigquery,
    provide_context=True,
    dag=dag,
)

# Set task dependencies
fetch_params >> initial_load >> load_parquet
