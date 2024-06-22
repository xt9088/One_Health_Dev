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
    mysql_hook = MySqlHook(mysql_conn_id='Cloud_SQL_db_compass')

    query = f"""
        SELECT * FROM {table} WHERE Id = '{id_config_table}'
    """
    connection = mysql_hook.get_conn()
    cursor = connection.cursor()
    cursor.execute(query)
    result = cursor.fetchall()
    
    column_names = [desc[0] for desc in cursor.description]

    if result:
        result_dict = dict(zip(column_names, result[0]))

        kwargs['ti'].xcom_push(key='file', value=result_dict['DESTINATION_FILE_NAME'])
        kwargs['ti'].xcom_push(key='source_bucket', value=result_dict['DESTINATION_BUCKET'])
        kwargs['ti'].xcom_push(key='source_path', value=result_dict['DESTINATION_DIRECTORY'])
        kwargs['ti'].xcom_push(key='project_id', value=result_dict['PROJECT_ID'])
        kwargs['ti'].xcom_push(key='dataset_id', value=result_dict['DESTINATION_DSET_LANDING'])
        kwargs['ti'].xcom_push(key='table_id', value=result_dict['DESTINATION_TABLE_LANDING'])
        kwargs['ti'].xcom_push(key='sql_script', value=result_dict['SQL_SCRIPT'])
    else:
        raise ValueError("No results found for the query.")

def load_parquet_to_bigquery(**kwargs):
    ti = kwargs['ti']
    file = ti.xcom_pull(key='file', task_ids='obtener_parametros')
    source_bucket = ti.xcom_pull(key='source_bucket', task_ids='obtener_parametros')
    source_path = ti.xcom_pull(key='source_path', task_ids='obtener_parametros')
    project_id = ti.xcom_pull(key='project_id', task_ids='obtener_parametros')
    dataset_id = ti.xcom_pull(key='dataset_id', task_ids='obtener_parametros')
    table_id = ti.xcom_pull(key='table_id', task_ids='obtener_parametros')
    sql_script = ti.xcom_pull(key='sql_script', task_ids='obtener_parametros')
    
    source_path_2 = f'{source_path}{file}.parquet'
    gcs_uri = f'gs://{source_bucket}/{source_path_2}'
    
    create_table_sql = f"{sql_script}"

    client = bigquery.Client(project=project_id)
    
    # Execute the create table SQL
    client.query(create_table_sql).result()

    job_config = bigquery.LoadJobConfig(
        source_format=bigquery.SourceFormat.PARQUET,
        write_disposition=bigquery.WriteDisposition.WRITE_APPEND,
    )

    table_ref = client.dataset(dataset_id).table(table_id)

    load_job = client.load_table_from_uri(
        gcs_uri,
        table_ref,
        job_config=job_config
    )

    load_job.result()

    print(f'Loaded {load_job.output_rows} rows into {dataset_id}:{table_id}.')

with DAG(
    'dev_dag_siniestros_parquet_GCS_BQ_Script2',
    catchup=False,
    default_args=default_dag_args,
    description='Import data de siniestros de GCS a BigQuery',
    schedule_interval=timedelta(days=1),
) as dag:

    task_fetch_bq_params = PythonOperator(
        task_id='obtener_parametros',
        python_callable=fetch_and_store_values,
        op_kwargs={
            'table': 'DATA_FLOW_CONFIG',
            'id_config_table': 'd10532fa-3222-47c9-a5b4-faf62b842a11'
        },
        provide_context=True,
    )

    task_load_parquet_to_bq = PythonOperator(
        task_id='importar_gcs_to_bq',
        python_callable=load_parquet_to_bigquery,
        provide_context=True,
    )

    task_fetch_bq_params >> task_load_parquet_to_bq
