from airflow import DAG
from airflow.operators.python import PythonOperator, BranchPythonOperator
from airflow.providers.google.cloud.sensors.gcs import GCSObjectExistenceSensor
from airflow.providers.mysql.hooks.mysql import MySqlHook
from google.cloud import bigquery
from google.cloud import storage
import pandas as pd
from datetime import datetime, timedelta

# Ajustes de la DAG
default_dag_args = {
    'start_date': datetime(2023, 1, 1),  # Fecha inicial estática
    'owner': 'airflow',
    'depends_on_past': False,
    'email_on_failure': False,
    'email_on_retry': False,    
    'retries': 1,
    'retry_delay': timedelta(minutes=5),
}

# Función para obtener y almacenar valores de MySQL
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
        
        return 'importar_gcs_to_bq'  # Continuar con la tarea de importación a BigQuery
    else:
        return 'no_se_encontro_archivo'  # Eliminar el archivo de GCS sin realizar la importación

def write_to_tmp(storage_client, bucket_name, source_path, local_path):
    source_bucket = storage_client.bucket(bucket_name)
    blob = source_bucket.blob(source_path)
    blob.download_to_filename(local_path)

# Función para cargar datos de Parquet a BigQuery
def load_parquet_to_bigquery(**kwargs):
    ti = kwargs['ti']
    file = ti.xcom_pull(key='file', task_ids='obtener_parametros')
    source_bucket = ti.xcom_pull(key='source_bucket', task_ids='obtener_parametros')
    source_path = ti.xcom_pull(key='source_path', task_ids='obtener_parametros')
    project_id = ti.xcom_pull(key='project_id', task_ids='obtener_parametros')
    dataset_id = ti.xcom_pull(key='dataset_id', task_ids='obtener_parametros')
    table_id = ti.xcom_pull(key='table_id', task_ids='obtener_parametros')
    sql_script = ti.xcom_pull(key='sql_script', task_ids='obtener_parametros')
    
    local_path = '/tmp/'
    source_path_2 = f'{source_path}{file}.parquet'
    local_path_2 = f'{local_path}{file}.parquet'
    
    # Inicializar los clientes de Google Cloud Storage y BigQuery 
    storage_client = storage.Client()
    bigquery_client = bigquery.Client(project=project_id)
    
    # Descargar el file de GCS al local /tmp/ directory
    write_to_tmp(storage_client, source_bucket, source_path_2, local_path_2)  

    # Ejecutar el query para crear tabla de destino
    create_table_sql = f"{sql_script}"
    bigquery_client.query(create_table_sql).result()
    
    # Carga data del local Parquet file a un Pandas DataFrame
    df = pd.read_parquet(local_path_2, engine='pyarrow')

    # Cargar el dataframe en la tabla de BigQuery
    job_config = bigquery.LoadJobConfig(write_disposition="WRITE_APPEND")
    dataset_ref = bigquery_client.dataset(dataset_id)
    table_ref = dataset_ref.table(table_id)
    
    job = bigquery_client.load_table_from_dataframe(df, table_ref, job_config=job_config)
    job.result()  # Wait for the job to complete

    print(f'Loaded {df.shape[0]} rows into {dataset_id}:{table_id}.')

# Función dummy para imprimir mensaje
def dummy_task(**kwargs):
    print("No se encontró archivo para eliminar.")

# Definición de la DAG
with DAG(
    'dev11_dag_trigger_siniestros_gcs_bq',
    catchup=False,
    default_args=default_dag_args,
    description='Import data de siniestros de GCS a BigQuery',
    schedule_interval=timedelta(minutes=5),
) as dag:

    # Sensor para monitorear el bucket de GCS por cambios en el archivo Parquet
    detect_parquet_file = GCSObjectExistenceSensor(
        task_id='detectar_parquet_file',
        bucket='us-east4-dev-airflow-data-9879bdea-bucket',
        object='data/data-ipress/ipress_clinicas/internacional/nombre_archivo.parquet',
        google_cloud_conn_id='google_cloud_storage_default',
        timeout=60 * 60 * 24,  # 24 horas
        poke_interval=60,  # Revisar cada 60 segundos
        dag=dag,
    )

    # Tarea para obtener parámetros de la base de datos
    task_fetch_bq_params = PythonOperator(
        task_id='obtener_parametros',
        python_callable=fetch_and_store_values,
        op_kwargs={
            'table': 'DATA_FLOW_CONFIG',
            'id_config_table': 'd10532fa-3222-47c9-a5b4-faf62b842a11'
        },
    )

    # Tarea para cargar datos de Parquet a BigQuery
    task_load_parquet_to_bq = PythonOperator(
        task_id='importar_gcs_to_bq',
        python_callable=load_parquet_to_bigquery,
        provide_context=True,
    )

    # Tarea dummy para indicar que no se encontró archivo
    task_no_file_found = PythonOperator(
        task_id='no_se_encontro_archivo',
        python_callable=dummy_task,
    )

    # Definir la secuencia de tareas y dependencias condicionales
    detect_parquet_file >> task_fetch_bq_params

    task_fetch_bq_params >> task_load_parquet_to_bq
    task_fetch_bq_params >> task_no_file_found  # Tarea dummy si no se encuentra el archivo

    task_load_parquet_to_bq >> task_no_file_found  # Conectar también la tarea dummy después de la carga a BigQuery