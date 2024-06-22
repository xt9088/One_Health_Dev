from google.cloud import bigquery
from airflow import DAG
from airflow.operators.python_operator import PythonOperator
from airflow.providers.mysql.hooks.mysql import MySqlHook
from datetime import datetime, timedelta

# ConfiguraciÃ³n de fechas
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
    # Crear un hook de MySQL
    mysql_hook = MySqlHook(mysql_conn_id='Cloud_SQL_db_compass')

    # Definir la consulta
    query = f"""
        SELECT * FROM {table} WHERE Id = '{id_config_table}'
    """
    # Ejecutar la consulta
    connection = mysql_hook.get_conn()
    cursor = connection.cursor()
    cursor.execute(query)
    result = cursor.fetchall()
    
    # Obtener nombres de columnas
    column_names = [desc[0] for desc in cursor.description]

    if result:
        # Convertir la primera fila a un diccionario
        result_dict = dict(zip(column_names, result[0]))

        # Empujar los parÃ¡metros a XCom
        kwargs['ti'].xcom_push(key='project_id', value=result_dict['PROJECT_ID'])
        kwargs['ti'].xcom_push(key='dataset_id_source', value=result_dict['DESTINATION_DSET_LANDING'])
        kwargs['ti'].xcom_push(key='table_id_source', value=result_dict['DESTINATION_TABLE_LANDING'])
        kwargs['ti'].xcom_push(key='dataset_id_destination', value=result_dict['DESTINATION_DSET_UNIVERSAL'])
        kwargs['ti'].xcom_push(key='table_id_destination', value=result_dict['DESTINATION_TABLE_UNIVERSAL'])
    else:
        raise ValueError("No results found for the query.")

def bigquery_to_bigquery(**kwargs):
    # Recuperar parÃ¡metros de XCom
    ti = kwargs['ti']
    project_id = ti.xcom_pull(key='project_id', task_ids='obtener_parametros')
    dataset_id_source = ti.xcom_pull(key='dataset_id_source', task_ids='obtener_parametros')
    table_id_source = ti.xcom_pull(key='table_id_source', task_ids='obtener_parametros')
    dataset_id_destination = ti.xcom_pull(key='dataset_id_destination', task_ids='obtener_parametros')
    table_id_destination = ti.xcom_pull(key='table_id_destination', task_ids='obtener_parametros')

    # Inicializar cliente de BigQuery
    client = bigquery.Client(project=project_id)

    # Definir la consulta de transferencia
    transfer_query = f"""
    INSERT INTO `{project_id}.{dataset_id_destination}.{table_id_destination}`
    SELECT * FROM `{project_id}.{dataset_id_source}.{table_id_source}`
    """

    # Ejecutar la consulta
    query_job = client.query(transfer_query)
    query_job.result()  # Esperar a que la consulta termine

    print(f"Loaded data from {dataset_id_source}.{table_id_source} to {dataset_id_destination}.{table_id_destination}")

# Definir el DAG
with DAG(
    'dev_dag_siniestros_bqlanding_buniversal',
    catchup=False,
    default_args=default_dag_args,
    description='Load data from one BigQuery table to another using parameters from MySQL',
    schedule_interval=timedelta(days=1),
) as dag:

    # Definir la tarea para obtener parÃ¡metros de MySQL
    task_fetch_bq_params = PythonOperator(
        task_id='obtener_parametros',
        python_callable=fetch_and_store_values,
        op_kwargs={
            'table': 'DATA_FLOW_CONFIG',
            'id_config_table': 'd10532fa-3222-47c9-a5b4-faf62b842a11'
        },
        provide_context=True,
    )

    # Definir la tarea para cargar datos de una tabla de BigQuery a otra
    task_load_bq_to_bq = PythonOperator(
        task_id='bigquery_to_bigquery',
        python_callable=bigquery_to_bigquery,
        provide_context=True,
    )

    # Establecer la secuencia de tareas
    task_fetch_bq_params >> task_load_bq_to_bq