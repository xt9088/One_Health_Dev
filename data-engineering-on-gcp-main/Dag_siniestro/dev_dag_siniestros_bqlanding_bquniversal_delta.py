from google.cloud import bigquery
from airflow import DAG
from airflow.operators.python_operator import PythonOperator, BranchPythonOperator
from airflow.providers.mysql.hooks.mysql import MySqlHook
from airflow.operators.dummy_operator import DummyOperator
from datetime import datetime, timedelta
import pandas as pd
import logging

# Configuración de fechas
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

        # Empujar los parámetros a XCom
        kwargs['ti'].xcom_push(key='project_id', value=result_dict['PROJECT_ID'])
        kwargs['ti'].xcom_push(key='dataset_id_source', value=result_dict['DESTINATION_DSET_LANDING'])
        kwargs['ti'].xcom_push(key='table_id_source', value=result_dict['DESTINATION_TABLE_LANDING'])
        kwargs['ti'].xcom_push(key='dataset_id_destination', value=result_dict['DESTINATION_DSET_UNIVERSAL'])
        kwargs['ti'].xcom_push(key='table_id_destination', value=result_dict['DESTINATION_TABLE_UNIVERSAL'])
    else:
        raise ValueError("No results found for the query.")

def bigquery_initial_load(**kwargs):
    # Recuperar parámetros de XCom
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

    logging.info(f"Loaded data from {dataset_id_source}.{table_id_source} to {dataset_id_destination}.{table_id_destination}")

def incremental_load(**kwargs):
    # Recuperar parámetros de XCom
    ti = kwargs['ti']
    project_id = ti.xcom_pull(key='project_id', task_ids='obtener_parametros')
    dataset_id_source = ti.xcom_pull(key='dataset_id_source', task_ids='obtener_parametros')
    table_id_source = ti.xcom_pull(key='table_id_source', task_ids='obtener_parametros')
    dataset_id_destination = ti.xcom_pull(key='dataset_id_destination', task_ids='obtener_parametros')
    table_id_destination = ti.xcom_pull(key='table_id_destination', task_ids='obtener_parametros')

    # Inicializar cliente de BigQuery
    client = bigquery.Client(project=project_id)

    # Definir la consulta para obtener los datos incrementales (ejemplo)
    incremental_query = f"""
    SELECT * FROM `{project_id}.{dataset_id_source}.{table_id_source}`
    """

    query_job = client.query(incremental_query)
    rows = query_job.result()  # Esperar a que la consulta termine

    # Convertir los resultados a un DataFrame de Pandas
    rows_list = [dict(row) for row in rows]
    df = pd.DataFrame(rows_list)

    # Definir la configuración de carga
    job_config = bigquery.LoadJobConfig(write_disposition="WRITE_APPEND")

    # Cargar los datos en la tabla de destino
    table_ref = client.dataset(dataset_id_destination).table(table_id_destination)
    load_job = client.load_table_from_dataframe(df, table_ref, job_config=job_config)
    load_job.result()  # Esperar a que la carga termine

    logging.info(f"Incrementally loaded data from {dataset_id_source}.{table_id_source} to {dataset_id_destination}.{table_id_destination}")

# Definir la lógica de ramificación si la tabla de destino no tiene registros
def choose_load_type(**kwargs):
    # Recuperar parámetros de XCom
    ti = kwargs['ti']
    project_id = ti.xcom_pull(key='project_id', task_ids='obtener_parametros')
    dataset_id_destination = ti.xcom_pull(key='dataset_id_destination', task_ids='obtener_parametros')
    table_id_destination = ti.xcom_pull(key='table_id_destination', task_ids='obtener_parametros')

    # Inicializar cliente de BigQuery
    client = bigquery.Client(project=project_id)

    # Consulta para contar el número de registros en la tabla de destino
    count_query = f"""
    SELECT COUNT(*) as total FROM `{project_id}.{dataset_id_destination}.{table_id_destination}`
    """
    query_job = client.query(count_query)
    results = query_job.result()

    # Obtener el número de registros
    total_rows = 0
    for row in results:
        total_rows = row.total

    # Imprimir mensaje de depuración
    logging.info(f"Rows in {dataset_id_destination}.{table_id_destination}: {total_rows}")

    # Decidir la tarea a realizar en función del número de registros
    if total_rows == 0:
        return 'bigquery_initial_load'
    else:
        return 'incremental_load'

# Definir el DAG
with DAG(
    'dev_dag_siniestros_bqlanding_bquniversal_delta',
    catchup=False,
    default_args=default_dag_args,
    description='Load data from one BigQuery table to another using parameters from MySQL',
    schedule_interval=timedelta(days=1),
) as dag:

    # Definir la tarea para obtener parámetros de MySQL
    task_fetch_bq_params = PythonOperator(
        task_id='obtener_parametros',
        python_callable=fetch_and_store_values,
        op_kwargs={
            'table': 'DATA_FLOW_CONFIG',
            'id_config_table': 'd10532fa-3222-47c9-a5b4-faf62b842a11'
        },
        provide_context=True,
    )

    # Definir la tarea para la carga inicial masiva
    task_initial_load = PythonOperator(
        task_id='bigquery_initial_load',
        python_callable=bigquery_initial_load,
        provide_context=True,
    )

    # Definir la tarea para la carga incremental
    task_incremental_load = PythonOperator(
        task_id='incremental_load',
        python_callable=incremental_load,
        provide_context=True,
    )

    # Definir la lógica de ramificación basada en el tipo de carga
    choose_load_type_task = BranchPythonOperator(
        task_id='choose_load_type',
        python_callable=choose_load_type,
        provide_context=True,
    )

    # Dummy tasks para finalizar el DAG correctamente
    dummy_initial_load = DummyOperator(task_id='dummy_initial_load')
    dummy_incremental_load = DummyOperator(task_id='dummy_incremental_load')

    # Establecer la secuencia de tareas
    task_fetch_bq_params >> choose_load_type_task
    choose_load_type_task >> task_initial_load >> dummy_initial_load
    choose_load_type_task >> task_incremental_load >> dummy_incremental_load
