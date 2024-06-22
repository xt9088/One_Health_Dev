from airflow import DAG
import pandas as pd
from airflow.operators.python_operator import PythonOperator
from airflow.providers.mysql.hooks.mysql import MySqlHook
from datetime import datetime, timedelta
from google.cloud import storage, bigquery
import gcsfs

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

    # Initialize Google Cloud Storage and BigQuery clients
    bigquery_client = bigquery.Client(project=project_id)

    # Read parquet file into a pandas DataFrame directly from GCS
    fs = gcsfs.GCSFileSystem()
    df = pd.read_parquet(f'gs://{source_bucket}/{source_path_2}', engine='pyarrow', filesystem=fs)
    
    create_table_sql = f"""
    CREATE TABLE IF NOT EXISTS `{project_id}.{dataset_id}.{table_id}` (
        id_siniestro STRING,
        id_origen STRING,
        id_siniestro_origen STRING,
        id_poliza STRING,
        id_contratante STRING,
        id_producto STRING,
        id_certificado STRING,
        id_titular STRING,
        fec_hora_ocurrencia TIMESTAMP,
        fec_notificacion TIMESTAMP,
        num_siniestro STRING,
        fec_operacion TIMESTAMP,
        id_estado_siniestro_origen STRING,
        des_estado_siniestro_origen STRING,
        id_estado_siniestro STRING,
        des_estado_siniestro STRING,
        fec_estado TIMESTAMP,
        id_ubicacion_geografica STRING,
        des_departamento_siniestro STRING,
        des_provincia_siniestro STRING,
        des_distrito_siniestro STRING,
        des_ubicacion_detallada STRING,
        id_unidad_asegurable STRING,
        id_asegurado STRING,
        des_unidad_asegurable STRING,
        sexo_asegurable STRING,
        fec_nacimiento_asegurable DATE,
        num_documento_asegurable STRING,
        tip_documento_asegurable STRING,
        rango_etareo_asegurable STRING,
        fec_constitucion_siniestro TIMESTAMP,
        fec_anulacion TIMESTAMP,
        fec_ult_liquidacion TIMESTAMP,
        mto_total_reserva NUMERIC,
        mto_total_reserva_usd NUMERIC,
        mto_total_reserva_ajustador NUMERIC,
        mto_total_reserva_usd_ajustador NUMERIC,
        mto_total_aprobado NUMERIC,
        mto_total_aprobado_usd NUMERIC,
        mto_aprobado_deduci NUMERIC,
        mto_aprobado_deduci_usd NUMERIC,
        mto_total_facturado NUMERIC,
        mto_total_facturado_usd NUMERIC,
        mto_total_pendiente NUMERIC,
        mto_total_pendiente_usd NUMERIC,
        mto_total_pagado NUMERIC,
        mto_total_pagado_usd NUMERIC,
        mto_total_pagado_ajustador NUMERIC,
        mto_total_pagado_usd_ajustador NUMERIC,
        mto_pagado_deduci NUMERIC,
        mto_pagado_deduci_usd NUMERIC,
        mto_potencial_usd NUMERIC,
        id_oficina_reclamo_origen STRING,
        id_oficina_reclamo STRING,
        des_oficina_reclamo STRING,
        id_motivo_estado_origen STRING,
        des_motivo_estado_origen STRING,
        id_motivo_estado STRING,
        des_motivo_estado STRING,
        id_moneda STRING,
        id_compania STRING,
        tipo_flujo STRING,
        porcentaje_coasegurador_rimac FLOAT64,
        mto_reserva_np FLOAT64,
        mto_reserva_usd_np FLOAT64,
        mto_aprobado_np FLOAT64,
        mto_aprobado_usd_np FLOAT64,
        mto_pagado_np FLOAT64,
        mto_pagado_usd_np FLOAT64,
        tip_atencion_siniestro STRING,
        tip_procedencia STRING,
        fec_insercion DATE,
        fec_modificacion DATE,
        bq__soft_deleted BOOL,
        partition_date TIMESTAMP
    )
    PARTITION BY TIMESTAMP_TRUNC(fec_hora_ocurrencia, MONTH);
    """

    # Create the table if it does not exist
    bigquery_client.query(create_table_sql).result()

    # Load DataFrame into BigQuery table
    job_config = bigquery.LoadJobConfig(write_disposition="WRITE_APPEND")
    dataset_ref = bigquery_client.dataset(dataset_id)
    table_ref = dataset_ref.table(table_id)
    
    job = bigquery_client.load_table_from_dataframe(df, table_ref, job_config=job_config)
    job.result()  # Wait for the job to complete

# Define the DAG
with DAG(
    'dev_dag_siniestros_parquet_gcs_bq',
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
