from google.cloud import bigquery
from google.auth import impersonated_credentials
import google.auth
from google.cloud import storage
import pyarrow.parquet as pq
from airflow.providers.mysql.hooks.mysql import MySqlHook
from datetime import datetime
import config_dag
from google.auth.transport.requests import Request
import re
from airflow import DAG
from airflow.operators.python_operator import PythonOperator
from airflow.providers.google.cloud.transfers.gcs_to_gcs import GCSToGCSOperator
from airflow.providers.google.cloud.transfers.gcs_to_bigquery import GCSToBigQueryOperator

def fetch_and_store_values(table=config_dag.TABLE, **kwargs):
    dag_run_conf = kwargs['dag_run'].conf
    ruta_completa = dag_run_conf.get('id')
    print(f"Fetch and Store Values: conf(id) del evento - {ruta_completa}")
    
    indice_slash_final = ruta_completa.rfind('/')
    ruta = ruta_completa[:indice_slash_final]
    print(f"Fetch and Store Values: Ruta completa del evento - {ruta}")
    
    file_name = ruta
    pre_file = file_name.split('/')[-1]
    file = pre_file.split('_')[-2]
    indice_slash_final_file = file_name.rfind('/')
    prefix_file = file_name[:indice_slash_final_file]
    file_path = prefix_file + '/' + file + '.parquet'
    print(f"Fetch and Store Values: Ruta a identificar en tabla parametros (la estructura debe ser mdm_NOMBRE_fecha)- {file_path}")
    
    mysql_hook = MySqlHook(mysql_conn_id=config_dag.MYSQL_CONN_ID)

    query = f"""
        SELECT * FROM {table} 
        WHERE CONCAT(DESTINATION_BUCKET, '/', DESTINATION_DIRECTORY, DESTINATION_FILE_NAME, ORIGIN_EXTENSION) = '{file_path}'
    """
    connection = mysql_hook.get_conn()
    cursor = connection.cursor()
    cursor.execute(query)
    result = cursor.fetchall()
    
    print(f"Fetch and Store Values: Resultado del Query ejecutado (Fila a Procesar por DAG) - {result}")
    
    column_names = [desc[0] for desc in cursor.description]

    if result:
        result_dict = dict(zip(column_names, result[0]))

        # Pushing values to XCom
        ti = kwargs['ti']
        ti.xcom_push(key='file', value=result_dict['DESTINATION_FILE_NAME'])
        ti.xcom_push(key='source_bucket', value=result_dict['DESTINATION_BUCKET'])
        ti.xcom_push(key='source_path', value=result_dict['DESTINATION_DIRECTORY'])
        ti.xcom_push(key='project_id', value=result_dict['PROJECT_ID'])
        ti.xcom_push(key='dataset_id', value=result_dict['DESTINATION_DSET_LANDING'])
        ti.xcom_push(key='table_id', value=result_dict['DESTINATION_TABLE_LANDING'])
        ti.xcom_push(key='sql_script', value=result_dict['SQL_SCRIPT'])
        ti.xcom_push(key='archive_path', value=result_dict['ARCHIVE_DIRECTORY'])
        ti.xcom_push(key='bucket_historico', value=result_dict['DESTINATION_BUCKET_HIST'])
        ti.xcom_push(key='file_path_raw', value=ruta)
        
        source_blob_name = f'{ruta[ruta.find("/") + 1:]}'
        today_date = datetime.now().strftime('%Y%m%d')
        destination_blob_name = f'{result_dict["ARCHIVE_DIRECTORY"]}{file}_{today_date}.parquet'

        ti.xcom_push(key='source_blob_name', value=source_blob_name)
        ti.xcom_push(key='destination_blob_name', value=destination_blob_name)
        
        print(f"source_bucket: {result_dict['DESTINATION_BUCKET']}")
        print(f"bucket_historico: {result_dict['DESTINATION_BUCKET_HIST']}")
        print(f"source_blob_name: {source_blob_name}")
        print(f"destination_blob_name: {destination_blob_name}")
        print(f"project_id: {result_dict['PROJECT_ID']}")
        
    else:
        raise ValueError("Fetch and Store Values: No hubo resultado del query.")

def fetch_bq_table_schema(client, dataset_id, table_id):
    table_ref = client.dataset(dataset_id).table(table_id)
    table = client.get_table(table_ref)
    return {schema_field.name: schema_field.field_type for schema_field in table.schema}

def fetch_parquet_schema(gcs_uri,project_id,impersonation_chain=None):
    
    credentials,project_id= google.auth.default()
    target_credentials = impersonated_credentials.Credentials(
    source_credentials=credentials,
    target_principal=impersonation_chain,
    target_scopes=['https://www.googleapis.com/auth/cloud-platform']
    )
    target_credentials.refresh(Request())

    storage_client = storage.Client(credentials=target_credentials)
    #storage_client = storage.Client()
    
    bucket_name, file_path = gcs_uri.replace('gs://', '').split('/', 1)
    bucket = storage_client.bucket(bucket_name)
    blob = bucket.blob(file_path)
    with blob.open('rb') as f:
        parquet_file = pq.ParquetFile(f)
        schema = {field.name: str(field.type) for field in parquet_file.schema_arrow}
        print(f"Fetched Parquet schema: {schema}")
        return schema

def parquet_type_to_bq_type(parquet_type):
    if 'string' in parquet_type:
        return 'STRING'
    elif 'int32' in parquet_type or 'int64' in parquet_type:
        return 'INTEGER'
    elif 'bool' in parquet_type:
        return 'BOOLEAN'
    elif 'float' in parquet_type or 'double' in parquet_type:
        return 'FLOAT'
    elif re.search(r'decimal.*', parquet_type):
        return 'NUMERIC'
    elif 'timestamp' in parquet_type:
        return 'TIMESTAMP'
    elif 'date' in parquet_type:
        return 'DATE'
    elif 'time' in parquet_type:
        return 'TIME'
    elif re.search(r'list\[struct\[2\]\]', parquet_type):
        return "STRUCT<list ARRAY<STRUCT<element STRUCT<especialidad STRING, fec_acreditacion STRING>>>>"
    else:
        return 'STRING'  # Default type

def add_missing_columns_to_bq(client, dataset_id, table_id, missing_columns):
    table_ref = client.dataset(dataset_id).table(table_id)
    table = client.get_table(table_ref)
    
    new_schema = table.schema[:]
    for column, column_type in missing_columns.items():
        bq_type = parquet_type_to_bq_type(column_type)
        new_schema.append(bigquery.SchemaField(name=column, field_type=bq_type))
    
    table.schema = new_schema
    client.update_table(table, ['schema'])
    print(f"Added missing columns: {missing_columns} to {dataset_id}.{table_id}")

def validate_schemas(project_id, dataset_id, table_id, gcs_uri, impersonation_chain,sql_script):
    # Obtain the default credentials
    credentials, _ = google.auth.default()
    
    # Set up the target service account to impersonate
    target_sa = impersonation_chain
    
    # Create impersonated credentials   
    target_credentials = impersonated_credentials.Credentials(
        source_credentials=credentials,
        target_principal=target_sa,
        target_scopes=['https://www.googleapis.com/auth/cloud-platform']
    )   
    
    create_table_sql = f"{sql_script}"
    
    # Create a BigQuery client using the impersonated credentials
    client = bigquery.Client(credentials=target_credentials, project=project_id)
    
    client.query(create_table_sql).result()
    
    print(f"Tabla Creada usando query : {create_table_sql}")
    
    # Fetch schemas
    bq_schema = fetch_bq_table_schema(client, dataset_id, table_id)
    print(f"Fetched BQ schema: {bq_schema}")
    parquet_schema = fetch_parquet_schema(gcs_uri,project_id,impersonation_chain=impersonation_chain)
    
    # Identify schema differences
    missing_columns = {column: parquet_schema[column] for column in parquet_schema if column not in bq_schema}
    conflicting_columns = {column: parquet_schema[column] for column in parquet_schema if column in bq_schema and parquet_type_to_bq_type(parquet_schema[column]) != bq_schema[column]}
    
    if conflicting_columns:
        print(f"Schema conflicts found: {conflicting_columns}")
    if missing_columns:
        add_missing_columns_to_bq(client, dataset_id, table_id, missing_columns)
        print(f"Schema updated with missing columns: {missing_columns}")
    #if useless_columns:
    #remove_useless_columns_to_bq(project_id, dataset_id, table_id, useless_columns)
    #print(f"Schema removed the useless columns: {useless_columns}")
    else:
        print("No schema mismatches found.")

# The function `remove_useless_columns_to_bq` is commented out, but you can uncomment and implement it if needed.
# def remove_useless_columns_to_bq(project_id, dataset_id, table_id, useless_columns):
#     client = bigquery.Client(project=project_id)
#     table_ref = client.dataset(dataset_id).table(table_id)
#     table = client.get_table(table_ref)   
#     for column, column_type in useless_columns.items():
#         incremental_query = f"""
#         ALTER TABLE `{project_id}.{dataset_id}.{table_id}`
#         DROP COLUMN {column};
#         """
#         query_job = client.query(incremental_query)
#         query_job.result()


default_dag_args = {
    'owner': 'airflow',
    'depends_on_past': False,
    'email_on_failure': config_dag.Email,
    'email_on_retry': False,
    'retries': 0,
    'start_date': datetime(2023, 1, 1),
    'project_id': config_dag.PROJECT_ID
}

with DAG(
    'dag_compass_gcs_to_bq',
    catchup=False,
    default_args=default_dag_args,
    description='Import data de siniestros de GCS a BigQuery',
    schedule_interval=None,
    max_active_runs=50,
    concurrency=1
) as dag:

    task_fetch_bq_params = PythonOperator(
        task_id='obtener_parametros',
        python_callable=fetch_and_store_values,
        op_kwargs={
        #    'table': 'DATA_FLOW_CONFIG',
        'impersonation_chain': config_dag.SA  
        },
        provide_context=True,
    )

    task_validate_schema = PythonOperator(
        task_id='validar_schema',
        python_callable=validate_schemas,
        op_kwargs={
            'project_id': "{{ ti.xcom_pull(task_ids='obtener_parametros', key='project_id') }}",
            'dataset_id': "{{ ti.xcom_pull(task_ids='obtener_parametros', key='dataset_id') }}",
            'table_id': "{{ ti.xcom_pull(task_ids='obtener_parametros', key='table_id') }}",
            'sql_script': "{{ ti.xcom_pull(task_ids='obtener_parametros', key='sql_script') }}",
            'gcs_uri': "{{ 'gs://' + ti.xcom_pull(task_ids='obtener_parametros', key='file_path_raw') }}",
            'impersonation_chain': config_dag.SA,            
        },
        provide_context=True,
    )

    task_load_parquet_to_bq = GCSToBigQueryOperator(
        task_id='importar_gcs_to_bq',
        bucket="{{ ti.xcom_pull(task_ids='obtener_parametros', key='source_bucket') }}",
        source_objects=[
            "{{ ti.xcom_pull(task_ids='obtener_parametros', key='source_blob_name') }}"
        ],
        destination_project_dataset_table="{{ ti.xcom_pull(task_ids='obtener_parametros', key='project_id') }}.{{ ti.xcom_pull(task_ids='obtener_parametros', key='dataset_id') }}.{{ ti.xcom_pull(task_ids='obtener_parametros', key='table_id') }}",
        source_format='PARQUET',
        write_disposition='WRITE_APPEND',
        schema_update_options=['ALLOW_FIELD_ADDITION'],
        create_disposition='CREATE_IF_NEEDED',
        gcp_conn_id='bigquery_default',
        #project_id=config_dag.PROJECT_ID,
        impersonation_chain=config_dag.SA, 
    )

    task_move_file = GCSToGCSOperator(
        task_id='move_file',
        source_bucket="{{ ti.xcom_pull(task_ids='obtener_parametros', key='source_bucket') }}",
        source_object="{{ ti.xcom_pull(task_ids='obtener_parametros', key='source_blob_name') }}",
        destination_bucket="{{ ti.xcom_pull(task_ids='obtener_parametros', key='bucket_historico') }}",
        destination_object="{{ ti.xcom_pull(task_ids='obtener_parametros', key='destination_blob_name') }}",
        move_object=True,
        gcp_conn_id='google_cloud_default',
        impersonation_chain=config_dag.SA, 
    )   
    
    task_fetch_bq_params >> task_validate_schema >> task_load_parquet_to_bq >> task_move_file
