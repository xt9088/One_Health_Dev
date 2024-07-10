from google.cloud import bigquery
from google.cloud import storage
import pyarrow.parquet as pq
from airflow.providers.mysql.hooks.mysql import MySqlHook
from datetime import datetime
import re

def fetch_and_store_values(table, **kwargs):
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
    file_path = prefix_file+'/'+file+'.parquet'
    print(f"Fetch and Store Values: Ruta a identificar en tabla parametros (la estructura debe ser mdm_NOMBRE_fecha)- {file_path}")
    
    mysql_hook = MySqlHook(mysql_conn_id='Cloud_SQL_db_compass')

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

        kwargs['ti'].xcom_push(key='file', value=result_dict['DESTINATION_FILE_NAME'])
        kwargs['ti'].xcom_push(key='source_bucket', value=result_dict['DESTINATION_BUCKET'])
        kwargs['ti'].xcom_push(key='source_path', value=result_dict['DESTINATION_DIRECTORY'])
        kwargs['ti'].xcom_push(key='project_id', value=result_dict['PROJECT_ID'])
        kwargs['ti'].xcom_push(key='dataset_id', value=result_dict['DESTINATION_DSET_LANDING'])
        kwargs['ti'].xcom_push(key='table_id', value=result_dict['DESTINATION_TABLE_LANDING'])
        kwargs['ti'].xcom_push(key='sql_script', value=result_dict['SQL_SCRIPT'])
        kwargs['ti'].xcom_push(key='archive_path', value=result_dict['ARCHIVE_DIRECTORY'])
        kwargs['ti'].xcom_push(key='bucket_historico', value=result_dict['DESTINATION_BUCKET_HIST'])
        kwargs['ti'].xcom_push(key='file_path_raw', value=ruta)
    else:
        raise ValueError("Fetch and Store Values: No hubo resultado del query.")

def fetch_bq_table_schema(project_id, dataset_id, table_id):
    client = bigquery.Client(project=project_id)
    table_ref = client.dataset(dataset_id).table(table_id)
    table = client.get_table(table_ref)
    return {schema_field.name: schema_field.field_type for schema_field in table.schema}

def fetch_parquet_schema(gcs_uri):
    storage_client = storage.Client()
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
    elif 'int32' in parquet_type:
        return 'INTEGER'
    elif 'int64' in parquet_type:
        return 'INTEGER'
    elif 'bool' in parquet_type:
        return 'BOOLEAN'
    elif 'float' in parquet_type:
        return 'FLOAT'
    elif 'double' in parquet_type:
        return 'FLOAT'
    elif re.search(r'decimal.*', parquet_type):
        return 'NUMERIC'
    elif 'timestamp' in parquet_type:
        return 'TIMESTAMP'
    elif 'date' in parquet_type:
        return 'DATE'
    elif 'time' in parquet_type:
        return 'TIME'
    else:
        return 'STRING'  # Default type

def add_missing_columns_to_bq(project_id, dataset_id, table_id, missing_columns):
    client = bigquery.Client(project=project_id)
    table_ref = client.dataset(dataset_id).table(table_id)
    table = client.get_table(table_ref)
    
    new_schema = table.schema[:]
    for column, column_type in missing_columns.items():
        bq_type = parquet_type_to_bq_type(column_type)
        new_schema.append(bigquery.SchemaField(name=column, field_type=bq_type))
    
    table.schema = new_schema
    client.update_table(table, ['schema'])
    print(f"Added missing columns: {missing_columns} to {dataset_id}.{table_id}")

def remove_useless_columns_to_bq(project_id, dataset_id, table_id, useless_columns): 
    client = bigquery.Client(project=project_id)
    table_ref = client.dataset(dataset_id).table(table_id)
    table = client.get_table(table_ref)   
    for column, column_type in useless_columns.items():
        incremental_query = f"""
        Alter table `{project_id}.{dataset_id}.{table_id}`
        drop column {column};
        """
        query_job = client.query(incremental_query)
        query_job.result() 
	

def validate_schemas(project_id, dataset_id, table_id, gcs_uri):
    bq_schema = fetch_bq_table_schema(project_id, dataset_id, table_id)
    print(f"Fetched BQ schema: {bq_schema}")
    parquet_schema = fetch_parquet_schema(gcs_uri)
    
    missing_columns = {column: parquet_schema[column] for column in parquet_schema if column not in bq_schema}
    useless_columns = {column: bq_schema[column] for column in bq_schema if column not in parquet_schema}
    conflicting_columns = {column: parquet_schema[column] for column in parquet_schema if column in bq_schema and parquet_type_to_bq_type(parquet_schema[column]) != bq_schema[column]}
    
    if conflicting_columns:
        #raise ValueError(f"Schema conflicts found: {conflicting_columns}")
        print(f"Schema conflicts found: {conflicting_columns}")
    if missing_columns:
        add_missing_columns_to_bq(project_id, dataset_id, table_id, missing_columns)
        print(f"Schema updated with missing columns: {missing_columns}")
    if useless_columns:
        remove_useless_columns_to_bq(project_id, dataset_id, table_id, useless_columns)
        print(f"Schema removed the useless columns: {useless_columns}")
    else:
        print("No schema mismatches found.")

def load_parquet_to_bigquery(**kwargs):
    ti = kwargs['ti']
    file = ti.xcom_pull(key='file', task_ids='obtener_parametros')
    project_id = ti.xcom_pull(key='project_id', task_ids='obtener_parametros')
    dataset_id = ti.xcom_pull(key='dataset_id', task_ids='obtener_parametros')
    table_id = ti.xcom_pull(key='table_id', task_ids='obtener_parametros')
    sql_script = ti.xcom_pull(key='sql_script', task_ids='obtener_parametros')
    file_path_raw = ti.xcom_pull(key='file_path_raw', task_ids='obtener_parametros')
    
    gcs_uri = f'gs://{file_path_raw}'

    print(f"Carga de Parquet a BigQuery: Inicio de carga para ruta evento {gcs_uri} en tabla {project_id}.{dataset_id}.{table_id}")

    client = bigquery.Client(project=project_id)
    client.query(sql_script).result()
    
    # Validate and update schemas if needed
    validate_schemas(project_id, dataset_id, table_id, gcs_uri)

    job_config = bigquery.LoadJobConfig(
        source_format=bigquery.SourceFormat.PARQUET,
        write_disposition=bigquery.WriteDisposition.WRITE_APPEND,
        schema_update_options=[bigquery.SchemaUpdateOption.ALLOW_FIELD_ADDITION],
    )

    table_ref = client.dataset(dataset_id).table(table_id)

    load_job = client.load_table_from_uri(
        gcs_uri,
        table_ref,
        job_config=job_config
    )

    load_job.result()

    print(f"Carga de Parquet to BigQuery: Ingestado {load_job.output_rows} filas a {dataset_id}.{table_id}")

def move_file(**kwargs):
    from google.cloud import storage
    from datetime import datetime

    ti = kwargs['ti']
    file = ti.xcom_pull(key='file', task_ids='obtener_parametros')
    source_bucket = ti.xcom_pull(key='source_bucket', task_ids='obtener_parametros')
    file_path_raw = ti.xcom_pull(key='file_path_raw', task_ids='obtener_parametros')
    archive_path = ti.xcom_pull(key='archive_path', task_ids='obtener_parametros')
    historicos_bucket = ti.xcom_pull(key='bucket_historico', task_ids='obtener_parametros')
    
    today_date = datetime.now().strftime('%Y%m%d')

    source_blob_name = f'{file_path_raw[file_path_raw.find("/") + 1:]}'
    
    destination_blob_name = f'{archive_path}{file}_{today_date}.parquet'

    print(f"Mover File: Tratando de mover file {source_blob_name} a {destination_blob_name} en bucket {historicos_bucket}")

    storage_client = storage.Client()
    
    source_bucket_obj = storage_client.bucket(source_bucket)
    destination_bucket_obj = storage_client.bucket(historicos_bucket)
    
    source_blob = source_bucket_obj.blob(source_blob_name)
    
    new_blob = source_bucket_obj.copy_blob(source_blob, destination_bucket_obj, destination_blob_name)
    
    print(f"Mover File: File {source_blob_name} copiado a {historicos_bucket}/{destination_blob_name}")
    
    source_blob.delete()
    
    print(f"Mover File: File {source_blob_name} borrado de {source_bucket}")
