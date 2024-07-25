from google.cloud import bigquery
from google.cloud import storage
import pyarrow.parquet as pq
from airflow.providers.mysql.hooks.mysql import MySqlHook
from datetime import datetime
import re

def fetch_and_store_values(table,**kwargs):
    dag_run_conf = kwargs['dag_run'].conf
    ruta_completa = dag_run_conf.get('id')
    
    indice_slash_final = ruta_completa.rfind('/')
    ruta = ruta_completa[:indice_slash_final]
    
    file_name = ruta
    pre_file = file_name.split('/')[-1]
    file = pre_file.split('_')[-2]
    indice_slash_final_file = file_name.rfind('/')
    prefix_file = file_name[:indice_slash_final_file]
    file_path = prefix_file+'/'+file+'.parquet'
    
        
    mysql_hook = MySqlHook(mysql_conn_id='Cloud_SQL_db_compass')

    query = f"""
        SELECT * FROM {table} 
        WHERE CONCAT(DESTINATION_BUCKET, '/', DESTINATION_DIRECTORY, DESTINATION_FILE_NAME, ORIGIN_EXTENSION) = '{file_path}'
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
    #if useless_columns:
        #remove_useless_columns_to_bq(project_id, dataset_id, table_id, useless_columns)
        #print(f"Schema removed the useless columns: {useless_columns}")
    else:
        print("No schema mismatches found.")