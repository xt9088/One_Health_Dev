import time
import datetime
import requests, json, pytz, io, os
import pandas as pd
from airflow import DAG
from airflow.models.baseoperator import chain
from airflow.providers.google.cloud.transfers.gcs_to_bigquery import GCSToBigQueryOperator
from airflow.contrib.operators.bigquery_operator import BigQueryOperator
from airflow.operators.python_operator import PythonOperator
from airflow.models import Connection
from airflow.settings import Session
from google.cloud import storage
from google.cloud import bigquery
from os import remove

yesterday = datetime.datetime.combine(datetime.datetime.today() - datetime.timedelta(1), datetime.datetime.min.time())

default_dag_args = {
        'start_date': yesterday,    
        'owner': 'airflow',
    'depends_on_past': False,
    'email_on_failure': False,
    'email_on_retry': False,
    'retries': 1,
    'retry_delay': datetime.timedelta(minutes=5),
}

def fetch_and_store_values(table, id_config_table):
    client = bigquery.Client()
    query = f"""
        SELECT * FROM {table} WHERE Id = '{id_config_table}'
    """
    query_job = client.query(query)
    result = query_job.result()
    row = [dict(row) for row in result][0]  # Fetch the first row and convert it to a dictionary
    
    file = row['File_name']
    source_bucket = row['Source_bucket']
    source_path = row['Source_path']
    project_id = row['Project_id']
    dataset_id = row['Dataset_id']
    table_id = row['Table_id']
    local_path = row['Local_path']
    source_path_2 = f'{source_path}{file}.parquet' #Ruta acceso al archivo bucket - origen: "he-dev-data-ipress/ipress_clinicas/internacional/data.parquet" 
    local_path_2 = f'{local_path}{file}.parquet' #Ruta Local Copia archivo Parquet -
    return ()

def write_to_tmp(storage_client, bucket_name, source_path, local_path):
    source_bucket = storage_client.get_bucket(bucket_name)
    blob = source_bucket.blob(source_path)
    blob.download_to_filename(local_path)


def load_parquet_to_bigquery():

    # Initialize Google Cloud Storage and BigQuery clients
    storage_client = storage.Client()
    bigquery_client = bigquery.Client()
    
    write_to_tmp(storage_client, source_bucket, source_path_2, local_path_2)        

    # Read parquet file into a pandas DataFrame
    df = pd.read_parquet(local_path_2, engine='pyarrow')

    # Load DataFrame into BigQuery table
    job_config = bigquery.LoadJobConfig(
        write_disposition="WRITE_APPEND",  # You can choose write disposition based on your requirements
    )

    dataset_ref = bigquery_client.dataset(dataset_id)
    table_ref = dataset_ref.table(table_id)
    
    # Load data into BigQuery table
    job = bigquery_client.load_table_from_dataframe(
        df, table_ref, job_config=job_config
    )
    job.result()  # Wait for the job to complete


with DAG(
    'dev-data-siniestro-storage-to-bigquery',
    catchup=False,    
    default_args=default_dag_args,
    description='Import data de siniestros de GCS a BigQuery'
    #schedule_interval=timedelta(days=1)
    ) as dag:
    
    #######################
    ### FLUJO DE TAREAS ###
    #######################

    task_dict = dict()
    op_list = []

    task_id_gcs_to_bq = 'importar_gcs_to_bq'
    task_id_param = 'bq_parametros'
    
    task_dict[task_id_param] = PythonOperator(
        task_id=task_id_param,
        python_callable=fetch_and_store_values,
        #op_args=[data, codigo_corredor_ft],
        provide_context=True, 
        dag=dag,
    )  
    
    task_dict[task_id_gcs_to_bq] = PythonOperator(
        task_id=task_id_gcs_to_bq,
        python_callable=load_parquet_to_bigquery,
        #op_args=[data, codigo_corredor_ft],
        provide_context=True, 
        dag=dag,
    )                                          

    task_dict[f"{task_id_gcs_to_bq}"]            
    op_list.append(task_dict[f"{task_id_gcs_to_bq}"])

    chain(*op_list)

    op_list[-1]