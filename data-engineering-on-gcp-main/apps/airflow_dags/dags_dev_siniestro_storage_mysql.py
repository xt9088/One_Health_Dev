import time
import datetime
import requests, json, pytz, io, os
import pandas as pd
import pymysql
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

from airflow.providers.mysql.operators.mysql import MySqlOperator #pip3 install apache-airflow-providers-mysql
from airflow.providers.postgres.operators.postgres import PostgresOperator #pip install apache-airflow-providers-postgres


def lectura_Parametros(table,Id_Config_table):
    select_database = MySqlOperator(
        task_id='Read_table_mysql',
        sql=r"""
        SELECT * FROM {table} where Id= {Id_Config_table}"
        );
        """,
        sqlite_conn_id="airflow_mysql" # https://6cbe23805fc14ee0a8bedf3c3751f16c-dot-us-east4.composer.googleusercontent.com/connection/list/
    )
    df = pd.read_sql(select_database)
    return df

df = lectura_Parametros(table='AGREGAR_NOMBRE_TABLA_MYSQL',
                        Id_Config_table='AGREGAR_Id',)



def lectura_Parametros1(table,Id_Config_table):
    select_database = PostgresOperator(
        task_id='Read_table_mysql',
        sql=r"""
        SELECT * FROM {table} where Id= {Id_Config_table}"
        );
        """,
        sqlite_conn_id="airflow_db" # https://6cbe23805fc14ee0a8bedf3c3751f16c-dot-us-east4.composer.googleusercontent.com/connection/list/
    )
    df1 = pd.read_sql(select_database)
    return df

df1 = lectura_Parametros1(table='AGREGAR_NOMBRE_TABLA_POSTGRES',
                        Id_Config_table='AGREGAR_Id',)


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

file= df['file_name']   # "Nombre File Parquet : data"
source_bucket = df['source_bucket']    # Nombre del bucket origen: "he-dev-data"
source_path = df['source_path']   # Ruta acceso al bucket origen: "he-dev-data-ipress/ipress_clinicas/internacional/" 
project_id = df['project_id']   # Id de proyecto:  "he-dev-data" 
dataset_id = df['dataset_id']   # Biquery dataset tabla: "dev_data_analytics"
table_id = df['table_id']   # Nombre tabla :anl_tmp_part_month_siniestro_t"
local_path = df['local_path']  # Ruta Local Copia archivo Parquet

source_path_2 = f'{source_path}{file}.parquet' #Ruta acceso al archivo bucket - origen: "he-dev-data-ipress/ipress_clinicas/internacional/data.parquet" 
local_path_2 = f'{local_path}{file}.parquet' #Ruta Local Copia archivo Parquet - 


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