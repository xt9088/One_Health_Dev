import time
import datetime
import os
from airflow import DAG
from airflow.models.baseoperator import chain
from airflow.providers.google.cloud.operators.bigquery import BigQueryInsertJobOperator
from airflow.providers.google.cloud.transfers.bigquery_to_gcs import BigQueryToGCSOperator
from airflow.providers.google.cloud.operators.bigquery import BigQueryDeleteTableOperator
from airflow.providers.google.cloud.transfers.gcs_to_gcs import GCSToGCSOperator
from airflow.operators.python import PythonOperator
from composer_plugins import get_scripts_in_folder, get_current_file_script, get_parameters, set_parameters_export, execute_query
from airflow.models import Connection
from airflow.settings import Session
from google.cloud import storage
from google.cloud import bigquery
from google.auth import impersonated_credentials

yesterday = datetime.datetime.combine(datetime.datetime.today() - datetime.timedelta(1), datetime.datetime.min.time())

env_project = os.environ["ENV_DESC"]
delivery_compass = "delivery_compass/*.sql"##os.environ["FOLDER_EXPORT_COMPASS"]##"export/delivery_compass/*.sql"

scripts_sql_compass = get_scripts_in_folder(delivery_compass)    

parameters_custom = "parameters_oh.json" 

parameters = get_parameters(delivery_compass, parameters_file=parameters_custom)

sa_impersonation_chain = parameters['services_account'] 

export_config =  parameters["export_config"] if "export_config" in parameters else None

dag_id = parameters['dag_id']
parameters_dag_id = dag_id.replace("{env}",env_project)

tables_export = export_config['tables']
project_name = export_config['project']
bucket_export = export_config['bucket_export']
project_trsv = export_config['project_trsv']
bucket_hub = export_config['bucket_hub']

table_export = tables_export[0]

dataset_name = table_export["dataset"]
table_name = table_export["table"]

def push_current_datetime(**kwargs):
    now = datetime.datetime.now().strftime("%Y%m%d%H%M%S")
    kwargs['ti'].xcom_push(key='current_datetime', value=now)

#now = datetime.datetime.now()

#date_time = now.strftime("%Y%m%d%H%M%S")

session = Session()
gcp_conn = Connection(
    conn_id='bigquery_compass',
    conn_type='google_cloud_platform',
    extra='{"extra__google_cloud_platform__project":"'+project_name+'"}')
if not session.query(Connection).filter(
    Connection.conn_id == gcp_conn.conn_id).first():
    session.add(gcp_conn)
    session.commit()

default_dag_args = {
    'start_date': yesterday,    
    'email_on_failure': False,
    'email_on_retry': False,
    'retries': 1,
    'retry_delay': datetime.timedelta(minutes=5),
    'project_id': project_name
}

with DAG(
    dag_id=parameters_dag_id,
    schedule_interval='0 14 * * *',
    catchup=False,    
    default_args=default_dag_args,
    description='Export data from BigQuery to Parquet'
    ) as dag:
    
    #######################
    ### FLUJO DE TAREAS ###
    #######################

    task_dict = dict()
    op_list = []

    task_id_timestamp = 'push_datetime'

    task_dict[task_id_timestamp] = PythonOperator(
        task_id=task_id_timestamp,
        python_callable=push_current_datetime,
        provide_context=True,
    )

    task_dict[f"{task_id_timestamp}"]            
    op_list.append(task_dict[f"{task_id_timestamp}"])        


    for current_script_sql in scripts_sql_compass:
    
        result_script_string = get_current_file_script(current_script_sql)

        task_id_compass = 'creacion_diferencial_compass'

        task_dict[task_id_compass] = BigQueryInsertJobOperator(
                task_id=task_id_compass,
                configuration={
                    "query": {
                        "query": result_script_string,
                        "useLegacySql": False,
                    }
                },
                gcp_conn_id='bigquery_default',
                impersonation_chain=sa_impersonation_chain,
                dag=dag)               

        task_dict[f"{task_id_compass}"]         
        op_list.append(task_dict[f"{task_id_compass}"])


    task_id_bq_to_gcs = 'exportar_bq_to_gcs'
    
    ###  ###
    
    task_dict[task_id_bq_to_gcs] = BigQueryToGCSOperator(
        task_id='exportar_bq_to_gcs',
        source_project_dataset_table=project_name + '.' + dataset_name + '.' + table_name,
        destination_cloud_storage_uris=['gs://' + bucket_export + '/mdm_siniestro_{{ ti.xcom_pull(task_ids="push_datetime", key="current_datetime") }}.parquet'],
        export_format='PARQUET',
        gcp_conn_id='bigquery_default',
        impersonation_chain=sa_impersonation_chain, # quitar comentario
        dag=dag
    )

    task_dict[f"{task_id_bq_to_gcs}"]            
    op_list.append(task_dict[f"{task_id_bq_to_gcs}"])


    task_id_delete_bq = 'delete_table_bq'
    
    task_dict[task_id_delete_bq] = BigQueryDeleteTableOperator(
        task_id='delete_table_bq',
        deletion_dataset_table=project_name + '.' + dataset_name + '.' + table_name,
        ignore_if_missing=True,
        gcp_conn_id='bigquery_default',
        impersonation_chain=sa_impersonation_chain,   
        dag=dag             
    )

    task_dict[f"{task_id_delete_bq}"]            
    op_list.append(task_dict[f"{task_id_delete_bq}"])


    task_id_copy_hub = 'copy_parquet_hub'

    task_dict[task_id_copy_hub] = GCSToGCSOperator(
        task_id="copy_parquet_hub",
        source_bucket=bucket_export,
        source_object='mdm_siniestro_{{ ti.xcom_pull(task_ids="push_datetime", key="current_datetime") }}.parquet',
        destination_bucket=bucket_hub,
        destination_object='mdm_siniestros/mdm_siniestro_{{ ti.xcom_pull(task_ids="push_datetime", key="current_datetime") }}.parquet',
        exact_match=True,
        move_object=False,          
        gcp_conn_id='bigquery_default',
        impersonation_chain=sa_impersonation_chain,
        #gcp_conn_id=google_cloud_conn_id,
        dag=dag        
    )

    task_dict[f"{task_id_copy_hub}"]            
    op_list.append(task_dict[f"{task_id_copy_hub}"])


    chain(*op_list)

    op_list[-1]