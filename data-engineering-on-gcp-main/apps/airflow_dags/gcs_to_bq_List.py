import datetime
from airflow import models
from airflow.models import Variable
from airflow.operators.bash import BashOperator
from airflow import DAG
from airflow.providers.google.cloud.transfers.gcs_to_bigquery import GCSToBigQueryOperator
from airflow.utils.dates import days_ago
from airflow.operators.python import PythonOperator
from ReadCsvOperator import ReadCsvOperator

default_args = {
                'owner': 'airflow',
                'start_date': days_ago(1),
                'retries':1,
}

def create_gcs_to_bq_tasks(task):
    return GCSToBigQueryOperator(
        task_id=f"load_{task['destination_table'].replace('.', '_')}",
        bucket=task['gcs_bucket'],
        source_objects=[task['gcs_key']],
        destination_project_dataset_table=[task['destination_table']],
        source_format='CSV',
        write_disposition='WRITE_TRUNCATE',
        skip_leading_rows=1
    )
    
with DAG(
     dag_id='csv_to_bigquery',
     default_args=default_args,
     schedule_interval=None,
     catchup=False,  
) as dag:

    read_csv = ReadCsvOperator(
        task_id='read_csv',
        gcs_bucket='your-gcs-bucket',
        gcs_key='path/to/your/buckets_to_tables.csv'
    )
    
    def create_taks(**context):
        tasks = context['task_instance'].xcom_pull(task_ids='read_csv')
        for task in tasks:
            create_gcs_to_bq_tasks = create_gcs_to_bq_tasks(task)
    
    create_tasks=PythonOperator(
        task_id='create_tasks',
        python_callable=create_gcs_to_bq_tasks,
        provide_context=True,
    )
    
    read_csv >> create_tasks
            
        
        
        
        
    