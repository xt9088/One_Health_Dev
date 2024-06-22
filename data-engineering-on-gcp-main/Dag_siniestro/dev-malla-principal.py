from airflow import DAG
from airflow.operators.trigger_dagrun import TriggerDagRunOperator
from airflow.providers.mysql.hooks.mysql import MySqlHook
from datetime import datetime, timedelta
from airflow.operators.python_operator import PythonOperator

default_args = {
    'owner': 'airflow',
    'depends_on_past': False,
    'start_date': datetime(2022, 1, 1),
    'email_on_failure': False,
    'email_on_retry': False,
    'retries': 1,
    'retry_delay': timedelta(minutes=5),
}

# Define the parent DAG
parent_dag = DAG(
    'parent_dag',
    default_args=default_args,
    description='Parent DAG to trigger multiple child DAGs',
    schedule_interval='@daily',
)

def fetch_and_store_values(table, **kwargs):
    mysql_hook = MySqlHook(mysql_conn_id='Cloud_SQL_db_compass')

    query = f"""
        SELECT WORKFLOW_NAME FROM {table} 
    """
    connection = mysql_hook.get_conn()
    cursor = connection.cursor()
    cursor.execute(query)
    result = cursor.fetchall()
    
    if result:
        # Extract IDs from the result and return as a list
        child_dag_ids = [row[0] for row in result]
        return child_dag_ids
    else:
        raise ValueError("No results found for the query.")

# Define the task to fetch parameters from MySQL
task_fetch_bq_params = PythonOperator(
    task_id='fetch_and_store_values',
    python_callable=fetch_and_store_values,
    op_kwargs={
        'table': 'DATA_FLOW_CONFIG'
    },
    provide_context=True,
    dag=parent_dag,
)

# Define the task to trigger child DAGs
def trigger_child_dags(child_dag_ids, **kwargs):
    for dag_id in child_dag_ids:
        TriggerDagRunOperator(
            task_id=f'trigger_{dag_id}',
            trigger_dag_id=dag_id,
            dag=parent_dag,
        )

# Define the task to trigger child DAGs based on the result of the SQL query
task_trigger_child_dags = PythonOperator(
    task_id='trigger_child_dags',
    python_callable=trigger_child_dags,
    op_kwargs={'child_dag_ids': '{{ task_instance.xcom_pull(task_ids="fetch_and_store_values") }}'},
    dag=parent_dag,
)

# Set dependencies
task_fetch_bq_params >> task_trigger_child_dags
