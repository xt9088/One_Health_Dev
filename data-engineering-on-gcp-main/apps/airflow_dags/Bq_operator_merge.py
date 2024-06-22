from airflow.providers.google.cloud.operators.bigquery import BigQueryOperator
from airflow import dag

merge_task = BigQueryOperator(
    task_id='merge_task',
    sql='MERGE INTO target_table USING source_table ON condition WHEN MATCHED THEN UPDATE SET ... WHEN NOT MATCHED THEN INSERT ...',
    use_legacy_sql=False,  # Use standard SQL if your MERGE statement is written in standard SQL
    bigquery_conn_id='google_cloud_default',
    dag=dag,
)
