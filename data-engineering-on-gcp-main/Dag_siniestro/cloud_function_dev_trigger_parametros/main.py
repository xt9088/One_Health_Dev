# main.py

import os
from typing import Any
from google.cloud import storage
import mysql.connector
import composer2_airflow_rest_api
from config import PREFIX, WEB_SERVER_URL  # Import configurations

def trigger_dag_gcf(data, context=None):
    prefix = PREFIX
    file_name = data['name']
    ruta_completa = data["id"]
    indice_slash_final = ruta_completa.rfind('/')
    ruta = ruta_completa[:indice_slash_final]
    print(ruta)

    if not file_name.startswith(prefix):
        print(f"File {ruta} is not in the prefix {prefix}. The DAG will not be executed.")
        return

    dag_id = get_dag_id(ruta)

    if not dag_id:
        print(f"No matching DAG found for the file: {ruta}")
        return

    print(f"data: {data}")

    composer2_airflow_rest_api.trigger_dag(WEB_SERVER_URL, dag_id, data)
    print(f"DAG {dag_id} triggered successfully for file {ruta}.")

def get_dag_id(ruta: str) -> str:
    instance_connection_name = os.getenv('INSTANCE_CONNECTION_NAME')
    db_user = os.getenv('DB_USER')
    db_password = os.getenv('DB_PASSWORD')
    db_name = os.getenv('DB_NAME')

    connection_config = {
        'user': db_user,
        'password': db_password,
        'host': '35.245.223.109',  # Replace with the IP address of your Cloud SQL instance
        'port': 3306,
        'database': db_name,
}

    conn = mysql.connector.connect(**connection_config)
    cursor = conn.cursor()

    query = f"SELECT DAG_STORAGE_DSET_NAME FROM `DATA_FLOW_CONFIG` WHERE concat(DESTINATION_BUCKET,'/',DESTINATION_DIRECTORY,DESTINATION_FILE_NAME,ORIGIN_EXTENSION) = '{ruta}'"
    cursor.execute(query)
    result = cursor.fetchone()
    print(result)
    cursor.close()
    conn.close()

    if result:
        return result[0]
    else:
        return None


