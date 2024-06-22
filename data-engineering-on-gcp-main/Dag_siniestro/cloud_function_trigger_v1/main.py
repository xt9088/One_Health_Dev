# Copyright 2021 Google LLC
#
# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License at
#
#     https://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.

"""
Trigger a DAG in a Cloud Composer 2 environment in response to an event,
using Cloud Functions.
"""

import os
from typing import Any
import composer2_airflow_rest_api

def trigger_dag_gcf(data, context=None):
    """
    Trigger a DAG and pass event data.

    Args:
      data: A dictionary containing the data for the event. Its format depends
      on the event.
      context: The context object for the event.

    For more information about the arguments, see:
    https://cloud.google.com/functions/docs/writing/background#function_parameters
    """

    # Obtener el prefijo del entorno
    prefix = os.getenv('PREFIX', '')

    # Obtener el nombre del archivo del evento
    file_name = data['name']

    # Verificar si el archivo está en el subdirectorio especificado
    if not file_name.startswith(prefix):
        print(f"Archivo {file_name} no está en el prefijo {prefix}. No se ejecutará la DAG.")
        return

    # URL del servidor web de Airflow
    web_server_url = (
        "https://80bb830408f34d3ca939b9b8d3524076-dot-us-east4.composer.googleusercontent.com"
    )
    # ID de la DAG que deseas ejecutar
    dag_id = 'dev_dag_siniestros_parquet_GCS_BQ_Script'

    # Disparar la DAG
    composer2_airflow_rest_api.trigger_dag(web_server_url, dag_id, data)
