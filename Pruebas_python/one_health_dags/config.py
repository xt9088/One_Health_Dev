##### config.py

import os

os.environ['ruta_scripts'] = 'one_health_sql_scripts/*.sql'
os.environ['ruta_especifica'] = 'one_health_sql_scripts/'
os.environ['ruta_file_json'] = 'one_health_dags/params.json'

# os.environ['ruta_scripts'] = 'Pruebas_python/one_health_sql_scripts/*.sql'