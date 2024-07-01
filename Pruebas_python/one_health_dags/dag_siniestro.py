# dag_siniestro.py

import os
import json
from pathlib import Path
import config 
from plugins import get_scripts_files_in_folder,get_script_from_file,get_parameters

ruta_scripts_sql = os.environ["ruta_scripts"] 
ruta_file_json = os.environ["ruta_file_json"] 
ruta_file_sql = os.environ["ruta_especifica"] 

config_data = get_parameters(ruta_file_json)


#print(ruta_scripts_sql)
#print(ruta_file_json)

lista_scripts = get_scripts_files_in_folder(ruta_scripts_sql)
print(lista_scripts)

write_disposition = 'WRITE_APPEND'

for current_sql_script in lista_scripts:
    sql_script = Path(current_sql_script).stem
    print(sql_script)
    query_script = get_script_from_file(ruta_file_sql,sql_script)
    #print(query_script)
    
    for config_job in config_data['write_dispositions']:
        if sql_script in config_job['scripts']:
            write_disposition_config = config_job['write_disposition']
            print(write_disposition_config) 
