#plugins.py
import glob 
import json

def get_scripts_files_in_folder(ruta_scripts_sql):
    import glob 
    return glob.glob(ruta_scripts_sql)


def get_script_from_file(ruta,file):
    with open(ruta+file+'.sql','r') as completo:
        return completo.read()
    

def get_parameters(parameters_file):
    with open(parameters_file, 'r') as file:
        return json.load(file)