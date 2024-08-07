# gs://he-dev-data/mdm_cartas_garantia_2020303/124124124124124     
kwargs = {
    'dag_run': {
        'conf': {
            'id': 'gs://he-dev-data/ipress/cartas_garantia/mdm_cartas_garantia_2020303/124124124124124'
                }
               }
}

metadata_dag = kwargs['dag_run']['conf']
ruta_completa = metadata_dag['id']

slash_position = ruta_completa.rfind('/')
ruta_completa = ruta_completa[:slash_position]
pre_file= ruta_completa.split('/')[-1]
parts = pre_file.split('_')
prueba = parts[1:-1]
file = '_'.join(parts[1:-1])
print (pre_file)