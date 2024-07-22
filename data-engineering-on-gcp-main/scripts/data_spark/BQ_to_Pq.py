import os
from pyspark.sql import SparkSession

# INICIAR CONECCION

spark = SparkSession.builder.getOrCreate()

# VALORES DE PARAMETROS
project_id = os.environ.get('PROJECT_ID')
dataset_name = os.environ.get('DATASET_NAME')
table_name = os.environ.get('TABLE_NAME')
gcs_output_uri = os.environ.get('GCS_OUTPUT_URI')

# CARGA DE TABLA

table = f'{project_id}.{dataset_name}.{table_name}'

data_today = spark.read \
    .format('bigquery') \
    .option('table',table) \
    .load()
    
data_today.write \
    .mode('overwrite') \
    .parquet(gcs_output_uri)
    







