import csv
from airflow.models.baseoperator import BaseOperator
from airflow.utils.decorators import apply_defaults
from google.cloud import storage

class ReadCsvOperator(BaseOperator):
    def __init__(self,gcs_bucket,gcs_key, *args,**kwargs):
        super().__init__(*args,**kwargs)
        self.gcs_bucket=gcs_bucket
        self.gcs_key = gcs_key

def execute (self,context):
    client = storage.Client()
    bucket = client.get_bucket(self.gcs_bucket)
    blob = bucket.blob(self.gcs_key)
    csv_content = blob.download_as_text().splitlines()
    
    reader = csv.DictReader(csv_content)
    tasks = [row for row in reader]
    
    self.log.info(f"Extracted tasks: {tasks}")
    return tasks



    