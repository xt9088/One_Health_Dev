from airflow import DAG
from airflow.operators.python_operator import PythonOperator
from airflow.utils.dates import days_ago
import requests
import csv
import tempfile
from google.cloud import storage

# Define the function to fetch posts from the API and save to GCS
def fetch_posts_to_gcs():
    # Fetch data from the API
    url = "https://jsonplaceholder.typicode.com/posts"
    response = requests.get(url)
    posts = response.json()

    # Define field names for CSV
    fieldnames = ['userId', 'id', 'title', 'body']

    # Create a temporary file to store CSV data
    with tempfile.NamedTemporaryFile(delete=False, mode='w', newline='', encoding='utf-8') as temp_file:
        writer = csv.DictWriter(temp_file, fieldnames=fieldnames)
        writer.writeheader()

        for post in posts:
            writer.writerow(post)

    # Upload the file to Google Cloud Storage
    upload_to_gcs(temp_file.name)

def upload_to_gcs(file_path):
    # Initialize Google Cloud Storage client
    storage_client = storage.Client()

    # Define your bucket and blob path
    bucket_name = "us-east4-dev-airflow-data-9879bdea-bucket"
    blob_path = "data/data-ipress/ipress_clinicas/internacional"

    # Upload the file to GCS
    bucket = storage_client.bucket(bucket_name)
    blob = bucket.blob(f"{blob_path}/posts.csv")
    blob.upload_from_filename(file_path)

    print(f"File {file_path} uploaded to gs://{bucket_name}/{blob_path}/posts.csv")

# Define default arguments for the DAG
default_args = {
    'owner': 'airflow',
    'start_date': days_ago(1),
    'retries': 1,
}

# Define the DAG
dag = DAG(
    'fetch_posts_dag5',
    default_args=default_args,
    description='A DAG to fetch posts from a REST API and save to CSV in GCS',
    schedule_interval='@daily',
)

# Define the task using PythonOperator
fetch_posts_task = PythonOperator(
    task_id='fetch_posts_to_gcs',
    python_callable=fetch_posts_to_gcs,
    dag=dag,
)

# Set dependencies in the DAG
fetch_posts_task