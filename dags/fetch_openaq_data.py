from datetime import datetime, timedelta
import requests
import json
from airflow import DAG
from airflow.operators.python_operator import PythonOperator
import boto3
from botocore.client import Config
from airflow.models import Variable

# Default args for the DAG
default_args = {
    'owner': 'airflow',
    'depends_on_past': False,
    'email_on_failure': False,
    'email_on_retry': False,
    'retries': 1,
    'retry_delay': timedelta(minutes=5),
}

# Define the DAG
with DAG(
    'openaq_ingestion',
    default_args=default_args,
    description='Fetch data from OpenAQ API every hour',
    schedule_interval=timedelta(hours=1), # Exécution toutes les heures
    start_date=datetime(2023, 1, 1),
    catchup=False, # Évite de rejouer les exécutions passées
) as dag:

    def fetch_and_upload():
        s3_client = boto3.client(
            's3',
            endpoint_url='http://minio:9000',
            aws_access_key_id='minioadmin',
            aws_secret_access_key='minioadmin',
            config=Config(signature_version='s3v4'),
            region_name='us-east-1'
        )

        BUCKET_NAME = 'raw'
        url = "https://api.openaq.org/v3/latest?limit=1000"

        headers = {
            "X-API-Key": Variable.get("OPENAQ_API_KEY")
        }

        response = requests.get(url, headers=headers)

        if response.status_code != 200:
            raise Exception(f"Erreur API OpenAQ: {response.status_code}, {response.text}")

        data = response.json()

        current_time = datetime.utcnow().strftime("%Y-%m-%d_%H-%M-%S")
        filename = f"openaq_latest_{current_time}.json"
        json_data = json.dumps(data)

        s3_client.put_object(
            Bucket=BUCKET_NAME,
            Key=f"openaq/{filename}",
            Body=json_data
        )
        print(f"File {filename} uploaded to MinIO successfully!")

    # Define the task
    ingestion_task = PythonOperator(
        task_id='fetch_and_upload_to_minio',
        python_callable=fetch_and_upload,
    )

    # This means the DAG is just this one task
    ingestion_task