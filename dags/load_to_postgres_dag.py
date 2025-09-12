from datetime import datetime, timedelta
from airflow import DAG
from airflow.operators.python_operator import PythonOperator
import sys
import os

# Ajoute le chemin des scripts au PATH Python pour pouvoir les importer
sys.path.insert(0, os.path.abspath(os.path.dirname(__file__)) + "/../scripts")

# Importe la fonction main de notre script
from load_to_postgres import main

default_args = {
    'owner': 'airflow',
    'depends_on_past': False,
    'retries': 1,
    'retry_delay': timedelta(minutes=5),
}

with DAG(
    'load_to_postgres',
    default_args=default_args,
    description='Load processed Parquet data into PostgreSQL',
    schedule_interval=timedelta(hours=1),  # Exécution après le DAG de transformation
    start_date=datetime(2023, 1, 1),
    catchup=False,
) as dag:

    load_task = PythonOperator(
        task_id='load_data_to_postgres',
        python_callable=main,  # Appelle directement la fonction main du script
    )

    load_task