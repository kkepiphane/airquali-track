from datetime import datetime, timedelta
from airflow import DAG
from airflow.operators.bash_operator import BashOperator

default_args = {
    'owner': 'airflow',
    'depends_on_past': False,
    'retries': 1,
    'retry_delay': timedelta(minutes=5),
}

with DAG(
    'process_air_quality',
    default_args=default_args,
    description='Process raw JSON data into Parquet',
    schedule_interval=timedelta(hours=1),
    start_date=datetime(2023, 1, 1),
    catchup=False,
) as dag:

    # Tâche qui utilise le conteneur Spark pour exécuter le script
    processing_task = BashOperator(
        task_id='run_pyspark_script',
        bash_command='docker exec airquali-track-pyspark-notebook-1 python /home/jovyan/scripts/process_air_quality.py ',
    )

    processing_task