from airflow import DAG
from airflow.operators.bash import BashOperator
from datetime import datetime, timedelta

default_args = {
    'owner': 'airflow',
    'start_date': datetime(2024, 1, 1),
    'retries': 1,
    'retry_delay': timedelta(minutes=5),
}

with DAG(
    'spark-batch-weather-anomaly-detection',
    default_args=default_args,
    description='Processamento Batch de Anomalias de Clima',
    schedule_interval=timedelta(hours=24), # A cada 24 horas
    catchup=False
) as dag:

    BashOperator(
        task_id='run_batch_weather_analysis',
        bash_command='python3 /opt/airflow/etl/batch-weather-anomaly-detection.py',
    )

