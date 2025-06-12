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
    'spark-weather-anomaly-detection',
    default_args=default_args,
    description='Processamento de anomalias de clima',
    schedule_interval=timedelta(minutes=30), # A cada 30 minutos
    catchup=False
) as dag:

    BashOperator(
        task_id='run_weather_analysis',
        bash_command='python3 /opt/airflow/etl/analyze-weather-anomalies.py',
    )

