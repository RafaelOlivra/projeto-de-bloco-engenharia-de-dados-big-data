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
    'spark-anomaly-detection',
    default_args=default_args,
    description='Daily detection of weather anomalies using local Python',
    schedule_interval='@daily',
    catchup=False
) as dag:
    
    BashOperator(
        task_id='run_aiq_analysis',
        bash_command='python3 /opt/airflow/etl/analyze-aiq.py',
    )

    BashOperator(
        task_id='run_weather_analysis',
        bash_command='python3 /opt/airflow/etl/analyze-weather.py',
    )

