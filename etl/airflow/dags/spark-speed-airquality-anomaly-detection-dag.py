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
    'spark-speed-airquality-anomaly-detection',
    default_args=default_args,
    description='Processamento Speed de Anomalias de Qualidade do Ar',
    schedule_interval=timedelta(minutes=15), # A cada 15 minutos
    catchup=False
) as dag:
    
    BashOperator(
        task_id='run_speed_aiq_analysis',
        bash_command='python3 /opt/airflow/etl/speed-airquality-anomaly-detection.py',
    )

