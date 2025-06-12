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
    'spark-airquality-anomaly-detection',
    default_args=default_args,
    description='Processamento de anomalias de qualidade do ar',
    schedule_interval=timedelta(minutes=30), # A cada 30 minutos
    catchup=False
) as dag:
    
    BashOperator(
        task_id='run_aiq_analysis',
        bash_command='python3 /opt/airflow/etl/analyze-airquality-anomalies.py',
    )

