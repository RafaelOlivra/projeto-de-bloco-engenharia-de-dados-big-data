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
    'spark-data-processing',
    default_args=default_args,
    description='Processamento de dados de clima e qualidade do ar',
    schedule_interval=timedelta(hours=5), # A cada 5 horas
    catchup=False
) as dag:
    
    BashOperator(
        task_id='run_data_processing',
        bash_command='python3 /opt/airflow/etl/process-data.py',
    )
