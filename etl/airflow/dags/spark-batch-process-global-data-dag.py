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
    'spark-batch-global-data-processing',
    default_args=default_args,
    description='Processamento de Dados Globais em Batch',
    schedule_interval=timedelta(hours=24), # A cada 24 horas
    catchup=False
) as dag:
    
    BashOperator(
        task_id='run_batch_data_processing',
        bash_command='python3 /opt/airflow/etl/batch-global-data-processing.py',
    )
