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
    'spark-data-refinement',
    default_args=default_args,
    description='Daily refinement of data using local Python',
    schedule_interval='@daily',
    catchup=False
) as dag:
    
    BashOperator(
        task_id='run_data_refinement',
        bash_command='python3 /opt/airflow/etl/refine-data.py',
    )
