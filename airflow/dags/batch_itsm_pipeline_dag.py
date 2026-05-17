from datetime import datetime, timedelta
import os
from airflow import DAG
from airflow.operators.bash import BashOperator

default_args = {
    'owner': 'data_engineering_team',
    'depends_on_past': False,
    'start_date': datetime(2026, 1, 1),
    'email_on_failure': False,
    'email_on_retry': False,
    'retries': 1,
    'retry_delay': timedelta(minutes=5),
}

# Absolute path to the python executable and the script
# We run this from the project root. The Docker container mounts the project root to /app.
PROJECT_ROOT = "/app/batch"

with DAG(
    'batch_itsm_pipeline_dag',
    default_args=default_args,
    description='Batch Pipeline for ITSM Historical CSV Data',
    schedule_interval='@monthly', # Or None for manual trigger
    catchup=False,
    tags=['itsm', 'batch'],
) as dag:

    run_batch_pipeline = BashOperator(
        task_id='run_full_batch_pipeline',
        bash_command=f'cd {PROJECT_ROOT} && python -m src.batch.run_batch_pipeline',
    )

    run_batch_pipeline
