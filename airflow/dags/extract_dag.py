from datetime import datetime, timedelta
from airflow import DAG
from airflow.operators.bash import BashOperator

# Default arguments for the DAG
default_args = {
    'owner': 'airflow',
    'depends_on_past': False,
    'email_on_failure': False,
    'email_on_retry': False,
    'retries': 3,
    'retry_delay': timedelta(minutes=5),
}

# Create the DAG
with DAG(
    'extract_dag',
    default_args=default_args,
    description='A simple data extraction DAG',
    schedule_interval=timedelta(days=1),
    start_date=datetime(2024, 1, 1),
    catchup=False,
) as dag:

    # Define the extract task
    extract_task = BashOperator(
        task_id='extract_task',
        bash_command=(
            'wget -O /workspace/lab/orchestrated/airflow-extract-data.csv '
            'https://lnkd.in/gfENQi7K'
        ),
    )

    # Task dependency (if there are multiple tasks)
    extract_task

  
