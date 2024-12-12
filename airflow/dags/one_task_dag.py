from datetime import datetime, timedelta
from airflow import DAG
from airflow.operators.bash import BashOperator


default_args = {
    'owner': 'airflow',
    'retries': 1,
    'retry_delay': timedelta(minutes=5),
    'email_on_failure': False,
    'email_on_retry': False
}

dag = DAG(
    'one_task_dag',
    default_args=default_args,
    description='A simple one-task DAG',
    schedule_interval=None,
    start_date=datetime(2024, 12, 12),
    catchup=False
)

task = BashOperator(
    task_id='create_file',
    bash_command='echo "Hello, LinkedIn Learning" > createthisfile.txt',
    dag=dag
)


