from datetime import datetime, timedelta
from airflow import DAG
from airflow.operators.bash import BashOperator  # Corrected import

# Define default_args for the DAG
default_args = {
    'owner': 'airflow',
    'retries': 1,
    'retry_delay': timedelta(minutes=5),
    'start_date': datetime(2024, 12, 12),
}

# Create the DAG instance
dag = DAG(
    'two_task_dag',  # DAG ID
    default_args=default_args,  # Default args passed here
    description='A simple 2 task DAG',
    schedule_interval=None,  # Set to None to run the DAG manually
)

# Define the first task (T0)
t0 = BashOperator(
    task_id='task_0',
    bash_command='echo "First Airflow task"',
    dag=dag,
)

# Define the second task (T1)
t1 = BashOperator(
    task_id='task_1',
    bash_command='sleep 5 && echo "Second Airflow task"',
    dag=dag,
)

# Set the task dependency: T1 will run only after T0 completes successfully
t0 >> t1  # This means t1 depends on t0
