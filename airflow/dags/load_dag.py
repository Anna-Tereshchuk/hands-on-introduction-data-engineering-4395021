from datetime import datetime
from airflow import DAG
from airflow.operators.bash import BashOperator

# Define default_args for the DAG
default_args = {
    'owner': 'airflow',
    'depends_on_past': False,
    'email_on_failure': False,
    'email_on_retry': False,
    'retries': 1
}

# Initialize the DAG
dag = DAG(
    'load_dag',
    default_args=default_args,
    description='Load CSV data into SQLite database',
    schedule_interval=None,  # Manual trigger
    start_date=datetime(2023, 1, 1),
    catchup=False
)

# Define the task to load CSV data into SQLite using BashOperator
load_data = BashOperator(
    task_id='load_csv_to_sqlite',
    bash_command='[ -f /workspace/airflow-transform-data.csv ] && \
                 sqlite3 /workspace/airflow-load-db.db ".mode csv" ".import /workspace/airflow-transform-data.csv top_level_domains" || echo "CSV file not found"',
    dag=dag
)

# Add tasks to the DAG (in this case, there's just one task)
load_data
