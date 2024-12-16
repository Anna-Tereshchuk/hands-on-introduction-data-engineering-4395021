from datetime import datetime
from airflow import DAG
from airflow.operators.bash import BashOperator
from airflow.operators.python import PythonOperator
import pandas as pd
import os

# Define the transform_data function
def transform_data():
    # Read the extracted CSV file
    input_file = '/lab/challenges/challenge-extract-dag.csv'
    output_file = '/lab/challenges/challenge-transform-data.csv'

    # Read the data into a Pandas DataFrame
    df = pd.read_csv(input_file)

    # Aggregate the count of companies per sector
    transformed_df = df.groupby('sector').size().reset_index(name='count')

    # Add a date column with today's date
    transformed_df['date'] = datetime.now().strftime('%Y-%m-%d')

    # Save the transformed data to a new CSV file
    transformed_df.to_csv(output_file, index=False)

# Define the DAG
default_args = {
    'owner': 'airflow',
    'depends_on_past': False,
    'email_on_failure': False,
    'email_on_retry': False,
    'retries': 1,
}

dag = DAG(
    'challenge_dag',
    default_args=default_args,
    description='ETL pipeline for S&P 500 sector data',
    schedule_interval=None,
    start_date=datetime(2024, 12, 16),
    catchup=False,
)

# Define tasks
# Extract Task: Download the CSV file
# Define tasks
# Extract Task: Download the CSV file
extract_task = BashOperator(
    task_id='extract_task',
    bash_command='curl -o /lab/challenges/challenge-extract-dag.csv https://raw.githubusercontent.com/LinkedInLearning/hands-on-introduction-data-engineering-4395021/main/data/constituents.csv',
    dag=dag,
)



# Transform Task: Process and aggregate the data
transform_task = PythonOperator(
    task_id='transform_task',
    python_callable=transform_data,
    dag=dag,
)

# Load Task: Load data into SQLite database
load_task = BashOperator(
    task_id='load_task',
    bash_command='sqlite3 /lab/challenges/challenge-load-db.db "\
    .mode csv \
    .import /lab/challenges/challenge-transform-data.csv sp_500_sector_count"',
    dag=dag,
)

# Define task dependencies
extract_task >> transform_task >> load_task
