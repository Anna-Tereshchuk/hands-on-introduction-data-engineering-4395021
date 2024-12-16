from airflow import DAG
from airflow.operators.python import PythonOperator
from datetime import datetime, timedelta

# Define the Python functions for Extract, Transform, and Load
def extract_task():
    with open('/tmp/basic-etl-extract-data.csv', 'w') as f:
        f.write("domain,rank\nexample.com,1\nexample.org,2\nexample.net,3")
    print("Data extracted and written to basic-etl-extract-data.csv")

def transform_task():
    with open('/tmp/basic-etl-extract-data.csv', 'r') as infile, open('/tmp/basic-etl-transform-data.csv', 'w') as outfile:
        lines = infile.readlines()
        header, rows = lines[0], lines[1:]
        outfile.write(header)
        for row in rows:
            domain, rank = row.strip().split(',')
            rank = int(rank) * 10  # Example transformation: Multiply rank by 10
            outfile.write(f"{domain},{rank}\n")
    print("Data transformed and written to basic-etl-transform-data.csv")

def load_task():
    import sqlite3
    conn = sqlite3.connect('/tmp/basic-etl-load-db.db')
    cursor = conn.cursor()
    cursor.execute("CREATE TABLE IF NOT EXISTS top_level_domains (domain TEXT, rank INTEGER)")
    with open('/tmp/basic-etl-transform-data.csv', 'r') as f:
        rows = f.readlines()[1:]  # Skip header
        for row in rows:
            domain, rank = row.strip().split(',')
            cursor.execute("INSERT INTO top_level_domains (domain, rank) VALUES (?, ?)", (domain, int(rank)))
    conn.commit()
    conn.close()
    print("Data loaded into SQLite database basic-etl-load-db.db")

# Default arguments for the DAG
default_args = {
    'owner': 'airflow',
    'retries': 1,
    'retry_delay': timedelta(minutes=5),
}

# Define the DAG
with DAG(
    'basic_etl_dag',
    default_args=default_args,
    description='A simple ETL pipeline using Airflow',
    schedule_interval='@daily',  # Run once daily
    start_date=datetime(2024, 12, 1),
    catchup=False
) as dag:

    extract = PythonOperator(
        task_id='extract',
        python_callable=extract_task
    )

    transform = PythonOperator(
        task_id='transform',
        python_callable=transform_task
    )

    load = PythonOperator(
        task_id='load',
        python_callable=load_task
    )

    # Define task dependencies
    extract >> transform >> load
