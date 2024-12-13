from airflow import DAG
from airflow.operators.python import PythonOperator
from datetime import datetime
import pandas as pd

def transform_data():
    today = datetime.today().strftime('%Y-%m-%d')
    
    # Adjust the path to your CSV (ensure it's wrapped in quotes)
    df = pd.read_csv('/workspaces/hands-on-introduction-data-engineering-4395021/lab/manual/manual-extract-data.csv')
    
    # Filter rows where "Type" is "generic"
    generic_type_df = df[df["Type"] == "generic"]
    
    # Add the "Date" column
    generic_type_df["Date"] = today
    
    # Save the transformed data
    output_file = f"/workspaces/hands-on-introduction-data-engineering-4395021/lab/orchestrated/transformed-extract-{today}.csv"
    generic_type_df.to_csv(output_file, index=False)

# Define the DAG
with DAG('transform_dag',
         default_args={
             'owner': 'airflow',
             'start_date': datetime(2023, 12, 1),
             'retries': 1
         },
         schedule_interval=None,  # Adjust as needed
         catchup=False) as dag:

    transform_task = PythonOperator(
        task_id='transform_data',
        python_callable=transform_data
    )
    
    transform_task
