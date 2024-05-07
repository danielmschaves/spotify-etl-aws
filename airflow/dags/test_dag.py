from datetime import datetime, timedelta
from airflow import DAG
from airflow.operators.python import PythonOperator
import os

def test_local_write():
    test_directory = 'data/'  # Example relative path
    test_file_path = os.path.join(os.getcwd(), test_directory, 'test_file.txt')
    
    try:
        with open(test_file_path, 'w') as test_file:
            test_file.write('Airflow has successfully written to this file.')
        return f"Successfully wrote to {test_file_path} (Absolute path)"
    except Exception as e:
        return f"Failed to write to {test_file_path} (Absolute path): {str(e)}"

default_args = {
    'owner': 'airflow',
    'depends_on_past': False,
    'start_date': datetime(2024, 5, 1),
    'email_on_failure': False,
    'email_on_retry': False,
    'retries': 1,
    'retry_delay': timedelta(minutes=1),
}

# Create a DAG to run the task
dag = DAG(
    'test_local_write_permissions',
    default_args=default_args,
    description='A simple DAG to test local file write permissions',
    schedule_interval=timedelta(days=1),  # This can be adjusted to 'None' for manual trigger
    catchup=False
)

# Define the Python operator to execute your function
write_test = PythonOperator(
    task_id='write_permission_test',
    python_callable=test_local_write,
    dag=dag
)

# Setting the DAG structure
write_test
