from datetime import datetime, timedelta
import os
import sys
from airflow.decorators import dag, task
from dotenv import load_dotenv
import logging

sys.path.append(os.path.abspath(os.path.join(os.path.dirname(__file__), "..", "..")))
from manager import DuckDBManager, AWSManager
from src.bronze import DataManager

# Load environment variables
load_dotenv()

# Configuration from environment variables
LOCAL_PATH = os.getenv("LOCAL_PATH", "data/bronze/")
AWS_BUCKET_NAME = os.getenv("AWS_BUCKET_NAME")
AWS_ACCESS_KEY = os.getenv("AWS_ACCESS_KEY")
AWS_SECRET_ACCESS_KEY = os.getenv("AWS_SECRET_ACCESS_KEY")
JSON_PATH = os.getenv("JSON_PATH", "data/raw/spotify_data.json")
TABLE_NAME = os.getenv("TABLE_NAME", "spotify_data")

default_args = {
    'owner': 'airflow',
    'depends_on_past': False,
    'start_date': datetime(2024, 4, 1),
    'email_on_failure': False,
    'email_on_retry': False,
    'catchup': False,
    'retries': 1,
    'retry_delay': timedelta(minutes=5),
}

logger = logging.getLogger(__name__)

@dag(default_args=default_args, schedule_interval='@daily', catchup=False, tags=['spotify_bronze_etl'])
def spotify_bronze_etl_dag():
    """
    Airflow DAG to load and transform data from raw JSON files into a DuckDB database, and then export it to local and S3 storage.
    """
    @task
    def load_transform_export_data():
        db_manager = DuckDBManager()
        aws_manager = AWSManager(db_manager, AWS_BUCKET_NAME, AWS_ACCESS_KEY, AWS_SECRET_ACCESS_KEY)
        data_manager = DataManager(db_manager, aws_manager, LOCAL_PATH, AWS_BUCKET_NAME)

        try:
            logger.info("Starting data load and transform")
            data_manager.load_and_transform_data(JSON_PATH, TABLE_NAME)
            logger.info("Starting data export")
            data_manager.export_data(TABLE_NAME)
            logger.success("Data processing completed successfully")
        except Exception as e:
            logger.error(f"An error occurred during data processing: {e}")

    # Setup task
    data_processing_task = load_transform_export_data()

    data_processing_task

# Instantiate the DAG
dag = spotify_bronze_etl_dag()
