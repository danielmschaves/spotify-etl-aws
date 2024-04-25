from datetime import datetime, timedelta
import os
import sys
from airflow.decorators import dag, task
from dotenv import load_dotenv
import logging

sys.path.append(os.path.abspath(os.path.join(os.path.dirname(__file__), "..", "..")))
from manager import DuckDBManager, AWSManager
from bronze import DataManager

# Load environment variables
load_dotenv()

# Configuration
AWS_ACCESS_KEY = os.getenv("AWS_ACCESS_KEY")
AWS_SECRET_ACCESS_KEY = os.getenv("AWS_SECRET_ACCESS_KEY")
AWS_BUCKET_NAME = os.getenv("AWS_BUCKET_NAME")
RAW_S3_PATH = os.getenv("RAW_S3_PATH")
BRONZE_S3_PATH = os.getenv("BRONZE_S3_PATH")
LOCAL_PATH = "data/bronze/"
TABLE_NAME = "spotify_data"

default_args = {
    'owner': 'airflow',
    'depends_on_past': False,
    'start_date': datetime(2024, 4, 1),
    'email_on_failure': True,
    'email_on_retry': False,
    'retries': 1,
    'retry_delay': timedelta(minutes=5),
    'catchup': False
}

logger = logging.getLogger(__name__)

@dag(default_args=default_args, schedule_interval='@daily', catchup=False, tags=['spotify_bronze_etl'])
def spotify_bronze_etl_dag():
    """
    Airflow DAG to load, transform, and export data from raw JSON files into a DuckDB database,
    and then to local and S3 storage.
    """
    @task
    def load_and_transform_data():
        db_manager = DuckDBManager()
        data_manager = DataManager(db_manager, LOCAL_PATH, AWS_BUCKET_NAME, TABLE_NAME)
        logger.info(f"Initiating load and transform for {RAW_S3_PATH} into {TABLE_NAME}")
        try:
            data_manager.load_and_transform_data(RAW_S3_PATH, TABLE_NAME)
            logger.info(f"Successfully loaded and transformed data for {TABLE_NAME}")
        except Exception as e:
            logger.error(f"Failed to load and transform data for {TABLE_NAME}: {e}")
            raise

    @task
    def export_data():
        db_manager = DuckDBManager()
        aws_manager = AWSManager(db_manager, AWS_BUCKET_NAME, AWS_ACCESS_KEY, AWS_SECRET_ACCESS_KEY)
        data_manager = DataManager(db_manager, aws_manager, LOCAL_PATH, AWS_BUCKET_NAME)
        logger.info(f"Initiating data export for {TABLE_NAME}")
        try:
            data_manager.export_data(TABLE_NAME)
            logger.info(f"Successfully exported data for {TABLE_NAME}")
        except Exception as e:
            logger.error(f"Failed to export data for {TABLE_NAME}: {e}")
            raise

    load_and_transform_task = load_and_transform_data()
    export_task = export_data()

    load_and_transform_task >> export_task

# Instantiate the DAG
dag = spotify_bronze_etl_dag()
