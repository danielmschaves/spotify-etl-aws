from datetime import datetime, timedelta
import os
import sys
from airflow.decorators import dag, task
from airflow.operators.trigger_dagrun import TriggerDagRunOperator
from dotenv import load_dotenv
import logging

sys.path.append(os.path.abspath(os.path.join(os.path.dirname(__file__), "..", "..")))

from manager import DuckDBManager, AWSManager, MotherDuckManager
from bronze import DataManager

# Load environment variables
load_dotenv()

# Configuration
aws_access_key = os.getenv("AWS_ACCESS_KEY")
aws_secret_access_key = os.getenv("AWS_SECRET_ACCESS_KEY")
bronze_bucket = os.getenv("BRONZE_BUCKET")
aws_region = os.getenv("AWS_REGION")
raw_s3_path = os.getenv("RAW_S3_PATH")
local_path = os.getenv("LOCAL_PATH")
table_names = ["playlists", "tracks", "albums", "artists"]
motherduck_token = os.getenv("MOTHERDUCK_TOKEN")
local_database = "memory"
remote_database = "playlist"
bronze_schema = "bronze"
bronze_s3_path = os.getenv("BRONZE_S3_PATH")


default_args = {
    "owner": "airflow",
    "depends_on_past": False,
    "start_date": datetime(2024, 4, 1),
    "email_on_failure": False,
    "email_on_retry": False,
    "retries": 1,
    "retry_delay": timedelta(minutes=5),
    "catchup": False,
}

logger = logging.getLogger("airflow.task")

@dag(default_args=default_args, schedule_interval="@daily", catchup=False, tags=["bronze_ingestion"])
def bronze_ingestion():
    """
    Airflow DAG to load, transform, and export data from raw JSON files into a DuckDB database,
    and then to local and S3 storage.
    """
    @task
    def bronze_playlist():
               
        try:
            # Initialize database and AWS managers
            logger.info(f"Initializing database and AWS managers")
            db_manager = DuckDBManager()
            motherduck_manager = MotherDuckManager(db_manager, motherduck_token)
            aws_manager = AWSManager(db_manager, aws_region, aws_access_key, aws_secret_access_key)
            data_manager = DataManager(db_manager, aws_manager, local_path, bronze_s3_path, local_database, remote_database, bronze_schema)
            logger.info("Start loading and transforming process")
            data_manager.load_and_transform_data(raw_s3_path, "spotify_data")
        except Exception as e:
            logger.error(f"Error loading and transforming data: {e}")
            raise

        for table_name in table_names:
            try:
                logger.info(f"Saving {table_name} table to local storage")
                data_manager.save_to_local(table_name)
                logger.info(f"Saving {table_name} table to S3 storage")
                data_manager.save_to_s3(table_name)
                logger.info(f"Saving {table_name} table to MotherDuck")
                data_manager.save_to_md(table_name)
            except Exception as e:
                logger.error(f"Error in the ingestion process for {table_name}: {e}")
                raise

    bronze_playlist_task = bronze_playlist()

# Instantiate the DAG
bronze_ingestion_dag = bronze_ingestion()

    # # Define task to trigger the silver DAG
    # trigger_silver_ingestion = TriggerDagRunOperator(
    #     task_id="trigger_silver_ingestion",
    #     trigger_dag_id="silver_ingestion",
    #     wait_for_completion=True,
    #     deferrable=True,
    # )

    # bronze_playlist_task >> trigger_silver_ingestion

# Instantiate the DAG