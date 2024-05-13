from datetime import datetime, timedelta
import os
import sys
import logging
from airflow.decorators import dag, task
from manager import DuckDBManager, AWSManager, MotherDuckManager
from bronze import DataManager

# Load environment variables from a .env file if present
from dotenv import load_dotenv
load_dotenv()

# Setup logging
logger = logging.getLogger("airflow.task")

# Airflow DAG arguments
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

# DAG definition
@dag(default_args=default_args, schedule_interval="@daily", catchup=False, tags=["bronze_ingestion"])
def bronze_ingestion():
    """
    Airflow DAG to load, transform, and export data from raw JSON files into a DuckDB database,
    and then to local and S3 storage.
    """

    @task
    def bronze_playlist():
        # Configuration
        aws_access_key = os.getenv("AWS_ACCESS_KEY")
        aws_secret_access_key = os.getenv("AWS_SECRET_ACCESS_KEY")
        aws_region = os.getenv("AWS_REGION")
        local_path = os.getenv("BRONZE_LOCAL_PATH")
        motherduck_token = os.getenv("MOTHERDUCK_TOKEN")
        local_database = "memory"
        remote_database = "playlist"
        bronze_schema = "bronze"
        table_names = ["playlists", "tracks", "albums", "artists"]
        bronze_s3_path = os.getenv("BRONZE_S3_PATH")
        json_key = os.getenv("JSON_KEY")
        raw_bucket = os.getenv("RAW_BUCKET")
        playlist_table = os.getenv("TABLE_NAME", "playlists")

        try:
            # Ensure local directory exists
            if not os.path.exists(local_path):
                os.makedirs(local_path)
                logger.info(f"Created local directory at {local_path}")

            # Initialize database and AWS managers
            logger.info("Initializing database and AWS managers")
            db_manager = DuckDBManager()
            aws_manager = AWSManager(db_manager, aws_region, aws_access_key, aws_secret_access_key)
            motherduck_manager = MotherDuckManager(db_manager, motherduck_token)
            data_manager = DataManager(db_manager, aws_manager, local_path, bronze_s3_path, local_database, remote_database, bronze_schema)

            # Start loading and transforming process
            logger.info("Start loading and transforming process")
            try:
                data_manager.load_and_transform_data(raw_bucket, json_key, playlist_table)
            except Exception as e:
                logger.error(f"Failed to load and transform data: {e}", exc_info=True)
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
        except Exception as e:
            logger.error(f"Error during the bronze playlist task: {e}")
            raise

    # Instantiate and schedule the bronze_playlist task
    bronze_playlist_task = bronze_playlist()

# Instantiate the DAG
bronze_ingestion_dag = bronze_ingestion()
