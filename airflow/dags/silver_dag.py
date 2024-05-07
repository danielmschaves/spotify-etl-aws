from datetime import datetime, timedelta
import os
import sys
from airflow.decorators import dag, task
from dotenv import load_dotenv
import logging

# Set the system path to include the custom manager modules directory
sys.path.append(os.path.abspath(os.path.join(os.path.dirname(__file__), "..", "..")))
from manager import DuckDBManager, AWSManager, MotherDuckManager
from silver import DataManager

load_dotenv()
logger = logging.getLogger(__name__)

# Configuration loaded outside of tasks for better manageability
aws_access_key = os.getenv("AWS_ACCESS_KEY")
aws_secret_access_key = os.getenv("AWS_SECRET_ACCESS_KEY")
aws_region = os.getenv("AWS_REGION")
bronze_s3_path = os.getenv("BRONZE_S3_PATH")
silver_s3_path = os.getenv("SILVER_S3_PATH")
bronze_path = os.getenv("BRONZE_PATH")
local_path = "data/silver/"
database_name = "playlist"
silver_schema = "silver"
local_database = "memory"
remote_database = "playlist"
tables = ["playlists", "tracks", "albums", "artists"]

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

# Define the DAG
@dag(default_args=default_args, schedule_interval="@daily", catchup=False, tags=["silver_ingestion"])
def silver_ingestion():
    """
    Airflow DAG to load data from Parquet files into DuckDB, process it, and save it to various storage.
    """
    
    @task
    def process_data():
        # Initialize the data manager with configuration parameters
        db_manager = DuckDBManager()
        aws_manager = AWSManager(db_manager, aws_region, aws_access_key, aws_secret_access_key)
        motherduck_manager = MotherDuckManager(db_manager, os.getenv("MOTHERDUCK_TOKEN"))
        data_manager = DataManager(
            db_manager,
            local_database,
            remote_database,
            silver_schema,
            database_name,
            local_path,
            bronze_s3_path,
            silver_s3_path,
            bronze_path,
        )
        # Data processing steps
        try:
            logger.info("Creating table from bronze")
            data_manager.create_table_from_bronze(tables)
            logger.info("Saving data to local, S3, and MotherDuck")
            data_manager.save_to_local(tables)
            data_manager.save_to_s3(tables)
            data_manager.save_to_md(tables)
            logger.info("Data processing completed successfully")
        except Exception as e:
            logger.error(f"Error during data processing: {e}")
            raise

    # Schedule the task
    process_data()

# Instantiate the DAG
silver_ingestion_dag = silver_ingestion()
