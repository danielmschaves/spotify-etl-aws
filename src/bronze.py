from dotenv import load_dotenv
import os
import sys
from loguru import logger

sys.path.append(os.path.abspath(os.path.join(os.path.dirname(__file__), "..", "..")))

from aws_manager import AWSManager
from duckdb_manager import DuckDBManager

class DataManager:
    """
    Manages data transformation and storage operations for Spotify data using DuckDB.
    Handles loading, validating, transforming, and exporting data to both local and AWS S3 storage.
    """

    def __init__(self, db_manager, aws_manager, local_path, s3_bucket):
        self.db_manager = db_manager
        self.aws_manager = aws_manager
        self.local_path = local_path
        self.s3_bucket = s3_bucket

    def load_and_transform_data(self, json_path, table_name):
        """
        Loads and transforms data from JSON file into DuckDB, preparing it for export.
        """
        logger.info(f"Starting to load data from {json_path} into table {table_name}")
        # Attempt to create a table if not exists and then insert new data only
        try:
            self.db_manager.execute_query(f"CREATE TABLE IF NOT EXISTS {table_name} (id VARCHAR PRIMARY KEY, data JSON);")
            load_query = f"""
            INSERT INTO {table_name}
            SELECT * FROM read_json_auto('{json_path}')
            WHERE id NOT IN (SELECT id FROM {table_name});
            """
            self.db_manager.execute_query(load_query)
            logger.info(f"Data loaded and transformed in table {table_name}.")
        except Exception as e:
            logger.error(f"Failed to load and transform data: {e}")

    def export_data(self, table_name):
        """
        Exports data from DuckDB to local storage and S3.
        """
        local_file = os.path.join(self.local_path, f"{table_name}.parquet")
        try:
            self.db_manager.execute_query(f"COPY {table_name} TO '{local_file}' (FORMAT 'parquet');")
            logger.info(f"Data exported to local Parquet file at {local_file}.")
            self.aws_manager.upload_file_to_s3(local_file, self.s3_bucket, f"{table_name}.parquet")
            logger.info("Data exported to S3 storage.")
        except Exception as e:
            logger.error(f"Failed to export data: {e}")

class Ingestor:
    """
    Orchestrates the entire data ingestion process.
    """

    def __init__(self, db_manager, aws_manager, data_manager):
        """
        Initializes Ingestor with database and AWS managers.
        """
        self.db_manager = db_manager
        self.aws_manager = aws_manager
        self.data_manager = data_manager

    def execute(self, json_path, table_name):
        """
        Executes the data ingestion process including loading data, transforming it,
        exporting it to local and S3 storage.
        """
        logger.info("Starting the data ingestion process.")
        try:
            self.data_manager.process_data(json_path, table_name)
            logger.success("Data ingestion completed successfully.")
        except Exception as e:
            logger.error(f"Error during data ingestion process: {e}")

if __name__ == "__main__":
    # Load environment variables
    load_dotenv()

    # Define configuration settings from environment variables
    json_path = os.getenv("JSON_PATH", "path_to_json")
    table_name = os.getenv("TABLE_NAME", "spotify_data")
    local_path = os.getenv("LOCAL_PATH", "/data/bronze/")
    s3_bucket = os.getenv("AWS_BUCKET_NAME")
    aws_region = os.getenv("AWS_REGION")
    aws_access_key = os.getenv("AWS_ACCESS_KEY")
    aws_secret_access_key = os.getenv("AWS_SECRET_ACCESS_KEY")

    # Initialize database and AWS managers
    db_manager = DuckDBManager()
    aws_manager = AWSManager(db_manager, aws_region, aws_access_key, aws_secret_access_key)

    # Initialize the data manager with all necessary managers and paths
    data_manager = DataManager(db_manager, aws_manager, local_path, s3_bucket)

    # Initialize the ingestor with all managers
    ingestor = Ingestor(db_manager, aws_manager, data_manager)

    # Execute the data ingestion process
    ingestor.execute(json_path, table_name)
