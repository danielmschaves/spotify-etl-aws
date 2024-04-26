from dotenv import load_dotenv
import os
import sys
from loguru import logger
from pydantic import ValidationError
import traceback
import json
from typing import List, Optional, Dict

sys.path.append(os.path.abspath(os.path.join(os.path.dirname(__file__), "..", "..")))
from manager import AWSManager, DuckDBManager
from model import Artist, Album, Track, Playlist, Image

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

    def load_and_transform_data(self, json_path: str, table_name: str):
        """
        Loads and transforms data from a JSON file into a database table.

        Args:
            json_path (str): Path to the JSON file containing the data.
            table_name (str): Name of the table to create or insert data into.  
        """
        logger.info(f"Attempting to load data from {json_path} into table {table_name}")
        try:
            with open(json_path, 'r') as file:
                data = json.load(file)

            model_mapping = {
                "playlists": Playlist,
                "artists": Artist,
                "albums": Album,
                "tracks": Track
            }

            model = model_mapping.get(table_name)
            if not model:
                raise ValueError(f"Unsupported table name: {table_name}")

            # Use the from_dict method for direct model construction if applicable
            if isinstance(data, list):
                validated_data = [model.from_dict(item) for item in data]
            else:
                validated_data = model.from_dict(data)

                # Create table dynamically based on model schema if not exists
                # Access field names directly for SQL schema creation
                field_definitions = ', '.join([
                    f"{name} {field_type.__name__ if hasattr(field_type, '__name__') else 'TEXT'}"
                    for name, field_type in model.__annotations__.items()
                ])
                if not field_definitions:
                    logger.warning(f"Model '{model.__name__}' has no defined fields. Fields found: {model.__fields__.keys()}. Skipping table creation for {table_name}.")
                    return  # Exit if no fields are found
                
                create_table_query = f"CREATE TABLE IF NOT EXISTS {table_name} ({field_definitions});"
                self.db_manager.execute_query(create_table_query)

            # Insert validated data into the database
            for item in validated_data:
                item_dict = item.dict()
                columns = ', '.join(item_dict.keys())
                placeholders = ', '.join(['?' for _ in item_dict.values()])
                values = tuple(item_dict.values())
                insert_query = f"INSERT INTO {table_name} ({columns}) VALUES ({placeholders})"
                try:
                    self.db_manager.execute_query(insert_query, values)
                except TypeError:
                    logger.error("Error executing query with parameters. Check the `execute_query` method signature.")

        except json.JSONDecodeError as e:
            logger.error(f"JSON decoding error: {e}")
        except ValidationError as e:
            logger.error(f"Validation error during data loading: {e}")
        except Exception as e:
            logger.error(f"An error occurred during data loading: {traceback.format_exc()}")

    def export_data(self, table_name):
        """
        Exports data from DuckDB to local storage and S3.
        """
        logger.info(f"Attempting to export data from table {table_name}")
        local_file = os.path.join(self.local_path, f"{table_name}.parquet")
        try:
            self.db_manager.execute_query(f"COPY {table_name} TO '{local_file}' (FORMAT 'parquet');")
            logger.info(f"Data exported to local Parquet file at {local_file}.")
            self.aws_manager.upload_file_to_s3(local_file, self.s3_bucket, f"{table_name}.parquet")
            # Verify local and S3 file existence
            if os.path.exists(local_file):
                logger.info("Local file export confirmed.")
            else:
                logger.error("Local file export failed.")
            if self.aws_manager.check_file_exists(self.s3_bucket, f"{table_name}.parquet"):
                logger.info("S3 file export confirmed.")
            else:
                logger.error("S3 file export failed.")
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
            self.data_manager.load_and_transform_data(json_path, table_name)
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
