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
from model import Artist, Album, Track, Playlist, PlaylistTrack

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
        logger.info(f"Attempting to load data from {json_path} into table {table_name}")
        try:
            with open(json_path, 'r') as file:
                data = json.load(file)
            logger.debug("JSON data loaded successfully")

            models = {
                "playlists": Playlist,
                "artists": Artist,
                "albums": Album,
                "tracks": Track
            }

            if table_name not in models:
                raise ValueError(f"Unsupported table name provided: {table_name}")
            model = models[table_name]
            logger.debug(f"Model {model.__name__} selected for table {table_name}")

            # Handle single playlist object
            if isinstance(data, dict):
                # Extract owner_id (if not already defined, make it optional)
                owner_id = data.get('owner', {}).get('id')

                # Extract follower count and convert to integer
                followers = data.get('followers', {}).get('total')

                # Call the _extract_tracks function to process tracks data
                tracks = self._extract_tracks(data)

                # Create validated_data with extracted information
                validated_data = model(
                    id=data.get('id'),
                    name=data.get('name'),
                    description=data.get('description'),
                    # ... other fields ...
                    owner_id=owner_id,
                    followers=followers,
                    tracks=tracks
                )

            # Handle list of playlist objects (not implemented here for simplicity)
            elif isinstance(data, list):
                validated_data = [model(**playlist) for playlist in data]
            else:
                raise ValueError(f"Unexpected data type for table {table_name}: {type(data)}")

            field_definitions = ', '.join([f"{name} {field.type.__name__}" for name, field in model.__fields__.items() if not isinstance(field.type_, pydantic.fields.UndefinedType)])
            create_table_query = f"CREATE TABLE IF NOT EXISTS {table_name} ({field_definitions});"
            logger.debug(f"SQL Create Table Query: {create_table_query}")
            self.db_manager.execute_query(create_table_query)

            for item in [item.dict() for item in validated_data]:
                columns = ', '.join(item.keys())
                placeholders = ', '.join(['?' for _ in item])
                values = tuple(item.values())
                insert_query = f"INSERT INTO {table_name} ({columns}) VALUES ({placeholders})"
                logger.debug(f"SQL Insert Query: {insert_query}")
                self.db_manager.execute_query(insert_query)

            record_count = self.db_manager.execute_query(f"SELECT COUNT(*) FROM {table_name};")[0][0]
            logger.info(f"{record_count} records loaded into {table_name}")

        except json.JSONDecodeError as e:
            logger.error(f"JSON decode error in {json_path}: {e}")
        except ValidationError as e:
            logger.error(f"Validation error using {model.__name__}: {e}")
        except Exception as e:
            logger.error(f"Unexpected error during loading data to {table_name}: {e}, Trace: {traceback.format_exc()}")

    def _extract_tracks(self, data: dict) -> List[PlaylistTrack]:
        tracks = []
        if 'tracks' in data and 'items' in data['tracks']:
            for item in data['tracks']['items']:
                try:
                    track_data = item.get('track')  # Assuming 'track' is the key
                    if track_data:  # Check if track_data is not None
                        # Create PlaylistTrack object with extracted data
                        tracks.append(PlaylistTrack(
                            id=track_data.get('id'),
                            name=track_data.get('name'),
                            # Add other relevant track fields based on your data
                        ))
                except (AttributeError, KeyError):
                    pass  # Handle missing keys gracefully
        return tracks

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
