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

            if isinstance(data, dict):  # Assuming the data is a single playlist dictionary
                self.handle_playlist(data, table_name)
            elif isinstance(data, list):  # Assuming a list of playlists
                for item in data:
                    self.handle_playlist(item, table_name)

        except json.JSONDecodeError as e:
            logger.error(f"JSON decoding error at {json_path}: {e}")
        except Exception as e:
            logger.error(f"An error occurred during data loading from {json_path}: {traceback.format_exc()}")

    def handle_playlist(self, playlist_data, table_name):
        """
        Handles the insertion of playlist data into the database.

        Args:
            playlist_data (dict): The playlist data to insert.
            table_name (str): The table where the data will be inserted.
        """
        try:
            playlist_info = {
                'id': playlist_data['id'],
                'name': playlist_data['name'],
                'description': playlist_data.get('description', ''),
                'owner_id': playlist_data['owner']['id'],
                'followers': playlist_data['followers']['total'],
                'public': playlist_data['public'],
            }

            field_definitions = ', '.join([f"{k} TEXT" for k in playlist_info.keys()])
            create_table_query = f"CREATE TABLE IF NOT EXISTS {table_name} ({field_definitions});"
            self.db_manager.execute_query(create_table_query)

            columns = ', '.join(playlist_info.keys())
            placeholders = ', '.join(['?' for _ in playlist_info])
            insert_query = f"INSERT INTO {table_name} ({columns}) VALUES ({placeholders})"
            self.db_manager.execute_query(insert_query, list(playlist_info.values()))

            # If tracks are present, handle them separately
            if 'tracks' in playlist_data:
                self.handle_tracks(playlist_data['tracks'], playlist_data['id'])

        except Exception as e:
            logger.error(f"Error processing playlist data for {table_name}: {traceback.format_exc()}")

    def handle_tracks(self, tracks_data, playlist_id):
        """
        Handles the insertion of track data related to a specific playlist into the database.

        Args:
            tracks_data (dict): The tracks data section from the playlist.
            playlist_id (str): The playlist ID to which these tracks are related.
        """
        try:
            track_table_name = "tracks"
            for track_item in tracks_data['items']:
                track_info = {
                    'track_id': track_item['track']['id'],
                    'name': track_item['track']['name'],
                    'playlist_id': playlist_id,
                }

                track_fields = ', '.join([f"{k} TEXT" for k in track_info.keys()])
                create_track_table_query = f"CREATE TABLE IF NOT EXISTS {track_table_name} ({track_fields});"
                self.db_manager.execute_query(create_track_table_query)

                track_columns = ', '.join(track_info.keys())
                track_placeholders = ', '.join(['?' for _ in track_info])
                insert_track_query = f"INSERT INTO {track_table_name} ({track_columns}) VALUES ({track_placeholders})"
                self.db_manager.execute_query(insert_track_query, list(track_info.values()))

        except Exception as e:
            logger.error(f"Error processing tracks for playlist {playlist_id}: {traceback.format_exc()}")

    def save_to_local(self, table_name) -> None:
        """
        Saves data to local disk in parquet format.
        """
        try:
            logger.info(f"Saving {table_name} table as parquet format locally")
            local_file_path = f"{self.local_path}/{table_name}.parquet"
            os.makedirs(os.path.dirname(local_file_path), exist_ok=True)
            query = f"""
                COPY (
                    SELECT *
                    FROM {table_name}
                )
                TO '{local_file_path}'
                (FORMAT PARQUET)
            """
            self.db_manager.execute_query(query)
            logger.success(f"{table_name} table saved locally as parquet!")
        except Exception as e:
            logger.error(f"Error saving {table_name} to local: {e}")

    def save_to_s3(self, table_name) -> None:
        """
        Saves data to Amazon S3 in parquet format.
        """
        try:
            logger.info(f"Saving {table_name} table to S3 as parquet")
            s3_file_path = f"s3://{self.s3_bucket}/{table_name}.parquet"
            query = f"""
                COPY (
                    SELECT *
                    FROM {table_name}
                )
                TO '{s3_file_path}'
                (FORMAT PARQUET)
            """
            self.db_manager.execute_query(query)
            logger.success(f"{table_name} table saved to S3!")
        except Exception as e:
            logger.error(f"Error saving {table_name} to S3: {e}")

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
        and exporting it to local and S3 storage.

        Args:
            json_path (str): Path to the JSON file containing the data.
            table_name (str): Name of the table to load data into.
        """
        logger.info(f"Starting the data ingestion process for table: {table_name}.")

        # Loading and Transforming Data
        try:
            self.data_manager.load_and_transform_data(json_path, table_name)
            logger.info(f"Data loaded and transformed successfully for table: {table_name}.")
        except Exception as e:
            logger.error(f"Error during data loading and transformation for {table_name}: {e}")
            return  # Exit if data loading fails to prevent further errors

        # Exporting Data to Local Storage
        try:
            self.data_manager.save_to_local(table_name)
            logger.success(f"Data successfully saved to local storage for table: {table_name}.")
        except Exception as e:
            logger.error(f"Error saving data to local storage for {table_name}: {e}")

        # Exporting Data to S3 Storage
        try:
            self.data_manager.save_to_s3(table_name)
            logger.success(f"Data successfully saved to S3 for table: {table_name}.")
        except Exception as e:
            logger.error(f"Error saving data to S3 for {table_name}: {e}")

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
