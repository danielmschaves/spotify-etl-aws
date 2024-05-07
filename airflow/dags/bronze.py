from dotenv import load_dotenv
import os
import sys
from loguru import logger
from pydantic import ValidationError
import traceback
import json
from typing import List, Optional, Dict
import boto3

sys.path.append(os.path.abspath(os.path.join(os.path.dirname(__file__), "..", "..")))
from manager import AWSManager, DuckDBManager, MotherDuckManager


class DataManager:
    """
    Manages data transformation and storage operations for Spotify data using DuckDB.
    Handles loading, validating, transforming, and exporting data to both local and AWS S3 storage.
    """

    def __init__(self, db_manager, aws_manager, local_path, bronze_s3_path, local_database: str, remote_database: str, bronze_schema: str):
        self.db_manager = db_manager
        self.aws_manager = aws_manager
        self.local_path = local_path
        self.bronze_s3_path = bronze_s3_path
        self.local_database = local_database
        self.remote_database = remote_database
        self.bronze_schema = bronze_schema
        self.s3_client = boto3.client('s3')
        

    def load_and_transform_data(self, json_path: str, table_name: str):
        """
        Loads and transforms data from a JSON file into a database table.
        Args:
            json_path (str): Path to the JSON file containing the data.
            table_name (str): Name of the table to create or insert data into.
        """
        logger.info(f"Attempting to load data from {json_path} into table {table_name}")
        try:
            with open(json_path, "r") as file:
                data = json.load(file)

            # Delegate data handling based on type, could be a list of playlists or a single playlist
            self.process_data(data, table_name)

        except json.JSONDecodeError as e:
            logger.error(f"JSON decoding error at {json_path}: {e}")
        except Exception as e:
            logger.error(
                f"An error occurred during data loading from {json_path}: {traceback.format_exc()}"
            )

    def process_data(self, data, table_name):
        """
        Processes data based on type, could be list of items or single item.
        """
        if isinstance(data, dict):  # Handling single playlist
            self.handle_playlist(data, table_name)
        elif isinstance(data, list):  # Handling multiple playlists
            for item in data:
                self.handle_playlist(item, table_name)

    def handle_playlist(self, playlist_data, table_name):
        """
        Handles the insertion of playlist data into the database.
        """
        try:
            playlist_info = {
                "id": playlist_data["id"],
                "name": playlist_data["name"],
                "description": playlist_data.get("description", ""),
                "owner_id": playlist_data["owner"]["id"],
                "followers": playlist_data["followers"]["total"],
                "public": playlist_data["public"],
            }
            self.insert_data("playlists", playlist_info)

            # Further data handling if tracks, albums, or artists are present
            if "tracks" in playlist_data:
                self.handle_tracks(playlist_data["tracks"], playlist_data["id"])

        except Exception as e:
            logger.error(
                f"Error processing playlist data for {table_name}: {traceback.format_exc()}"
            )

    def handle_tracks(self, tracks_data, playlist_id):
        """
        Handles the insertion of track data related to a specific playlist into the database.
        """
        try:
            track_table_name = "tracks"
            for track_item in tracks_data["items"]:
                track = track_item["track"]
                album = track["album"]
                artists = track["artists"]

                track_info = {
                    "track_id": track["id"],
                    "name": track["name"],
                    "playlist_id": playlist_id,
                    "album_id": album["id"] if album else None,
                    "duration_ms": track.get("duration_ms"),
                    "popularity": track.get("popularity"),
                    "explicit": track.get("explicit", False),
                    "track_number": track.get("track_number"),
                    "album_release_date": album.get("release_date") if album else None,
                    "artist_id": artists[0]["id"] if artists else None,
                }
                self.insert_data(track_table_name, track_info)

                # Assuming album and artists handling functions exist
                if album:
                    self.handle_album(album, track["id"])
                if artists:
                    self.handle_artists(artists, track["id"])

        except Exception as e:
            logger.error(
                f"Error processing tracks for playlist {playlist_id}: {traceback.format_exc()}"
            )

    def handle_album(self, album_data, track_id):
        """
        Handles the insertion of album data related to a specific track.
        """
        album_info = {
            "album_id": album_data["id"],
            "name": album_data["name"],
            "release_date": album_data["release_date"],
            "total_tracks": album_data["total_tracks"],
            "track_id": track_id,  # Linking album to track
        }
        self.insert_data("albums", album_info)

    def handle_artists(self, artists_data, track_id):
        """
        Handles the insertion of artist data related to a specific track.
        """
        for artist in artists_data:
            artist_info = {
                "artist_id": artist["id"],
                "name": artist["name"],
                "track_id": track_id,  # Linking artist to track
            }
            self.insert_data("artists", artist_info)

    def insert_data(self, table_name, data):
        """
        Generic method to insert data into the specified table.
        """
        field_definitions = ", ".join([f"{k} TEXT" for k in data.keys()])
        create_table_query = (
            f"CREATE TABLE IF NOT EXISTS {table_name} ({field_definitions});"
        )
        self.db_manager.execute_query(create_table_query)

        columns = ", ".join(data.keys())
        placeholders = ", ".join(["?" for _ in data])
        insert_query = f"INSERT INTO {table_name} ({columns}) VALUES ({placeholders})"
        self.db_manager.execute_query(insert_query, list(data.values()))

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
            result = self.db_manager.execute_query(query)
            logger.info(f"Query result: {result}")
            if os.path.exists(local_file_path):
                logger.success(f"{table_name} table saved locally as parquet at {local_file_path}")
            else:
                logger.error(f"File was not created at {local_file_path}")
        except Exception as e:
            logger.error(f"Error saving {table_name} to local: {e}", exc_info=True)

    def save_to_s3(self, table_name) -> None:
        """
        Manually uploads a parquet file from local disk to Amazon S3.
        Args:
            table_name (str): The name of the table to save.
        Returns:
            None
        """
        local_file_path = os.path.join(self.local_path, f"{table_name}.parquet")
        s3_file_path = f"{self.bronze_s3_path}{table_name}.parquet".replace('s3://', '')

        if not os.path.exists(local_file_path):
            logger.error(f"Local file {local_file_path} does not exist.")
            return

        bucket = s3_file_path.split('/')[0]
        key = '/'.join(s3_file_path.split('/')[1:])

        try:
            with open(local_file_path, 'rb') as data:
                self.s3_client.upload_fileobj(data, bucket, key)
            logger.success(f"Successfully saved {table_name} to S3 at {s3_file_path}")
        except Exception as e:
            logger.error(f"Error uploading {table_name} to S3: {e}")


    # def save_to_s3(self, table_name) -> None:
    #     """
    #     Saves data to Amazon S3 in parquet format.
    #     Args:
    #         table_name (str): The name of the table to save.
    #     Returns:
    #         None
    #     """
    #     try:
    #     # Construct the full S3 path for the file
    #         s3_file_path = f"{self.bronze_s3_path}{table_name}.parquet"
    #         logger.info(f"Full S3 path to save: {s3_file_path}")
            
    #         query = f"""
    #             COPY (
    #                 SELECT *
    #                 FROM {table_name}
    #             )
    #             TO '{s3_file_path}'
    #             WITH (FORMAT PARQUET);
    #         """
    #         logger.info(f"Executing query to save {table_name} to S3 at {s3_file_path}")
    #         self.db_manager.execute_query(query)
    #         logger.success(f"Successfully saved {table_name} to S3 at {s3_file_path}")
    #     except Exception as e:
    #         logger.error(f"Error saving {table_name} to S3: {e}")

    def save_to_md(self, table_name) -> None:
        """
        Saves data to MotherDuck for the specified table.

        Args:
            table_name (str): The name of the table to save.
        """
        try:
            logger.info(f"Saving {table_name} table to Mother Duck")
            self.db_manager.execute_query(
                f"CREATE DATABASE IF NOT EXISTS {self.remote_database}"
            )
            self.db_manager.execute_query(
                f"CREATE SCHEMA IF NOT EXISTS {self.remote_database}.{self.bronze_schema};"
            )
            query = f"""
                CREATE OR REPLACE TABLE {self.remote_database}.{self.bronze_schema}.{table_name} AS
                    SELECT
                        *
                    FROM {self.local_database}.{table_name};
                """
            self.db_manager.execute_query(query)
            logger.info(f"{table_name} table saved to MotherDuck!")
        except Exception as e:
            logger.error(f"Error saving {table_name} to MotherDuck: {traceback.format_exc()}")


class Ingestor:
    """
    Orchestrates the entire data ingestion process.
    """

    def __init__(self, db_manager, aws_manager, data_manager, motherduck_manager):
        """
        Initializes Ingestor with database and AWS managers.
        """
        self.db_manager = db_manager
        self.aws_manager = aws_manager
        self.data_manager = data_manager
        self.motherduck_manager = motherduck_manager

    def execute(self, json_path, playlist_table):
        """
        Executes the data ingestion process including loading data, transforming it,
        and exporting it to local and S3 storage.

        Args:
            json_path (str): Path to the JSON file containing the data.
            playlist_table (str): Name of the playlist table to load data into.
            Other tables such as 'tracks', 'albums', and 'artists' are handled internally.
        """
        logger.info(f"Starting the data ingestion process for {playlist_table}.")

        # Loading and Transforming Data
        try:
            data = self.data_manager.load_and_transform_data(json_path, playlist_table)
            logger.info(
                f"Data loaded and transformed successfully for {playlist_table}."
            )
        except Exception as e:
            logger.error(
                f"Error during data loading and transformation for {playlist_table}: {e}"
            )
            return  # Exit if data loading fails to prevent further errors

        # Prepare tables for export
        tables = ["playlists", "tracks", "albums", "artists"]

        # Exporting Data to Local Storage
        for table in tables:
            try:
                self.data_manager.save_to_local(table)
                logger.success(
                    f"Data successfully saved to local storage for table: {table}."
                )
            except Exception as e:
                logger.error(
                    f"Error saving data to local storage for table {table}: {e}"
                )

        # Exporting Data to S3 Storage
        for table in tables:
            try:
                self.data_manager.save_to_s3(table)
                logger.success(f"Data successfully saved to S3 for table: {table}.")
            except Exception as e:
                logger.error(f"Error saving data to S3 for table {table}: {e}")

        # Exporting Data to MotherDuck
        for table in tables:
            try:
                self.data_manager.save_to_md(table)
                logger.success(f"Data successfully saved to MotherDuck for table: {table}.")
            except Exception as e:
                logger.error(f"Error saving data to MotherDuck for table {table}: {e}")


if __name__ == "__main__":
    # Load environment variables
    load_dotenv()

    # Define configuration settings from environment variables
    json_path = os.getenv("JSON_PATH")
    table_name = os.getenv("TABLE_NAME", "spotify_data")
    local_path = os.getenv("LOCAL_PATH", "/data/bronze/")
    aws_region = os.getenv("AWS_REGION")
    aws_access_key = os.getenv("AWS_ACCESS_KEY")
    aws_secret_access_key = os.getenv("AWS_SECRET_ACCESS_KEY")
    playlist_table = os.getenv("TABLE_NAME", "playlists")
    motherduck_token = os.getenv("MOTHERDUCK_TOKEN")
    local_database = "memory"
    remote_database = "playlist"
    bronze_schema = "bronze"
    bronze_s3_path = os.getenv("BRONZE_S3_PATH")
    

    # Initialize database and AWS managers
    db_manager = DuckDBManager()
    aws_manager = AWSManager(
        db_manager, aws_region, aws_access_key, aws_secret_access_key
    )
    motherduck_manager = MotherDuckManager(db_manager, motherduck_token)

    # Initialize the data manager with all necessary managers and paths
    data_manager = DataManager(db_manager, aws_manager, local_path, bronze_s3_path, local_database, remote_database, bronze_schema)

    # Initialize the ingestor with all managers
    ingestor = Ingestor(db_manager, aws_manager, data_manager, motherduck_manager)

    # Execute the data ingestion process
    try:
        ingestor.execute(json_path, playlist_table)
        logger.info("Data ingestion completed successfully.")
    except Exception as e:
        logger.error(f"An error occurred during data ingestion: {e}")
