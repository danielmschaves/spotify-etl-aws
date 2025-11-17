"""Data manager for bronze layer transformations."""

import os
import json
import traceback
from typing import Dict, List, Any, Optional
from loguru import logger
import chardet
from botocore.exceptions import BotoCoreError, ClientError

from ...managers.duckdb import DuckDBManager
from ...managers.aws import AWSManager
from ...ingestion.base import BaseDataManager


class DataManager(BaseDataManager):
    """
    Manages data transformation and storage operations for Spotify data using DuckDB.

    Handles loading, validating, transforming, and exporting data to both local and AWS S3 storage.
    """

    def __init__(
        self,
        db_manager: DuckDBManager,
        aws_manager: AWSManager,
        local_path: str,
        bronze_s3_path: str,
        local_database: str,
        remote_database: str,
        bronze_schema: str,
    ):
        """
        Initialize DataManager.

        Args:
            db_manager: DuckDB manager instance.
            aws_manager: AWS manager instance.
            local_path: Local path for saving data.
            bronze_s3_path: S3 path for bronze data.
            local_database: Local database name.
            remote_database: Remote database name.
            bronze_schema: Bronze schema name.
        """
        self.db_manager = db_manager
        self.aws_manager = aws_manager
        self.local_path = local_path
        self.bronze_s3_path = bronze_s3_path
        self.local_database = local_database
        self.remote_database = remote_database
        self.bronze_schema = bronze_schema
        self.s3_client = self.aws_manager.s3_client

    def load_and_transform_data(
        self, raw_bucket: str, json_key: str, table_name: str
    ) -> None:
        """
        Loads and transforms data from a JSON file in S3.

        Args:
            raw_bucket: Name of the S3 bucket.
            json_key: The key (path) in the S3 bucket where the JSON file is stored.
            table_name: Name of the table to create or insert data into.
        """
        s3_file_path = f"{raw_bucket}/{json_key}"
        logger.info(
            f"Attempting to load and process data from {s3_file_path} into table {table_name}"
        )

        try:
            # Fetch the object from S3
            response = self.s3_client.get_object(Bucket=raw_bucket, Key=json_key)
            json_bytes = response["Body"].read()

            # Detect encoding using chardet
            detected_encoding = chardet.detect(json_bytes)["encoding"]
            logger.info(f"Detected encoding: {detected_encoding}")

            # Decode using detected encoding, fallback to ISO-8859-1 if necessary
            try:
                json_string = json_bytes.decode(detected_encoding)
            except UnicodeDecodeError:
                json_string = json_bytes.decode("iso-8859-1", errors="replace")
                logger.warning(f"Fallback decoding with replacement for {json_key}")

            # Load data into a JSON object
            data = json.loads(json_string)

            # Process the data
            self.process_data(data, table_name)
            logger.info(f"Data successfully loaded and processed for table {table_name}")

        except json.JSONDecodeError as e:
            logger.error(f"JSON decoding error for {json_key} in {raw_bucket}: {e}")
            raise
        except (BotoCoreError, ClientError) as e:
            logger.error(f"An AWS error occurred while accessing {s3_file_path}: {e}")
            raise
        except Exception as e:
            logger.error(
                f"An unexpected error occurred while loading data from {s3_file_path}: {e}"
            )
            raise

    def process_data(self, data: Any, table_name: str) -> None:
        """
        Processes data based on type, could be list of items or single item.

        Args:
            data: The data to process (dict or list).
            table_name: The name of the table to insert data into.
        """
        if isinstance(data, dict):  # Handling single playlist
            self.handle_playlist(data, table_name)
        elif isinstance(data, list):  # Handling multiple playlists
            for item in data:
                self.handle_playlist(item, table_name)

    def handle_playlist(self, playlist_data: Dict[str, Any], table_name: str) -> None:
        """
        Handles the insertion of playlist data into the database.

        Args:
            playlist_data: The playlist data to insert.
            table_name: The name of the table to insert data into.
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
            raise

    def handle_tracks(self, tracks_data: Dict[str, Any], playlist_id: str) -> None:
        """
        Handles the insertion of track data related to a specific playlist.

        Args:
            tracks_data: The track data to insert.
            playlist_id: The ID of the playlist the tracks belong to.
        """
        try:
            track_table_name = "tracks"
            for track_item in tracks_data["items"]:
                track = track_item["track"]
                album = track.get("album")
                artists = track.get("artists", [])

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

                if album:
                    self.handle_album(album, track["id"])
                if artists:
                    self.handle_artists(artists, track["id"])

        except Exception as e:
            logger.error(
                f"Error processing tracks for playlist {playlist_id}: {traceback.format_exc()}"
            )
            raise

    def handle_album(self, album_data: Dict[str, Any], track_id: str) -> None:
        """
        Handles the insertion of album data related to a specific track.

        Args:
            album_data: The album data to insert.
            track_id: The ID of the track the album is related to.
        """
        album_info = {
            "album_id": album_data["id"],
            "name": album_data["name"],
            "release_date": album_data["release_date"],
            "total_tracks": album_data["total_tracks"],
            "track_id": track_id,
        }
        self.insert_data("albums", album_info)

    def handle_artists(self, artists_data: List[Dict[str, Any]], track_id: str) -> None:
        """
        Handles the insertion of artist data related to a specific track.

        Args:
            artists_data: The artist data to insert.
            track_id: The ID of the track the artists are related to.
        """
        for artist in artists_data:
            artist_info = {
                "artist_id": artist["id"],
                "name": artist["name"],
                "track_id": track_id,
            }
            self.insert_data("artists", artist_info)

    def insert_data(self, table_name: str, data: Dict[str, Any]) -> None:
        """
        Generic method to insert data into the specified table.

        Args:
            table_name: The name of the table to insert data into.
            data: The data to insert.
        """
        field_definitions = ", ".join([f"{k} TEXT" for k in data.keys()])
        create_table_query = (
            f"CREATE TABLE IF NOT EXISTS {table_name} ({field_definitions});"
        )
        self.db_manager.execute_query(create_table_query)

        columns = ", ".join(data.keys())
        placeholders = ", ".join(["?" for _ in data])
        insert_query = f"INSERT INTO {table_name} ({columns}) VALUES ({placeholders})"
        self.db_manager.execute_query(insert_query, tuple(data.values()))

    def save_to_local(self, table_name: str) -> None:
        """
        Saves data to local disk in parquet format.

        Args:
            table_name: The name of the table to save.
        """
        try:
            logger.info(f"Saving {table_name} table as parquet format locally")
            local_file_path = os.path.join(self.local_path, f"{table_name}.parquet")
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
            if os.path.exists(local_file_path):
                logger.success(
                    f"{table_name} table saved locally as parquet at {local_file_path}"
                )
            else:
                logger.error(f"File was not created at {local_file_path}")
        except Exception as e:
            logger.error(f"Error saving {table_name} to local: {e}", exc_info=True)
            raise

    def save_to_s3(self, table_name: str) -> None:
        """
        Uploads a parquet file from local disk to Amazon S3.

        Args:
            table_name: The name of the table to save.
        """
        local_file_path = os.path.join(self.local_path, f"{table_name}.parquet")
        s3_file_path = f"{self.bronze_s3_path}{table_name}.parquet".replace("s3://", "")

        if not os.path.exists(local_file_path):
            logger.error(f"Local file {local_file_path} does not exist.")
            return

        bucket = s3_file_path.split("/")[0]
        key = "/".join(s3_file_path.split("/")[1:])

        try:
            with open(local_file_path, "rb") as data:
                self.s3_client.upload_fileobj(data, bucket, key)
            logger.success(f"Successfully saved {table_name} to S3 at {s3_file_path}")
        except Exception as e:
            logger.error(f"Error uploading {table_name} to S3: {e}")
            raise

    def save_to_md(self, table_name: str) -> None:
        """
        Saves data to MotherDuck for the specified table.

        Args:
            table_name: The name of the table to save.
        """
        try:
            logger.info(f"Saving {table_name} table to MotherDuck")
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
            raise

