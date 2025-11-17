"""Data manager for silver layer transformations."""

import os
import traceback
from typing import List, Dict
from loguru import logger

from ...managers.duckdb import DuckDBManager
from ...managers.aws import AWSManager
from ...ingestion.base import BaseDataManager


class DataManager(BaseDataManager):
    """
    Manages data operations for loading, transforming, and saving data from parquet files in S3.

    Attributes:
        db_manager: Instance of DuckDBManager for executing DuckDB queries.
        aws_manager: Instance of AWSManager for interacting with AWS services.
        local_database: Name of the local DuckDB database.
        remote_database: Name of the remote database (e.g., for MotherDuck).
        silver_schema: Schema for silver-level data.
        database_name: Name of the database.
        local_path: Local path for saving data.
        bronze_s3_path: S3 path for bronze-level data.
        silver_s3_path: S3 path for silver-level data.
        bronze_bucket: Name of the S3 bucket for bronze-level data.
        s3_client: AWS S3 client for interacting with S3.
    """

    def __init__(
        self,
        db_manager: DuckDBManager,
        aws_manager: AWSManager,
        local_database: str,
        remote_database: str,
        silver_schema: str,
        database_name: str,
        local_path: str,
        bronze_s3_path: str,
        silver_s3_path: str,
        bronze_bucket: str,
    ):
        """
        Initialize the DataManager with the required managers and paths.

        Args:
            db_manager: Instance of DuckDBManager for executing DuckDB queries.
            aws_manager: Instance of AWSManager for interacting with AWS services.
            local_database: Name of the local DuckDB database.
            remote_database: Name of the remote database.
            silver_schema: Schema for silver-level data.
            database_name: Name of the database.
            local_path: Local path for saving data.
            bronze_s3_path: S3 path for bronze-level data.
            silver_s3_path: S3 path for silver-level data.
            bronze_bucket: Name of the S3 bucket for bronze-level data.
        """
        self.db_manager = db_manager
        self.aws_manager = aws_manager
        self.local_database = local_database
        self.remote_database = remote_database
        self.silver_schema = silver_schema
        self.database_name = database_name
        self.local_path = local_path
        self.bronze_s3_path = bronze_s3_path
        self.silver_s3_path = silver_s3_path
        self.bronze_bucket = bronze_bucket
        self.s3_client = self.aws_manager.s3_client

    def create_table_from_bronze(self, tables: List[str]) -> None:
        """
        Creates tables in DuckDB from the bronze tables in S3.

        Args:
            tables: List of table names to create.
        """
        table_configs = {
            "playlists": {
                "columns": "id, name, description, owner_id, followers, public"
            },
            "tracks": {
                "columns": "track_id, name, playlist_id, album_id, duration_ms, popularity, explicit, track_number, album_release_date, artist_id"
            },
            "albums": {"columns": "album_id, name, release_date, total_tracks, track_id"},
            "artists": {"columns": "artist_id, name, track_id"},
        }

        for table in tables:
            if table in table_configs:
                details = table_configs[table]
                s3_file_path = f"s3://{self.bronze_bucket}/{table}.parquet"
                try:
                    logger.debug(f"Read parquet file from: {s3_file_path}")
                    logger.info(
                        f"Creating table {table} in {self.local_database} schema with specific fields"
                    )
                    query = f"""
                        CREATE OR REPLACE TABLE {self.local_database}.{table} AS
                        SELECT {details['columns']}
                        FROM read_parquet('{s3_file_path}')
                    """
                    self.db_manager.execute_query(query)
                    logger.success(
                        f"Table {table} created in {self.local_database} schema with fields specified"
                    )
                except Exception as e:
                    logger.error(
                        f"Error creating table {table} in {self.local_database} schema: {e}"
                    )
                    raise

    def save_to_local(self, tables: List[str]) -> None:
        """
        Saves the specified tables to local disk in parquet format.

        Args:
            tables: List of table names to save locally.
        """
        for table in tables:
            try:
                local_file_path = os.path.join(self.local_path, f"{table}.parquet")
                logger.info(f"Saving table {table} to local disk at {local_file_path}")
                os.makedirs(os.path.dirname(local_file_path), exist_ok=True)
                query = f"""
                    COPY (
                        SELECT * 
                        FROM {self.local_database}.{table}
                    ) 
                    TO '{local_file_path}' (FORMAT 'parquet')
                """
                self.db_manager.execute_query(query)
                logger.success(f"Table {table} saved to local disk as parquet")
            except Exception as e:
                logger.error(f"Error saving table {table} to local disk: {e}")
                raise

    def save_to_s3(self, tables: List[str]) -> None:
        """
        Uploads the specified tables from local disk to Amazon S3 in parquet format.

        Args:
            tables: List of table names to upload to S3.
        """
        for table in tables:
            local_file_path = os.path.join(self.local_path, f"{table}.parquet")
            s3_file_path = f"{self.silver_s3_path}/{table}.parquet"
            logger.debug(f"Uploading table {table} to S3 at {s3_file_path}")

            bucket = s3_file_path.split("/")[2]
            key = "/".join(s3_file_path.split("/")[3:])

            if not os.path.exists(local_file_path):
                logger.error(f"Local file {local_file_path} does not exist.")
                continue

            try:
                with open(local_file_path, "rb") as data:
                    self.s3_client.upload_fileobj(data, bucket, key)
                logger.success(f"Successfully saved {table} to S3 at {s3_file_path}")
            except Exception as e:
                logger.error(f"Error uploading {table} to S3: {e}")
                raise

    def save_to_md(self, tables: List[str]) -> None:
        """
        Saves the specified tables to MotherDuck for further use.

        Args:
            tables: List of table names to save to MotherDuck.
        """
        for table in tables:
            try:
                logger.debug(
                    f"Saving table {table} to MotherDuck in {self.remote_database}.{self.silver_schema}"
                )
                self.db_manager.execute_query(
                    f"CREATE SCHEMA IF NOT EXISTS {self.remote_database}.{self.silver_schema};"
                )
                query = f"""
                    CREATE OR REPLACE TABLE {self.remote_database}.{self.silver_schema}.{table} AS
                    SELECT * FROM {self.local_database}.{table};
                """
                self.db_manager.execute_query(query)
                logger.success(
                    f"Table {table} saved to MotherDuck in schema {self.remote_database}.{self.silver_schema}"
                )
            except Exception as e:
                logger.error(f"Error saving table {table} to MotherDuck: {traceback.format_exc()}")
                raise

