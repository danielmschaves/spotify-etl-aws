"""Orchestrates the bronze data ingestion process."""

from typing import List
from loguru import logger

from ...managers.duckdb import DuckDBManager
from ...managers.aws import AWSManager
from ...managers.motherduck import MotherDuckManager
from .manager import DataManager
from ...ingestion.base import BaseIngestor


class Ingestor(BaseIngestor):
    """Orchestrates the entire data ingestion process."""

    def __init__(
        self,
        db_manager: DuckDBManager,
        aws_manager: AWSManager,
        data_manager: DataManager,
        motherduck_manager: MotherDuckManager,
    ):
        """
        Initialize Ingestor with database and AWS managers.

        Args:
            db_manager: DuckDB manager instance.
            aws_manager: AWS manager instance.
            data_manager: Data manager instance.
            motherduck_manager: MotherDuck manager instance.
        """
        self.db_manager = db_manager
        self.aws_manager = aws_manager
        self.data_manager = data_manager
        self.motherduck_manager = motherduck_manager

    def execute(self, raw_bucket: str, json_key: str, playlist_table: str) -> None:
        """
        Execute the data ingestion process.

        Args:
            raw_bucket: Name of the S3 bucket containing the JSON data.
            json_key: The S3 key of the JSON file containing the data.
            playlist_table: Name of the playlist table to load data into.
        """
        logger.info(f"Starting the data ingestion process for {playlist_table}.")

        # Loading and Transforming Data
        try:
            self.data_manager.load_and_transform_data(raw_bucket, json_key, playlist_table)
            logger.info(f"Data loaded and transformed successfully for {playlist_table}.")
        except Exception as e:
            logger.error(f"Error during data loading and transformation for {playlist_table}: {e}")
            raise

        # Prepare tables for export
        tables = ["playlists", "tracks", "albums", "artists"]

        # Exporting Data to Local Storage
        for table in tables:
            try:
                self.data_manager.save_to_local(table)
                logger.success(f"Data successfully saved to local storage for table: {table}.")
            except Exception as e:
                logger.error(f"Error saving data to local storage for table {table}: {e}")
                raise

        # Exporting Data to S3 Storage
        for table in tables:
            try:
                self.data_manager.save_to_s3(table)
                logger.success(f"Data successfully saved to S3 for table: {table}.")
            except Exception as e:
                logger.error(f"Error saving data to S3 for table {table}: {e}")
                raise

        # Exporting Data to MotherDuck
        for table in tables:
            try:
                self.data_manager.save_to_md(table)
                logger.success(f"Data successfully saved to MotherDuck for table: {table}.")
            except Exception as e:
                logger.error(f"Error saving data to MotherDuck for table {table}: {e}")
                raise

