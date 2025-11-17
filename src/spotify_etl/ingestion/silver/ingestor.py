"""Orchestrates the silver data ingestion process."""

from typing import List
from loguru import logger

from ...managers.duckdb import DuckDBManager
from ...managers.aws import AWSManager
from ...managers.motherduck import MotherDuckManager
from .manager import DataManager
from ...ingestion.base import BaseIngestor


class Ingestor(BaseIngestor):
    """
    Orchestrates the data ingestion process.

    Attributes:
        db_manager: Instance of DuckDBManager for executing DuckDB queries.
        motherduck_manager: Instance of MotherDuckManager for interacting with MotherDuck.
        aws_manager: Instance of AWSManager for interacting with AWS services.
        data_manager: Instance of DataManager for handling data operations.
    """

    def __init__(
        self,
        db_manager: DuckDBManager,
        motherduck_manager: MotherDuckManager,
        aws_manager: AWSManager,
        data_manager: DataManager,
    ):
        """
        Initialize Ingestor with necessary managers.

        Args:
            db_manager: Instance of DuckDBManager for executing DuckDB queries.
            motherduck_manager: Instance of MotherDuckManager for interacting with MotherDuck.
            aws_manager: Instance of AWSManager for interacting with AWS services.
            data_manager: Instance of DataManager for handling data operations.
        """
        self.db_manager = db_manager
        self.motherduck_manager = motherduck_manager
        self.aws_manager = aws_manager
        self.data_manager = data_manager

    def execute(self, tables: List[str]) -> None:
        """
        Execute the data ingestion process for specified tables.

        Args:
            tables: List of table names to process.
        """
        try:
            logger.info("Starting data ingestion process")
            self.data_manager.create_table_from_bronze(tables)
            self.data_manager.save_to_local(tables)
            self.data_manager.save_to_s3(tables)
            self.data_manager.save_to_md(tables)
            logger.success("Data ingestion process completed successfully")
        except Exception as e:
            logger.error(f"Error during data ingestion process: {e}")
            raise

