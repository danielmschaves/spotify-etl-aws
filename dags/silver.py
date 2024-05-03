# use duck db to load data from parquet files to a more structured format
# methods: 1) create_table_from_bronze, 2) save_to_s3, 3) save_to_local, 4) save_to_md

import os
import sys
from dotenv import load_dotenv
from loguru import logger
from typing import Any

sys.path.append(os.path.abspath(os.path.join(os.path.dirname(__file__), "..", "..")))

from manager import DuckDBManager, MotherDuckManager, AWSManager


class DataManager:
    """
    Manages data operations
    """

    def __init__(
        self,
        db_manager,
        local_database: str,
        remote_database: str,
        silver_schema: str,
        table_name: str,
        local_path: str,
        bronze_s3_path: str,
        silver_s3_path: str,
    ):
        """Initializes DataManager

        Args:
            db_manager (DuckDBManager): Instance of DuckDBManager."""
        self.db_manager = db_manager
        self.local_database = local_database
        self.remote_database = remote_database
        self.silver_schema = silver_schema
        self.table_name = table_name
        self.local_path = local_path
        self.bronze_s3_path = bronze_s3_path
        self.silver_s3_path = silver_s3_path

    def create_table_from_bronze(self) -> None:
        """
        Creates tables in DuckDB from the bronze tables in S3, with specific transformations and cleanups.
        """
        tables = {
            "playlists": {
                "columns": "id, name, description, owner_id, followers, public"
            },
            "tracks": {
                "columns": "name, playlist_id, album_id, duration_ms, popularity, explicit, track_number, album_release_date, artist_id"
            },
            "albums": {
                "columns": "name, release_date, total_tracks, popularity, artist_id"
            },
            "artists": {"columns": "artist_id, name, track_id"},
        }

        for table, details in tables.items():
            try:
                logger.info(f"Creating table {table} in {self.silver_schema} schema")
                query = f"""
                    CREATE OR REPLACE TABLE {self.local_database}.{table} AS
                    SELECT {details['columns']}
                    FROM read_parquet('s3://{self.bronze_s3_path}/{table}.parquet')
                """
                self.db_manager.execute_query(query)
                logger.success(f"Table {table} created in {self.silver_schema} schema")
            except Exception as e:
                logger.error(
                    f"Error creating table {table} in {self.silver_schema} schema: {e}"
                )
                raise e

    def save_to_local(self) -> None:
        """Saves the table to local disk"""
        try:
            logger.info(f"Saving table {self.table_name} to local disk")
            query = f"""
                COPY {self.local_database}.{self.table_name} 
                TO '{self.local_path}{self.table_name}.parquet' (FORMAT 'parquet')
            """
            self.db_manager.execute_query(query)
            logger.success(f"Table {self.table_name} saved to local disk")
        except Exception as e:
            logger.error(f"Error saving table {self.table_name} to local disk: {e}")
            raise e

    def save_to_s3(self) -> None:
        """ "Saves data to S3.
        Returns:
            None
        """
        try:
            logger.info(f"Saving table {self.table_name} to S3 as parquet.")
            query = f"""
                        COPY (
                            SELECT * FROM {self.local_database}.{self.table_name}
                        )
                        TO '{self.silver_s3_path}{self.table_name}.parquet'
                        (FORMAT 'parquet')
                    """
            self.db_manager.execute_query(query)
            logger.success(f"Table {self.table_name} saved to S3")
        except Exception as e:
            logger.error(f"Error saving table {self.table_name} to S3: {e}")

    def save_to_md(self) -> None:
        """Saves data to MotherDuck.
        Returns:
            None
        """
        try:
            logger.info(f"Saving table {self.table_name} to MotherDuck.")
            self.db_manager.execute_query(
                f"""
                        CREATE SCHEMA IF NOT EXISTS {self.remote_database}{self.silver_schema};
                    """
            )
            query = f"""
                        CREATE OR REPLACE TABLE {self.remote_database}{self.silver_schema}.{self.table_name} AS
                        SELECT * FROM {self.local_database}.{self.table_name};
                        """
            self.db_manager.execute_query(query)
            logger.success(f"Table {self.table_name} saved to MotherDuck")
        except Exception as e:
            logger.error(f"Error saving table {self.table_name} to MotherDuck: {e}")


class Ingestor:
    """
    Orquestates the data ingestion process.
    """

    def __init__(
        self,
        db_manager,
        motherduck_manager,
        aws_manager,
        data_manager,
    ):
        """
        Initializes Ingestor
        """
        self.db_manager = db_manager
        self.motherduck_manager = motherduck_manager
        self.aws_manager = aws_manager
        self.db_manager = db_manager

    def execute(self) -> None:
        """Executes the data ingestion process"""
        try:
            logger.info("Executing data ingestion process")
            self.data_manager.create_table_from_bronze()
            self.data_manager.save_to_local()
            self.data_manager.save_to_s3()
            self.data_manager.save_to_md()
            logger.success("Data ingestion process executed successfully")
        except Exception as e:
            logger.error(f"Error executing data ingestion process: {e}")
            raise e


load_dotenv()

# Load environment variables
MOTHERDUCK_TOKEN = os.getenv("MOTHERDUCK_TOKEN")
AWS_ACCESS_KEY = os.getenv("AWS_ACCESS_KEY")
AWS_SECRET_ACCESS_KEY = os.getenv("AWS_SECRET_ACCESS_KEY")
AWS_REGION = os.getenv("AWS_REGION")
BRONZE_S3_PATH = os.getenv("BRONZE_S3_PATH")
SILVER_S3_PATH = os.getenv("SILVER_S3_PATH")
LOCAL_PATH = "data/silver/"
TABLE_NAME = "playlist"
SILVER_SCHEMA = "silver"
LOCAL_DATABASE = "memory"
REMOTE_DATABASE = "playlist"


if __name__ == "__main__":
    # Create instances of the managers
    db_manager = DuckDBManager()
    motherduck_manager = MotherDuckManager(MOTHERDUCK_TOKEN)
    aws_manager = AWSManager(AWS_ACCESS_KEY, AWS_SECRET_ACCESS_KEY, AWS_REGION)
    data_manager = DataManager(
        db_manager,
        LOCAL_DATABASE,
        REMOTE_DATABASE,
        SILVER_SCHEMA,
        TABLE_NAME,
        LOCAL_PATH,
        BRONZE_S3_PATH,
        SILVER_S3_PATH,
    )

    # Create instance of Ingestor
    ingestor = Ingestor(db_manager, motherduck_manager, aws_manager, data_manager)
    ingestor.execute()
