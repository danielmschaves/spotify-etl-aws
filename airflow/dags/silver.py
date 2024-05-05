# use duck db to load data from parquet files to a more structured format
# methods: 1) create_table_from_bronze, 2) save_to_s3, 3) save_to_local, 4) save_to_md

import os
import sys
from dotenv import load_dotenv
from loguru import logger
import boto3

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
        database_name: str,
        local_path: str,
        bronze_s3_path: str,
        silver_s3_path: str,
        bronze_path: str,
    ):
        """Initializes DataManager

        Args:
            db_manager (DuckDBManager): Instance of DuckDBManager."""
        self.db_manager = db_manager
        self.local_database = local_database
        self.remote_database = remote_database
        self.silver_schema = silver_schema
        self.database_name = database_name
        self.local_path = local_path
        self.bronze_s3_path = bronze_s3_path
        self.silver_s3_path = silver_s3_path
        self.bronze_path = bronze_path
        self.s3_client = boto3.client("s3")

    def create_table_from_bronze(self, tables) -> None:
        """
        Creates tables in DuckDB from the bronze tables in S3, with specific transformations and cleanups.
        """
        tables = {
            "playlists": {
                "columns": "id, name, description, owner_id, followers, public"
            },
            "tracks": {
                "columns": "track_id, name, playlist_id, album_id, duration_ms, popularity, explicit, track_number, album_release_date, artist_id"
            },
            "albums": {
                "columns": "album_id, name, release_date, total_tracks, track_id"
            },
            "artists": {"columns": "artist_id, name, track_id"},
        }

        for table, details in tables.items():
            try:
                bronze_file_path = f"{self.bronze_path}/{table}.parquet"
                if not os.path.exists(bronze_file_path):
                    logger.error(f"File not found: {bronze_file_path}")
                    continue  # Skip to the next table if file does not exist

                logger.info(
                    f"Creating table {table} in {self.local_database} schema with specific fields"
                )
                query = f"""
                    CREATE OR REPLACE TABLE {self.local_database}.{table} AS
                    SELECT {details['columns']}
                    FROM read_parquet('{bronze_file_path}')
                """
                self.db_manager.execute_query(query)
                logger.success(
                    f"Table {table} created in {self.local_database} schema with fields specified"
                )
            except Exception as e:
                logger.error(
                    f"Error creating table {table} in {self.local_database} schema: {e}"
                )
                raise e

    def save_to_local(self, tables) -> None:
        """
        Saves the specified tables to local disk in parquet format.
        """
        for table in tables:
            try:
                logger.info(f"Saving table {table} to local disk")
                os.makedirs(os.path.dirname(self.local_path), exist_ok=True)
                query = f"""
                    COPY (
                        SELECT * 
                        FROM {self.local_database}.{table}
                        ) 
                    TO '{self.local_path}{table}.parquet' (FORMAT 'parquet')
                """
                self.db_manager.execute_query(query)
                logger.success(f"Table {table} saved to local disk as parquet")
            except Exception as e:
                logger.error(f"Error saving table {table} to local disk: {e}")
                raise e

    def save_to_s3(self, tables) -> None:
        """
        Manually uploads a parquet file from local disk to Amazon S3.
        Args:
            table_name (str): The name of the table to save.
        Returns:
            None
        """

        tables = [
            "playlists",
            "tracks",
            "albums",
            "artists",
        ]  # List of tables to process
        for table in tables:
            local_file_path = os.path.join(self.local_path, f"{table}.parquet")
            s3_file_path = f"{self.silver_s3_path}{table}.parquet".replace("s3://", "")

            if not os.path.exists(local_file_path):
                logger.error(f"Local file {local_file_path} does not exist.")
                continue  # Skip to the next table if the file does not exist

            bucket = s3_file_path.split("/")[0]
            key = "/".join(s3_file_path.split("/")[1:])

            try:
                with open(local_file_path, "rb") as data:
                    self.s3_client.upload_fileobj(data, bucket, key)
                logger.success(f"Successfully saved {table} to S3 at {s3_file_path}")
            except Exception as e:
                logger.error(f"Error uploading {table} to S3: {e}")

    # def save_to_s3(self, tables) -> None:
    #     """
    #     Saves the specified tables to Amazon S3 in parquet format.
    #     """
    #     for table in tables:
    #         try:
    #             logger.info(f"Saving table {table} to S3 as parquet.")
    #             query = f"""
    #                 COPY (
    #                     SELECT * FROM playlist.bronze.{table}
    #                 )
    #                 TO '{self.silver_s3_path}{table}.parquet'
    #                 WITH (FORMAT 'parquet')
    #             """
    #             self.db_manager.execute_query(query)
    #             logger.success(f"Table {table} saved to S3 as parquet")
    #         except Exception as e:
    #             logger.error(f"Error saving table {table} to S3: {e}")

    def save_to_md(self, tables) -> None:
        """
        Saves the specified tables to MotherDuck for further use.
        """
        for table in tables:
            try:
                logger.info(f"Saving table {table} to MotherDuck.")
                self.db_manager.execute_query(
                    f"CREATE SCHEMA IF NOT EXISTS {self.remote_database}.{self.silver_schema};"
                )
                query = f"""
                    CREATE OR REPLACE TABLE {self.remote_database}.{self.silver_schema}.{table} AS
                    SELECT * FROM playlist.bronze.{table};
                """
                self.db_manager.execute_query(query)
                logger.success(f"Table {table} saved to MotherDuck")
            except Exception as e:
                logger.error(f"Error saving table {table} to MotherDuck: {e}")


class Ingestor:
    """
    Orchestrates the data ingestion process.
    """

    def __init__(
        self,
        db_manager,
        motherduck_manager,
        aws_manager,
        data_manager,
    ):
        """
        Initializes Ingestor with necessary managers.
        """
        self.db_manager = db_manager
        self.motherduck_manager = motherduck_manager
        self.aws_manager = aws_manager
        self.data_manager = data_manager

    def execute(self, tables) -> None:
        """
        Executes the data ingestion process for specified tables.

        Args:
            tables (list): List of table names to process.
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
            raise e


load_dotenv()

# Load environment variables
motherduck_token = os.getenv("MOTHERDUCK_TOKEN")
aws_access_key = os.getenv("AWS_ACCESS_KEY")
aws_secret_access_key = os.getenv("AWS_SECRET_ACCESS_KEY")
aws_region = os.getenv("AWS_REGION")
bronze_s3_path = os.getenv("BRONZE_S3_PATH")
silver_s3_path = os.getenv("SILVER_S3_PATH")
bronze_path = os.getenv("BRONZE_PATH")
local_path = "data/silver/"
database_name = "playlist"
silver_schema = "silver"
local_database = "memory"
remote_database = "playlist"


if __name__ == "__main__":
    # Create instances of the managers
    db_manager = DuckDBManager()
    motherduck_manager = MotherDuckManager(db_manager, motherduck_token)
    aws_manager = AWSManager(
        db_manager, aws_access_key, aws_secret_access_key, aws_region
    )
    data_manager = DataManager(
        db_manager,
        local_database,
        remote_database,
        silver_schema,
        database_name,
        local_path,
        bronze_s3_path,
        silver_s3_path,
        bronze_path,
    )

    # List of tables to process
    tables = ["playlists", "tracks", "albums", "artists"]

    # Create instance of Ingestor
    ingestor = Ingestor(db_manager, motherduck_manager, aws_manager, data_manager)
    ingestor.execute(tables)
