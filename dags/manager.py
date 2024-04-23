from loguru import logger
import duckdb
from typing import Any

class DuckDBManager:
    """
    Manages DuckDB connection and executes queries.
    """

    def __init__(self):
        """
        Initializes DuckDBManager.
        """
        self.connection = self.create_connection()

    def create_connection(self) -> Any:
        """
        Create a connection to DuckDB.

        Returns:
            duckdb.Connection: DuckDB connection object.
        """
        try:
            logger.info("Creating DuckDB connection")
            duckdb_conn = duckdb.connect()
            logger.success("DuckDB connection created!")
            return duckdb_conn
        except Exception as e:
            logger.error(f"Error creating DuckDB connection: {e}")
            return None

    def execute_query(self, query: str) -> None:
        """
        Executes a SQL query.

        Args:
            query (str): SQL query to execute.

        Returns:
            None
        """
        try:
            logger.info("Executing query")
            self.connection.execute(query)
            logger.success("Query executed")
        except Exception as e:
            logger.error(f"Error executing query: {e}")
                      
class AWSManager:
    """
    Manages AWS credentials and operations.
    """

    def __init__(
        self,
        duckdb_manager: DuckDBManager,
        aws_region: str,
        aws_access_key: str,
        aws_secret_access_key: str,
    ):
        """
        Initializes AWSManager.

        Args:
            duckdb_manager (DuckDBManager): Instance of DuckDBManager.
            aws_region (str): AWS region.
            aws_access_key (str): AWS access key ID.
            aws_secret_access_key (str): AWS secret access key.
        """
        self.duckdb_manager = duckdb_manager
        self.load_credentials(aws_region, aws_access_key, aws_secret_access_key)

    def load_credentials(
        self, aws_region: str, aws_access_key: str, aws_secret_access_key: str
    ) -> None:
        """
        Loads AWS credentials.

        Args:
            aws_region (str): AWS region.
            aws_access_key (str): AWS access key ID.
            aws_secret_access_key (str): AWS secret access key.

        Returns:
            None
        """
        try:
            logger.info("Loading AWS credentials")
            self.duckdb_manager.execute_query("INSTALL httpfs;")
            self.duckdb_manager.execute_query("LOAD httpfs;")
            self.duckdb_manager.execute_query(f"SET s3_region='{aws_region}'")
            self.duckdb_manager.execute_query(
                f"SET s3_access_key_id='{aws_access_key}';"
            )
            self.duckdb_manager.execute_query(
                f"SET s3_secret_access_key='{aws_secret_access_key}';"
            )
            self.duckdb_manager.execute_query("CALL load_aws_credentials();")
            logger.success("AWS credentials loaded!")
        except Exception as e:
            logger.error(f"Error loading AWS credentials: {e}")