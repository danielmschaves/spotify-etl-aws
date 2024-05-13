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
            duckdb_conn.execute("SET s3_endpoint='s3.us-east-2.amazonaws.com'")
            return duckdb_conn
        except Exception as e:
            logger.error(f"Error creating DuckDB connection: {e}")
            return None

    def execute_query(self, query: str, params=None) -> None:
        """
        Executes a SQL query with optional parameters.

        Args:
            query (str): SQL query to execute.
            params (tuple, optional): Parameters to substitute into the query.

        Returns:
            None
        """
        try:
            logger.info("Executing query with parameters")
            if params is not None:
                self.connection.execute(query, params)
            else:
                self.connection.execute(query)
            logger.success("Query executed successfully")
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
            db_manager (DuckDBManager): Instance of DuckDBManager.
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
            #self.duckdb_manager.execute_query(f"SET s3_endpoint='127.0.0.1:9900';")
            #self.duckdb_manager.execute_query(f"CREATE SECRET (TYPE S3, KEY_ID '{aws_access_key}', SECRET '{aws_secret_access_key}', REGION '{aws_region}');")
            self.duckdb_manager.execute_query(f"SET s3_region='{aws_region}';")
            self.duckdb_manager.execute_query(
               f"SET s3_access_key_id='{aws_access_key}';"
            )
            self.duckdb_manager.execute_query(
               f"SET s3_secret_access_key='{aws_secret_access_key}';"
            )
            #self.duckdb_manager.execute_query("SET s3_url_style = 'path';")
            #self.duckdb_manager.execute_query("SET s3_use_ssl = false;")
            self.duckdb_manager.execute_query("CALL load_aws_credentials();")
            logger.success("AWS credentials loaded!")
        except Exception as e:
            logger.error(f"Error loading AWS credentials: {e}")


class MotherDuckManager:
    """
    Manages connection to MotherDuck.
    """

    def __init__(self, duckdb_manager: DuckDBManager, motherduck_token: str):
        """
        Initializes MotherDuckManager.

        Args:
            db_manager (DuckDBManager): Instance of DuckDBManager.
            motherduck_token (str): Token for accessing MotherDuck.
        """
        self.duckdb_manager = duckdb_manager
        self.connect(motherduck_token)

    def connect(self, motherduck_token: str) -> None:
        """
        Connects to MotherDuck.

        Args:
            motherduck_token (str): Token for accessing MotherDuck.

        Returns:
            None
        """
        try:
            logger.info("Connecting to Mother Duck")
            self.duckdb_manager.execute_query("INSTALL md;")
            self.duckdb_manager.execute_query("LOAD md;")
            self.duckdb_manager.execute_query(
                f"SET motherduck_token='{motherduck_token}'"
            )
            self.duckdb_manager.execute_query("ATTACH 'md:'")
            logger.success("Connected to Mother Duck!")
        except Exception as e:
            logger.error(f"Error connecting to MotherDuck: {e}")