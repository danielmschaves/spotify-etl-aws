"""DuckDB connection and query management."""

from typing import Any, Optional
from loguru import logger
import duckdb


class DuckDBManager:
    """Manages DuckDB connection and executes queries."""

    def __init__(self, database_path: Optional[str] = None):
        """
        Initialize DuckDBManager.

        Args:
            database_path: Optional path to DuckDB database file. If None, uses in-memory database.
        """
        self.database_path = database_path
        self.connection = self.create_connection()

    def create_connection(self) -> Optional[Any]:
        """
        Create a connection to DuckDB.

        Returns:
            duckdb.Connection: DuckDB connection object, or None if connection fails.
        """
        try:
            logger.info("Creating DuckDB connection")
            if self.database_path:
                duckdb_conn = duckdb.connect(self.database_path)
            else:
                duckdb_conn = duckdb.connect()
            logger.success("DuckDB connection created!")
            duckdb_conn.execute("SET s3_endpoint='s3.us-east-2.amazonaws.com'")
            return duckdb_conn
        except Exception as e:
            logger.error(f"Error creating DuckDB connection: {e}")
            return None

    def execute_query(self, query: str, params: Optional[tuple] = None) -> None:
        """
        Executes a SQL query with optional parameters.

        Args:
            query: SQL query to execute.
            params: Optional parameters to substitute into the query.

        Returns:
            None
        """
        if not self.connection:
            logger.error("No DuckDB connection available")
            return

        try:
            logger.debug(f"Executing query: {query[:100]}...")
            if params is not None:
                self.connection.execute(query, params)
            else:
                self.connection.execute(query)
            logger.success("Query executed successfully")
        except Exception as e:
            logger.error(f"Error executing query: {e}")
            raise

    def close(self) -> None:
        """Close the DuckDB connection."""
        if self.connection:
            try:
                self.connection.close()
                logger.info("DuckDB connection closed")
            except Exception as e:
                logger.error(f"Error closing DuckDB connection: {e}")

    def __enter__(self):
        """Context manager entry."""
        return self

    def __exit__(self, exc_type, exc_val, exc_tb):
        """Context manager exit."""
        self.close()

