"""MotherDuck connection management."""

from typing import Optional
from loguru import logger

from .duckdb import DuckDBManager


class MotherDuckManager:
    """Manages connection to MotherDuck."""

    def __init__(self, duckdb_manager: DuckDBManager, motherduck_token: str):
        """
        Initialize MotherDuckManager.

        Args:
            duckdb_manager: Instance of DuckDBManager.
            motherduck_token: Token for accessing MotherDuck.
        """
        self.duckdb_manager = duckdb_manager
        self.motherduck_token = motherduck_token
        self.connect(motherduck_token)

    def connect(self, motherduck_token: str) -> None:
        """
        Connects to MotherDuck.

        Args:
            motherduck_token: Token for accessing MotherDuck.

        Returns:
            None
        """
        try:
            logger.info("Connecting to MotherDuck")
            self.duckdb_manager.execute_query("INSTALL md;")
            self.duckdb_manager.execute_query("LOAD md;")
            self.duckdb_manager.execute_query(f"SET motherduck_token='{motherduck_token}'")
            self.duckdb_manager.execute_query("ATTACH 'md:'")
            logger.success("Connected to MotherDuck!")
        except Exception as e:
            logger.error(f"Error connecting to MotherDuck: {e}")
            raise

