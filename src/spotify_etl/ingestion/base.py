"""Base classes for ingestion components."""

from abc import ABC, abstractmethod
from typing import List, Dict, Optional, Any
from loguru import logger


class BaseIngestor(ABC):
    """Abstract base class for data ingestion."""

    @abstractmethod
    def execute(self, *args, **kwargs) -> Any:
        """Execute the ingestion process."""
        pass


class BaseDataManager(ABC):
    """Abstract base class for data management operations."""

    @abstractmethod
    def save_to_local(self, table_name: str) -> None:
        """Save data to local storage."""
        pass

    @abstractmethod
    def save_to_s3(self, table_name: str) -> None:
        """Save data to S3 storage."""
        pass

    @abstractmethod
    def save_to_md(self, table_name: str) -> None:
        """Save data to MotherDuck."""
        pass

