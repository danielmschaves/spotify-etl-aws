"""Data parser for JSON data."""

from typing import List, Dict, Optional, Any
import json
from loguru import logger


class DataParser:
    """Class for parsing data from JSON."""

    @staticmethod
    def parse_json_data(json_data: str) -> Optional[List[Dict[str, Any]]]:
        """
        Parses JSON data from a JSON string.

        Args:
            json_data: The JSON data as a string.

        Returns:
            The parsed data as a list of dictionaries, or None if parsing fails.
        """
        try:
            parsed_data = json.loads(json_data)
            return parsed_data
        except json.JSONDecodeError as e:
            logger.error(f"JSON decode error: {e.msg}")
        except Exception as e:
            logger.error(f"Error parsing JSON data: {e}")
        return None

