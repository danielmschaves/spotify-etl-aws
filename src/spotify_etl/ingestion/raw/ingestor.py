"""Orchestrates the raw data ingestion process."""

from typing import List, Dict, Optional, Any
from loguru import logger

from .client import SpotifyAPIClient
from .parser import DataParser
from .saver import DataSaver
from ...ingestion.base import BaseIngestor


class Ingestor(BaseIngestor):
    """
    Orchestrates the data fetching, parsing, and saving processes.

    Attributes:
        api_client: Client for interacting with Spotify API.
        data_parser: Parser for JSON data.
        data_saver: Saver for local and S3 storage.
    """

    def __init__(
        self,
        api_client: SpotifyAPIClient,
        data_parser: DataParser,
        data_saver: DataSaver,
    ) -> None:
        """
        Initialize the Ingestor with the required components.

        Args:
            api_client: Client for interacting with Spotify API.
            data_parser: Parser for JSON data.
            data_saver: Saver for local and S3 storage.
        """
        self.api_client = api_client
        self.data_parser = data_parser
        self.data_saver = data_saver

    def execute(
        self,
        search_query: str,
        search_type: str,
        genre: Optional[str] = None,
        limit: Optional[int] = 20,
        playlist_id: Optional[str] = None,
    ) -> Optional[str]:
        """
        Executes the data ingestion process.

        Args:
            search_query: The search query.
            search_type: Type of search (e.g., 'track', 'artist', 'playlist').
            genre: Genre to filter the search results.
            limit: Limit the number of items to return.
            playlist_id: Specific ID of the playlist to fetch directly.

        Returns:
            The name of the file saved, or None if the process fails.
        """
        logger.info(
            f"Starting data ingestion for: {search_type}, Query: {search_query}, "
            f"Genre: {genre}, Limit: {limit}, Playlist ID: {playlist_id}"
        )
        try:
            fetched_data = self.api_client.search(
                search_query, search_type, genre, limit, playlist_id
            )
            if fetched_data:
                import json

                parsed_data = self.data_parser.parse_json_data(json.dumps(fetched_data))
                if parsed_data:
                    sanitized_playlist_id = (
                        playlist_id.replace("?", "_").replace(":", "_")
                        if playlist_id
                        else None
                    )
                    if playlist_id:
                        file_name = f"playlist_{sanitized_playlist_id}_{limit}.json"
                    else:
                        file_name = (
                            f"{search_query.replace(' ', '_')}_{search_type}_"
                            f"{genre}_{limit}.json"
                        )

                    self.data_saver.save_local(parsed_data, file_name)
                    if self.data_saver.bucket_name:
                        self.data_saver.save_s3(parsed_data, file_name)
                    logger.success("Data ingestion process completed successfully.")
                    return file_name
                else:
                    logger.warning("Parsing fetched data resulted in no output.")
            else:
                logger.warning("No data fetched from the Spotify API.")
        except Exception as e:
            logger.error(f"An error occurred during the data ingestion process: {e}")
            raise
        return None

    def execute_multiple(self, playlist_ids: List[str], limit: int = 20) -> List[str]:
        """
        Executes the ingestion process for multiple playlists.

        Args:
            playlist_ids: List of playlist IDs to fetch.
            limit: Limit the number of items to return for each playlist.

        Returns:
            List of file names saved, or an empty list if the process fails.
        """
        file_names = []
        for playlist_id in playlist_ids:
            file_name = self.execute("", "playlist", playlist_id=playlist_id, limit=limit)
            if file_name:
                file_names.append(file_name)
        return file_names

