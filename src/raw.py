from typing import Dict, List, Optional, Union
from dotenv import load_dotenv
import requests
import os
import json
import boto3
from datetime import datetime
from loguru import logger
from dotenv import load_dotenv

# Load environment variables
load_dotenv()

# Configuration
API_BASE_URL = "https://api.spotify.com/v1/"
DATASET_NAME = "default_cards"
TABLE_NAME = "cards"
TABLE_PATH = "data/raw/"
AWS_BUCKET_NAME = os.getenv("AWS_BUCKET_NAME")
AWS_ACCESS_KEY = os.getenv("AWS_ACCESS_KEY")
AWS_SECRET_ACCESS_KEY = os.getenv("AWS_SECRET_ACCESS_KEY")
ACCES_TOKEN = ""

class SpotifyAPIClient:
    """
    Class for interacting with the Spotify API.
    """

    def __init__(self, base_url, access_token: str) -> None:
        self.base_url = base_url
        self.access_token = access_token
        self.headers = {
            "Authorization": f"Bearer {self.access_token}",
            "Content-Type": "application/json"
        }

    def _make_request(self, endpoint: str, params: Optional[Dict[str, str]] = None) -> Optional[Union[Dict, List[Dict]]]:
        """
        Makes an API request to the specified endpoint.

        Args:
            endpoint (str): The API endpoint.
            params (Optional[Dict[str, str]]): Optional parameters for the request.

        Returns:
            Optional[Union[Dict, List[Dict]]]: The response data, or None if request fails.
        """
        try:
            url = f"{self.base_url}{endpoint}"
            response = requests.get(url, headers=self.headers, params=params)
            response.raise_for_status()
            return response.json()
        except requests.exceptions.RequestException as e:
            logger.error(f"Error making request to {endpoint}: {e}")
            return None

    def search_tracks(self, query: str, limit: Optional[int] = None) -> Optional[List[Dict]]:
        """
        Searches for tracks based on a query.

        Args:
            query (str): The search query.
            limit (Optional[int]): Limit the number of tracks to fetch.

        Returns:
            Optional[List[Dict]]: List of track data, or None if the request fails.
        """
        try:
            params = {"q": query, "type": "track", "limit": limit} if limit else {"q": query, "type": "track"}
            endpoint = "search"
            response = self._make_request(endpoint, params=params)

            if response:
                logger.info("Search tracks request successful")
                return response.get("tracks", {}).get("items", [])
            else:
                logger.error("Failed to retrieve tracks")
                return None
        except Exception as e:
            logger.error(f"Error searching tracks: {e}")
            return None

    def search_artists(self, query: str, limit: Optional[int] = None) -> Optional[List[Dict]]:
        """
        Searches for artists based on a query.

        Args:
            query (str): The search query.
            limit (Optional[int]): Limit the number of artists to fetch.

        Returns:
            Optional[List[Dict]]: List of artist data, or None if the request fails.
        """
        try:
            params = {"q": query, "type": "artist", "limit": limit} if limit else {"q": query, "type": "artist"}
            endpoint = "search"
            response = self._make_request(endpoint, params=params)

            if response:
                logger.info("Search artists request successful")
                return response.get("artists", {}).get("items", [])
            else:
                logger.error("Failed to retrieve artists")
                return None
        except Exception as e:
            logger.error(f"Error searching artists: {e}")
            return None
    
    def get_artist_albums(self, artist_id: str, limit: Optional[int] = None) -> Optional[List[dict]]:
        """
        Fetches albums from a Spotify artist.

        Args:
            artist_id (str): The ID of the artist.
            limit (int, optional): Limit the number of albums to fetch.

        Returns:
            Optional[List[dict]]: List of album data, or None if the request fails.
        """
        try:
            params = {"limit": limit} if limit else None
            endpoint = f"artists/{artist_id}/albums"
            response = self._make_request(endpoint, params=params)
            if response:
                logger.info(f"Fetched albums for artist {artist_id}")
                return response.get("items", [])
            else:
                logger.error(f"Failed to fetch albums for artist {artist_id}")
                return None
        except Exception as e:
            logger.error(f"Error fetching albums for artist {artist_id}: {e}")
            return None    

    def get_track_audio_features(self, track_id: str) -> Optional[dict]:
        """
        Fetches audio features for a Spotify track.

        Args:
            track_id (str): The ID of the track.

        Returns:
            Optional[dict]: Audio features data, or None if the request fails.
        """
        try:
            endpoint = f"audio-features/{track_id}"
            response = self._make_request(endpoint)
            if response:
                logger.info(f"Fetched audio features for track {track_id}")
                return response
            else:
                logger.error(f"Failed to fetch audio features for track {track_id}")
                return None
        except Exception as e:
            logger.error(f"Error fetching audio features for track {track_id}: {e}")
            return None    

class DataParser:
    """
    Class for parsing data.
    """
    def parse_json_data(data: requests.Response) -> Optional[List[Dict]]:
        """
        Parses JSON data.

        Args:
            data (requests.Response): The JSON data.

        Returns:
            Optional[List[Dict]]: The parsed data, or None if parsing fails.
        """
        try:
            parsed_data = data.json()
            return parsed_data
        except Exception as e:
            # Log the error if parsing fails
            logger.error(f"Error parsing JSON data: {e}")
            return None
        
class DataSaver:
    """
    Class for saving data locally or to an AWS S3 bucket.
    """

    def __init__(
        self,
        table_name: str,
        bucket_name: Optional[str] = None,
        access_key_id: Optional[str] = None,
        secret_access_key: Optional[str] = None,
    ) -> None:
        """
        Initialize the DataSaver with the required parameters.

        Args:
            table_name (str): Name of the table.
            bucket_name (str, optional): Name of the AWS S3 bucket.
            access_key_id (str, optional): AWS access key ID.
            secret_access_key (str, optional): AWS secret access key.
        """
        self.table_name = table_name
        self.bucket_name = bucket_name
        if bucket_name:
            self.s3_client = boto3.client(
                "s3",
                aws_access_key_id=access_key_id,
                aws_secret_access_key=secret_access_key,
            )

    def save_local(self, data: List[Dict]) -> None:
        """
        Save parsed data to a local file.

        Args:
            data (List[Dict]): List of parsed data.

        Returns:
            None
        """
        try:
            logger.info("Saving data locally")
            file_path = os.path.join(TABLE_PATH, f"{self.table_name}.json")
            os.makedirs(os.path.dirname(file_path), exist_ok=True)
            with open(file_path, "w") as file:
                json.dump(data, file, indent=4)
            logger.success(f"Data saved locally to {file_path}")
        except Exception as e:
            logger.error(f"An error occurred while saving data locally: {e}")

    def save_s3(self, data: List[Dict]) -> None:
        """
        Save parsed data to an AWS S3 bucket.

        Args:
            data (List[Dict]): List of parsed data.

        Returns:
            None
        """
        try:
            logger.info("Saving data to S3 bucket")
            json_bytes = json.dumps(data, indent=4).encode("utf-8")
            key = f"{self.table_name}.json"
            self.s3_client.put_object(Body=json_bytes, Bucket=self.bucket_name, Key=key)
            logger.success(f"Data saved successfully to S3 bucket: {self.bucket_name}")
        except Exception as e:
            logger.error(f"An error occurred while saving data to S3 bucket: {e}")

class Ingestor:
    """
    Class for ingesting data from the Spotify API, parsing it, and saving it.
    """

    def __init__(self, api_client, data_parser, data_saver):
        """
        Initialize the Ingestor with the API client, data parser, and data saver.

        Args:
            api_client: An instance of the API client for interacting with the Spotify API.
            data_parser: An instance of the data parser for parsing the fetched data.
            data_saver: An instance of the data saver for saving the parsed data.
        """
        self.api_client = api_client
        self.data_parser = data_parser
        self.data_saver = data_saver

    def execute(self, query: str, limit: Optional[int] = None) -> None:
        """
        Executes the data ingestion process by fetching, parsing, and saving data from the Spotify API.

        Args:
            query (str): The search query to fetch data from the Spotify API.
            limit (int, optional): Limit the number of items to fetch.

        Returns:
            None
        """
        try:
            logger.info("Starting ingestion process")
            # Fetch data from the Spotify API
            tracks_data = self.api_client.search_tracks(query, limit)

            if tracks_data:
                # Parse fetched data
                parsed_data = self.data_parser.parse_tracks(tracks_data)

                if parsed_data:
                    # Save parsed data
                    self.data_saver.save_local(parsed_data)
                    self.data_saver.save_s3(parsed_data)
                    logger.success("Ingestion completed!")
        except Exception as e:
            logger.error(f"Error executing data ingestion process: {e}")

if __name__ == "__main__":
    # Initialize the necessary components
    api_client = SpotifyAPIClient(API_BASE_URL, ACCES_TOKEN)
    data_parser = DataParser()
    data_saver = DataSaver(
        TABLE_NAME, TABLE_PATH, AWS_BUCKET_NAME, AWS_ACCESS_KEY, AWS_SECRET_ACCESS_KEY
    )
    ingestor = Ingestor(api_client, data_parser, data_saver)

    # User input for dynamic operation
    print("Welcome to the Spotify Data Ingestor!")
    search_type = input("Enter the type of data to search (track, artist, album): ").strip().lower()
    query = input(f"Enter a query to search {search_type}s on Spotify: ").strip()
    limit = input("Enter the maximum number of items to fetch (press Enter for default limit, 20): ").strip()
    limit = int(limit) if limit.isdigit() else 20

    # Execute the ingestion process
    ingestor.execute(query, search_type, limit)