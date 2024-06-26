from typing import Dict, List, Optional, Union
from dotenv import load_dotenv
import requests
import os
import json
import boto3
from loguru import logger
from botocore.exceptions import ClientError
from requests.auth import HTTPBasicAuth

# Load environment variables
load_dotenv()

# Environmental configuration
API_BASE_URL = os.getenv("API_BASE_URL", "https://api.spotify.com/v1/")
TABLE_NAME = os.getenv("TABLE_NAME", "spotify_data")
TABLE_PATH = "data/raw/"
AWS_ACCESS_KEY = os.getenv("AWS_ACCESS_KEY")
AWS_SECRET_ACCESS_KEY = os.getenv("AWS_SECRET_ACCESS_KEY")
client_id = os.getenv("client_id")
client_secret = os.getenv("client_secret")
bucket_name = os.getenv("RAW_BUCKET")

class SpotifyAPIClient:
    """
    Class for interacting with the Spotify API, providing methods to search for different entities.
    
    Attributes:
        base_url (str): Base URL for the Spotify API.
        client_id (str): Spotify API client ID.
        client_secret (str): Spotify API client secret.
        session (requests.Session): HTTP session for making requests.
        access_token (str): Bearer token for API authentication.
    """

    def __init__(self, base_url: str, client_id: str, client_secret: str) -> None:
        """
        Initialize the Spotify API client.

        Args:
            base_url (str): Base URL for the Spotify API.
            client_id (str): Spotify API client ID.
            client_secret (str): Spotify API client secret.
        """ 
        self.base_url = base_url
        self.client_id = client_id
        self.client_secret = client_secret
        self.session = requests.Session()
        self.access_token = self.refresh_access_token()

    def refresh_access_token(self) -> str:
        """
        Retrieves a new access token from the Spotify API.

        Returns:
            str: Access token.
        """
        url = "https://accounts.spotify.com/api/token"
        headers = {"Content-Type": "application/x-www-form-urlencoded"}
        data = {"grant_type": "client_credentials"}

        response = self.session.post(url, headers=headers, data=data, auth=HTTPBasicAuth(self.client_id, self.client_secret))
        if response.status_code != 200:
            logger.error(f"Failed to retrieve token: {response.status_code} - {response.text}")
        response.raise_for_status()
        return response.json()['access_token']

    def _make_request(self, endpoint: str, params: Optional[Dict[str, str]] = None) -> Optional[Dict]:
        """
        Makes an API request to the specified endpoint.

        Args:
            endpoint (str): The API endpoint.
            params (Optional[Dict[str, str]]): Optional parameters for the request.

        Returns:
            Optional[Dict]: The response data, or None if the request fails.
        """
        url = f"{self.base_url}{endpoint}"
        headers = {
            "Authorization": f"Bearer {self.access_token}",
            "Content-Type": "application/json"
        }
        try:
            response = self.session.get(url, headers=headers, params=params)
            response.raise_for_status()
            return response.json()
        except requests.exceptions.HTTPError as e:
            logger.error(f"HTTP error occurred: {e.response.status_code} {e.response.reason} for URL {url}")
            if e.response.status_code == 401:  # Unauthorized access, refresh token
                self.access_token = self.refresh_access_token()  # Attempt to refresh access token
                return self._make_request(endpoint, params)  # Retry the request
        except requests.exceptions.ConnectionError:
            logger.error("Connection error occurred")
        except requests.exceptions.Timeout:
            logger.error("Timeout occurred")
        except requests.exceptions.RequestException as e:
            logger.error(f"Request exception: {e}")
        return None

    def search(self, query: str, search_type: str, genre: Optional[str] = None, limit: Optional[int] = 20, playlist_id: Optional[str] = None) -> Optional[List[Dict]]:
        """
        Generic search function for different Spotify entities like tracks, artists, playlists.
        Adds the ability to filter by genre and directly fetch playlists.

        Args:
            query (str): The search query.
            search_type (str): Type of search (e.g., 'track', 'artist', 'playlist').
            genre (Optional[str]): Genre to filter the search results (e.g., 'rock', 'jazz').
            limit (Optional[int]): Limit the number of items to return.
            playlist_id (Optional[str]): Specific ID of the playlist to fetch directly.

        Returns:
            Optional[List[Dict]]: List of entities data, or None if the request fails.
        """
        if playlist_id:
            endpoint = f"playlists/{playlist_id}"
        else:
            query_string = f"{query} genre:{genre}" if genre else query
            params = {"q": query_string, "type": search_type, "limit": limit}
            endpoint = "search"
        
        response = self._make_request(endpoint, params if not playlist_id else None)
        if response:
            if playlist_id:
                items = [response]  # Wrap the playlist response in a list
            else:
                items = response.get(search_type + 's', {}).get("items", [])
            
            logger.info(f"Search for {search_type}s '{query}' with genre '{genre}' returned {len(items)} items.")
            return items
        else:
            logger.error(f"Failed to retrieve {search_type}s for query '{query}' with genre '{genre}'")
        return None
    
class DataParser:
    """
    Class for parsing data from JSON.
    """

    @staticmethod
    def parse_json_data(json_data: str) -> Optional[List[Dict]]:
        """
        Parses JSON data from a JSON string.

        Args:
            json_data (str): The JSON data as a string.

        Returns:
            Optional[List[Dict]]: The parsed data as a list of dictionaries, or None if parsing fails.
        """
        try:
            parsed_data = json.loads(json_data)
            return parsed_data
        except json.JSONDecodeError as e:
            logger.error(f"JSON decode error: {e.msg}")
        except Exception as e:
            logger.error(f"Error parsing JSON data: {e}")
        return None

class DataSaver:
    """
    Class for saving data locally or to an AWS S3 bucket, ensuring data integrity and handling errors gracefully.
    
    Attributes:
        table_name (str): Name of the table.
        table_path (str): Local path where the data files will be stored.
        bucket_name (Optional[str]): Name of the AWS S3 bucket.
        s3_client (Optional[boto3.client]): Boto3 S3 client.
    """    
    def __init__(
        
        self,
        table_name: str,
        table_path: str,
        bucket_name: Optional[str] = None,
        access_key_id: Optional[str] = None,
        secret_access_key: Optional[str] = None,
    ) -> None:
        """
        Initialize the DataSaver with the required parameters.

        Args:
            table_name (str): Name of the table.
            table_path (str): Local path where the data files will be stored.
            bucket_name (str, optional): Name of the AWS S3 bucket.
            access_key_id (str, optional): AWS access key ID.
            secret_access_key (str, optional): AWS secret access key.
        """
        self.table_name = table_name
        self.table_path = table_path
        self.bucket_name = bucket_name
        if bucket_name:
            self.s3_client = boto3.client(
                "s3",
                aws_access_key_id=access_key_id,
                aws_secret_access_key=secret_access_key,
            )

    def save_local(self, data: List[Dict], file_name: str) -> None:
        """
        Save parsed data to a local file system, handling any file system errors that might occur.

        Args:
            data (List[Dict]): List of parsed data.
            file_name (str): Name of the file to save the data in.

        Returns:
            None: Indicates successful save or logs an error.
        """
        file_path = os.path.join(self.table_path, file_name)
        try:
            os.makedirs(os.path.dirname(file_path), exist_ok=True)
            with open(file_path, "w") as file:
                json.dump(data, file, indent=4)
            logger.success(f"Data saved locally to {file_path}")
        except IOError as e:
            logger.error(f"Failed to save data locally: {e}")

    def save_s3(self, data: List[Dict], file_name: str) -> None:
        """
        Save parsed data to an AWS S3 bucket, handling any AWS client errors that might occur.

        Args:
            data (List[Dict]): List of parsed data.
            file_name (str): Name of the file to save the data in.

        Returns:
            None: Indicates successful save or logs an error.
        """
        if not self.bucket_name:
            logger.error("No S3 bucket configured for saving data.")
            return

        json_bytes = json.dumps(data, indent=4).encode("utf-8")
        key = file_name
        try:
            self.s3_client.put_object(Body=json_bytes, Bucket=self.bucket_name, Key=key)
            logger.success(f"Data saved successfully to S3 bucket: {self.bucket_name}, Key: {key}")
        except ClientError as e:
            logger.error(f"Failed to save data to S3: {e.response['Error']['Message']}")

class Ingestor:
    """
    Orchestrates the data fetching, parsing, and saving processes.

    Attributes:
        api_client (SpotifyAPIClient): Client for interacting with Spotify API.
        data_parser (DataParser): Parser for JSON data.
        data_saver (DataSaver): Saver for local and S3 storage.
    """
    def __init__(self, api_client, data_parser, data_saver):
        """
        Initialize the Ingestor with the required components.

        Args:
            api_client (SpotifyAPIClient): Client for interacting with Spotify API.
            data_parser (DataParser): Parser for JSON data.
            data_saver (DataSaver): Saver for local and S3 storage.
        """
        self.api_client = api_client
        self.data_parser = data_parser
        self.data_saver = data_saver

    def execute(self, search_query: str, search_type: str, genre: Optional[str] = None, limit: Optional[int] = 20, playlist_id: Optional[str] = None) -> str:
        """
        Executes the data ingestion process: fetches data from Spotify, parses it, and saves it locally and/or to S3.

        Args:
            search_query (str): The search query.
            search_type (str): Type of search (e.g., 'track', 'artist', 'playlist').
            genre (Optional[str]): Genre to filter the search results.
            limit (Optional[int]): Limit the number of items to return.
            playlist_id (Optional[str]): Specific ID of the playlist to fetch directly.

        Returns:
            str: The name of the file saved, or None if the process fails.
        """
        logger.info(f"Starting data ingestion for: {search_type}, Query: {search_query}, Genre: {genre}, Limit: {limit}, Playlist ID: {playlist_id}")
        try:
            fetched_data = self.api_client.search(search_query, search_type, genre, limit, playlist_id)
            if fetched_data:
                parsed_data = self.data_parser.parse_json_data(json.dumps(fetched_data))
                if parsed_data:
                    sanitized_playlist_id = playlist_id.replace('?', '_').replace(':', '_') if playlist_id else None
                    file_name = f"playlist_{sanitized_playlist_id}_{limit}.json" if playlist_id else f"{search_query.replace(' ', '_')}_{search_type}_{genre}_{limit}.json"
                    
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
        return None

    def execute_multiple(self, playlist_ids: List[str], limit: int = 20) -> List[str]:
        """
        Executes the ingestion process for multiple playlists, storing the results in a list.

        Args:
            playlist_ids (List[str]): List of playlist IDs to fetch.
            limit (int): Limit the number of items to return for each playlist.

        Returns:
            List[str]: List of file names saved, or an empty list if the process fails.
        """
        file_names = []
        for playlist_id in playlist_ids:
            file_name = self.execute("", "playlist", playlist_id=playlist_id, limit=limit)
            if file_name:
                file_names.append(file_name)
        return file_names

if __name__ == "__main__":
    # Initialize the API client, data parser, data saver, and ingestor
    client_id = os.getenv("client_id")
    client_secret = os.getenv("client_secret")
    api_client = SpotifyAPIClient(API_BASE_URL, client_id, client_secret)
    data_parser = DataParser()
    data_saver = DataSaver(TABLE_NAME, TABLE_PATH, bucket_name, AWS_ACCESS_KEY, AWS_SECRET_ACCESS_KEY)
    ingestor = Ingestor(api_client, data_parser, data_saver)

    # Example usage
    query = input("Enter a query to search on Spotify (e.g., 'Radiohead'): ")
    type = input("Enter the type to search (e.g., 'artist', 'track', 'album'): ")
    genre = input("Enter the genre (or leave blank for no specific genre): ")
    limit = int(input("Enter the maximum number of items to fetch (default 20): ") or "20")
    playlist_id = input("Enter a playlist ID (or leave blank for general search): ")
    ingestor.execute(query, type, genre if genre else None, limit, playlist_id if playlist_id else None)