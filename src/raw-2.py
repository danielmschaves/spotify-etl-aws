from typing import Dict, List, Optional, Union
from dotenv import load_dotenv
import requests
import os
import json
import boto3
from loguru import logger
from botocore.exceptions import ClientError

# Load environment variables
load_dotenv()

# Environmental configuration
API_BASE_URL = os.getenv("API_BASE_URL", "https://api.spotify.com/v1/")
ACCESS_TOKEN = os.getenv("SPOTIFY_ACCESS_TOKEN")
TABLE_NAME = os.getenv("TABLE_NAME", "spotify_data")
TABLE_PATH = "data/raw/"
AWS_BUCKET_NAME = os.getenv("AWS_BUCKET_NAME")
AWS_ACCESS_KEY = os.getenv("AWS_ACCESS_KEY")
AWS_SECRET_ACCESS_KEY = os.getenv("AWS_SECRET_ACCESS_KEY")

class SpotifyAPIClient:
    """
    Class for interacting with the Spotify API, providing methods to search for different entities.
    """

    def __init__(self, base_url: str, access_token: str) -> None:
        self.base_url = base_url
        self.access_token = access_token
        self.headers = {
            "Authorization": f"Bearer {self.access_token}",
            "Content-Type": "application/json"
        }
        self.session = requests.Session()

    def _make_request(self, endpoint: str, params: Optional[Dict[str, str]] = None) -> Optional[Dict]:
        """
        Makes an API request to the specified endpoint.

        Args:
            endpoint (str): The API endpoint.
            params (Optional[Dict[str, str]]): Optional parameters for the request.

        Returns:
            Optional[Dict]: The response data, or None if the request fails.
        """
        try:
            url = f"{self.base_url}{endpoint}"
            response = self.session.get(url, headers=self.headers, params=params)
            response.raise_for_status()
            return response.json()
        except requests.exceptions.HTTPError as e:
            logger.error(f"HTTP error occurred: {e.response.status_code} {e.response.reason} for URL {url}")
        except requests.exceptions.ConnectionError:
            logger.error("Connection error occurred")
        except requests.exceptions.Timeout:
            logger.error("Timeout occurred")
        except requests.exceptions.RequestException as e:
            logger.error(f"Request exception: {e}")
        return None

    def search(self, query: str, search_type: str, limit: Optional[int] = 20) -> Optional[List[Dict]]:
        """
        Generic search function for different Spotify entities like tracks and artists.

        Args:
            query (str): The search query.
            search_type (str): Type of search (e.g., 'track', 'artist').
            limit (Optional[int]): Limit the number of items to return.

        Returns:
            Optional[List[Dict]]: List of entities data, or None if the request fails.
        """
        params = {"q": query, "type": search_type, "limit": limit}
        endpoint = "search"
        response = self._make_request(endpoint, params=params)
        if response:
            items = response.get(search_type + 's', {}).get("items", [])
            logger.info(f"Search for {search_type}s '{query}' returned {len(items)} items.")
            return items
        else:
            logger.error(f"Failed to retrieve {search_type}s for query '{query}'")
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

    def save_local(self, data: List[Dict]) -> None:
        """
        Save parsed data to a local file system, handling any file system errors that might occur.

        Args:
            data (List[Dict]): List of parsed data.

        Returns:
            None: Indicates successful save or logs an error.
        """
        file_path = os.path.join(self.table_path, f"{self.table_name}.json")
        try:
            os.makedirs(os.path.dirname(file_path), exist_ok=True)
            with open(file_path, "w") as file:
                json.dump(data, file, indent=4)
            logger.success(f"Data saved locally to {file_path}")
        except IOError as e:
            logger.error(f"Failed to save data locally: {e}")

    def save_s3(self, data: List[Dict]) -> None:
        """
        Save parsed data to an AWS S3 bucket, handling any AWS client errors that might occur.

        Args:
            data (List[Dict]): List of parsed data.

        Returns:
            None: Indicates successful save or logs an error.
        """
        if not self.bucket_name:
            logger.error("No S3 bucket configured for saving data.")
            return

        json_bytes = json.dumps(data, indent=4).encode("utf-8")
        key = f"{self.table_name}.json"
        try:
            self.s3_client.put_object(Body=json_bytes, Bucket=self.bucket_name, Key=key)
            logger.success(f"Data saved successfully to S3 bucket: {self.bucket_name}")
        except ClientError as e:
            logger.error(f"Failed to save data to S3: {e.response['Error']['Message']}")

class Ingestor:
    """
    Class for ingesting data from the Spotify API, parsing it, and saving it. This class coordinates the
    interaction between the API client, data parser, and data saver.
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

    def execute(self, search_query: str, search_type: str, limit: Optional[int] = 20) -> None:
        """
        Executes the data ingestion process by fetching, parsing, and saving data from the Spotify API.

        Args:
            search_query (str): The search query to fetch data from the Spotify API.
            search_type (str): The type of data to search for (e.g., 'track', 'artist').
            limit (int, optional): Limit the number of items to fetch.

        Returns:
            None: A description of the output of this method.
        """
        logger.info(f"Starting data ingestion for: {search_type}, Query: {search_query}")
        try:
            # Fetch data from the Spotify API
            fetched_data = self.api_client.search(search_query, search_type, limit)
            if fetched_data:
                # Parse fetched data
                parsed_data = self.data_parser.parse_json_data(json.dumps(fetched_data))
                if parsed_data:
                    # Save parsed data locally and to S3
                    self.data_saver.save_local(parsed_data)
                    if self.data_saver.bucket_name:
                        self.data_saver.save_s3(parsed_data)
                    logger.success("Data ingestion process completed successfully.")
                else:
                    logger.warning("Parsing fetched data resulted in no output.")
            else:
                logger.warning("No data fetched from the Spotify API.")
        except Exception as e:
            logger.error(f"An error occurred during the data ingestion process: {e}")

if __name__ == "__main__":
    api_client = SpotifyAPIClient(API_BASE_URL, ACCESS_TOKEN)
    data_saver = DataSaver(TABLE_NAME, TABLE_PATH, AWS_BUCKET_NAME, AWS_ACCESS_KEY, AWS_SECRET_ACCESS_KEY)
    ingestor = Ingestor(api_client, data_saver)

    # Example usage
    query = input("Enter a query to search on Spotify (e.g., 'Radiohead'): ")
    type = input("Enter the type to search (e.g., 'artist', 'track', 'album'): ")
    limit = int(input("Enter the maximum number of items to fetch (default 20): ") or "20")
    ingestor.execute(query, type, limit)
