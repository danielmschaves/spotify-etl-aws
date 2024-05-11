from datetime import datetime, timedelta
import os
import sys
from airflow.decorators import dag, task
from dotenv import load_dotenv
import logging


sys.path.append(os.path.abspath(os.path.join(os.path.dirname(__file__), "..", "..")))
from raw import SpotifyAPIClient, DataParser, DataSaver

# Load environment variables
load_dotenv()

# Configuration from environment variables
api_base_url = os.getenv("API_BASE_URL", "https://api.spotify.com/v1/")
client_id = os.getenv("CLIENT_ID")
client_secret = os.getenv("CLIENT_SECRET")
table_name = os.getenv("TABLE_NAME", "spotify_data")
table_path = "data/raw/"
aws_bucket_name = os.getenv("AWS_BUCKET_NAME")
aws_access_key = os.getenv("AWS_ACCESS_KEY")
aws_secret_access_key = os.getenv("AWS_SECRET_ACCESS_KEY")
query = "genre:'hip hop'"  # Search query including genre
search_type = "artist"  # Search type
playlist_id = "37i9dQZEVXbMDoHDwVN2tF"  # Spotify Playlist ID

default_args = {
    "owner": "airflow",
    "depends_on_past": False,
    "start_date": datetime(2024, 4, 1),
    "email_on_failure": False,
    "email_on_retry": False,
    "catchup": False,
    "retries": 1,
    "retry_delay": timedelta(minutes=5),
}

logger = logging.getLogger("airflow.task")


@dag(
    default_args=default_args,
    schedule_interval="@daily",
    catchup=False,
    tags=["spotify_raw_etl"],
)
def raw_ingestion():
    """
    Airflow DAG to ingest data from Spotify API, parse it, and save it both locally and to S3.
    """

    @task
    def fetch_and_save_spotify_data():
        try:
            api_client = SpotifyAPIClient(api_base_url, client_id, client_secret)
            data_parser = DataParser()
            data_saver = DataSaver(
                table_name,
                table_path,
                aws_bucket_name,
                aws_access_key,
                aws_secret_access_key,
            )

            # Fetch playlist data from Spotify API
            logger.info("START FETCHING PLAYLIST DATA")
            fetched_data = api_client.search(
                query="", search_type="playlist", playlist_id=playlist_id
            )
            if fetched_data:
                # Parse data
                logger.info("START PARSING DATA")
                parsed_data = data_parser.parse_json_data(fetched_data)
                # Define a file name for saving
                file_name = f"playlist_{playlist_id}.json"
                # Save data locally and to S3
                logger.info("START SAVING DATA LOCALLY")
                data_saver.save_local(parsed_data, file_name)
                logger.info("START SAVING DATA TO S3")
                data_saver.save_s3(parsed_data, file_name)
        except Exception as e:
            logger.error(f"An error occured: {e}")

    # Setup task
    fetch_and_save_spotify_data_task = fetch_and_save_spotify_data()

    fetch_and_save_spotify_data_task


# Instantiate the DAG
dag = raw_ingestion()
