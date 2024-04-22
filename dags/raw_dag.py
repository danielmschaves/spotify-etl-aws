from datetime import datetime, timedelta
import os
import sys
from airflow.decorators import dag, task
from dotenv import load_dotenv

sys.path.append(os.path.abspath(os.path.join(os.path.dirname(__file__), "..", "..")))
from raw import SpotifyAPIClient, DataParser, DataSaver

# Load environment variables
load_dotenv()

# Configuration from environment variables
API_BASE_URL = os.getenv("API_BASE_URL", "https://api.spotify.com/v1/")
CLIENT_ID = os.getenv("client_id")
CLIENT_SECRET = os.getenv("client_secret")
TABLE_NAME = os.getenv("TABLE_NAME", "spotify_data")
TABLE_PATH = "data/raw/"
AWS_BUCKET_NAME = os.getenv("AWS_BUCKET_NAME")
AWS_ACCESS_KEY = os.getenv("AWS_ACCESS_KEY")
AWS_SECRET_ACCESS_KEY = os.getenv("AWS_SECRET_ACCESS_KEY")
QUERY = "genre:'hip hop'"  # Search query including genre
SEARCH_TYPE = "artist"  # Search type

default_args = {
    'owner': 'airflow',
    'depends_on_past': False,
    'start_date': datetime(2024, 4, 1),
    'email_on_failure': False,
    'email_on_retry': False,
    'catchup': False,
    'retries': 1,
    'retry_delay': timedelta(minutes=5),
}

@dag(default_args=default_args, schedule_interval='@daily', catchup=False, tags=['spotify_etl'])
def spotify_etl_dag():
    """
    Airflow DAG to ingest data from Spotify API, parse it, and save it both locally and to S3.
    """
    @task
    def fetch_and_save_spotify_data():
        api_client = SpotifyAPIClient(API_BASE_URL, CLIENT_ID, CLIENT_SECRET)
        data_parser = DataParser()
        data_saver = DataSaver(TABLE_NAME, TABLE_PATH, AWS_BUCKET_NAME, AWS_ACCESS_KEY, AWS_SECRET_ACCESS_KEY)

        # Fetch data from Spotify API
        fetched_data = api_client.search(QUERY, SEARCH_TYPE)
        if fetched_data:
            # Parse data
            parsed_data = data_parser.parse_json_data(fetched_data)
            # Define a file name for saving
            file_name = f"{QUERY.replace(' ', '_')}_{SEARCH_TYPE}.json"
            # Save data locally and to S3
            data_saver.save_local(parsed_data, file_name)
            data_saver.save_s3(parsed_data, file_name)

    # Setup task
    fetch_and_save_spotify_data_task = fetch_and_save_spotify_data()

    fetch_and_save_spotify_data_task 

# Instantiate the DAG
dag = spotify_etl_dag()
