from datetime import datetime, timedelta
import os
from airflow.decorators import dag, task
from airflow.operators.dagrun_operator import TriggerDagRunOperator
from dotenv import load_dotenv
from src.raw import SpotifyAPIClient, DataParser, DataSaver

# Load environment variables
load_dotenv()

# Configuration from environment variables
API_BASE_URL = os.getenv("API_BASE_URL", "https://api.spotify.com/v1/")
CLIENT_ID = os.getenv("SPOTIFY_CLIENT_ID")
CLIENT_SECRET = os.getenv("SPOTIFY_CLIENT_SECRET")
TABLE_NAME = os.getenv("TABLE_NAME", "spotify_data")
TABLE_PATH = "data/raw/"
AWS_BUCKET_NAME = os.getenv("AWS_BUCKET_NAME")
AWS_ACCESS_KEY = os.getenv("AWS_ACCESS_KEY")
AWS_SECRET_ACCESS_KEY = os.getenv("AWS_SECRET_ACCESS_KEY")
QUERY = "The Beatles"  # Example search query
SEARCH_TYPE = "artist"  # Could be 'track', 'album', etc.

default_args = {
    'owner': 'airflow',
    'depends_on_past': False,
    'start_date': datetime(2024, 4, 1),
    'email_on_failure': False,
    'email_on_retry': False,
    'catchup': False,  # Set to False to skip intervals where the DAG is inactive
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
            # Save data locally and to S3
            data_saver.save_local(parsed_data)
            data_saver.save_s3(parsed_data)

    # Setup task
    fetch_and_save_spotify_data_task = fetch_and_save_spotify_data()

    fetch_and_save_spotify_data_task 

# Instantiate the DAG
dag = spotify_etl_dag()
