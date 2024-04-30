from datetime import datetime, timedelta
import os
import sys
from airflow.decorators import dag, task
from dotenv import load_dotenv
import logging

sys.path.append(os.path.abspath(os.path.join(os.path.dirname(__file__), "..", "..")))
from manager import DuckDBManager, AWSManager
from bronze import DataManager

# Load environment variables
load_dotenv()

# Configuration
aws_access_key = os.getenv("AWS_ACCESS_KEY")
aws_secret_access_key = os.getenv("AWS_SECRET_ACCESS_KEY")
bronze_bucket = os.getenv("BRONZE_BUCKET")
aws_region = os.getenv("AWS_REGION")
raw_s3_path = os.getenv("RAW_S3_PATH")
bronze_s3_path = os.getenv("BRONZE_S3_PATH")
local_path = "data/bronze/"
table_names = ["playlist", "tracks", "album", "artist"]

# Initialize the database and AWS managers
db_manager = DuckDBManager()
aws_manager = AWSManager(db_manager, aws_region, aws_access_key, aws_secret_access_key)
data_manager = DataManager(db_manager, aws_manager, local_path, bronze_bucket)


default_args = {
    "owner": "airflow",
    "depends_on_past": False,
    "start_date": datetime(2024, 4, 1),
    "email_on_failure": False,
    "email_on_retry": False,
    "retries": 1,
    "retry_delay": timedelta(minutes=5),
    "catchup": False,
}

logger = logging.getLogger("airflow.task")


@dag(
    default_args=default_args,
    schedule_interval="@daily",
    catchup=False,
    tags=["spotify_bronze_etl"],
)
def spotify_bronze_etl_dag():
    """
    Airflow DAG to load, transform, and export data from raw JSON files into a DuckDB database,
    and then to local and S3 storage.
    """

    @task
    def load_and_transform_data():
        logger.info(f"Initiating load and transform for {raw_s3_path} into playlists")
        try:
            data = data_manager.load_and_transform_data(raw_s3_path, "playlists")
            if data:
                logger.info("Successfully loaded and transformed data for playlists")
                return data
            else:
                logger.error("No data returned from load and transform")
                return None
        except Exception as e:
            logger.error(f"Failed to load and transform data: {e}")
            raise

    @task
    def process_tracks(data):
        if data:
            logger.info("Processing tracks from playlists")
            if 'tracks' in data:
                data_manager.handle_tracks(data['tracks'], data['id'])
        else:
            logger.warning("No playlist data to process tracks from")

    @task
    def process_albums(data):
        if data:
            logger.info("Processing albums from tracks")
            if 'albums' in data:
                for track in data['tracks']:
                    data_manager.handle_album(track['album'], track['track']['id'])

    @task
    def process_artists(data):
        if data:
            logger.info("Processing artists from tracks")
            if 'artists' in data:
                for track in data['tracks']:
                    data_manager.handle_artists(track['artists'], track['track']['id'])

    @task
    def save_to_local():
        logger.info("Saving data to local storage")
        for table_name in table_names:
            data_manager.save_to_local(table_name)

    @task
    def save_to_s3():
        logger.info("Saving data to S3")
        for table_name in table_names:
            data_manager.save_to_s3(table_name)

    # Task dependencies
    playlist_data = load_and_transform_data()
    tracks_data = process_tracks(playlist_data)
    albums_data = process_albums(playlist_data)
    artists_data = process_artists(playlist_data)

    # Final saving steps
    save_local_task = save_to_local()
    save_s3_task = save_to_s3()

    # Setting dependencies
    [tracks_data, albums_data, artists_data] >> save_local_task
    save_local_task >> save_s3_task

# Instantiate the DAG
spotify_etl_dag = spotify_bronze_etl_dag()