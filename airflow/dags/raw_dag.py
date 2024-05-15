from datetime import datetime, timedelta
import os
import sys
from airflow.decorators import dag, task
from dotenv import load_dotenv
import logging
from airflow.operators.trigger_dagrun import TriggerDagRunOperator

# Set the system path to include the custom manager modules directory
sys.path.append(os.path.abspath(os.path.join(os.path.dirname(__file__), "..", "..")))
from raw import SpotifyAPIClient, DataParser, DataSaver, Ingestor

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
playlist_ids = ["37i9dQZEVXbMDoHDwVN2tF", "37i9dQZF1DXcBWIGoYBM5M", "37i9dQZF1DWXRqgorJj26U"]

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
    def raw_ingestion():
        file_names = []
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
            ingestor = Ingestor(api_client, data_parser, data_saver)
            for playlist_id in playlist_ids:
                ingestor.execute("", "playlist", playlist_id=playlist_id)
                file_name = f"playlist_{playlist_id}_20.json"
                file_names.append(file_name)
        except Exception as e:
            logger.error(f"An error occurred: {e}")
        
        logger.info(f"File names generated: {file_names}")
        return file_names

    fetch_and_save_spotify_data_task = raw_ingestion()

    trigger_bronze_ingestion = TriggerDagRunOperator(
        task_id="trigger_bronze_ingestion",
        trigger_dag_id="bronze_ingestion",
        conf={"file_names": "{{ task_instance.xcom_pull(task_ids='raw_ingestion') }}"},
    )

    fetch_and_save_spotify_data_task >> trigger_bronze_ingestion

# Instantiate the DAG
dag = raw_ingestion()