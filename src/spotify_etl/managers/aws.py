"""AWS S3 client and credential management."""

from typing import Optional
from loguru import logger
import boto3
from botocore.exceptions import ClientError

from .duckdb import DuckDBManager


class AWSManager:
    """Manages AWS credentials and operations."""

    def __init__(
        self,
        duckdb_manager: DuckDBManager,
        aws_region: str,
        aws_access_key: str,
        aws_secret_access_key: str,
    ):
        """
        Initialize AWSManager.

        Args:
            duckdb_manager: Instance of DuckDBManager.
            aws_region: AWS region.
            aws_access_key: AWS access key ID.
            aws_secret_access_key: AWS secret access key.
        """
        self.duckdb_manager = duckdb_manager
        self.aws_region = aws_region
        self.aws_access_key = aws_access_key
        self.aws_secret_access_key = aws_secret_access_key
        self.s3_client = self.create_s3_client(aws_region, aws_access_key, aws_secret_access_key)
        self.load_credentials(aws_region, aws_access_key, aws_secret_access_key)

    def create_s3_client(
        self, aws_region: str, aws_access_key: str, aws_secret_access_key: str
    ) -> boto3.client:
        """
        Creates a boto3 S3 client with the given credentials.

        Args:
            aws_region: AWS region.
            aws_access_key: AWS access key ID.
            aws_secret_access_key: AWS secret access key.

        Returns:
            boto3.client: Configured boto3 S3 client.
        """
        return boto3.client(
            "s3",
            region_name=aws_region,
            aws_access_key_id=aws_access_key,
            aws_secret_access_key=aws_secret_access_key,
        )

    def load_credentials(
        self, aws_region: str, aws_access_key: str, aws_secret_access_key: str
    ) -> None:
        """
        Loads AWS credentials into DuckDB for S3 access.

        Args:
            aws_region: AWS region.
            aws_access_key: AWS access key ID.
            aws_secret_access_key: AWS secret access key.

        Returns:
            None
        """
        try:
            logger.info("Loading AWS credentials into DuckDB")
            self.duckdb_manager.execute_query("INSTALL httpfs;")
            self.duckdb_manager.execute_query("LOAD httpfs;")
            self.duckdb_manager.execute_query(f"SET s3_region='{aws_region}';")
            self.duckdb_manager.execute_query(f"SET s3_access_key_id='{aws_access_key}';")
            self.duckdb_manager.execute_query(f"SET s3_secret_access_key='{aws_secret_access_key}';")
            self.duckdb_manager.execute_query("CALL load_aws_credentials();")
            logger.success("AWS credentials loaded!")
        except Exception as e:
            logger.error(f"Error loading AWS credentials: {e}")
            raise

    def upload_file(self, local_path: str, bucket: str, key: str) -> bool:
        """
        Upload a file to S3.

        Args:
            local_path: Local file path.
            bucket: S3 bucket name.
            key: S3 object key.

        Returns:
            bool: True if successful, False otherwise.
        """
        try:
            self.s3_client.upload_file(local_path, bucket, key)
            logger.success(f"File uploaded to s3://{bucket}/{key}")
            return True
        except ClientError as e:
            logger.error(f"Failed to upload file to S3: {e}")
            return False

    def download_file(self, bucket: str, key: str, local_path: str) -> bool:
        """
        Download a file from S3.

        Args:
            bucket: S3 bucket name.
            key: S3 object key.
            local_path: Local file path to save to.

        Returns:
            bool: True if successful, False otherwise.
        """
        try:
            self.s3_client.download_file(bucket, key, local_path)
            logger.success(f"File downloaded from s3://{bucket}/{key}")
            return True
        except ClientError as e:
            logger.error(f"Failed to download file from S3: {e}")
            return False

