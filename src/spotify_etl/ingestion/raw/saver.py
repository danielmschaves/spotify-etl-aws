"""Data saver for local and S3 storage."""

from typing import List, Dict, Optional, Any
import os
import json
import boto3
from loguru import logger
from botocore.exceptions import ClientError


class DataSaver:
    """
    Class for saving data locally or to an AWS S3 bucket.

    Attributes:
        table_name: Name of the table.
        table_path: Local path where the data files will be stored.
        bucket_name: Name of the AWS S3 bucket.
        s3_client: Boto3 S3 client.
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
            table_name: Name of the table.
            table_path: Local path where the data files will be stored.
            bucket_name: Name of the AWS S3 bucket.
            access_key_id: AWS access key ID.
            secret_access_key: AWS secret access key.
        """
        self.table_name = table_name
        self.table_path = table_path
        self.bucket_name = bucket_name
        if bucket_name and access_key_id and secret_access_key:
            self.s3_client = boto3.client(
                "s3",
                aws_access_key_id=access_key_id,
                aws_secret_access_key=secret_access_key,
            )
        else:
            self.s3_client = None

    def save_local(self, data: List[Dict[str, Any]], file_name: str) -> None:
        """
        Save parsed data to a local file system.

        Args:
            data: List of parsed data.
            file_name: Name of the file to save the data in.
        """
        file_path = os.path.join(self.table_path, file_name)
        try:
            os.makedirs(os.path.dirname(file_path), exist_ok=True)
            with open(file_path, "w", encoding="utf-8") as file:
                json.dump(data, file, indent=4)
            logger.success(f"Data saved locally to {file_path}")
        except IOError as e:
            logger.error(f"Failed to save data locally: {e}")
            raise

    def save_s3(self, data: List[Dict[str, Any]], file_name: str) -> None:
        """
        Save parsed data to an AWS S3 bucket.

        Args:
            data: List of parsed data.
            file_name: Name of the file to save the data in.
        """
        if not self.bucket_name:
            logger.error("No S3 bucket configured for saving data.")
            return

        if not self.s3_client:
            logger.error("S3 client not initialized.")
            return

        json_bytes = json.dumps(data, indent=4).encode("utf-8")
        key = file_name
        try:
            self.s3_client.put_object(Body=json_bytes, Bucket=self.bucket_name, Key=key)
            logger.success(
                f"Data saved successfully to S3 bucket: {self.bucket_name}, Key: {key}"
            )
        except ClientError as e:
            logger.error(f"Failed to save data to S3: {e.response['Error']['Message']}")
            raise

