"""Module contains S3Storage class."""

import os

import boto3
from boto3.s3.transfer import TransferConfig
from botocore.exceptions import ClientError

from esperoj.storage import BaseStorage

default_config = {
    "bucket_name": "esperoj",
    "client_config": {
        "aws_access_key_id": os.getenv("AWS_ACCESS_KEY_ID"),
        "aws_secret_access_key": os.getenv("AWS_SECRET_ACCESS_KEY"),
        "endpoint_url": os.getenv("AWS_ENDPOINT_URL"),
    },
    "transfer_config": TransferConfig(
        multipart_threshold=64 * 2**20, max_concurrency=8, multipart_chunksize=64 * 2**20
    ),
}


class S3Storage(BaseStorage):
    """S3Storage class for handling S3 storage operations.

    Attributes
    ----------
        name (str): The name of the storage.
        config (dict): Configuration dictionary for the S3 client.
        s3 (boto3.client): The S3 client instance.
    """

    def __init__(self, name: str, config: dict = default_config) -> None:
        """
        Initialize a S3Storage instance.

        Args:
            name (str): The name of the storage.
            config (dict, optional): Configuration dictionary for the S3 client. Defaults to default_config.
        """
        super().__init__(name, config=default_config)
        self.config.update(config)
        self.s3 = boto3.client("s3", **self.config["client_config"])

    def delete_file(self, path: str) -> None:
        """Delete a file from the S3 bucket.

        Args:
            path (str): The path of the file to delete.

        Raises
        ------
            FileNotFoundError: If the file does not exist.
        """
        if not self.file_exists(path):
            raise FileNotFoundError(f"No such file: '{path}'")
        try:
            self.s3.delete_object(Bucket=self.config["bucket_name"], Key=path)
        except ClientError as e:
            raise FileNotFoundError(f"No such file: '{path}'") from e

    def download_file(self, src: str, dst: str) -> None:
        """Download a file from the S3 bucket.

        Args:
            src (str): The source path of the file in the S3 bucket.
            dst (str): The destination path to save the downloaded file.

        Raises
        ------
            FileNotFoundError: If the source file does not exist.
        """
        try:
            self.s3.download_file(
                self.config["bucket_name"], src, dst, Config=self.config["transfer_config"]
            )
        except ClientError as e:
            raise FileNotFoundError(f"No such file: '{src}'") from e

    def file_exists(self, path: str) -> bool:
        """Check if a file exists in the S3 bucket.

        Args:
            path (str): The path of the file to check.

        Returns
        -------
            bool: True if the file exists, False otherwise.
        """
        try:
            self.s3.head_object(Bucket=self.config["bucket_name"], Key=path)
            return True
        except ClientError as e:
            if e.response["Error"]["Code"] == "404":
                return False
            raise e

    def list_files(self, path: str) -> list:
        """List all files in the specified path of the S3 bucket.

        Args:
            path (str): The path to list files from.

        Returns
        -------
            list: A list of file keys.

        Raises
        ------
            FileNotFoundError: If the specified path does not exist.
        """
        paginator = self.s3.get_paginator("list_objects_v2")
        files: list[str] = []
        for page in paginator.paginate(Bucket=self.config["bucket_name"], Prefix=path):
            files.extend(obj["Key"] for obj in page.get("Contents", []))
        if not files:
            raise FileNotFoundError(f"No such directory: '{path}'")
        return files

    def upload_file(self, src: str, dst: str) -> None:
        """Upload a file to the S3 bucket.

        Args:
            src (str): The source path of the file to upload.
            dst (str): The destination path in the S3 bucket.

        Raises
        ------
            FileNotFoundError: If the source file does not exist.
        """
        try:
            self.s3.upload_file(
                src, self.config["bucket_name"], dst, Config=self.config["transfer_config"]
            )
        except ClientError as e:
            raise e
        except FileNotFoundError as e:
            raise FileNotFoundError(f"No such file: '{src}'") from e
