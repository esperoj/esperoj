"""Module contains S3Storage class."""

import boto3
from boto3.s3.transfer import TransferConfig
from botocore.exceptions import ClientError

from esperoj.storage.storage import DeleteFilesResponse, Storage
from typing import Iterator


class S3Storage(Storage):
    """S3Storage class for handling S3 storage operations.

    Attributes:
    ----------
        config (dict): Configuration for S3Storage.
        client (boto3.client): The S3 client instance.
    """

    def __init__(self, config: dict) -> None:
        """Initialize a S3Storage instance.

        Args:
            config (dict): Configuration for S3Storage.
        """
        self.__DEFAULT_CONFIG = {
            "name": "S3 Storage",
            "bucket_name": "esperoj",
            "aliases": [],
            "client_config": {},
            "transfer_config": {
                "multipart_threshold": 8 * 2**20,
                "max_concurrency": 10,
                "multipart_chunksize": 8 * 2**20,
            },
        }
        self.config = self.__DEFAULT_CONFIG | config
        self.config["aliases"] = [*self.__DEFAULT_CONFIG["aliases"], *config.get("aliases", [])]
        self.config["client_config"] = self.__DEFAULT_CONFIG["client_config"] | config.get(
            "client_config", {}
        )
        self.config["transfer_config"] = TransferConfig(
            **(self.__DEFAULT_CONFIG["transfer_config"] | config.get("transfer_config", {}))
        )
        self.client = boto3.client("s3", **self.config["client_config"])

    def delete_files(self, paths: list[str]) -> DeleteFilesResponse:
        """Delete files from the S3 bucket.

        Args:
            paths (list[str]): The paths of the files to delete.

        Returns:
        -------
            response (DeleteFilesResponse): Response includes list of errors.
        """
        response = self.client.delete_objects(
            Bucket=self.config["bucket_name"], Delete={"Objects": [{"Key": path} for path in paths]}
        )
        if response.get("Errors") is None:
            return {"errors": []}
        return {"errors": [{"path": e["Key"], "message": e["Message"]} for e in response["Errors"]]}

    def download_file(self, src: str, dst: str) -> None:
        self.client.download_file(
            self.config["bucket_name"], src, dst, Config=self.config["transfer_config"]
        )

    def file_exists(self, path: str) -> bool:
        """Check if a file exists in the S3 bucket.

        Args:
            path (str): The path of the file to check.

        Returns:
        -------
            bool: True if the file exists, False otherwise.
        """
        try:
            self.client.head_object(Bucket=self.config["bucket_name"], Key=path)
            return True
        except ClientError as e:
            if e.response["Error"]["Code"] == "404":
                return False
            raise e

    def get_link(self, path: str) -> str:
        """Get a pre-signed URL for a file in the S3 bucket.

        Args:
            path (str): The path of the file to get the URL for.

        Returns:
            str: A pre-signed URL for the file.

        Raises:
            FileNotFoundError: If the file does not exist.
        """
        if not self.file_exists(path):
            raise FileNotFoundError(f"No such file: '{path}'")
        return self.client.generate_presigned_url(
            "get_object",
            Params={"Bucket": self.config["bucket_name"], "Key": path},
            ExpiresIn=3600 * 24 * 7,
        )

    def get_file(self, src: str) -> Iterator:
        return self.client.get_object(Bucket=self.config["bucket_name"], Key=src)[
            "Body"
        ].iter_chunks(2**20)

    def list_files(self, path: str) -> list:
        """List all files in the specified path of the S3 bucket.

        Args:
            path (str): The path to list files from.

        Returns:
        -------
            files (list[str]): A list of file paths.

        Raises:
        ------
            FileNotFoundError: If the specified path does not exist.
        """
        paginator = self.client.get_paginator("list_objects_v2")
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

        Raises:
        ------
            FileNotFoundError: If the source file does not exist.
        """
        try:
            self.client.upload_file(
                src, self.config["bucket_name"], dst, Config=self.config["transfer_config"]
            )
        except ClientError as e:
            raise e
        except FileNotFoundError as e:
            raise FileNotFoundError(f"No such file: '{src}'") from e
