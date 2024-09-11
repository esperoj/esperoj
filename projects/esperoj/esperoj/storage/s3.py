"""Module contains S3Storage class."""

from collections.abc import Iterator
import boto3
from boto3.s3.transfer import TransferConfig
from botocore.exceptions import ClientError
from esperoj.storage.storage import Storage
from pathlib import Path

class S3Storage(Storage):
    """S3Storage class for handling S3 storage operations.

    This class provides methods for interacting with an S3 bucket, including
    uploading, downloading, deleting, and listing files and folders.

    Attributes:
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
            "client_config": {},
            "transfer_config": {
                "multipart_threshold": 8 * 2**20,
                "max_concurrency": 10,
                "multipart_chunksize": 8 * 2**20,
            },
        }
        self.config = self.__DEFAULT_CONFIG | config
        self.config["client_config"] = self.__DEFAULT_CONFIG["client_config"] | config.get("client_config", {})
        self.config["transfer_config"] = TransferConfig(
            **(self.__DEFAULT_CONFIG["transfer_config"] | config.get("transfer_config", {}))
        )
        self.client = boto3.client("s3", **self.config["client_config"])

    def delete(self, paths: list[str]) -> bool:
        """Delete files or folders from the S3 bucket.

        Args:
            paths (list[str]): The paths of the files or folders to delete.

        Returns:
            bool: Return True if operation succeeded and False if not.
        """
        response = self.client.delete_objects(
            Bucket=self.config["bucket_name"], Delete={"Objects": [{"Key": path} for path in paths]}
        )
        return True if response.get("Errors") is None else False

    def download(self, src: str, dst: str) -> None:
        """Download a file or folder from the S3 bucket.

        Args:
            src (str): The path of the file or folder to download.
            dst (str): The destination path where the file or folder will be saved.

        Raises:
            ClientError: If an error occurs while downloading.
        """
        objects = self.list(src)
        if not objects:
            raise FileNotFoundError(f"No such file or directory: '{src}'")

        for obj in objects:
            if obj.endswith('/'):
                continue
            dst_path = obj.replace(src, dst, 1)
            self.client.download_file(self.config["bucket_name"], obj, dst_path, Config=self.config["transfer_config"])

    def exists(self, path: str) -> bool:
        """Check if a file or folder exists in the S3 bucket.

        Args:
            path (str): The path of the file or folder to check.

        Returns:
            bool: True if the file or folder exists, False otherwise.
        """
        paginator = self.client.get_paginator("list_objects_v2")
        for page in paginator.paginate(Bucket=self.config["bucket_name"], Prefix=path):
            if "Contents" in page:
                return True
        return False

    def link(self, path: str) -> str:
        """Get a pre-signed URL for a file in the S3 bucket.

        Args:
            path (str): The path of the file to get the URL for.

        Returns:
            str: A pre-signed URL for the file.

        Raises:
            FileNotFoundError: If the file does not exist.
        """
        if not self.exists(path):
            raise FileNotFoundError(f"No such file or directory: '{path}'")
        return self.client.generate_presigned_url(
            "get_object",
            Params={"Bucket": self.config["bucket_name"], "Key": path},
            ExpiresIn=3600 * 24 * 7,
        )

    def stream(self, src: str) -> Iterator:
        """Get a file from the S3 bucket and return an Iterator.

        Args:
            src (str): The path of the file to download.

        Returns:
            Iterator: An Iterator of the file content.

        Raises:
            ClientError: If an error occurs while downloading the file.
        """
        return self.client.get_object(Bucket=self.config["bucket_name"], Key=src)["Body"].iter_chunks(2**20)

    def list(self, path: str) -> list[str]:
        """List all files and folders in the specified path of the S3 bucket.

        Args:
            path (str): The path to list files and folders from.

        Returns:
            list[str]: A list of file and folder paths.

        Raises:
            FileNotFoundError: If the specified path does not exist.
        """
        paginator = self.client.get_paginator("list_objects_v2")
        files: list[str] = []
        for page in paginator.paginate(Bucket=self.config["bucket_name"], Prefix=path, Delimiter='/'):
            if "Contents" in page:
                files.extend(obj["Key"] for obj in page.get("Contents", []))
            if "CommonPrefixes" in page:
                files.extend(prefix["Prefix"] for prefix in page.get("CommonPrefixes", []))
        if not files:
            raise FileNotFoundError(f"No such directory: '{path}'")
        return files

    def upload(self, src: str, dst: str) -> None:
        """Upload a file or folder to the S3 bucket.
    
        Args:
            src (str): The source path of the file or folder to upload.
            dst (str): The destination path in the S3 bucket.
    
        Raises:
            ClientError: If an error occurs while uploading.
            FileNotFoundError: If the source file does not exist.
        """
        src_path = Path(src)

        if not src_path.exists():
            raise FileNotFoundError(f"No such file or directory: '{src}'")

        if src_path.is_file():
            self.client.upload_file(str(src_path), self.config["bucket_name"], dst, Config=self.config["transfer_config"])
        else:
            for file_path in src_path.rglob('*'):
                if file_path.is_file():
                    relative_path = file_path.relative_to(src_path)
                    s3_path = Path(dst) / relative_path
                    self.client.upload_file(str(file_path), self.config["bucket_name"], str(s3_path).replace("\\", "/"))

    def size(self, src: str) -> int:
        """Check file size.

        Args:
            src (str): The path of the file.

        Returns:
            int: Size of the object.
        """
        return self.client.head_object(Bucket=self.config["bucket_name"], Key=src)["ContentLength"]
