"""Module contains S3Storage class."""

from collections.abc import Iterator
from datetime import timedelta

from minio import Minio
from minio.error import S3Error

from esperoj.storage.storage import DeleteFilesResponse, Storage


class StreamResponse(Iterator):
    """The stream response."""

    def __init__(self, response):
        self.response = response
        self.stream = response.stream()

    def __iter__(self):
        return self

    def __next__(self):
        chunk = next(self.stream)
        if not chunk:
            self.response.close()
            self.response.release_conn()
            raise StopIteration
        return chunk


class S3Storage(Storage):
    """S3Storage class for handling S3 storage operations.

    This class provides methods for interacting with an S3 bucket, including
    uploading, downloading, deleting, and listing files.

    Attributes:
        config (dict): Configuration for S3Storage.
        client (Minio): The MinIO client instance.
    """

    def __init__(self, config: dict) -> None:
        """Initialize a S3Storage instance.

        Args:
            config (dict): Configuration for S3Storage.
        """
        self.__DEFAULT_CONFIG = {
            "name": "S3 Storage",
            "bucket_name": "esperoj",
            "endpoint": "localhost:9000",
            "access_key": "minioadmin",
            "secret_key": "minioadmin",
            "secure": True,
            "region": "eu-central-1",
            "multipart_chunksize": 2**20 * 64,
        }
        self.config = self.__DEFAULT_CONFIG | config
        self.client = Minio(
            endpoint=self.config["endpoint"],
            access_key=self.config["access_key"],
            secret_key=self.config["secret_key"],
            secure=self.config["secure"],
            region=self.config["region"],
        )

    def delete_files(self, paths: list[str]) -> DeleteFilesResponse:
        """Delete files from the S3 bucket.

        Args:
            paths (list[str]): The paths of the files to delete.

        Returns:
            DeleteFilesResponse: A response containing a list of errors encountered while deleting files.
        """
        errors = []
        for path in paths:
            try:
                self.client.remove_object(self.config["bucket_name"], path)
            except S3Error as e:
                errors.append({"path": path, "message": str(e)})
        return {"errors": errors}

    def download_file(self, src: str, dst: str) -> None:
        """Download a file from the S3 bucket.

        Args:
            src (str): The path of the file to download.
            dst (str): The destination path where the file will be saved.

        Raises:
            S3Error: If an error occurs while downloading the file.
        """
        self.client.fget_object(self.config["bucket_name"], src, dst)

    def file_exists(self, path: str) -> bool:
        """Check if a file exists in the S3 bucket.

        Args:
            path (str): The path of the file to check.

        Returns:
            bool: True if the file exists, False otherwise.

        Raises:
            S3Error: If an error occurs while checking for the file's existence.
        """
        try:
            self.client.stat_object(self.config["bucket_name"], path)
            return True
        except S3Error as e:
            if e.code == "NoSuchKey":
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
        return self.client.presigned_get_object(
            self.config["bucket_name"], path, expires=timedelta(days=7)
        )

    def get_file(self, src: str) -> Iterator:
        """Get a file from the S3 bucket and return an Iterator.

        Args:
            src (str): The path of the file to download.

        Returns:
            Iterator: An Iterator of the file content.

        Raises:
            S3Error: If an error occurs while downloading the file.
        """
        response = self.client.get_object(self.config["bucket_name"], src)
        return StreamResponse(response)

    def list_files(self, path: str) -> list:
        """List all files in the specified path of the S3 bucket.

        Args:
            path (str): The path to list files from.

        Returns:
            list[str]: A list of file paths.

        Raises:
            FileNotFoundError: If the specified path does not exist.
        """
        objects = self.client.list_objects(
            self.config["bucket_name"], prefix=path, recursive=True
        )
        files = [obj.object_name for obj in objects]
        if not files:
            raise FileNotFoundError(f"No such directory: '{path}'")
        return files

    def upload_file(self, src: str, dst: str) -> None:
        """Upload a file to the S3 bucket.

        Args:
            src (str): The source path of the file to upload.
            dst (str): The destination path in the S3 bucket.

        Raises:
            S3Error: If an error occurs while uploading the file.
        """
        try:
            self.client.fput_object(
                self.config["bucket_name"],
                dst,
                src,
                part_size=self.config["multipart_chunksize"],
            )
        except S3Error as e:
            raise e

    def size(self, src: str) -> int:
        """Check file size

        Args:
            src (str): The path of the file.

        Returns:
            size (int): Size of the object.
        """
        try:
            stats = self.client.stat_object(self.config["bucket_name"], src)
            if stats.size:
                return stats.size
            raise FileNotFoundError()
        except S3Error as e:
            raise e
