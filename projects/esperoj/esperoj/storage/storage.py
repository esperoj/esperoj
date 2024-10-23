"""Storage module."""

from abc import ABC, abstractmethod
from collections.abc import Iterator


class Storage(ABC):
    """Abstract base class for storage.

    This class defines the interface for storage implementations.
    """

    @abstractmethod
    def __init__(self, config: dict) -> None:
        """Initialize the Storage.

        Args:
            config (dict): Configuration for the storage.
        """

    @abstractmethod
    def delete(self, paths: list[str]) -> bool:
        """Delete files or folders from the S3 bucket.

        Args:
            paths (list[str]): The paths of the files or folders to delete.

        Returns:
            bool: Return True if operation succeeded and False if not.
        """

    @abstractmethod
    def download(self, src: str, dst: str) -> None:
        """Download a file or folder from the S3 bucket.

        Args:
            src (str): The path of the file or folder to download.
            dst (str): The destination path where the file or folder will be saved.

        Raises:
            ClientError: If an error occurs while downloading.
        """

    @abstractmethod
    def exists(self, path: str) -> bool:
        """Check if a file or folder exists in the S3 bucket.

        Args:
            path (str): The path of the file or folder to check.

        Returns:
            bool: True if the file or folder exists, False otherwise.
        """

    @abstractmethod
    def link(self, path: str) -> str:
        """Get a download link for a file in the storage.

        Args:
            path (str): The path to the file.

        Returns:
            str: The URL to download the file.
        """

    @abstractmethod
    def stream(self, src: str) -> Iterator[bytes]:
        """Get a file from the source and return an Iterator.

        Args:
            src (str): The source path of the file to download.

        Returns:
            Iterator: An Iterator of the file content.
        """

    @abstractmethod
    def list(self, path: str) -> list[str]:
        """List all files and folders in the specified path of the S3 bucket.

        Args:
            path (str): The path to list files and folders from.

        Returns:
            list[str]: A list of file and folder paths.

        Raises:
            FileNotFoundError: If the specified path does not exist.
        """

    @abstractmethod
    def upload(self, src: str, dst: str) -> None:
        """Upload a file or folder to the S3 bucket.

        Args:
            src (str): The source path of the file or folder to upload.
            dst (str): The destination path in the S3 bucket.

        Raises:
            ClientError: If an error occurs while uploading.
            FileNotFoundError: If the source file does not exist.
        """

    @abstractmethod
    def size(self, src: str) -> int:
        """Check file size

        Args:
            src (str): The path of the file.

        Returns:
            size (int): Size of the object.
        """


class StorageFactory:
    """StorageFactory class.

    A factory class for creating storage instances based on the provided configuration.
    """

    @staticmethod
    def create(config: dict):
        """Create a storage instance.

        Args:
            config (dict): The configuration for the storage.

        Returns:
            Storage: An instance of the appropriate Storage implementation.

        Raises:
            ValueError: If the storage type in the configuration is unknown.
        """
        storage_type = config["type"]
        match storage_type:
            case "s3":
                from esperoj.storage.s3 import S3Storage

                return S3Storage(config)
        raise ValueError(f"Unknown storage type: {storage_type}")
