"""Storage module."""

from abc import ABC, abstractmethod
from typing import TypedDict, Iterator


class DeleteFileError(TypedDict):
    """DeleteFileError type.

    TypedDict for errors encountered while deleting files.

    Attributes:
        path (str): The path of the file that failed to be deleted.
        message (str): The error message related to the failure.
    """

    path: str
    message: str


class DeleteFilesResponse(TypedDict):
    """DeleteFilesResponse type.

    TypedDict for the response of the delete_files method.

    Attributes:
        errors (list[DeleteFileError]): A list of DeleteFileError objects representing errors encountered while deleting files.
    """

    errors: list[DeleteFileError]


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
    def delete_files(self, paths: list[str]) -> DeleteFilesResponse:
        """Delete files at the specified paths.

        Args:
            paths (list[str]): A list of paths to the files to be deleted.

        Returns:
            DeleteFilesResponse: A response containing a list of errors encountered while deleting files.
        """

    @abstractmethod
    def download_file(self, src: str, dst: str) -> None:
        """Download a file from the source to the destination.

        Args:
            src (str): The source path of the file to download.
            dst (str): The destination path where the file will be saved.
        """

    @abstractmethod
    def file_exists(self, path: str) -> bool:
        """Check if a file exists at the specified path.

        Args:
            path (str): The path of the file to check.

        Returns:
            bool: True if the file exists, False otherwise.
        """

    @abstractmethod
    def get_link(self, path: str) -> str:
        """Get a download link for a file in the storage.

        Args:
            path (str): The path to the file.

        Returns:
            str: The URL to download the file.
        """

    @abstractmethod
    def get_file(self, src: str) -> Iterator:
        """Get a file from the source and return an Iterator.

        Args:
            src (str): The source path of the file to download.

        Returns:
            Iterator: An Iterator of the file content.
        """

    @abstractmethod
    def list_files(self, path: str) -> list[str]:
        """List all files at the specified path.

        Args:
            path (str): The path to list files from.

        Returns:
            list[str]: A list of filenames.
        """

    @abstractmethod
    def upload_file(self, src: str, dst: str) -> None:
        """Upload a file from the source to the destination.

        Args:
            src (str): The source path of the file to upload.
            dst (str): The destination path where the file will be saved.
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
