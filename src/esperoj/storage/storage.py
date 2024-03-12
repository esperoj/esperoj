"""Storage module."""

from abc import ABC, abstractmethod
from typing import TypedDict


class DeleteFileError(TypedDict):
    """DeleteFileError type."""

    path: str
    message: str


class DeleteFilesResponse(TypedDict):
    """DeleteFilesResponse type."""

    errors: list[DeleteFileError]


class Storage(ABC):
    """Abstract base class for storage."""

    @abstractmethod
    def __init__(self, config: dict) -> None:
        """Initalize the Storage.

        Args:
            config (dict): Config for the storage.
        """

    @abstractmethod
    def delete_files(self, paths: list[str]) -> DeleteFilesResponse:
        """Deletes files at the paths.

        Args:
            paths (list[str]): The paths of the files to delete.

        Returns:
            response (DeleteFilesResponse): Response includes list of errors.
        """

    @abstractmethod
    def download_file(self, src: str, dst: str) -> None:
        """Downloads a file from the source to the destination.

        Args:
            src (str): The source path of the file to download.
            dst (str): The destination path where the file will be saved.
        """

    @abstractmethod
    def file_exists(self, path: str) -> bool:
        """Checks if a file exists at the specified path.

        Args:
            path (str): The path of the file to check.

        Returns:
            status (bool): True if the file exists, False otherwise.
        """

    @abstractmethod
    def get_link(self, path: str) -> str:
        """Get a download link from storage.

        Args:
            path (str): The path to the file.

        Returns:
            utl (str): Url to download file.
        """

    @abstractmethod
    def list_files(self, path: str) -> list[str]:
        """Lists all files at the specified path.

        Args:
            path (str): The path to list files from.

        Returns:
            list[str]: A list of filenames.
        """

    @abstractmethod
    def upload_file(self, src: str, dst: str) -> None:
        """Uploads a file from the source to the destination.

        Args:
            src (str): The source path of the file to upload.
            dst (str): The destination path where the file will be saved.
        """


class StorageFactory:
    """StorageFactory class."""

    @staticmethod
    def create(config: dict):
        """Method to create storage.

        Args:
            config (dict): The configs of the storage.
        """
        storage_type = config["type"]
        match storage_type:
            case "s3":
                from esperoj.storage.s3 import S3Storage

                return S3Storage(config)
        raise ValueError(f"Unknown storage type: {storage_type}")
