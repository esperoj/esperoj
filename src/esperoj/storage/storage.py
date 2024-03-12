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

    def __init__(self, name: str) -> None:
        """Initalize the Storage.

        Args:
            name (str): The name of the storage.
        """
        self.name = name

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
    def create(storage_type: str, config: dict):
        """Method to create storage."""
        if storage_type == "s3":
            from esperoj.storage.s3 import S3Storage

            configs = {
                "bucket_name": config["bucket_name"],
                "client_config": config["client_config"],
            }
            transfer_config = config.get("transfer_config")
            if transfer_config is not None:
                configs["transfer_config"] = transfer_config
            return S3Storage(config["name"], configs)
        raise ValueError(f"Unknown storage type: {storage_type}")
