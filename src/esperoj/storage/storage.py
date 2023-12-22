"""Storage class."""

from abc import ABC, abstractmethod


class Storage(ABC):
    """Abstract base class for storage."""

    @abstractmethod
    def delete_file(self, path: str) -> bool:
        """Deletes a file at the specified path.

        Args:
            path (str): The path of the file to delete.

        Returns:
            bool: True if the file was deleted successfully, False otherwise.
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
            bool: True if the file exists, False otherwise.
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
