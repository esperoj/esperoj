"""Storage class."""

import shutil
from pathlib import Path


class BaseStorage:
    """Abstract base class for storage.

    Attributes:
    ----------
        name (str): The name of the storage.
        config (dict): Configuration for the storage.
    """

    def __init__(self, name: str, config: dict | None = None) -> None:
        """Initialize the BaseStorage.

        Args:
            name (str): The name of the storage.
            config (dict, optional): Configuration for the storage. Defaults to {}.
        """
        if config is None:
            config = {}
        self.name = name
        self.config = config


class LocalStorage(BaseStorage):
    """Class for local storage.

    Attributes:
    ----------
        base_path (Path): The base path for the local storage.
    """

    def __init__(self, name: str, config: dict | None = None) -> None:
        """Initialize the LocalStorage.

        Args:
            name (str): The name of the storage.
            config (dict, optional): Configuration for the storage. Defaults to {}.
        """
        if config is None:
            config = {}
        super().__init__(name, config)
        self.base_path = Path(self.config["base_path"])
        self.base_path.mkdir(parents=True, exist_ok=True)

    def _get_full_path(self, path: str) -> Path:
        """Get the full path for a given relative path.

        Args:
            path (str): The relative path.

        Returns:
        -------
            Path: The full path.
        """
        return self.base_path / Path(path)

    def delete_file(self, path: str) -> None:
        """Delete a file at a given path.

        Args:
            path (str): The path of the file to delete.

        Raises:
        ------
            FileNotFoundError: If the file does not exist.
        """
        full_path = self._get_full_path(path)
        if not full_path.is_file():
            raise FileNotFoundError(f"No such file: '{full_path}'")
        full_path.unlink()

    def download_file(self, src: str, dst: str) -> None:
        """Download a file from a source to a destination.

        Args:
            src (str): The source file path.
            dst (str): The destination file path.

        Raises:
        ------
            FileNotFoundError: If the source file does not exist.
        """
        source = self._get_full_path(src)
        if not source.is_file():
            raise FileNotFoundError(f"No such file: '{source}'")
        destination = Path(dst)
        destination.parent.mkdir(parents=True, exist_ok=True)
        shutil.copy2(source, destination)

    def file_exists(self, path: str) -> bool:
        """Check if a file exists at a given path.

        Args:
            path (str): The path of the file.

        Returns:
        -------
            bool: True if the file exists, False otherwise.
        """
        full_path = self._get_full_path(path)
        return full_path.is_file()

    def list_files(self, path: str) -> list[str]:
        """List all files in a given directory.

        Args:
            path (str): The path of the directory.

        Returns:
        -------
            list[str]: A list of file paths.

        Raises:
        ------
            FileNotFoundError: If the directory does not exist.
        """
        full_path = self._get_full_path(path)
        if not full_path.is_dir():
            raise FileNotFoundError(f"No such directory: '{full_path}'")
        return [
            str(file.relative_to(self.base_path)) for file in full_path.iterdir() if file.is_file()
        ]

    def upload_file(self, src: str, dst: str) -> None:
        """Upload a file from a source to a destination.

        Args:
            src (str): The source file path.
            dst (str): The destination file path.

        Raises:
        ------
            FileNotFoundError: If the source file does not exist.
        """
        source = Path(src)
        if not source.is_file():
            raise FileNotFoundError(f"No such file: '{source}'")
        destination = self._get_full_path(dst)
        destination.parent.mkdir(parents=True, exist_ok=True)
        shutil.copy2(source, destination)
