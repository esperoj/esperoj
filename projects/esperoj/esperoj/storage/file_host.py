from abc import ABC, abstractmethod
from collections.abc import Iterator
from pathlib import Path
from typing import Any, Self


class FileHost(ABC):
    def __init__(self, name: str, config: dict[Any, Any]):
        self.name = name
        self.config = config

    def __enter__(self) -> Self:
        return self

    def __exit__(self, exc_type, exc_value, traceback) -> None:
        self.close()

    @abstractmethod
    def close(self) -> None:
        raise NotImplementedError

    def download(self, src: str, dest: str) -> None:
        with Path(dest).open("wb") as file:
            for chunk in self.stream(src):
                file.write(chunk)

    @abstractmethod
    def size(self, src: str) -> int:
        raise NotImplementedError

    @abstractmethod
    def stream(self, src: str) -> Iterator:
        raise NotImplementedError

    @abstractmethod
    def upload(self, src: str) -> str:
        raise NotImplementedError


class FileHostFactory:
    """FileHostFactory class.

    A factory class for creating FileHost instances based on the provided configuration.
    """

    @staticmethod
    def create(config: dict):
        """Create a storage instance.

        Args:
            config (dict): The configuration for the storage.

        Returns:
            FileHost: An instance of the appropriate FileHost implementation.

        Raises:
            ValueError: If the storage type in the configuration is unknown.
        """
        file_host_type = config["type"]
        match file_host_type:
            case "internet_archive":
                from esperoj.storage.internet_archive import InternetArchive

                return InternetArchive(config["name"], config)
            case "lain_la":
                from esperoj.storage.lain_la import LainLa

                return LainLa(config["name"], config)
            case "local_file_host":
                from esperoj.storage.local_file_host import LocalFileHost

                return LocalFileHost(config["name"], config)
            case "file_haus":
                from esperoj.storage.file_haus import FileHaus

                return FileHaus(config["name"], config)
        raise ValueError(f"Unknown file host type: {file_host_type}")
