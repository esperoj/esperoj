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
    def stream(self, src: str, chunk_size: int = 64 * 2**10) -> Iterator:
        raise NotImplementedError

    @abstractmethod
    def upload(self, src: str) -> str:
        raise NotImplementedError
