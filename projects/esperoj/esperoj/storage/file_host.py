from abc import ABC, abstractmethod
from collections.abc import Iterator
from os import getenv
from pathlib import Path
from typing import Any, Self

DEFAULT_CONFIG = {
    "name": "File Host",
    "probabilities": {"small": 50, "large": 50},
    "proxy": getenv("ESPEROJ_WORKER_PROXY", "https://proxy.esperoj.workers.dev/"),
}


class FileHost(ABC):
    def __init__(self, config: dict[Any, Any]):
        self.config = DEFAULT_CONFIG | config
        self.name = self.config["name"]
        self.probabilities = self.config["probabilities"]
        self.proxy = self.config["proxy"]

    def __enter__(self) -> Self:
        return self

    def __exit__(self, exc_type, exc_value, traceback) -> None:
        self.client.close()  # type: ignore

    def download(self, src: str, dest: str) -> None:
        with Path(dest).open("wb") as file:
            for chunk in self.stream(src):
                file.write(chunk)

    def size(self, src: str) -> int:
        response = self.client.head(src)  # type: ignore
        response.raise_for_status()
        return int(response.headers.get("Content-Length", 0))

    def stream(self, src: str, chunk_size: int = 64 * 2**10) -> Iterator[bytes]:
        headers = {"User-Agent": "Esperoj CLI"}
        with self.client.stream("GET", self.proxy + src, headers=headers) as response:  # type: ignore
            response.raise_for_status()
            yield from response.iter_bytes(chunk_size=chunk_size)

    @abstractmethod
    def upload(self, src: str) -> str:
        raise NotImplementedError
