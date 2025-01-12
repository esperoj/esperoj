import shutil
from collections.abc import Iterator
from pathlib import Path
from typing import Any

from esperoj.storage.file_host import FileHost


class LocalFileHost(FileHost):
    def __init__(self, config: dict[Any, Any]):
        super().__init__(config)
        self.base_src = Path(config["base_src"])
        self.base_src.mkdir(parents=True, exist_ok=True)

    def size(self, src: str) -> int:
        file_path = self.base_src / Path(src)
        if not file_path.is_file():
            raise FileNotFoundError(f"File not found: {file_path}")
        return file_path.stat().st_size

    def stream(self, src: str, chunk_size: int = 64 * 2**10) -> Iterator[bytes]:
        file_path = self.base_src / Path(src)
        if not file_path.is_file():
            raise FileNotFoundError(f"File not found: {file_path}")
        with file_path.open("rb") as file:
            yield from iter(lambda: file.read(chunk_size), b"")

    def upload(self, src: str) -> str:
        file_path = Path(src)
        if not file_path.is_file():
            raise FileNotFoundError(f"File not found: {file_path}")
        destination = self.base_src / file_path.name
        shutil.copy2(file_path, destination)
        return str(destination)

    def close(self) -> None:
        pass
