from collections.abc import Iterator
from pathlib import Path
from typing import Any

from httpx import Client, Timeout
from httpx_ratelimiter import LimiterTransport

from esperoj.storage.file_host import FileHost


class InternetArchive(FileHost):
    def __init__(self, name: str, config: dict[Any, Any]):
        super().__init__(name, config)
        self.client = Client(http2=True, transport=LimiterTransport(per_minute=60), timeout=Timeout(120.0))

    def close(self) -> None:
        self.client.close()

    def stream(self, src: str) -> Iterator[bytes]:
        with self.client.stream("GET", src) as response:
            response.raise_for_status()
            yield from response.iter_bytes()

    def upload(self, src: str) -> str:
        url = "https://pomf.lain.la/upload.php"
        with Path(src).open("rb") as file:
            files = {"files[]": file}
            response = self.client.post(url, files=files)
            response.raise_for_status()
            json_response = response.json()
            return json_response["files"][0]["url"]
