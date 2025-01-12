from collections.abc import Iterator
from pathlib import Path
from typing import Any

from httpx import Client, Timeout
from httpx_ratelimiter import LimiterTransport

from esperoj.storage.file_host import FileHost


class Catbox(FileHost):
    def __init__(self, config: dict[Any, Any]):
        super().__init__(config)
        self.client = Client(http2=True, transport=LimiterTransport(per_minute=60), timeout=Timeout(60.0))
        self.max_file_size = 200 * 2**20

    def close(self) -> None:
        self.client.close()

    def size(self, src: str) -> int:
        response = self.client.head(src)
        response.raise_for_status()
        return int(response.headers.get("Content-Length", 0))

    def stream(self, src: str, chunk_size: int = 64 * 2**10) -> Iterator[bytes]:
        with self.client.stream("GET", src) as response:
            response.raise_for_status()
            yield from response.iter_bytes(chunk_size=chunk_size)

    def upload(self, src: str) -> str:
        file_path = Path(src)
        url = "https://catbox.moe/user/api.php"
        with file_path.open("rb") as file:
            files = {"fileToUpload": file}
            data = {"reqtype": "fileupload", "userhash": ""}
            response = self.client.post(url, files=files, data=data)
            response.raise_for_status()
            return response.text
