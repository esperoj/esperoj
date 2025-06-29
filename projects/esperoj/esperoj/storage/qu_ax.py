from pathlib import Path
from typing import Any

from httpx import Client, Timeout

from esperoj.storage.file_host import FileHost


class QuAx(FileHost):
    def __init__(self, config: dict[Any, Any]):
        super().__init__(config)
        self.max_file_size = 250 * 2**20
        self.client = Client(http2=True, timeout=Timeout(60.0))

    def upload(self, src: str) -> str:
        file_path = Path(src)
        url = "https://qu.ax/upload.php"
        with file_path.open("rb") as file:
            files = {"files[]": file}
            data = {"expiry": "-1"}
            response = self.client.post(url, files=files, data=data)
            response.raise_for_status()
            return response.json()["files"][0]["url"]
