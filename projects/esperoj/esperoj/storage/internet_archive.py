import time
from collections.abc import Iterator
from os import getenv
from pathlib import Path
from typing import Any
from urllib.parse import urlparse

from httpx import Client, HTTPStatusError, Timeout
from httpx_ratelimiter import LimiterTransport

from esperoj.storage.file_host import FileHost


class InternetArchive(FileHost):
    def __init__(self, config: dict[Any, Any]):
        super().__init__(config)
        self.proxy = getenv("ESPEROJ_WORKER_PROXY", "https://proxy.esperoj.workers.dev/")
        self.max_file_size = 2 * 2**30
        mounts = {"all://": LimiterTransport(per_second=5), "all://*archive.org": LimiterTransport(per_minute=15)}
        self.client = Client(http2=True, mounts=mounts, timeout=Timeout(120.0))

    def _archive_url(self, url: str) -> str:
        api_key = self.config.get("access_key")
        api_secret = self.config.get("secret_key")

        headers = {
            "Accept": "application/json",
            "Authorization": f"LOW {api_key}:{api_secret}",
        }

        params = {
            "url": url,
            "capture_all": 0,
            "capture_outlinks": 0,
            "capture_screenshot": 0,
            "delay_wb_availability": 0,
            "force_get": 1,
            "skip_first_archive": 1,
            "outlinks_availability": 0,
            "email_result": 1,
            "js_behavior_timeout": 0,
        }

        try:
            response = self.client.post("https://web.archive.org/save", headers=headers, data=params)
            response.raise_for_status()
            job_id = response.json()["job_id"]

            start_time = time.time()
            timeout = 60 * 15

            while True:
                if time.time() - start_time > timeout:
                    raise RuntimeError("Error: Archiving process timed out.")
                response = self.client.get(f"https://web.archive.org/save/status/{job_id}", headers=headers)
                response.raise_for_status()
                status = response.json()
                match status["status"]:
                    case "pending":
                        time.sleep(16)
                    case "success":
                        return f'https://web.archive.org/web/{status["timestamp"]}/{status["original_url"]}'
                    case _:
                        raise RuntimeError(
                            f"Error: Unexpected status {status['status']} with message {status.get('message', '')}"
                        )
        except HTTPStatusError as e:
            raise RuntimeError(f"HTTP error occurred: {e!s}") from e

    def _convert_url(self, url: str) -> str:
        timestamp_end = url.find("/", 30)
        return f"{url[:timestamp_end]}im_{url[timestamp_end:]}"

    def _upload_to_temporary_host(self, src: str) -> str:
        src_path = Path(src)
        upload_url = f"https://transfer.adminforge.de/{src_path.name}"
        with src_path.open("rb") as file:
            response = self.client.put(upload_url, content=file)
            response.raise_for_status()
            return f'https://transfer.adminforge.de/{"get" + urlparse(response.text).path}'

    def close(self) -> None:
        self.client.close()

    def size(self, src: str) -> int:
        response = self.client.head(self.proxy + src)
        response.raise_for_status()
        return int(response.headers.get("Content-Length", 0))

    def stream(self, src: str, chunk_size: int = 64 * 2**10) -> Iterator[bytes]:
        headers = {"User-Agent": "esperoj cli"}
        with self.client.stream("GET", self.proxy + src, headers=headers) as response:
            response.raise_for_status()
            yield from response.iter_bytes(chunk_size=chunk_size)

    def upload(self, src: str) -> str:
        url = self._upload_to_temporary_host(src)
        return self._convert_url(self._archive_url(url))
