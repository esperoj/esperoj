import os
import time
from collections.abc import Iterator
from pathlib import Path
from typing import Any

from httpx import Client, HTTPStatusError, Timeout
from httpx_ratelimiter import LimiterTransport

from esperoj.storage.file_host import FileHost


class InternetArchive(FileHost):
    def __init__(self, name: str, config: dict[Any, Any]):
        super().__init__(name, config)
        self.client = Client(http2=True, transport=LimiterTransport(per_minute=15), timeout=Timeout(120.0))
        self.proxy = os.getenv("ESPEROJ_WORKER_PROXY", "https://proxy.esperoj.workers.dev/")

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
            "force_get": 0,
            "skip_first_archive": 1,
            "outlinks_availability": 0,
            "email_result": 1,
            "js_behavior_timeout": 30,
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
        return url[:timestamp_end] + "if_" + url[timestamp_end:]

    def _upload_to_temporary_host(self, src: str) -> str:
        url = "https://up1.fileditch.com/temp/upload.php"
        with Path(src).open("rb") as file:
            files = {"files[]": file}
            response = self.client.post(url, files=files)
            response.raise_for_status()
            json_response = response.json()
            return json_response["files"][0]["url"]

    def close(self) -> None:
        self.client.close()

    def stream(self, src: str) -> Iterator[bytes]:
        with self.client.stream("GET", self.proxy + src) as response:
            response.raise_for_status()
            yield from response.iter_bytes()

    def upload(self, src: str) -> str:
        url = self._upload_to_temporary_host(src)
        return self._convert_url(self._archive_url("https://x.0ms.dev/q70/" + url))
