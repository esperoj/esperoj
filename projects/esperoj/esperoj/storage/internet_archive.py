import time
from os import getenv
from pathlib import Path
from typing import Any
from urllib.parse import urlparse

from httpx import Client, HTTPStatusError, Timeout

from esperoj.storage.file_host import FileHost


class InternetArchive(FileHost):
    def __init__(self, config: dict[Any, Any]):
        super().__init__(config)
        self.max_file_size = 2 * 2**30
        self.client = Client(http2=True, timeout=Timeout(120.0))
        self.transfer_host = getenv("TRANSFER_HOST", "transfer.sh")

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
                        return f"https://web.archive.org/web/{status['timestamp']}/{status['original_url']}"
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
        upload_url = f"https://{self.transfer_host}/{src_path.name}"
        with src_path.open("rb") as file:
            response = self.client.put(upload_url, content=file)
            response.raise_for_status()
            return f"https://{self.transfer_host}/{'get' + urlparse(response.text).path}"

    def upload(self, src: str) -> str:
        url = self._upload_to_temporary_host(src)
        return self._convert_url(self._archive_url(url))
