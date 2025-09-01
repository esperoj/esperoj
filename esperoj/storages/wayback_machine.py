"""
Custom fsspec backend for the Wayback Machine (archive.org).

This module provides a `WaybackFileSystem`, an fsspec-compatible file system
that interacts with the Internet Archive's Wayback Machine via its "Save Page Now 2" (SPN2) API.

The file system supports opening files for writing (`_open` with mode 'wb'), which triggers
an archival request for a given HTTP or HTTPS URL. The write operation is asynchronous and
involves polling the SPN2 API until the capture is complete. The result of a successful
write is a file-like object containing the URL of the archived snapshot.

Reading files (`_open` with mode 'rb') streams the content directly from a given
Wayback Machine URL. File deletion is not supported by the Wayback Machine's public API.
"""

import io
import logging
import time
import requests

from fsspec.spec import AbstractFileSystem
from typing import Any, cast, Dict, Union

logger = logging.getLogger(__name__)


class WaybackFile(io.BytesIO):
    """
    A file-like object for handling asynchronous captures with the Wayback Machine's SPN2 API.

    This class manages the process of initiating a web page capture and polling for its completion.
    The content written to this file-like object is expected to be a valid HTTP or HTTPS URL.
    When the file is closed, it initiates the capture process.
    """

    def __init__(self, fs: "WaybackFileSystem", path: str, mode: str = "wb", **kwargs: Any) -> None:
        """
        Initializes the WaybackFile.

        Args:
            fs: The WaybackFileSystem instance.
            path: The logical path for the operation (not directly used for naming).
            mode: The file mode (only 'wb' is supported for writing).
            **kwargs: Additional keyword arguments.
        """
        if mode != "wb":
            raise ValueError("WaybackFile only supports write-binary ('wb') mode.")
        super().__init__(**kwargs)
        self.fs = fs
        self.path = path
        self.capture_result_url: str | None = None

    def close(self) -> None:
        """
        Finalizes the file by capturing the URL and waiting for the result.

        This method orchestrates the SPN2 API calls to capture the URL provided
        to the file object. It polls for the capture status and, on success,
        stores the resulting Wayback Machine URL.

        Raises:
            IOError: If the capture process fails for any reason (e.g., API error, timeout).
        """
        self.seek(0)
        target_url_bytes = self.getvalue()
        if not target_url_bytes:
            logger.warning("No URL provided to capture for path %s. Aborting.", self.path)
            super().close()
            return

        target_url = target_url_bytes.decode("utf-8")
        if not (target_url.startswith("http://") or target_url.startswith("https://")):
            raise ValueError("The provided input must be a valid HTTP or HTTPS URL.")

        try:
            # 1. Initiate capture request
            job_id = self._start_capture(target_url)
            logger.info("Capture request sent for %s. Job ID: %s", target_url, job_id)

            # 2. Poll for status
            final_status = self._poll_status(job_id)

            # 3. Process final status
            if final_status.get("status") == "success":
                timestamp = final_status["timestamp"]
                original_url = final_status["original_url"]
                self.capture_result_url = f"https://web.archive.org/web/{timestamp}/{original_url}"
                logger.info("Successfully captured %s: %s", target_url, self.capture_result_url)
                # Write the result back to the buffer for the user to read
                self.seek(0)
                self.truncate()
                self.write(self.capture_result_url.encode("utf-8"))
                self.seek(0)
            else:
                error_message = final_status.get("message", "Unknown error during capture.")
                raise IOError(f"Failed to capture {target_url}: {error_message}")

        except requests.RequestException as e:
            logger.error("API request failed during capture of %s: %s", target_url, e)
            raise IOError(f"API interaction failed: {e}") from e
        except (ValueError, KeyError) as e:
            logger.error("Unexpected API response for %s: %s", target_url, e)
            raise IOError(f"Invalid API response received: {e}") from e

        super().close()

    def _start_capture(self, url: str) -> str:
        """Sends the initial capture request to the SPN2 API."""
        headers = {
            "Accept": "application/json",
            "Authorization": f"LOW {self.fs.access_key}:{self.fs.secret_key}",
        }
        # Use efficient options as per SPN2 docs
        data = {
            "url": url,
            "skip_first_archive": "1",
            "js_behavior_timeout": "0",
        }
        response = requests.post(self.fs.api_url_save, headers=headers, data=data, timeout=60)
        response.raise_for_status()
        response_data = response.json()
        if "job_id" not in response_data:
            raise ValueError("'job_id' not found in capture initiation response.")
        return response_data["job_id"]

    def _poll_status(self, job_id: str) -> Dict[str, Any]:
        """Polls the SPN2 status API until the job is complete or times out."""
        status_url = f"{self.fs.api_url_status}/{job_id}"
        headers = {
            "Accept": "application/json",
            "Authorization": f"LOW {self.fs.access_key}:{self.fs.secret_key}",
        }
        # Max capture duration is 2m (120s), we poll for a bit longer to be safe.
        timeout = time.time() + 150
        while time.time() < timeout:
            response = requests.get(status_url, headers=headers, timeout=30)
            response.raise_for_status()
            status_data = response.json()
            if status_data.get("status") not in ["pending", None]:  # pending or missing status key
                return status_data
            # Respect rate limits and avoid busy-waiting
            time.sleep(5)
        raise IOError("Capture timed out after 150 seconds.")


class WaybackFileSystem(AbstractFileSystem):
    """
    An fsspec-compatible file system for the Wayback Machine (archive.org).

    This file system allows programmatic archival of web pages by "writing" a URL
    to the filesystem. It is designed for archival purposes and does not support
    directory listings or file metadata.
    """

    protocol = ("http", "https")

    def __init__(
        self,
        access_key: str | None = None,
        secret_key: str | None = None,
        **storage_options: Any,
    ) -> None:
        """
        Initializes the WaybackFileSystem.

        Args:
            access_key: The S3-style access key for the archive.org account.
            secret_key: The S3-style secret key for the archive.org account.
            **storage_options: Additional options for the parent class.
        """
        super().__init__(**storage_options)
        self.api_url_save = "https://web.archive.org/save"
        self.api_url_status = "https://web.archive.org/save/status"
        if not access_key or not secret_key:
            raise ValueError("`access_key` and `secret_key` must be provided.")
        self.access_key = access_key
        self.secret_key = secret_key

    def _open(self, path: str, mode: str = "rb", **kwargs: Any) -> Union[io.RawIOBase, WaybackFile]:
        """
        Opens a file for reading or writing.

        - Reading ('rb'): The `path` is expected to be a full Wayback Machine URL.
          The content is streamed directly from this URL.
        - Writing ('wb'): Initiates an archival request. The URL to be archived
          should be written to the returned file-like object.

        Args:
            path: The path to the resource. For 'rb', it is the full Wayback URL.
                  For 'wb', it is a logical path (the URL to archive is written to the file).
            mode: The file mode ('rb' for read-binary, 'wb' for write-binary).
            **kwargs: Additional keyword arguments.

        Returns:
            A file-like object for reading or writing.
        """
        if mode == "rb":
            try:
                response = requests.get(path, stream=True, timeout=60)
                response.raise_for_status()
                return cast(io.RawIOBase, response.raw)
            except requests.RequestException as e:
                raise IOError(f"Failed to stream file from Wayback URL {path}: {e}") from e

        elif mode == "wb":
            return WaybackFile(self, path, mode=mode, **kwargs)

        else:
            raise NotImplementedError(f"Mode '{mode}' is not supported.")

    def exists(self, path: str, **kwargs: Any) -> bool:
        """
        Checks if a file exists by sending a HEAD request to its URL.

        Args:
            path: The URL of the file to check.
            **kwargs: Additional keyword arguments (ignored).

        Returns:
            True if the file exists, False otherwise.
        """
        try:
            # A HEAD request is sufficient to check for existence without downloading content.
            response = requests.head(path, timeout=10)
            return response.status_code == 200
        except requests.RequestException:
            return False

    def rm(self, path: str, **kwargs: Any) -> None:
        """
        Removes a file. This operation is not supported by the Wayback Machine.

        A warning is logged indicating that captures cannot be deleted via the API.

        Args:
            path: The path of the file to remove.
            **kwargs: Additional keyword arguments (ignored).
        """
        logger.warning(
            "File deletion is not supported by the Wayback Machine API. The capture will remain in the archive: %s.",
            path,
        )
        return
