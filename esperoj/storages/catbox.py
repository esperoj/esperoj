"""
Custom fsspec backend for a Catbox-like file storage service.

This module provides a `CatboxFileSystem`, an fsspec-compatible file system
that interacts with a simple file hosting service (like Catbox.moe).

The file system supports opening files for reading and writing (`_open`).
Writing a file involves uploading it to the external service. Reading a file
streams it directly from the external service's URL. File deletion is
supported if a `userhash` is provided.
"""

import io
import logging
import requests

from fsspec.spec import AbstractFileSystem
from typing import Any, cast, Union

logger = logging.getLogger(__name__)


class CatboxFile(io.BytesIO):
    """
    A file-like object for handling uploads to the Catbox service.

    This class buffers the written content in memory. When the file is closed,
    it uploads the content to the Catbox service using the userhash provided
    by the `CatboxFileSystem` instance.
    """

    def __init__(self, fs: "CatboxFileSystem", path: str, mode: str = "wb", **kwargs: Any) -> None:
        """
        Initializes the CatboxFile.

        Args:
            fs: The CatboxFileSystem instance.
            path: The logical path of the file (used for naming).
            mode: The file mode (only 'wb' is supported for writing).
            **kwargs: Additional keyword arguments (passed to parent class).
        """
        if mode != "wb":
            raise ValueError("CatboxFile only supports write-binary ('wb') mode.")
        super().__init__(**kwargs)
        self.fs = fs
        self.path = path
        self.storage_url: str | None = None

    def close(self) -> None:
        """
        Finalizes the file by uploading its content.

        If a `userhash` is present in the filesystem instance, it is used for
        the upload, enabling future deletion.

        Returns:
            None
        """
        self.seek(0)
        file_content = self.getvalue()
        size = len(file_content)

        if size == 0:
            logger.warning("Attempted to upload an empty file for path %s. Aborting.", self.path)
            super().close()
            return

        # Upload the file to the Catbox service
        try:
            # The filename is taken from the last part of the path
            filename = self.path.split("/")[-1]
            files = {"fileToUpload": (filename, file_content)}
            data = {"reqtype": "fileupload", "userhash": self.fs.userhash or ""}
            response = requests.post(self.fs.api_url, files=files, data=data, timeout=300)
            response.raise_for_status()
            self.storage_url = response.text
            logger.info("Successfully uploaded %s to Catbox: %s", self.path, self.storage_url)
        except requests.RequestException as e:
            logger.error("Failed to upload file %s to Catbox service: %s", self.path, e)
            raise IOError(f"File upload failed: {e}") from e

        super().close()


class CatboxFileSystem(AbstractFileSystem):
    """
    An fsspec-compatible file system for a Catbox-like service.

    This file system is designed to be a storage backend. It does not handle
    metadata or directory listings. It supports anonymous uploads, and if a
    `userhash` is provided, it can also delete files.
    """

    protocol = "catbox"

    def __init__(self, api_url: str | None = None, userhash: str | None = None, **storage_options: Any) -> None:
        """
        Initializes the CatboxFileSystem.

        Args:
            api_url: The API endpoint for the Catbox service.
            userhash: The user hash for authenticated actions like deletion.
            **storage_options: Additional options for the parent class (fsspec.AbstractFileSystem).
        """
        super().__init__(**storage_options)
        self.api_url = api_url or "https://catbox.moe/user/api.php"
        self.userhash = userhash

    def _open(self, path: str, mode: str = "rb", **kwargs: Any) -> Union[io.RawIOBase, CatboxFile]:
        """
        Opens a file for reading or writing.

        For reading ('rb'), path is expected to be a URL.
        For writing ('wb'), path is a logical path used for naming the upload.

        Args:
            path: The path to the file. For 'rb', it's the full URL. For 'wb', it's a logical name.
            mode: The file mode ('rb' for read-binary, 'wb' for write-binary).
            **kwargs: Additional keyword arguments (ignored in 'rb' mode, passed to CatboxFile in 'wb' mode).

        Returns:
            A file-like object for reading or writing.
        """
        path = cast(str, self._strip_protocol(path))

        if mode == "rb":
            try:
                # The path is the URL to the file
                response = requests.get(path, stream=True, timeout=60)
                response.raise_for_status()
                # requests.Response.raw (urllib3.response.HTTPResponse) acts as a RawIOBase,
                # but doesn't explicitly inherit from it. Cast to satisfy type checkers.
                return cast(io.RawIOBase, response.raw)
            except requests.RequestException as e:
                raise IOError(f"Failed to stream file from Catbox URL {path}: {e}") from e

        elif mode == "wb":
            return CatboxFile(self, path, mode=mode, **kwargs)

        else:
            raise NotImplementedError(f"Mode '{mode}' is not supported.")

    def exists(self, path: str, **kwargs: Any) -> bool:
        """
        Checks if a file exists by sending a HEAD request to its URL.

        Args:
            path: The URL of the file to check.
            **kwargs: Additional keyword arguments (ignored in this implementation).

        Returns:
            True if the file exists, False otherwise.
        """
        path = cast(str, self._strip_protocol(path))
        try:
            response = requests.head(path, timeout=10)
            return response.status_code == 200
        except requests.RequestException:
            return False

    def rm(self, path: str, **kwargs: Any) -> None:
        """
        Removes a file from the Catbox service.

        This operation requires a `userhash` to have been provided during
        filesystem initialization. If no `userhash` is available, a warning
        is logged and the file is orphaned.

        Args:
            path: The URL of the file to remove.
            **kwargs: Additional keyword arguments (ignored in this implementation).

        Returns:
            None
        """
        if not self.userhash:
            logger.warning(
                "File deletion is not supported without a userhash. The file will be orphaned on the storage: %s.",
                path,
            )
            return

        path = cast(str, self._strip_protocol(path))
        filename = path.split("/")[-1]

        try:
            data = {"reqtype": "deletefiles", "userhash": self.userhash, "files": filename}
            response = requests.post(self.api_url, data=data, timeout=60)
            response.raise_for_status()
            logger.info("Successfully requested deletion of %s from Catbox.", path)
        except requests.RequestException as e:
            logger.error("Failed to delete file %s from Catbox service: %s", path, e)
            raise IOError(f"File deletion failed: {e}") from e
