"""
Custom fsspec backend for the Internet Archive.

This module provides an InternetArchiveFileSystem, an fsspec-compatible file system
that interacts with the Internet Archive for file storage.

The file system supports opening files for reading and writing (`_open`).
Writing a file involves creating a unique "item" on the Internet Archive and
uploading the file to it. Reading a file streams it directly from the
Internet Archive's public URL.
"""

import io
import logging
import re
import uuid

import internetarchive
import requests
from fsspec.spec import AbstractFileSystem
from typing import cast

logger = logging.getLogger(__name__)


class InternetArchiveFile(io.BytesIO):
    """
    A file-like object for handling uploads to the Internet Archive.

    This class buffers the written content in memory. When the file is closed,
    it uploads the content as a new item to the Internet Archive.
    """

    def __init__(self, fs: "InternetArchiveFileSystem", path: str, mode: str = "wb", **kwargs):
        """
        Initializes the InternetArchiveFile.

        Args:
            fs: The InternetArchiveFileSystem instance.
            path: The logical path of the file (used for naming).
            mode: The file mode (only 'wb' is supported for writing).
        """
        if mode != "wb":
            raise ValueError("InternetArchiveFile only supports write-binary ('wb') mode.")
        super().__init__()
        self.fs = fs
        self.path = path
        self.storage_url = None

    def _generate_item_identifier(self, path: str) -> str:
        """
        Generates a unique and valid Internet Archive item identifier from a path.

        Args:
            path: The logical path of the file.

        Returns:
            A string to be used as the Internet Archive item identifier.
        """
        sanitized_path = re.sub(r"[^a-zA-Z0-9_.-]", "-", path)
        unique_suffix = str(uuid.uuid4())
        identifier = f"esperoj-{sanitized_path[:50]}-{unique_suffix}"
        return identifier[:100]

    def close(self):
        """
        Finalizes the file by uploading its content to the Internet Archive.
        """
        self.seek(0)
        file_content = self.getvalue()
        size = len(file_content)

        if size == 0:
            logger.warning("Attempted to upload an empty file for path %s. Aborting.", self.path)
            super().close()
            return

        item_identifier = self._generate_item_identifier(self.path)
        filename = self.path.split("/")[-1]

        metadata = {
            "title": f"Esperoj File: {filename}",
            "collection": self.fs.collection,
            "mediatype": "data",
            "description": f"File uploaded from Esperoj system. Original path: {self.path}",
        }

        try:
            logger.info("Uploading %s to Internet Archive with identifier %s...", self.path, item_identifier)

            files = {filename: io.BytesIO(file_content)}

            internetarchive.upload(
                identifier=item_identifier,
                files=files,
                metadata=metadata,
                access_key=self.fs.access_key,
                secret_key=self.fs.secret_key,
                retries=3,
                retries_sleep=5,
            )

            self.storage_url = f"https://archive.org/download/{item_identifier}/{filename}"

            logger.info("Successfully uploaded %s to Internet Archive. URL: %s", self.path, self.storage_url)

        except Exception as e:
            logger.error("Failed to upload file %s to Internet Archive: %s", self.path, e, exc_info=True)
            raise IOError(f"Internet Archive upload failed: {e}") from e

        super().close()


class InternetArchiveFileSystem(AbstractFileSystem):
    """
        An fsspec-compatible file system for the Internet Archive.

        This file system is designed as a storage backend. It does not handle
        metadata or directory listings; that is the responsibility of a
    -    higher-level file system like `EsperojFileSystem`.
    """

    protocol = "internetarchive"

    def __init__(self, access_key: str, secret_key: str, collection: str | None = None, **storage_options):
        super().__init__(**storage_options)
        if not access_key or not secret_key:
            raise ValueError("InternetArchiveFileSystem requires 'access_key' and 'secret_key'.")
        self.access_key = access_key
        self.secret_key = secret_key
        self.collection = collection or "test_collection"

    def _open(self, path, mode="rb", **kwargs):
        """
        Opens a file for reading or writing.

        For reading ('rb'), path is expected to be a URL.
        For writing ('wb'), path is a logical path used for naming the upload.
        """
        path = cast(str, self._strip_protocol(path))

        if mode == "rb":
            try:
                response = requests.get(path, stream=True, timeout=60)
                response.raise_for_status()
                return response.raw
            except requests.RequestException as e:
                raise IOError(f"Failed to stream file from Internet Archive URL {path}: {e}") from e

        elif mode == "wb":
            return InternetArchiveFile(self, path, mode=mode, **kwargs)

        else:
            raise NotImplementedError(f"Mode '{mode}' is not supported.")

    def exists(self, path, **kwargs):
        """
        Checks if a file exists by sending a HEAD request to its URL.
        """
        path = cast(str, self._strip_protocol(path))
        try:
            response = requests.head(path, timeout=10, allow_redirects=True)
            return response.status_code == 200
        except requests.RequestException:
            return False

    def rm(self, path, **kwargs):
        """
        Placeholder for removing a file.

        The Internet Archive API does not support file deletion for anonymous uploads.
        This method is a no-op.
        """
        logger.warning(
            "File deletion is not supported by the Internet Archive backend for path/URL: %s. "
            "The file will be orphaned on the storage.",
            path,
        )
        pass
