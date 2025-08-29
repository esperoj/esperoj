"""
Custom fsspec backend for the Internet Archive.

This module provides an InternetArchiveFileSystem, an fsspec-compatible file system
that interacts with the Internet Archive for file storage.

The file system supports opening files for reading and writing (`_open`).
Writing a file involves uploading to the Internet Archive using the official client
with metadata. The client will automatically create an item if needed. The collection
and other metadata fields are defined within the upload process rather than
being strictly limited by an enum. Reading a file streams it directly from the
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
    it uploads the content as a new item to the Internet Archive, automatically
    creating the item if it doesn't exist based on the generated identifier.
    Metadata, including the collection, is passed during the upload.
    """

    def __init__(self, fs: "InternetArchiveFileSystem", path: str, mode: str = "wb", **kwargs):
        """
        Initializes the InternetArchiveFile.

        Args:
            fs: The InternetArchiveFileSystem instance.
            path: The logical path of the file within the Esperoj system. This path
                  is used to generate a unique item identifier and filename on IA.
            mode: The file mode (only 'wb' is supported for writing).
            **kwargs: Additional keyword arguments, primarily for metadata customization.
                      Expected arguments might include 'collection', 'mediatype', etc.,
                      which will override defaults.
        """
        if mode != "wb":
            raise ValueError("InternetArchiveFile only supports write-binary ('wb') mode.")
        super().__init__()
        self.fs = fs
        self.path = path
        self.storage_url = None
        self.upload_metadata = kwargs.get("metadata", {})

    def _generate_item_identifier(self, path: str) -> str:
        """
        Generates a unique and valid Internet Archive item identifier from a path.

        The identifier is prefixed with "esperoj-", includes a sanitized portion
        of the file's logical path, and a UUID suffix to ensure uniqueness.

        Args:
            path: The logical path of the file.

        Returns:
            A string to be used as the Internet Archive item identifier.
        """
        # Sanitize path to be suitable for an IA identifier (alphanumeric, -, _, .)
        sanitized_path = re.sub(r"[^a-zA-Z0-9_.-]", "-", path)
        unique_suffix = str(uuid.uuid4())
        # Truncate sanitized_path to keep the identifier within reasonable limits
        identifier = f"esperoj-{sanitized_path[:50]}-{unique_suffix}"
        return identifier[:100]  # Ensure total length does not exceed typical IA limits

    def close(self):
        """
        Finalizes the file by uploading its content to the Internet Archive.

        This method buffers the content, then constructs metadata (including a default
        collection if not overridden), and uses the `internetarchive.upload` client
        to create or update an item with the file.
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

        # Default metadata, can be overridden by self.upload_metadata from kwargs
        default_metadata = {
            "title": f"Esperoj File: {filename}",
            "collection": self.fs.collection,  # Use collection from FS config
            "mediatype": "data",
            "description": f"File uploaded from Esperoj system. Original path: {self.path}",
        }
        # Merge default metadata with any provided during file open,
        # with provided metadata taking precedence.
        metadata = {**default_metadata, **self.upload_metadata}

        try:
            logger.info("Uploading %s to Internet Archive with identifier %s...", self.path, item_identifier)

            # The internetarchive client expects files as a dictionary where keys are filenames
            # and values are file-like objects or paths.
            files = {filename: io.BytesIO(file_content)}

            internetarchive.upload(
                identifier=item_identifier,
                files=files,
                metadata=metadata,
                access_key=self.fs.access_key,
                secret_key=self.fs.secret_key,
                retries=3,  # Number of retries for network operations
                retries_sleep=5,  # Seconds to wait between retries
            )

            # Construct the public download URL for the uploaded file
            self.storage_url = f"https://archive.org/download/{item_identifier}/{filename}"

            logger.info("Successfully uploaded %s to Internet Archive. URL: %s", self.path, self.storage_url)

        except Exception as e:
            logger.error("Failed to upload file %s to Internet Archive: %s", self.path, e, exc_info=True)
            raise IOError(f"Internet Archive upload failed: {e}") from e

        super().close()


class InternetArchiveFileSystem(AbstractFileSystem):
    """
    An fsspec-compatible file system for the Internet Archive.

    This file system is designed as a storage backend for Esperoj. It handles
    file uploads by creating or updating Internet Archive items and manages
    reads by streaming content from public IA URLs. It does not natively
    handle metadata listings or directory structures; that is the responsibility
    of a higher-level file system like `EsperojFileSystem` which uses the
    Django ORM for such metadata.
    """

    protocol = "internetarchive"

    def __init__(self, access_key: str, secret_key: str, collection: str | None = None, **storage_options):
        """
        Initializes the InternetArchiveFileSystem.

        Args:
            access_key: The Internet Archive API access key.
            secret_key: The Internet Archive API secret key.
            collection: The default collection to use for uploads if not specified
                        per-file in metadata. Defaults to "test_collection".
            **storage_options: Additional options passed to the fsspec AbstractFileSystem.
        """
        super().__init__(**storage_options)
        if not access_key or not secret_key:
            raise ValueError("InternetArchiveFileSystem requires 'access_key' and 'secret_key'.")
        self.access_key = access_key
        self.secret_key = secret_key
        self.collection = collection or "test_collection"
        logger.info(
            "InternetArchiveFileSystem initialized with collection: %s (Access Key: %s****)",
            self.collection,
            self.access_key[:4],
        )

    def _open(self, path, mode="rb", **kwargs):
        """
        Opens a file for reading or writing.

        For reading ('rb'), the `path` is expected to be a direct URL to the
        file on the Internet Archive. The content is streamed.
        For writing ('wb'), the `path` is a logical path within Esperoj, used
        for naming and identifying the upload. Additional `kwargs` can be
        passed to customize upload metadata (e.g., `metadata={'title': 'Custom Title'}`).
        """
        path = cast(str, self._strip_protocol(path))

        if mode == "rb":
            try:
                # The path here is expected to be the direct public URL of the file
                logger.debug("Attempting to stream file from Internet Archive URL: %s", path)
                response = requests.get(path, stream=True, timeout=60)
                response.raise_for_status()
                return response.raw
            except requests.RequestException as e:
                logger.error("Failed to stream file from Internet Archive URL %s: %s", path, e)
                raise IOError(f"Failed to stream file from Internet Archive URL {path}: {e}") from e

        elif mode == "wb":
            # Pass kwargs directly to InternetArchiveFile for metadata customization
            return InternetArchiveFile(self, path, mode=mode, **kwargs)

        else:
            raise NotImplementedError(f"Mode '{mode}' is not supported.")

    def exists(self, path, **kwargs):
        """
        Checks if a file exists by sending a HEAD request to its URL.

        This method is primarily intended for checking the existence of a file
        given its public Internet Archive URL.

        Args:
            path: The public URL of the file on the Internet Archive.
            **kwargs: Additional keyword arguments.
        Returns:
            True if the file exists and is accessible, False otherwise.
        """
        path = cast(str, self._strip_protocol(path))
        try:
            # IA URLs might redirect, so follow redirects.
            response = requests.head(path, timeout=10, allow_redirects=True)
            return response.status_code == 200
        except requests.RequestException as e:
            logger.debug("HEAD request failed for %s: %s", path, e)
            return False

    def rm(self, path, **kwargs):
        """
        Removes a file from the Internet Archive.

        The Internet Archive's `internetarchive` client does not provide a direct
        `delete_file` or `delete_item` method accessible in the same manner as
        other fsspec backends (e.g., `rm` for individual files). Items and files
        on the Internet Archive are generally considered immutable for public access.
        Deletion is typically a more involved process (e.g., setting a `noindex` flag,
        or requiring manual intervention/specific permissions beyond typical API keys).

        This implementation logs a warning and performs no-op, indicating that
        direct file deletion via this fsspec backend is not supported in a simple way.
        Files will effectively be "orphaned" or remain publicly accessible if previously uploaded.

        Args:
            path: The public URL or item identifier of the file/item to be removed.
            **kwargs: Additional keyword arguments.
        """
        # The internetarchive library's `delete` function is for deleting an entire item,
        # and typically requires elevated privileges beyond what's usually provided
        # via access/secret keys for anonymous uploads.
        # For typical use cases with Esperoj, we're treating IA as an append-only archive.
        logger.warning(
            "Direct file deletion (rm) is not supported by the Internet Archive backend for path/URL: %s. "
            "Files uploaded via this method are typically immutable or require manual intervention for removal. "
            "The file will likely remain on the storage.",
            path,
        )
        pass  # No-op for deletion
