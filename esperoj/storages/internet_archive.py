"""
Custom fsspec backend for the Internet Archive.

This module provides an InternetArchiveFileSystem, an fsspec-compatible file system
that interacts with the Internet Archive for file storage.

The file system supports opening files for reading and writing (`_open`).
Writing a file involves uploading to a *pre-existing* Internet Archive item,
with metadata and identifier assumed to be managed by a higher-level service layer.
Reading a file streams it directly from the Internet Archive's public URL.
Paths within this file system are expected to start with the Internet Archive item identifier,
followed by the file path within that item (e.g., "item_id/path/to/file.txt").
"""

import io
import logging
import internetarchive
import requests
from fsspec.spec import AbstractFileSystem
from typing import cast, Any, Union
from io import RawIOBase

logger = logging.getLogger(__name__)


class InternetArchiveFile(io.BytesIO):
    """
    A file-like object for handling uploads to the Internet Archive.

    This class buffers the written content in memory. When the file is closed,
    it uploads the content to a specified Internet Archive item. The item
    identifier and file path within the item are parsed from the provided
    fsspec path. Any necessary metadata is provided externally via kwargs.
    """

    def __init__(self, fs: "InternetArchiveFileSystem", path: str, mode: str = "wb", **kwargs: Any) -> None:
        """
        Initializes the InternetArchiveFile.

        Args:
            fs: The InternetArchiveFileSystem instance.
            path: The full fsspec path (e.g., "item_identifier/path/to/file.txt").
                  The item_identifier and path_in_item are parsed from this.
            mode: The file mode (only 'wb' is supported for writing).
            **kwargs: Additional keyword arguments, which *may* include:
                      'metadata': A dictionary of pre-formed metadata for the IA item.
        """
        if mode != "wb":
            raise ValueError("InternetArchiveFile only supports write-binary ('wb') mode.")
        super().__init__()
        self.fs = fs
        # Parse item_identifier and path_in_item directly from the fsspec path
        self.item_identifier, self.path_in_item = self.fs._parse_ia_path(path)
        self.metadata_for_upload: dict[str, Any] = kwargs.pop("metadata", {})

        self.storage_url: Union[str, None] = None  # This will be set after successful upload

        logger.debug(
            "InternetArchiveFile initialized for item: %s, path: %s (fsspec_path: %s)",
            self.item_identifier,
            self.path_in_item,
            path,
        )

    def close(self) -> None:
        """
        Finalizes the file by uploading its content to the Internet Archive.

        This method buffers the content and uses the `internetarchive.upload` client
        to add the file to the specified item. The item is assumed to exist
        with its metadata already defined by a service layer.

        Returns:
            None
        """
        if self.closed:
            return

        self.seek(0)
        file_content = self.getvalue()
        size = len(file_content)

        if size == 0:
            logger.warning(
                "Attempted to upload an empty file for item %s, path %s. Aborting.",
                self.item_identifier,
                self.path_in_item,
            )
            super().close()
            return

        try:
            logger.info(
                "Uploading file at path %s to Internet Archive item %s...", self.path_in_item, self.item_identifier
            )

            # The internetarchive client expects files as a dictionary where keys are file paths
            # and values are file-like objects or paths.
            files = {self.path_in_item: io.BytesIO(file_content)}

            internetarchive.upload(
                identifier=self.item_identifier,
                files=files,
                metadata=self.metadata_for_upload,  # Use the pre-formed metadata directly
                access_key=self.fs.access_key,
                secret_key=self.fs.secret_key,
                retries=3,  # Number of retries for network operations
                retries_sleep=5,  # Seconds to wait between retries
            )

            # Construct the public download URL for the uploaded file
            self.storage_url = f"https://archive.org/download/{self.item_identifier}/{self.path_in_item}"

            logger.info(
                "Successfully uploaded file at path %s to Internet Archive item %s. URL: %s",
                self.path_in_item,
                self.item_identifier,
                self.storage_url,
            )

        except Exception as e:
            logger.error(
                "Failed to upload file at path %s to Internet Archive item %s: %s",
                self.path_in_item,
                self.item_identifier,
                e,
                exc_info=True,
            )
            raise IOError(f"Internet Archive upload failed for {self.item_identifier}/{self.path_in_item}: {e}") from e

        super().close()


class InternetArchiveFileSystem(AbstractFileSystem):
    """
    An fsspec-compatible file system for the Internet Archive.

    This file system is designed as a storage backend for Esperoj. It handles
    file uploads by adding files to pre-existing Internet Archive items and
    manages reads by streaming content from public IA URLs. It does not natively
    handle metadata listings or directory structures; that is the responsibility
    of a higher-level file system like `EsperojFileSystem` which uses the
    Django ORM for such metadata.

    Paths within this file system are expected to be in the format
    "item_identifier/path_within_item" (e.g., "myitem/data/file.csv").
    The 'path_within_item' part can include slashes itself to denote paths
    within an Internet Archive item (e.g., "myitem/folder/subfolder/file.txt").
    """

    protocol = "internetarchive"

    def __init__(self, access_key: str, secret_key: str, **storage_options: Any) -> None:
        """
        Initializes the InternetArchiveFileSystem.

        Args:
            access_key: The Internet Archive API access key.
            secret_key: The Internet Archive API secret key.
            **storage_options: Additional options passed to the fsspec AbstractFileSystem.
        """
        super().__init__(**storage_options)
        if not access_key or not secret_key:
            raise ValueError("InternetArchiveFileSystem requires 'access_key' and 'secret_key'.")
        self.access_key = access_key
        self.secret_key = secret_key
        logger.info(
            "InternetArchiveFileSystem initialized (Access Key: %s****)",
            self.access_key[:4],
        )

    def _parse_ia_path(self, path: str) -> tuple[str, str]:
        """
        Parses an fsspec path into an Internet Archive item identifier and a file path within that item.

        Expected path format: "item_identifier/path_within_item"
        The path_within_item can contain slashes if representing a sub-path within the IA item.

        Args:
            path: The fsspec path without the protocol (e.g., "myitem/data/file.csv").

        Returns:
            A tuple (item_identifier, path_within_item).

        Raises:
            ValueError: If the path does not conform to the expected "identifier/path" format.
        """
        parts = path.split("/", 1)  # Split only on the first slash

        if len(parts) < 2:
            raise ValueError(
                f"Invalid Internet Archive path format. Expected 'item_identifier/path_within_item', got '{path}'."
            )

        item_identifier = parts[0]
        path_in_item = parts[1]

        if not item_identifier:
            raise ValueError(f"Internet Archive path '{path}' has an empty item identifier.")
        if not path_in_item:
            # This means path was "identifier/"
            raise ValueError(f"Internet Archive path '{path}' has an empty path within the item.")

        return item_identifier, path_in_item

    def _open(self, path: str, mode: str = "rb", **kwargs: Any) -> Union[RawIOBase, "InternetArchiveFile"]:
        """
        Opens a file for reading or writing.

        For reading ('rb'), the `path` is expected to be in the format "item_identifier/path_within_item".
        The content is streamed from the constructed public IA URL.
        For writing ('wb'), the `path` is also "item_identifier/path_within_item".
        The `kwargs` may include a `metadata` dictionary for the upload.

        Args:
            path: The fsspec path (e.g., "item_identifier/path/to/file.txt").
            mode: The file mode ('rb' for read-binary, 'wb' for write-binary).
            **kwargs: Additional keyword arguments, which *may* include:
                      'metadata': A dictionary of pre-formed metadata for the IA item (for 'wb' mode).

        Returns:
            A file-like object; either a raw byte stream (RawIOBase) for reading,
            or an `InternetArchiveFile` instance for writing.
        """
        path = cast(str, self._strip_protocol(path))
        item_identifier, path_in_item = self._parse_ia_path(path)

        if mode == "rb":
            try:
                # Construct the direct public URL for the file
                file_url = f"https://archive.org/download/{item_identifier}/{path_in_item}"
                logger.debug("Attempting to stream file from Internet Archive URL: %s", file_url)
                response = requests.get(file_url, stream=True, timeout=60)
                response.raise_for_status()
                return cast(io.RawIOBase, response.raw)
            except requests.RequestException as e:
                logger.error("Failed to stream file from Internet Archive URL %s: %s", file_url, e)
                raise IOError(f"Failed to stream file from Internet Archive URL {file_url}: {e}") from e

        elif mode == "wb":
            # InternetArchiveFile will parse item_identifier and path_in_item from `path` internally.
            # Only pass optional metadata via kwargs.
            return InternetArchiveFile(
                self,
                path=path,
                mode=mode,
                metadata=kwargs.pop("metadata", {}),  # Ensure metadata is explicitly passed by service layer
            )

        else:
            raise NotImplementedError(f"Mode '{mode}' is not supported.")

    def exists(self, path: str, **kwargs: Any) -> bool:
        """
        Checks if a file exists on the Internet Archive by sending a HEAD request to its URL.

        Args:
            path: The fsspec path in the format "item_identifier/path_within_item".
            **kwargs: Additional keyword arguments (not used by this method, but passed for fsspec compatibility).
        Returns:
            True if the file exists and is accessible, False otherwise.
        """
        path = cast(str, self._strip_protocol(path))
        try:
            item_identifier, path_in_item = self._parse_ia_path(path)
            file_url = f"https://archive.org/download/{item_identifier}/{path_in_item}"
            # IA URLs might redirect, so follow redirects.
            response = requests.head(file_url, timeout=10, allow_redirects=True)
            return response.status_code == 200
        except ValueError as e:
            logger.debug("Path parsing failed for exists check of %s: %s", path, e)
            return False
        except requests.RequestException as e:
            logger.debug("HEAD request failed for %s (URL: %s): %s", path, file_url, e)
            return False

    def rm(self, path: str, **kwargs: Any) -> None:
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
            path: The fsspec path in the format "item_identifier/path_within_item".
            **kwargs: Additional keyword arguments (not used by this method, but passed for fsspec compatibility).

        Returns:
            None
        """
        # The internetarchive library's `delete` function is for deleting an entire item,
        # and typically requires elevated privileges beyond what's usually provided
        # via access/secret keys for anonymous uploads.
        # For typical use cases with Esperoj, we're treating IA as an append-only archive.
        logger.warning(
            "Direct file deletion (rm) is not supported by the Internet Archive backend for path: %s. "
            "Files uploaded via this method are typically immutable or require manual intervention for removal. "
            "The file will likely remain on the storage.",
            path,
        )
        pass  # No-op for deletion
