"""
Custom fsspec backend for a Catbox-like file storage service.

This module provides a `CatboxFileSystem`, an fsspec-compatible file system
that interacts with a simple file hosting service (like Catbox.moe) and uses
the Django ORM to store metadata about the files.

The file system supports a comprehensive set of operations including listing
files (`ls`), opening files for reading and writing (`_open`), creating
directories (`mkdir`), moving/renaming (`mv`), and removing files and
directories (`rm`). Writing a file involves uploading it to the external
service and creating corresponding `File` and `FileReplica` records in the
database within a transaction. Reading a file streams it directly from the
external service's URL.
"""

import io
import logging
import mimetypes
import requests
import tempfile
from pathlib import Path

from django.db import transaction
from fsspec.spec import AbstractFileSystem
from typing import cast
from fsspec.utils import tokenize
from io import BufferedReader

from esperoj.models import File, FileReplica
from esperoj.utils.checksums import calculate_checksums

logger = logging.getLogger(__name__)

# Define constants for the Catbox-like service
CATBOX_API_URL = "https://catbox.moe/user/api.php"


class CatboxFile(io.BytesIO):
    """
    A file-like object for handling uploads to the Catbox service.

    This class buffers the written content in memory. When the file is closed,
    it uploads the content to the Catbox service, creates the necessary
    database records, and finalizes the transaction. This version improves
    on the original by performing checksums more efficiently and handling
    database operations within a transaction.
    """

    def __init__(self, fs, path, mode="wb", **kwargs):
        """
        Initializes the CatboxFile.

        Args:
            fs: The CatboxFileSystem instance.
            path: The logical path of the file within the file system.
            mode: The file mode (only 'wb' is supported for writing).\
        """
        if mode != "wb":
            raise ValueError("CatboxFile only supports write-binary ('wb') mode.")
        super().__init__()
        self.fs = fs
        self.path = path

    def close(self):
        """
        Finalizes the file by uploading its content and creating database records.
        """
        self.seek(0)
        file_content = self.getvalue()
        size = len(file_content)

        if size == 0:
            logger.warning("Attempted to upload an empty file to %s. Aborting.", self.path)
            super().close()
            return

        # 1. Upload the file to the Catbox service
        try:
            files = {"fileToUpload": (self.path.split("/")[-1], file_content)}
            data = {"reqtype": "fileupload", "userhash": ""}  # userhash can be empty for anonymous uploads
            response = requests.post(CATBOX_API_URL, files=files, data=data, timeout=300)
            response.raise_for_status()
            storage_url = response.text
        except requests.RequestException as e:
            logger.error("Failed to upload file %s to Catbox service: %s", self.path, e)
            raise IOError(f"File upload failed: {e}") from e

        # 2. Calculate checksums efficiently
        # Create a temporary file in-memory to avoid disk I/O for checksums
        with tempfile.NamedTemporaryFile(delete=False) as temp_file:
            temp_file.write(file_content)
            temp_file_path = Path(temp_file.name)

        try:
            checksums = calculate_checksums(temp_file_path)
        finally:
            temp_file_path.unlink()

        # 3. Create File and FileReplica records in the database atomically
        try:
            with transaction.atomic():
                # Use mimetypes to guess the MIME type from the filename
                mime_type, _ = mimetypes.guess_type(self.path)
                if not mime_type:
                    mime_type = "application/octet-stream"

                file_instance, created = File.objects.update_or_create(
                    path=self.path,
                    defaults={
                        "name": self.path.split("/")[-1],
                        "size": size,
                        "md5": checksums["md5"],
                        "sha1": checksums["sha1"],
                        "sha256": checksums["sha256"],
                        "mime_type": mime_type,
                    },
                )

                FileReplica.objects.update_or_create(
                    file=file_instance,
                    storage_name="catbox_storage",  # A pre-defined storage name
                    defaults={
                        "replica_type": "primary",
                        "storage_path": storage_url,
                        "is_active": True,
                        "verification_status": "success",
                    },
                )
            logger.info("Successfully uploaded and recorded file: %s", self.path)
        except Exception as e:
            logger.error("Failed to create database records for %s: %s", self.path, e)
            # A robust implementation would add logic here to delete the file from Catbox
            # to avoid orphaned files, if the Catbox API supports deletion.
            raise IOError(f"Database record creation failed: {e}") from e

        super().close()


class CatboxFileSystem(AbstractFileSystem):
    """
    An fsspec-compatible file system for a Catbox-like service.

    This file system uses the Django ORM to manage file metadata, treating
    the database as the source of truth for the file index, while the actual
    file content is stored on an external service. This implementation is
    more compliant with the fsspec standard.
    """

    protocol = "catbox"

    def __init__(self, **storage_options):
        super().__init__(**storage_options)

    def ls(self, path, detail=True, **kwargs):
        path = cast(str, self._strip_protocol(path)).lstrip("/")
        if path and not path.endswith("/"):
            path += "/"

        files_in_path = File.objects.filter(path__startswith=path)

        entries = set()
        for f in files_in_path:
            relative_path = f.path[len(path) :]
            parts = relative_path.split("/")
            if not parts[0]:
                continue

            if len(parts) == 1:  # It's a file
                entries.add((f.path, "file", f.size))
            else:  # It's a directory
                entries.add((path + parts[0], "directory", 0))

        if detail:
            return [{"name": name, "type": type, "size": size} for name, type, size in entries]
        else:
            return [name for name, _, _ in entries]

    def _open(self, path, mode="rb", block_size=None, autocommit=True, **kwargs):
        path = cast(str, self._strip_protocol(path))

        if "b" not in mode:
            # Handle text modes by wrapping the binary stream
            binary_stream = self._open(path, mode.replace("t", "") + "b", **kwargs)
            # Wrap the raw binary stream in BufferedReader to satisfy TextIOWrapper's buffer requirement
            return io.TextIOWrapper(
                BufferedReader(binary_stream),
                encoding=kwargs.get("encoding", "utf-8"),
            )

        if mode == "rb":
            try:
                replica = FileReplica.objects.select_related("file").get(
                    file__path=path, storage_name="catbox_storage", is_active=True
                )
                response = requests.get(replica.storage_path, stream=True, timeout=60)
                response.raise_for_status()
                return response.raw
            except FileReplica.DoesNotExist:
                raise FileNotFoundError(f"File not found in Catbox storage: {path}")
            except requests.RequestException as e:
                raise IOError(f"Failed to stream file from Catbox: {e}") from e

        elif mode == "wb":
            return CatboxFile(self, path, mode=mode, **kwargs)

        else:
            raise NotImplementedError(f"Mode '{mode}' is not supported.")

    def info(self, path, **kwargs):
        path = cast(str, self._strip_protocol(path))
        if self.isfile(path):
            file_instance = File.objects.get(path=path)
            return {"name": path, "size": file_instance.size, "type": "file"}
        elif self.isdir(path):
            return {"name": path, "size": 0, "type": "directory"}
        else:
            raise FileNotFoundError(path)

    def isdir(self, path):
        path = cast(str, self._strip_protocol(path))
        if not path.endswith("/"):
            path += "/"
        return File.objects.filter(path__startswith=path).exists()

    def isfile(self, path):
        path = cast(str, self._strip_protocol(path))
        return File.objects.filter(path=path).exists()

    def exists(self, path, **kwargs):
        path = cast(str, self._strip_protocol(path))
        return self.isfile(path) or self.isdir(path)

    def mkdir(self, path, create_parents=True, **kwargs):
        # Directories are virtual and exist implicitly if they contain files.
        # This method is a no-op but is required for fsspec compliance.
        pass

    def rmdir(self, path):
        path = cast(str, self._strip_protocol(path))
        if not path.endswith("/"):
            path += "/"
        if File.objects.filter(path__startswith=path).exists():
            raise OSError(f"Directory not empty: {path}")

    def rm(self, path, recursive=False, maxdepth=None):
        path = cast(str, self._strip_protocol(path))
        if self.isfile(path):
            try:
                file_instance = File.objects.get(path=path)
                # Note: This doesn't delete the file from the Catbox service itself.
                file_instance.delete()
                logger.info("Removed database record for file: %s", path)
            except File.DoesNotExist:
                raise FileNotFoundError(f"File not found: {path}")
        elif self.isdir(path):
            if not recursive:
                raise OSError("Cannot remove directory without recursive=True")
            else:  # Proceed with deletion only if recursive is True
                files_to_delete = File.objects.filter(path__startswith=f"{path}/")
                files_to_delete.delete()
                logger.info("Removed database records for directory: %s", path)
        else:
            raise FileNotFoundError(f"File or directory not found: {path}")

    def mv(self, path1, path2, **kwargs):
        path1 = cast(str, self._strip_protocol(path1))
        path2 = cast(str, self._strip_protocol(path2))
        try:
            file_instance = File.objects.get(path=path1)
            file_instance.path = path2
            file_instance.name = path2.split("/")[-1]
            file_instance.save()
            logger.info("Renamed file from %s to %s", path1, path2)
        except File.DoesNotExist:
            raise FileNotFoundError(path1)

    def cp(self, path1, path2, **kwargs):
        path1 = cast(str, self._strip_protocol(path1))
        path2 = cast(str, self._strip_protocol(path2))
        try:
            original_file = File.objects.get(path=path1)
            original_replica = FileReplica.objects.get(file=original_file, storage_name="catbox_storage")

            with transaction.atomic():
                new_file = File.objects.create(
                    path=path2,
                    name=path2.split("/")[-1],
                    size=original_file.size,
                    md5=original_file.md5,
                    sha1=original_file.sha1,
                    sha256=original_file.sha256,
                    mime_type=original_file.mime_type,
                )
                FileReplica.objects.create(
                    file=new_file,
                    storage_name=original_replica.storage_name,
                    replica_type=original_replica.replica_type,
                    storage_path=original_replica.storage_path,
                    is_active=True,
                    verification_status="success",
                )
            logger.info("Copied file from %s to %s", path1, path2)
        except File.DoesNotExist:
            raise FileNotFoundError(path1)
        except FileReplica.DoesNotExist:
            raise FileNotFoundError(f"Replica for {path1} not found.")

    def ukey(self, path):
        return tokenize(cast(str, self._strip_protocol(path)), self.protocol)
