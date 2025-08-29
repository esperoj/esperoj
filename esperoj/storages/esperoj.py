"""
Custom fsspec backend for Esperoj with multi-backend support.

This module provides `EsperojFileSystem`, an fsspec-compatible file system
that uses the Django ORM for metadata storage while leveraging other fsspec-compliant
file systems as storage backends.

The file system supports a comprehensive set of operations including listing
files (`ls`), opening files for reading and writing (`_open`), creating
directories (`mkdir`), moving/renaming (`mv`), copying (`cp`), and removing
files and directories (`rm`).

Writing a file involves uploading it to a specified storage backend and
creating corresponding `File` and `FileReplica` records in the database.
Reading a file involves looking up a replica and streaming it directly from
the corresponding backend. This architecture allows for flexible, multi-cloud,
or hybrid storage strategies.
"""

import io
import logging
import mimetypes
import tempfile
from pathlib import Path
from typing import cast, BinaryIO

from django.db import transaction
from fsspec.spec import AbstractFileSystem
from fsspec.utils import tokenize
from io import BufferedReader

from esperoj.models import File, FileReplica
from esperoj.utils.checksums import calculate_checksums

logger = logging.getLogger(__name__)


class EsperojFile(io.BytesIO):
    """
    A file-like object for handling uploads to a specified storage backend.

    This class buffers written content in memory. When the file is closed,
    it uploads the content to the designated fsspec backend, calculates
    checksums, and creates the necessary database records within a transaction.
    """

    def __init__(self, fs: "EsperojFileSystem", path: str, mode: str = "wb", storage_name: str | None = None, **kwargs):
        """
        Initializes the EsperojFile.

        Args:
            fs: The EsperojFileSystem instance.
            path: The logical path of the file within the file system.
            mode: The file mode (only 'wb' is supported for writing).
            storage_name: The name of the storage backend to upload to.
        """
        if mode != "wb":
            raise ValueError("EsperojFile only supports write-binary ('wb') mode.")
        super().__init__()
        self.fs = fs
        self.path = path
        self.storage_name = storage_name or fs.default_storage
        if self.storage_name not in self.fs.filesystems:
            raise ValueError(f"Storage backend '{self.storage_name}' not configured in EsperojFileSystem.")

    def close(self) -> None:
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

        backend_fs = self.fs.filesystems[self.storage_name]

        # 1. Upload the file to the chosen storage backend
        try:
            with cast(BinaryIO, backend_fs.open(self.path, "wb")) as f:
                f.write(file_content)
        except Exception as e:
            logger.error(
                "Failed to upload file %s to storage backend '%s': %s", self.path, self.storage_name, e, exc_info=True
            )
            raise IOError(f"File upload to backend '{self.storage_name}' failed: {e}") from e

        # 2. Calculate checksums efficiently
        with tempfile.NamedTemporaryFile(mode="wb", delete=False) as temp_file:
            temp_file.write(file_content)  # type: ignore
            temp_file_path = Path(temp_file.name)

        try:
            checksums = calculate_checksums(temp_file_path)
        finally:
            temp_file_path.unlink()

        # 3. Create File and FileReplica records in the database atomically
        try:
            with transaction.atomic():
                mime_type, _ = mimetypes.guess_type(self.path)
                if not mime_type:
                    mime_type = "application/octet-stream"

                file_instance, _ = File.objects.update_or_create(
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
                    storage_name=self.storage_name,
                    defaults={
                        "replica_type": "primary",  # Or determine this based on config
                        "storage_path": self.path,  # Assuming logical path equals storage path
                        "is_active": True,
                        "verification_status": "success",
                    },
                )
            logger.info("Successfully uploaded and recorded file: %s to %s", self.path, self.storage_name)
        except Exception as e:
            logger.error("Failed to create database records for %s: %s", self.path, e, exc_info=True)
            # Attempt to clean up the orphaned file on the backend
            try:
                backend_fs.rm(self.path)
                logger.warning("Cleaned up orphaned file %s from backend '%s'.", self.path, self.storage_name)
            except Exception as cleanup_e:
                logger.error(
                    "Failed to clean up orphaned file %s from backend '%s': %s", self.path, self.storage_name, cleanup_e
                )
            raise IOError(f"Database record creation failed: {e}") from e

        super().close()


class EsperojFileSystem(AbstractFileSystem):
    """
    An fsspec-compatible file system for Esperoj with multi-backend support.

    This file system uses the Django ORM to manage file metadata and can use
    multiple fsspec-compliant file systems (e.g., s3, gcs, local) as storage
    backends for the actual file content.
    """

    protocol = "esperoj"

    def __init__(
        self,
        filesystems: dict[str, AbstractFileSystem] | None = None,
        default_storage: str | None = None,
        **storage_options,
    ):
        """
        Initializes the EsperojFileSystem.

        Args:
            filesystems: A dictionary mapping storage names to instantiated
                         fsspec filesystem objects.
            default_storage: The name of the default storage backend to use for writes
                             if not otherwise specified.
            **storage_options: Additional options passed to the superclass.
        """
        super().__init__(**storage_options)
        self.filesystems = filesystems or {}
        if not self.filesystems:
            raise ValueError("EsperojFileSystem requires at least one configured filesystem backend.")

        if default_storage and default_storage not in self.filesystems:
            raise ValueError(f"Default storage '{default_storage}' is not in the configured filesystems.")

        self.default_storage = default_storage or next(iter(self.filesystems.keys()))
        logger.info("EsperojFileSystem initialized with backends: %s", list(self.filesystems.keys()))

    def ls(self, path: str, detail: bool = True, **kwargs):
        path = cast(str, self._strip_protocol(path)).lstrip("/")
        if path and not path.endswith("/"):
            path += "/"

        # Query for files and directories within the specified path
        files_in_path = File.objects.filter(path__startswith=path)

        entries = set()
        for f in files_in_path:
            # Get the part of the path relative to the ls path
            relative_path = f.path[len(path) :]
            if not relative_path:
                continue

            # The first part of the relative path is either a file or a directory
            name = relative_path.split("/")[0]

            if "/" in relative_path:  # It's a directory
                entries.add((path + name, "directory", 0))
            else:  # It's a file
                entries.add((f.path, "file", f.size))

        if detail:
            return [{"name": name, "type": type, "size": size} for name, type, size in entries]
        else:
            return sorted([name for name, _, _ in entries])

    def _open(
        self,
        path: str,
        mode: str = "rb",
        block_size: int | None = None,
        autocommit: bool = True,
        storage_name: str | None = None,
        **kwargs,
    ):
        path = cast(str, self._strip_protocol(path))

        if "b" not in mode:
            binary_stream = self._open(path, mode.replace("t", "b"), storage_name=storage_name, **kwargs)
            return io.TextIOWrapper(
                BufferedReader(binary_stream),
                encoding=kwargs.get("encoding", "utf-8"),
            )

        if mode == "rb":
            query = FileReplica.objects.select_related("file").filter(file__path=path, is_active=True)
            if storage_name:
                query = query.filter(storage_name=storage_name)

            replica = query.first()
            if not replica:
                raise FileNotFoundError(f"No active replica found for file: {path}")

            backend_name = replica.storage_name
            backend_fs = self.filesystems.get(backend_name)
            if not backend_fs:
                raise IOError(f"Storage backend '{backend_name}' for replica of '{path}' is not configured.")

            try:
                return backend_fs.open(replica.storage_path, "rb")
            except Exception as e:
                logger.error("Failed to open file %s from backend '%s': %s", path, backend_name, e, exc_info=True)
                raise IOError(f"Failed to stream file from backend '{backend_name}': {e}") from e

        elif mode == "wb":
            return EsperojFile(self, path, mode=mode, storage_name=storage_name, **kwargs)

        else:
            raise NotImplementedError(f"Mode '{mode}' is not supported.")

    def info(self, path: str, **kwargs) -> dict:
        path = cast(str, self._strip_protocol(path))
        try:
            file_instance = File.objects.get(path=path)
            return {"name": path, "size": file_instance.size, "type": "file"}
        except File.DoesNotExist:
            # Check if it's a directory
            if self.isdir(path):
                return {"name": path, "size": 0, "type": "directory"}
            raise FileNotFoundError(path)

    def isdir(self, path: str) -> bool:
        path = cast(str, self._strip_protocol(path)).lstrip("/")
        if not path.endswith("/"):
            path += "/"
        return File.objects.filter(path__startswith=path).exists()

    def isfile(self, path: str) -> bool:
        path = cast(str, self._strip_protocol(path))
        return File.objects.filter(path=path).exists()

    def exists(self, path: str, **kwargs) -> bool:
        path = cast(str, self._strip_protocol(path))
        return self.isfile(path) or self.isdir(path)

    def mkdir(self, path: str, create_parents: bool = True, **kwargs):
        # Directories are virtual and exist implicitly if they contain files.
        # This method is a no-op but is required for fsspec compliance.
        pass

    def rmdir(self, path: str):
        path = cast(str, self._strip_protocol(path))
        if not path.endswith("/"):
            path += "/"
        if self.exists(path) and self.ls(path):
            raise OSError(f"Directory not empty: {path}")

    def rm(self, path: str, recursive: bool = False, maxdepth: int | None = None) -> None:
        path = cast(str, self._strip_protocol(path))

        if self.isfile(path):
            self._rm_file(path)
        elif self.isdir(path):
            if not recursive:
                raise OSError(f"Cannot remove directory '{path}' without recursive=True.")

            # Find all files in the directory and its subdirectories
            files_to_delete = list(File.objects.filter(path__startswith=path))
            for file_instance in files_to_delete:
                self._rm_file(file_instance.path)
        else:
            raise FileNotFoundError(f"File or directory not found: {path}")

    def _rm_file(self, path: str) -> None:
        """Removes a single file and all its replicas."""
        try:
            with transaction.atomic():
                file_instance = File.objects.select_for_update().get(path=path)
                replicas = list(file_instance.replicas.all())

                # Delete from storage backends first
                for replica in replicas:
                    backend_fs = self.filesystems.get(replica.storage_name)
                    if backend_fs:
                        try:
                            if backend_fs.exists(replica.storage_path):
                                backend_fs.rm(replica.storage_path)
                                logger.info(
                                    "Removed replica %s from backend '%s'", replica.storage_path, replica.storage_name
                                )
                        except Exception as e:
                            logger.error(
                                "Failed to remove replica %s from backend '%s': %s",
                                replica.storage_path,
                                replica.storage_name,
                                e,
                            )
                            # Decide if failure should halt the whole process.
                            # For now, we log and continue to remove the DB record.
                    else:
                        logger.warning(
                            "Backend '%s' for replica %s not configured. Cannot delete from storage.",
                            replica.storage_name,
                            replica.storage_path,
                        )

                # Delete the database record
                file_instance.delete()
                logger.info("Removed database record for file: %s", path)

        except File.DoesNotExist:
            raise FileNotFoundError(f"File not found: {path}")
        except Exception as e:
            logger.error("Failed during removal of file %s: %s", path, e, exc_info=True)
            raise IOError(f"Failed to remove file {path}: {e}") from e

    def mv(self, path1: str, path2: str, **kwargs) -> None:
        path1 = cast(str, self._strip_protocol(path1))
        path2 = cast(str, self._strip_protocol(path2))

        try:
            with transaction.atomic():
                file_instance = File.objects.select_for_update().get(path=path1)
                replicas = list(file_instance.replicas.all())

                # Move files on all backends
                for replica in replicas:
                    backend_fs = self.filesystems.get(replica.storage_name)
                    if not backend_fs:
                        raise IOError(f"Backend '{replica.storage_name}' not configured, cannot move file.")

                    new_storage_path = replica.storage_path.replace(path1, path2, 1)
                    backend_fs.mv(replica.storage_path, new_storage_path)
                    replica.storage_path = new_storage_path
                    replica.save()

                # Update the main file record
                file_instance.path = path2
                file_instance.name = path2.split("/")[-1]
                file_instance.save()
                logger.info("Renamed file from %s to %s", path1, path2)

        except File.DoesNotExist:
            raise FileNotFoundError(path1)
        except Exception as e:
            logger.error("Failed to move %s to %s: %s", path1, path2, e, exc_info=True)
            raise IOError(f"Move operation failed: {e}") from e

    def cp(self, path1: str, path2: str, **kwargs) -> None:
        path1 = cast(str, self._strip_protocol(path1))
        path2 = cast(str, self._strip_protocol(path2))

        try:
            with transaction.atomic():
                original_file = File.objects.get(path=path1)
                original_replicas = list(original_file.replicas.all())

                if not original_replicas:
                    raise FileNotFoundError(f"No replicas found for source file {path1} to copy.")

                # Create new logical file
                new_file, created = File.objects.get_or_create(
                    path=path2,
                    defaults={
                        "name": path2.split("/")[-1],
                        "size": original_file.size,
                        "md5": original_file.md5,
                        "sha1": original_file.sha1,
                        "sha256": original_file.sha256,
                        "mime_type": original_file.mime_type,
                    },
                )

                if not created:
                    logger.warning("Destination file %s already exists. Overwriting metadata.", path2)

                # Copy on backends and create new replica records
                for replica in original_replicas:
                    backend_fs = self.filesystems.get(replica.storage_name)
                    if not backend_fs:
                        logger.warning(
                            "Backend '%s' not configured. Skipping copy for this replica.", replica.storage_name
                        )
                        continue

                    new_storage_path = replica.storage_path.replace(path1, path2, 1)
                    backend_fs.cp(replica.storage_path, new_storage_path)

                    FileReplica.objects.update_or_create(
                        file=new_file,
                        storage_name=replica.storage_name,
                        defaults={
                            "replica_type": replica.replica_type,
                            "storage_path": new_storage_path,
                            "is_active": True,
                            "verification_status": "success",
                        },
                    )

            logger.info("Copied file from %s to %s", path1, path2)

        except File.DoesNotExist:
            raise FileNotFoundError(f"Source file not found: {path1}")
        except Exception as e:
            logger.error("Failed to copy %s to %s: %s", path1, path2, e, exc_info=True)
            raise IOError(f"Copy operation failed: {e}") from e

    def ukey(self, path: str) -> str:
        return tokenize(cast(str, self._strip_protocol(path)), self.protocol)
