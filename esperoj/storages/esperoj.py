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
or hybrid storage strategies with tiered storage types (primary, backup, archive).
"""

import io
import logging
import mimetypes
import tempfile
from pathlib import Path
from typing import cast, BinaryIO

from django.db import transaction
from fsspec.spec import AbstractFileSystem
from io import BufferedReader

from esperoj.models import File, FileReplica
from esperoj.constants import ReplicaType, StorageName
from esperoj.utils.checksums import calculate_checksums

logger = logging.getLogger(__name__)


class EsperojFile(io.BytesIO):
    """
    A file-like object for handling uploads to a specified storage backend(s).

    This class buffers written content in memory. When the file is closed,
    it uploads the content to the designated fsspec backend(s), calculates
    checksums, and creates the necessary database records within a transaction.
    """

    DEFAULT_CHUNK_SIZE = 65536  # 64 KB

    def __init__(
        self,
        fs: "EsperojFileSystem",
        path: str,
        mode: str = "wb",
        replica_types: list[str] | None = None,
    ):
        """
        Initializes the EsperojFile.

        Args:
            fs: The EsperojFileSystem instance.
            path: The logical path of the file within the file system.
            mode: The file mode (only 'wb' is supported for writing).
            replica_types: A list of ReplicaType values (e.g., ['original', 'access_copy'])
                           to upload this file to. If None, defaults to ORIGINAL and ACCESS.
        """
        if mode != "wb":
            raise ValueError("EsperojFile only supports write-binary ('wb') mode.")
        super().__init__()

        self.fs = fs
        self.path = path
        self.replica_types = replica_types
        self.file_content: bytes | None = None
        self.checksums: dict[str, str] | None = None
        self.uploaded_storage_paths: dict[str, str] = {}

    @property
    def size(self) -> int:
        """Return the size of the file content currently in the buffer."""
        return self.getbuffer().nbytes

    def _upload_replica_to_backend(
        self, file_instance: File, replica_type_value: str, backend_name: str, file_obj: BinaryIO
    ) -> str | None:
        """
        Helper to upload content to a single backend and create a FileReplica record.
        Returns the storage_path_on_backend if successful, None otherwise.
        """
        backend_fs = self.fs.filesystems.get(backend_name)
        if not backend_fs:
            logger.error(
                "Backend '%s' for replica type '%s' not configured. Skipping upload for %s.",
                backend_name,
                replica_type_value,
                self.path,
            )
            return None

        storage_path_on_backend = self.path  # Default to logical path
        try:
            with cast(BinaryIO, backend_fs.open(self.path, "wb")) as f:
                file_obj.seek(0)  # Ensure we read from the beginning
                while True:
                    chunk = file_obj.read(self.DEFAULT_CHUNK_SIZE)
                    if not chunk:
                        break
                    f.write(chunk)
                if hasattr(f, "storage_url") and getattr(f, "storage_url"):
                    storage_path_on_backend = getattr(f, "storage_url")

            FileReplica.objects.update_or_create(
                file=file_instance,
                storage_name=backend_name,
                defaults={
                    "replica_type": replica_type_value,
                    "storage_path": storage_path_on_backend,
                    "is_active": True,
                    "verification_status": "success",
                },
            )
            logger.info(
                "Successfully uploaded replica for %s to %s (%s) at %s",
                self.path,
                backend_name,
                replica_type_value,
                storage_path_on_backend,
            )
            return storage_path_on_backend

        except Exception as e:
            logger.error(
                "Failed to upload file %s to backend '%s' (replica type: %s): %s",
                self.path,
                backend_name,
                replica_type_value,
                e,
                exc_info=True,
            )
            # Attempt to clean up orphaned file on the backend if upload failed
            try:
                if storage_path_on_backend and backend_fs.exists(storage_path_on_backend):
                    backend_fs.rm(storage_path_on_backend)
                    logger.warning(
                        "Cleaned up orphaned file %s from backend '%s'.", storage_path_on_backend, backend_name
                    )
            except Exception as cleanup_e:
                logger.error(
                    "Failed to clean up orphaned file %s from backend '%s': %s",
                    storage_path_on_backend,
                    backend_name,
                    cleanup_e,
                )
            return None

    def _calculate_and_store_checksums(self) -> dict[str, str]:
        """Calculates checksums for the file content."""
        with tempfile.NamedTemporaryFile(mode="wb", delete=False) as temp_file:
            temp_file.write(self.file_content)  # type: ignore
            temp_file_path = Path(temp_file.name)
        try:
            return calculate_checksums(temp_file_path)
        finally:
            temp_file_path.unlink()

    def _create_or_update_file_metadata(self, size: int, checksums: dict[str, str]) -> File:
        """Creates or updates the main File record."""
        mime_type, _ = mimetypes.guess_type(self.path)
        mime_type = mime_type or "application/octet-stream"

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
        return file_instance

    def _upload_replicas(self, file_instance: File, target_replica_types_for_upload: list[str]) -> list[str]:
        """Uploads content to target backends and creates FileReplica records."""
        successful_uploads = []
        for replica_type_value in target_replica_types_for_upload:
            backend_names_for_replica_type = self.fs.replica_type_backend_mapping.get(replica_type_value)

            if not backend_names_for_replica_type:
                logger.warning(
                    "No backend configured for replica type '%s' for file %s. Skipping this replica type.",
                    replica_type_value,
                    self.path,
                )
                continue

            for backend_name in backend_names_for_replica_type:
                storage_path = self._upload_replica_to_backend(file_instance, replica_type_value, backend_name, self)
                if storage_path:
                    self.uploaded_storage_paths[backend_name] = storage_path
                    successful_uploads.append(f"{backend_name} ({replica_type_value})")
        return successful_uploads

    def close(self) -> None:
        """
        Finalizes the file by uploading its content to specified storage types
        and creating database records.
        """
        self.seek(0)  # Ensure the internal pointer is at the beginning before calculating size.
        size = self.size

        if size == 0:
            logger.warning("Attempted to upload an empty file to %s. Aborting.", self.path)
            super().close()
            return

        target_replica_types_for_upload = self.replica_types or [ReplicaType.ORIGINAL.value, ReplicaType.ACCESS.value]
        if self.replica_types is None:
            logger.debug("No replica_types specified for %s, defaulting to ORIGINAL and ACCESS.", self.path)

        self.checksums = self._calculate_and_store_checksums()

        with transaction.atomic():
            file_instance = self._create_or_update_file_metadata(size, self.checksums)
            successful_uploads = self._upload_replicas(file_instance, target_replica_types_for_upload)

            if not successful_uploads:
                logger.critical(
                    "No successful uploads for file %s across any configured storage backends. File metadata exists without content.",
                    self.path,
                )
                raise IOError(f"No replicas created for file {self.path}. Upload failed for all target backends.")

        super().close()


class EsperojFileSystem(AbstractFileSystem):
    """
    An fsspec-compatible file system for Esperoj with multi-backend support.

    This file system uses the Django ORM to manage file metadata and can use
    multiple fsspec-compliant file systems (e.g., s3, gcs, local) as storage
    backends for the actual file content.

    It supports tiered storage (primary, backup, archive) for read priority
    and flexible upload strategies.
    """

    protocol = "esperoj"

    def __init__(
        self,
        filesystems: dict[str, AbstractFileSystem],
        default_storage: str,
        primary_storages: list[str],
        backup_storages: list[str],
        archive_storages: list[str],
        replica_type_backend_mapping: dict[str, list[str]],
        **storage_options,
    ):
        """
        Initializes the EsperojFileSystem.

        Args:
            filesystems: A dictionary mapping storage names to instantiated
                         fsspec filesystem objects.
            default_storage: The name of the default storage backend to use for writes
                             if not otherwise specified.
            primary_storages: List of storage names considered 'primary' for reads.
            backup_storages: List of storage names considered 'backup' for reads.
            archive_storages: List of storage names considered 'archive' for reads.
            replica_type_backend_mapping: A dictionary mapping ReplicaType values (str)
                                          to a list of storage backend names (str) where
                                          replicas of that type should be stored.
            **storage_options: Additional options passed to the superclass.
        """
        super().__init__(**storage_options)
        if not filesystems:
            raise ValueError("EsperojFileSystem requires at least one configured filesystem backend.")
        if default_storage not in filesystems:
            raise ValueError(f"Default storage '{default_storage}' is not in the configured filesystems.")

        self.filesystems = filesystems
        self.default_storage = default_storage
        self.primary_storages = primary_storages
        self.backup_storages = backup_storages
        self.archive_storages = archive_storages
        self.replica_type_backend_mapping = replica_type_backend_mapping

        logger.info(
            "EsperojFileSystem initialized. Backends: %s, Default: %s, Primary for Reads: %s, Backup for Reads: %s, Archive for Reads: %s, Replica Type Mappings: %s",
            list(self.filesystems.keys()),
            self.default_storage,
            self.primary_storages,
            self.backup_storages,
            self.archive_storages,
            self.replica_type_backend_mapping,
        )

    def ls(self, path: str, detail: bool = True) -> list[dict] | list[str]:
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

    def _get_read_candidate_backends(self, storage_name: str | None, replica_types: list[str] | None) -> list[str]:
        """
        Determines the ordered list of backend names to attempt for reading.

        Args:
            storage_name: An optional specific storage backend to prioritize.
            replica_types: An optional list of replica types to prioritize.

        Returns:
            An ordered list of backend names to try for reading.
        """
        candidate_backends_order: list[str] = []

        # 1. Prioritize specific storage_name if provided
        if storage_name and storage_name in self.filesystems:
            candidate_backends_order.append(storage_name)

        # 2. Prioritize backends for specific replica_types if requested
        if replica_types:
            ordered_replica_types = [rt.value for rt in ReplicaType if rt.value in replica_types]
            for r_type in ordered_replica_types:
                backends_for_type = self.replica_type_backend_mapping.get(r_type)
                if backends_for_type:
                    candidate_backends_order.extend(backends_for_type)
        else:
            # 3. Fallback to the traditional primary -> backup -> archive hierarchy for reads
            candidate_backends_order.extend(self.primary_storages)
            candidate_backends_order.extend(self.backup_storages)
            candidate_backends_order.extend(self.archive_storages)

        # Remove duplicates while preserving order
        final_candidate_backends = []
        seen = set()
        for backend in candidate_backends_order:
            if backend not in seen:
                final_candidate_backends.append(backend)
                seen.add(backend)

        # 4. Final fallback if no candidates found at all
        if not final_candidate_backends and self.filesystems:
            # Try all available backends as a last resort, ordered by default storage first.
            all_other_backends = [b for b in self.filesystems if b != self.default_storage]
            final_candidate_backends = [self.default_storage] + sorted(all_other_backends)
            logger.warning(
                "No specific replica types or storage names provided, and no primary/backup/archive config found. "
                "Falling back to all configured backends: %s",
                final_candidate_backends,
            )
        return final_candidate_backends

    def _open(
        self,
        path: str,
        mode: str = "rb",
        storage_name: str | None = None,
        replica_types: list[str] | None = None,
        encoding: str | None = None,
    ):
        path = cast(str, self._strip_protocol(path))

        if "b" not in mode:
            binary_stream = self._open(
                path,
                mode.replace("t", "b"),
                storage_name=storage_name,
                replica_types=replica_types,
                encoding=encoding,
            )
            return io.TextIOWrapper(
                BufferedReader(binary_stream),
                encoding=encoding or "utf-8",
            )

        if mode == "rb":
            final_candidate_backends = self._get_read_candidate_backends(storage_name, replica_types)

            for backend_name in final_candidate_backends:
                if backend_name not in self.filesystems:
                    logger.warning(
                        "Configured storage backend '%s' not found in active filesystems. Skipping.", backend_name
                    )
                    continue

                try:
                    # Fetch replica that matches the current backend_name
                    # If specific replica_types were requested for read, filter by them.
                    replica_query = FileReplica.objects.select_related("file").filter(
                        file__path=path, storage_name=backend_name, is_active=True
                    )
                    if replica_types:
                        replica_query = replica_query.filter(replica_type__in=replica_types)

                    replica = replica_query.first()

                    if replica:
                        backend_fs = self.filesystems.get(backend_name)
                        if not backend_fs:  # Should have been caught earlier, but for safety
                            logger.error(
                                "Storage backend '%s' for replica of '%s' is not configured. Skipping.",
                                backend_name,
                                path,
                            )
                            continue

                        logger.debug(
                            "Attempting to read file %s from backend '%s' (type: %s) at %s.",
                            path,
                            backend_name,
                            replica.replica_type,
                            replica.storage_path,
                        )
                        return backend_fs.open(replica.storage_path, "rb")
                    else:
                        logger.debug(
                            "No active replica found for file %s in backend %s (matching replica types: %s). Trying next.",
                            path,
                            backend_name,
                            replica_types,
                        )
                except Exception as e:
                    logger.warning(
                        "Failed to open file %s from backend '%s': %s. Trying next backend.", path, backend_name, e
                    )
            raise FileNotFoundError(
                f"No active replica found for file: {path} across configured backends and replica types: {replica_types}."
            )

        elif mode == "wb":
            # Pass replica_types to EsperojFile for write operations
            return EsperojFile(self, path, mode=mode, replica_types=replica_types)

        else:
            raise NotImplementedError(f"Mode '{mode}' is not supported.")

    def info(self, path: str) -> dict:
        path = cast(str, self._strip_protocol(path))
        try:
            file_instance = File.objects.get(path=path)
            return {"name": path, "size": file_instance.size, "type": "file"}
        except File.DoesNotExist:
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

    def exists(self, path: str) -> bool:
        path = cast(str, self._strip_protocol(path))
        return self.isfile(path) or self.isdir(path)

    def mkdir(self, path: str) -> None:
        # Directories are virtual and exist implicitly if they contain files.
        # This method is a no-op but is required for fsspec compliance.
        pass

    def rmdir(self, path: str) -> None:
        path = cast(str, self._strip_protocol(path))
        if not path.endswith("/"):
            path += "/"
        if self.exists(path) and self.ls(path):
            raise OSError(f"Directory not empty: {path}")

    def rm(self, path: str, recursive: bool = False) -> None:
        path = cast(str, self._strip_protocol(path))

        if self.isfile(path):
            self._rm_file(path)
        elif self.isdir(path):
            if not recursive:
                raise OSError(f"Cannot remove directory '{path}' without recursive=True.")

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

                for replica in replicas:
                    backend_fs = self.filesystems.get(replica.storage_name)
                    if backend_fs:
                        try:
                            # Use replica.storage_path for deletion
                            if backend_fs.exists(replica.storage_path):
                                backend_fs.rm(replica.storage_path)
                                logger.info(
                                    "Removed replica %s from backend '%s' (type: %s)",
                                    replica.storage_path,
                                    replica.storage_name,
                                    replica.replica_type,
                                )
                        except Exception as e:
                            logger.error(
                                "Failed to remove replica %s from backend '%s': %s",
                                replica.storage_path,
                                replica.storage_name,
                                e,
                                exc_info=True,
                            )
                            # Log and continue to remove DB record.
                    else:
                        logger.warning(
                            "Backend '%s' for replica %s not configured. Cannot delete from storage.",
                            replica.storage_name,
                            replica.storage_path,
                        )

                file_instance.delete()
                logger.info("Removed database record for file: %s", path)

        except File.DoesNotExist:
            raise FileNotFoundError(f"File not found: {path}")
        except Exception as e:
            logger.error("Failed during removal of file %s: %s", path, e, exc_info=True)
            raise IOError(f"Failed to remove file {path}: {e}") from e

    def mv(self, path1: str, path2: str) -> None:
        path1 = cast(str, self._strip_protocol(path1))
        path2 = cast(str, self._strip_protocol(path2))

        try:
            with transaction.atomic():
                file_instance = File.objects.select_for_update().get(path=path1)
                replicas = list(file_instance.replicas.all())

                for replica in replicas:
                    backend_fs = self.filesystems.get(replica.storage_name)
                    if not backend_fs:
                        raise IOError(f"Backend '{replica.storage_name}' not configured, cannot move file.")

                    new_storage_path = replica.storage_path
                    # Only modify storage_path for backends that use logical paths
                    # Catbox (and similar URL-based) backends will have `mv` as a no-op at the backend level
                    # so their storage_path (URL) does not change, only the logical path in Esperoj's metadata.
                    if replica.storage_name not in [
                        StorageName.CATBOX.value,
                        StorageName.INTERNET_ARCHIVE.value,
                    ]:  # Explicitly list URL-based backends here
                        new_storage_path = replica.storage_path.replace(path1, path2, 1)
                        try:
                            backend_fs.mv(replica.storage_path, new_storage_path)
                        except NotImplementedError:
                            logger.warning(
                                "Backend '%s' does not support 'mv' operation. Replica storage path will be updated "
                                "in metadata only, but actual file may remain at old path on backend.",
                                replica.storage_name,
                            )
                        except Exception as e:
                            logger.error(
                                "Failed to move replica on backend '%s' from %s to %s: %s",
                                replica.storage_name,
                                replica.storage_path,
                                new_storage_path,
                                e,
                            )
                            raise IOError(f"Backend move failed for {replica.storage_name}: {e}") from e
                    else:
                        logger.debug(
                            "Skipping backend 'mv' for '%s' as it's a URL-based storage. Only metadata will be updated.",
                            replica.storage_name,
                        )

                    replica.storage_path = new_storage_path
                    replica.save()

                file_instance.path = path2
                file_instance.name = path2.split("/")[-1]
                file_instance.save()
                logger.info("Renamed file from %s to %s", path1, path2)

        except File.DoesNotExist:
            raise FileNotFoundError(path1)
        except Exception as e:
            logger.error("Failed to move %s to %s: %s", path1, path2, e, exc_info=True)
            raise IOError(f"Move operation failed: {e}") from e

    def cp(self, path1: str, path2: str) -> None:
        path1 = cast(str, self._strip_protocol(path1))
        path2 = cast(str, self._strip_protocol(path2))

        try:
            with transaction.atomic():
                original_file = File.objects.get(path=path1)
                original_replicas = list(original_file.replicas.all())

                if not original_replicas:
                    raise FileNotFoundError(f"No replicas found for source file {path1} to copy.")

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

                for replica in original_replicas:
                    backend_fs = self.filesystems.get(replica.storage_name)
                    if not backend_fs:
                        logger.warning(
                            "Backend '%s' not configured. Skipping copy for this replica.", replica.storage_name
                        )
                        continue

                    new_storage_path_for_backend = replica.storage_path
                    if replica.storage_name not in [
                        StorageName.CATBOX.value,
                        StorageName.INTERNET_ARCHIVE.value,
                    ]:  # Explicitly list URL-based backends here
                        new_storage_path_for_backend = replica.storage_path.replace(path1, path2, 1)
                        try:
                            backend_fs.cp(replica.storage_path, new_storage_path_for_backend)
                        except NotImplementedError:
                            logger.warning(
                                "Backend '%s' does not support 'cp' operation. A new replica will be created "
                                "in metadata, but no actual file copy on backend.",
                                replica.storage_name,
                            )
                        except Exception as e:
                            logger.error(
                                "Failed to copy replica on backend '%s' from %s to %s: %s",
                                replica.storage_name,
                                replica.storage_path,
                                new_storage_path_for_backend,
                                e,
                            )
                            raise IOError(f"Backend copy failed for {replica.storage_name}: {e}") from e
                    else:
                        logger.warning(
                            "Copy operation for URL-based backends (like Catbox or Internet Archive) means "
                            "referencing the same URL. No new file will be uploaded to these backends. "
                            "A new replica record will point to the original URL."
                        )
                        # For URL-based backends, the backend_fs.cp (if it existed) would be a no-op,
                        # so we simply point the new replica to the existing URL.

                    FileReplica.objects.update_or_create(
                        file=new_file,
                        storage_name=replica.storage_name,
                        defaults={
                            "replica_type": replica.replica_type,
                            "storage_path": new_storage_path_for_backend,
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
