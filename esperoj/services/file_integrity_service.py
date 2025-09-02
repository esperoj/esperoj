import logging
import os
import tempfile
import time
from concurrent.futures import ProcessPoolExecutor, ThreadPoolExecutor, as_completed
from shutil import copyfileobj
from typing import BinaryIO, Union, cast
from uuid import UUID

from django.db import transaction
from django.db.models import F

from esperoj.constants import StorageName
from esperoj.models.files import File, FileReplica

logger = logging.getLogger(__name__)


# --- Top-level Functions for Process/Thread Pools ---
# These must be at the top level of the module to be "pickleable" by multiprocessing.


def _download_to_temp_file(fsspec_uri: str, temp_path: str) -> None:
    """
    I/O-bound task: Downloads content from a URI directly to a temporary file path.

    Args:
        fsspec_uri: The URI of the file to download.
        temp_path: The local file system path to write the downloaded content to.
    """
    # Imports are within the function to ensure they are available in the worker context.
    import fsspec

    with fsspec.open(fsspec_uri, "rb") as source, open(temp_path, "wb") as dest:
        # Cast to satisfy the type checker, as fsspec's return type is broad.
        copyfileobj(cast(BinaryIO, source), dest)


def _calculate_checksums_from_file(file_path: str, checksums_to_verify: list[tuple[str, str]]) -> list[str]:
    """
    CPU-bound task: Calculates checksums from a file on disk.

    Args:
        file_path: The path to the local file to be checksummed.
        checksums_to_verify: A list of tuples, each containing an algorithm name
                             and its expected hex digest.

    Returns:
        A list of algorithm names for which the checksum calculation failed.
    """
    # Import is within the function to ensure it's available in the new process.
    from esperoj.utils.checksums import calculate_checksum

    failed_algorithms = []
    for algorithm, expected_checksum in checksums_to_verify:
        with open(file_path, "rb") as f:
            calculated_checksum = calculate_checksum(f, algorithm)
            if calculated_checksum != expected_checksum:
                failed_algorithms.append(algorithm)
    return failed_algorithms


# --- Service Class ---


class FileIntegrityService:
    """
    Manages file replica integrity verification using shared resource pools.

    This class encapsulates ThreadPoolExecutor and ProcessPoolExecutor to perform
    concurrent verification tasks efficiently. It uses temporary files to avoid
    passing large amounts of data between processes, making it suitable for large files.

    It is designed to be used as a context manager to ensure graceful shutdown of its pools.
    Example:
        with FileIntegrityService() as service:
            results = service.run_routine_verification()
    """

    def __init__(self, max_io_workers: int | None = None, max_cpu_workers: int | None = None):
        """
        Initializes the service with shared thread and process pools.

        Args:
            max_io_workers: Max number of threads for I/O tasks. Defaults to CPU count * 2.
            max_cpu_workers: Max number of processes for CPU tasks. Defaults to CPU count.
        """
        io_workers = max_io_workers or (os.cpu_count() or 1) * 2
        cpu_workers = max_cpu_workers or os.cpu_count() or 1
        self._io_pool = ThreadPoolExecutor(max_workers=io_workers)
        self._cpu_pool = ProcessPoolExecutor(max_workers=cpu_workers)
        self._temp_dir = tempfile.TemporaryDirectory()
        logger.info("FileIntegrityService initialized with %d I/O workers and %d CPU workers.", io_workers, cpu_workers)

    def shutdown(self, wait: bool = True):
        """Gracefully shuts down the thread and process pools and cleans up temp files."""
        try:
            self._io_pool.shutdown(wait=wait)
            self._cpu_pool.shutdown(wait=wait)
            self._temp_dir.cleanup()
            logger.info("FileIntegrityService shut down successfully.")
        except Exception as e:
            logger.error("Error during FileIntegrityService shutdown: %s", e)

    def _get_fsspec_uri(self, replica: FileReplica) -> str:
        """Constructs the fsspec URI for a given replica."""
        uri_formatters = {
            StorageName.CATBOX.value: "catbox://{path}",
            StorageName.INTERNET_ARCHIVE.value: "internetarchive://{path}",
            StorageName.WAYBACK_MACHINE.value: "https://web.archive.org{path}",
        }
        formatter = uri_formatters.get(replica.storage_name)
        if not formatter:
            raise ValueError(f"Unsupported storage_name '{replica.storage_name}' for replica {replica.id}")
        return formatter.format(path=replica.storage_path)

    def _get_checksums_to_verify(self, file_obj: File) -> list[tuple[str, str]]:
        """Gathers a list of checksums to verify from a file object."""
        fields = ["sha256", "sha1", "md5"]
        return [(alg, getattr(file_obj, alg)) for alg in fields if getattr(file_obj, alg)]

    def verify_replica_integrity(self, replica_or_id: Union[FileReplica, UUID]) -> str:
        """
        Verifies a single file replica using the service's shared resource pools.

        This method orchestrates downloading to a temporary file, calculating
        checksums in a separate process, and updating the database record.

        Args:
            replica_or_id: The FileReplica object or its UUID.

        Returns:
            A status string: 'verified', 'failed', or 'error'.
        """
        replica_id = replica_or_id.id if isinstance(replica_or_id, FileReplica) else replica_or_id
        temp_file_path = None
        try:
            if isinstance(replica_or_id, UUID):
                replica = FileReplica.objects.select_related("file").get(id=replica_id)
            else:
                replica = replica_or_id

            if not replica.file:
                raise ValueError("Replica is not linked to a File object.")

            checksums_to_verify = self._get_checksums_to_verify(replica.file)
            if not checksums_to_verify:
                logger.warning("File %s has no checksums. Marking replica %s as error.", replica.file.id, replica.id)
                replica.mark_verified(status="error")
                return "error"

            fsspec_uri = self._get_fsspec_uri(replica)

            # Create a temporary file path for the download
            fd, temp_file_path = tempfile.mkstemp(dir=self._temp_dir.name)
            os.close(fd)

            # 1. Download in I/O pool and wait for completion.
            download_future = self._io_pool.submit(_download_to_temp_file, fsspec_uri, temp_file_path)
            download_future.result()  # Wait for download to finish

            # 2. Calculate checksum in CPU pool and wait for completion.
            checksum_future = self._cpu_pool.submit(_calculate_checksums_from_file, temp_file_path, checksums_to_verify)
            failed_checksums = checksum_future.result()

            # 3. Update database in the main process.
            with transaction.atomic():
                replica_to_update = FileReplica.objects.get(id=replica.id)
                if not failed_checksums:
                    replica_to_update.mark_verified(status="success")
                    return "verified"
                else:
                    replica_to_update.mark_inactive()
                    logger.warning(
                        "Replica %s failed verification. Mismatched: %s", replica.id, ", ".join(failed_checksums)
                    )
                    return "failed"
        except Exception as e:
            logger.error("Error verifying replica %s: %s", replica_id, e, exc_info=True)
            FileReplica.objects.filter(id=replica_id).update(is_active=False, verification_status="error")
            return "error"
        finally:
            # 4. Clean up the temporary file
            if temp_file_path and os.path.exists(temp_file_path):
                os.remove(temp_file_path)
        # Fallback return to ensure all code paths return a string
        return "error"

    def run_routine_verification(
        self, max_files: int = 1000, max_size_gb: float = 100.0, max_duration_minutes: int = 60
    ) -> dict[str, int | float]:
        """
        Runs a routine check on multiple replicas using the service's shared pools.
        """
        start_time = time.monotonic()
        deadline = start_time + (max_duration_minutes * 60)
        summary: dict[str, int | float] = {"selected": 0, "verified": 0, "failed": 0, "errors": 0}

        candidates = self._select_verification_candidates(max_files, int(max_size_gb * 1024**3), deadline)
        summary["selected"] = len(candidates)
        if not candidates:
            return summary

        logger.info("Submitting %d replicas for concurrent verification.", len(candidates))

        futures = {self._io_pool.submit(self.verify_replica_integrity, r): r.id for r in candidates}

        for future in as_completed(futures):
            replica_id = futures[future]
            try:
                status = future.result()
                summary[status] = summary.get(status, 0) + 1
            except Exception as e:
                logger.error("Verification task for replica %s raised an exception: %s", replica_id, e)
                summary["errors"] = summary.get("errors", 0) + 1

        summary["duration_seconds"] = time.monotonic() - start_time
        logger.info("Routine verification finished. Summary: %s", summary)
        return summary

    def _select_verification_candidates(
        self, max_files: int, max_size_bytes: int, deadline: float
    ) -> list[FileReplica]:
        """Queries the database to select replicas for verification based on priority and constraints."""
        selected_replicas: list[FileReplica] = []
        total_size = 0
        checked_files: set[UUID] = set()

        # Prioritize never-verified replicas, then the oldest verified
        qs = (
            FileReplica.objects.select_related("file")
            .filter(is_active=True)
            .order_by(F("last_verified").asc(nulls_first=True))
        )

        for replica in qs.iterator():
            if len(selected_replicas) >= max_files or time.monotonic() > deadline:
                break
            if replica.file.id in checked_files:
                continue
            if (replica.file.size + total_size > max_size_bytes) and selected_replicas:
                continue  # Skip if it exceeds size limit, unless it's the very first file

            selected_replicas.append(replica)
            total_size += replica.file.size
            checked_files.add(replica.file.id)

        return selected_replicas

    def __enter__(self):
        return self

    def __exit__(self, exc_type, exc_val, exc_tb):
        self.shutdown()
