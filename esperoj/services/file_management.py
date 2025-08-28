"""
High-level services for file management in the esperoj application.

This module provides service-layer functions that orchestrate complex operations
involving file storage, database models, and utility functions. These services
act as a bridge between the command-line interface (or other entry points) and
the underlying components like fsspec backends and the Django ORM.
"""

import logging
from pathlib import Path
from typing import BinaryIO, cast

import fsspec
from django.db import transaction

from esperoj.models import File, FileReplica
from esperoj.storage.fsspec_backends.catbox import CatboxFileSystem  # noqa: F401, pylint: disable=unused-import

logger = logging.getLogger(__name__)


def upload_local_file(local_file_path: Path, remote_fs_path: str, filesystem: str = "catbox") -> File | None:
    """
    Uploads a local file to a specified fsspec-compatible file system.

    This function handles reading a local file and writing it to a remote
    file system using fsspec. The underlying file system implementation
    (e.g., CatboxFileSystem) is responsible for the actual upload and
    creation of database records.

    Args:
        local_file_path: The path to the local file to upload.
        remote_fs_path: The destination path on the remote file system
                        (e.g., 'my-collection/my-file.txt').
        filesystem: The protocol of the target fsspec file system (e.g., 'catbox').

    Returns:
        The created or updated `File` instance if the upload was successful,
        otherwise None.
    """
    if not local_file_path.is_file():
        logger.error("Local file not found: %s", local_file_path)
        raise FileNotFoundError(f"The specified local file does not exist: {local_file_path}")

    full_remote_path = f"{filesystem}://{remote_fs_path}"
    logger.info("Starting upload of '%s' to '%s'", local_file_path, full_remote_path)

    try:
        # Use a transaction to ensure that the file upload and database record
        # creation are atomic. The CatboxFile's close() method will handle
        # the database logic.
        with transaction.atomic():
            # fsspec.open uses the protocol ('catbox://') to find the correct filesystem
            with cast(BinaryIO, fsspec.open(full_remote_path, "wb")) as remote_file:
                with local_file_path.open("rb") as local_file:
                    remote_file.write(local_file.read())

        # After the 'with' blocks, the remote file is closed, triggering the upload
        # and database record creation in CatboxFile.close()
        logger.info("Successfully uploaded '%s'", full_remote_path)

        # Retrieve the newly created File instance
        file_instance = File.objects.get(path=remote_fs_path)
        return file_instance

    except Exception as e:
        logger.error(
            "An error occurred during the upload of '%s' to '%s': %s",
            local_file_path,
            full_remote_path,
            e,
            exc_info=True,
        )
        return None


def verify_replica_integrity(replica: FileReplica) -> bool:
    """
    Verifies the integrity of a file replica against its master File record.

    This function streams the content from the replica's storage URL,
    re-calculates its checksums, and compares them to the checksums stored
    in the parent `File` model. It then updates the replica's verification
    status accordingly.

    Args:
        replica: The `FileReplica` instance to verify.

    Returns:
        True if the verification was successful, False otherwise.
    """
    from esperoj.utils.checksums import calculate_checksums
    import tempfile
    import requests

    logger.info("Verifying integrity of replica ID %s for file '%s'", replica.id, replica.file.path)

    if not replica.storage_path:
        logger.error("Replica %s has no storage path to verify.", replica.id)
        replica.mark_verified(status="error")
        return False

    try:
        # Stream the file content to a temporary file to avoid loading large files into memory
        with tempfile.NamedTemporaryFile(delete=False) as temp_file:
            with requests.get(replica.storage_path, stream=True, timeout=300) as r:
                r.raise_for_status()
                for chunk in r.iter_content(chunk_size=8192):
                    temp_file.write(chunk)
            temp_file_path = Path(temp_file.name)

        # Calculate checksums of the downloaded file
        try:
            calculated_checksums = calculate_checksums(temp_file_path)
        finally:
            temp_file_path.unlink()  # Clean up the temporary file

        # Compare checksums
        master_file = replica.file
        is_valid = True
        if master_file.sha256 and master_file.sha256 != calculated_checksums.get("sha256"):
            logger.warning(
                "SHA256 mismatch for replica %s. Expected: %s, Got: %s",
                replica.id,
                master_file.sha256,
                calculated_checksums.get("sha256"),
            )
            is_valid = False

        if master_file.md5 and master_file.md5 != calculated_checksums.get("md5"):
            logger.warning(
                "MD5 mismatch for replica %s. Expected: %s, Got: %s",
                replica.id,
                master_file.md5,
                calculated_checksums.get("md5"),
            )
            is_valid = False

        # Update replica status
        if is_valid:
            replica.mark_verified(status="success")
            logger.info("Successfully verified replica %s.", replica.id)
            return True
        else:
            replica.mark_verified(status="failed")
            logger.error("Verification failed for replica %s.", replica.id)
            return False

    except requests.RequestException as e:
        logger.error("Failed to download replica %s for verification: %s", replica.id, e)
        replica.mark_verified(status="error")
        return False
    except Exception as e:
        logger.error("An unexpected error occurred during verification of replica %s: %s", replica.id, e, exc_info=True)
        replica.mark_verified(status="error")
        return False
