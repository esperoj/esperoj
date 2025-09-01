import logging
from uuid import UUID
import io  # Import io for BytesIO
import fsspec  # Import fsspec for storage access
from typing import cast  # Import cast for type hinting

from django.db import transaction

from esperoj.models.files import FileReplica
from esperoj.utils.checksums import calculate_checksum  # Import calculate_checksum from utils
from esperoj.constants import StorageName

logger = logging.getLogger(__name__)


def verify_replica_integrity(replica_id: UUID) -> None:
    """
    Verifies the integrity of a specific FileReplica by comparing its content
    with the stored checksum.

    Args:
        replica_id: The UUID of the FileReplica to verify.
    """
    with transaction.atomic():
        try:
            # 1. Fetch data from models
            replica = FileReplica.objects.select_related("file").get(id=replica_id)
            file_obj = replica.file

            if not file_obj:
                logger.error("FileReplica %s is not linked to a File.", replica_id)
                replica.mark_inactive()
                return

            # Determine which checksum to use based on availability and preference
            expected_checksum = None
            algorithm = None
            if file_obj.sha256:
                expected_checksum = file_obj.sha256
                algorithm = "sha256"
            elif file_obj.sha1:
                expected_checksum = file_obj.sha1
                algorithm = "sha1"
            elif file_obj.md5:
                expected_checksum = file_obj.md5
                algorithm = "md5"

            if not expected_checksum:
                logger.warning(
                    "File %s (replica %s) has no checksums defined. Cannot verify integrity.",
                    file_obj.id,
                    replica_id,
                )
                replica.mark_verified(status="error")  # Mark as error since it can't be verified
                return

            # 2. Interact with infrastructure (storage)
            # fsspec will dispatch to the correct backend based on the URI scheme

            # Construct the fsspec URI based on storage_name and storage_path
            fsspec_uri: str
            if replica.storage_name == StorageName.CATBOX:
                # For Catbox, the storage_path is the direct URL, which fsspec expects for 'catbox://'
                fsspec_uri = f"catbox://{replica.storage_path}"
            elif replica.storage_name == StorageName.INTERNET_ARCHIVE:
                # For Internet Archive, storage_path is 'item_identifier/path_within_item'
                fsspec_uri = f"internetarchive://{replica.storage_path}"
            elif replica.storage_name == StorageName.WAYBACK_MACHINE:
                # For Wayback Machine, the storage_path is the URL to the archived page
                # The WaybackMachine fsspec backend expects the full URL in `path`, so we prepend the base.
                fsspec_uri = f"https://web.archive.org{replica.storage_path}"
            # Add other storage types here
            else:
                logger.error("Unsupported storage_name '%s' for FileReplica %s.", replica.storage_name, replica_id)
                replica.mark_inactive()
                return

            # Read file content using fsspec.open, which dispatches to the correct backend
            file_content = b""
            with fsspec.open(fsspec_uri, "rb") as f:
                # Cast f to a RawIOBase to help type checkers understand its methods
                file_content = cast(io.RawIOBase, f).read()

            # 3. Apply business logic (checksum validation)
            # Use BytesIO to make the bytes data appear as a file-like object for calculate_checksum
            with io.BytesIO(file_content) as data_stream:
                assert algorithm is not None, "Checksum algorithm must be defined for verification."
                # The algorithm is guaranteed to be a string here due to the preceding logic and assertion.
                calculated_checksum = calculate_checksum(data_stream, algorithm)

            is_valid = calculated_checksum == expected_checksum

            # 4. Persist results back to models
            if is_valid:
                replica.mark_verified(status="success")
                logger.info("FileReplica %s integrity verified successfully.", replica_id)
            else:
                replica.mark_inactive()
                logger.warning(
                    "FileReplica %s integrity verification failed. Checksum mismatch for %s.",
                    replica_id,
                    algorithm,
                )

        except FileReplica.DoesNotExist:
            logger.error("FileReplica with ID %s not found.", replica_id)
            raise
        except IOError as e:
            logger.error("Error accessing storage for FileReplica %s from %s: %s", replica_id, fsspec_uri, e)
            replica.mark_inactive()
        except Exception as e:
            logger.exception("An unexpected error occurred during verification of FileReplica %s.", replica_id)
            replica.mark_inactive()
