import logging
from uuid import UUID

import fsspec  # Import fsspec for storage access
from typing import cast, BinaryIO  # Import cast and BinaryIO for type hinting

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

            checksums_to_verify = []
            if file_obj.sha256:
                checksums_to_verify.append(("sha256", file_obj.sha256))
            if file_obj.sha1:
                checksums_to_verify.append(("sha1", file_obj.sha1))
            if file_obj.md5:
                checksums_to_verify.append(("md5", file_obj.md5))

            if not checksums_to_verify:
                logger.warning(
                    "File %s (replica %s) has no checksums defined. Cannot verify integrity.",
                    file_obj.id,
                    replica_id,
                )
                replica.mark_verified(status="error")
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

            # 3. Apply business logic (checksum validation)
            is_overall_valid = True
            failed_checksums = []

            # Open the fsspec stream once and pass it directly to calculate_checksum
            # fsspec's file objects are generally seekable when opened in 'rb' mode,
            # allowing multiple passes for checksum calculation without re-downloading.
            with fsspec.open(fsspec_uri, "rb") as raw_data_stream:
                data_stream: BinaryIO = cast(BinaryIO, raw_data_stream)
                for algorithm, expected_checksum in checksums_to_verify:
                    # Ensure the stream is at the beginning for each checksum calculation.
                    # fsspec file objects are generally seekable.
                    data_stream.seek(0)
                    calculated_checksum = calculate_checksum(data_stream, algorithm)

                    if calculated_checksum != expected_checksum:
                        is_overall_valid = False
                        failed_checksums.append(algorithm)
                        logger.warning(
                            "FileReplica %s: Checksum mismatch for %s. Expected '%s', Got '%s'.",
                            replica_id,
                            algorithm,
                            expected_checksum,
                            calculated_checksum,
                        )
                    else:
                        logger.info(
                            "FileReplica %s: Checksum %s verified successfully.",
                            replica_id,
                            algorithm,
                        )

            # 4. Persist results back to models
            if is_overall_valid:
                replica.mark_verified(status="success")
                logger.info("FileReplica %s integrity verified successfully for all present checksums.", replica_id)
            else:
                replica.mark_inactive()
                logger.warning(
                    "FileReplica %s integrity verification failed. Mismatched checksums: %s.",
                    replica_id,
                    ", ".join(failed_checksums),
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
