"""
Centralized constants for the esperoj application.

This module defines various choices and configurations that are used across
different parts of the application, such as models, admin, and storage backends.
"""

from django.db import models


class ReplicaType(models.TextChoices):
    """
    Defines the types of replicas a File can have across different storage systems.
    """

    ORIGINAL = "original", "Original Copy"
    ACCESS = "access_copy", "Access Copy"
    PRESERVATION = "preservation", "Preservation Copy"
    BACKUP = "backup", "Backup Copy"
    THUMBNAIL = "thumbnail", "Thumbnail"
    PREVIEW = "preview", "Preview"


class StorageName(models.TextChoices):
    """
    Defines the names of available storage backends.

    These names should correspond to the keys in the `EsperojFileSystem`
    `filesystems` dictionary.
    """

    LOCAL_DEFAULT = "local_default", "Local Default Storage"
    CATBOX = "catbox", "Catbox.moe"
    # Add other storage names as they are implemented, e.g.:
    # S3_PRIMARY = "s3_primary", "AWS S3 Primary"
    # GCS_ARCHIVE = "gcs_archive", "Google Cloud Storage Archive"


# Define REPLICA_TYPES and STORAGE_CHOICES for Django settings compatibility,
# although it's better to directly reference ReplicaType.choices and StorageName.choices
# where possible.
REPLICA_TYPES = ReplicaType.choices
STORAGE_CHOICES = StorageName.choices
