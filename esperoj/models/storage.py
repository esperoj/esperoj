from django.db import models
from django.db.models import Q, Index, UniqueConstraint

from .base import BaseModel
from .files import File


class BaseStorage(BaseModel):
    """
    An abstract base model for file storage locations.
    """

    file = models.ForeignKey(File, on_delete=models.CASCADE, related_name="storages")
    is_primary = models.BooleanField(
        default=False, help_text="Is this the primary, canonical location for this file?"
    )

    class Meta:
        abstract = True
        ordering = ["-is_primary", "-updated_at"]
        app_label = "esperoj"
        constraints = [
            UniqueConstraint(
                fields=["file"],
                condition=Q(is_primary=True),
                name="unique_primary_storage_per_file",
            )
        ]

    def __str__(self):
        return f"{self.file.path} @ {self.__class__.__name__}"


class LocalStorage(BaseStorage):
    """Represents a file stored on a local filesystem."""

    path = models.CharField(
        max_length=2048, help_text="The full path to the file on the local filesystem."
    )

    class Meta(BaseStorage.Meta):
        verbose_name = "Local Storage"
        verbose_name_plural = "Local Storages"
        db_table = "storage_local"
        constraints = [
            UniqueConstraint(fields=["file", "path"], name="unique_local_file_path")
        ]


class S3Storage(BaseStorage):
    """Represents a file stored in an AWS S3 bucket or compatible service."""

    bucket = models.CharField(max_length=255, help_text="The S3 bucket name.")
    key = models.CharField(max_length=2048, help_text="The key of the file in the bucket.")
    region = models.CharField(
        max_length=50, blank=True, null=True, help_text="The AWS region for the bucket."
    )
    endpoint_url = models.URLField(
        max_length=1024,
        blank=True,
        null=True,
        help_text="The endpoint URL for S3-compatible services.",
    )

    class Meta(BaseStorage.Meta):
        verbose_name = "S3 Storage"
        verbose_name_plural = "S3 Storages"
        db_table = "storage_s3"
        constraints = [
            UniqueConstraint(fields=["bucket", "key"], name="unique_s3_object")
        ]


class GCSStorage(BaseStorage):
    """Represents a file stored in Google Cloud Storage."""

    bucket = models.CharField(max_length=255, help_text="The GCS bucket name.")
    blob_name = models.CharField(
        max_length=2048, help_text="The name of the blob (file) in the bucket."
    )

    class Meta(BaseStorage.Meta):
        verbose_name = "GCS Storage"
        verbose_name_plural = "GCS Storages"
        db_table = "storage_gcs"
        constraints = [
            UniqueConstraint(fields=["bucket", "blob_name"], name="unique_gcs_object")
        ]


class AzureStorage(BaseStorage):
    """Represents a file stored in Azure Blob Storage."""

    container = models.CharField(max_length=255, help_text="The Azure container name.")
    blob_name = models.CharField(
        max_length=2048, help_text="The name of the blob in the container."
    )
    account_name = models.CharField(
        max_length=255, help_text="The Azure Storage account name."
    )

    class Meta(BaseStorage.Meta):
        verbose_name = "Azure Storage"
        verbose_name_plural = "Azure Storages"
        db_table = "storage_azure"
        constraints = [
            UniqueConstraint(
                fields=["account_name", "container", "blob_name"],
                name="unique_azure_blob",
            )
        ]


class OtherStorage(BaseStorage):
    """Represents a file stored in a different type of storage."""

    location_details = models.JSONField(
        default=dict,
        blank=True,
        help_text="A JSON object with details about the storage location.",
    )

    class Meta(BaseStorage.Meta):
        verbose_name = "Other Storage"
        verbose_name_plural = "Other Storages"
        db_table = "storage_other"
