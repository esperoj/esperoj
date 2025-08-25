from typing import TYPE_CHECKING

from django.core.validators import MinValueValidator
from django.db import models
from django.db.models import Q, UniqueConstraint
from django.conf import settings
from .base import BaseModel

if TYPE_CHECKING:
    from .items import Item


class File(BaseModel):
    """
    Represents a digital file's intrinsic metadata and a user-defined name.
    """
    name = models.CharField(
        max_length=255,
        db_index=True,
        help_text="User-defined name for the file.",
    )
    size = models.PositiveBigIntegerField(
        validators=[MinValueValidator(0)],
        help_text="File size in bytes.",
    )
    mime_type = models.CharField(max_length=255, blank=True, null=True, default=None)

    # --- Checksums ---
    md5 = models.CharField(
        max_length=32, blank=True, null=True, default=None, db_index=True,
        help_text="MD5 hash of the file.",
    )
    sha1 = models.CharField(
        max_length=40, blank=True, null=True, default=None, db_index=True,
        help_text="SHA1 hash of the file.",
    )
    sha256 = models.CharField(
        max_length=64, blank=True, null=True, default=None, db_index=True,
        help_text="SHA256 hash of the file.",
    )

    # --- Type hints for reverse relationships ---
    replicas: models.Manager["FileReplica"]
    items: models.Manager["Item"]

    class Meta:
        ordering = ["name"]
        verbose_name = "File"
        verbose_name_plural = "Files"
        db_table = "file"
        constraints = [
            UniqueConstraint(fields=["md5"], condition=Q(md5__isnull=False), name="unique_md5_if_not_null"),
            UniqueConstraint(fields=["sha1"], condition=Q(sha1__isnull=False), name="unique_sha1_if_not_null"),
            UniqueConstraint(fields=["sha256"], condition=Q(sha256__isnull=False), name="unique_sha256_if_not_null"),
        ]

    def __str__(self) -> str:
        """Returns the user-defined name as the string representation."""
        return self.name

class FileReplica(BaseModel):
    """A specific, complete copy of the LogicalFile."""
    file = models.ForeignKey(File, on_delete=models.CASCADE, related_name='replicas')
    replica_type = models.CharField(max_length=50, choices=settings.REPLICA_TYPES)
    storage_name = models.CharField(max_length=50, choices=settings.STORAGE_CHOICES)

    class Meta:
        db_table = "file_replica"

    def __str__(self):
        return f"{self.file.name} ({self.replica_type})"

class FileBlock(BaseModel):
    """An individual chunk of a specific FileReplica."""

    replica = models.ForeignKey(FileReplica, on_delete=models.CASCADE, related_name='blocks')
    block_order = models.PositiveIntegerField()
    file_path = models.CharField(
        max_length=1024,
        help_text="Path for the file.",
    )
    size = models.PositiveBigIntegerField(
        validators=[MinValueValidator(0)],
        help_text="File size in bytes.",
    )
    mime_type = models.CharField(max_length=255, blank=True, null=True, default=None)

    # --- Checksums ---
    md5 = models.CharField(
        max_length=32, blank=True, null=True, default=None, db_index=True,
        help_text="MD5 hash of the file.",
    )
    sha1 = models.CharField(
        max_length=40, blank=True, null=True, default=None, db_index=True,
        help_text="SHA1 hash of the file.",
    )
    sha256 = models.CharField(
        max_length=64, blank=True, null=True, default=None, db_index=True,
        help_text="SHA256 hash of the file.",
    )

    class Meta:
        unique_together = ('replica', 'block_order')
        ordering = ['block_order']
        db_table = "file_block"

    def __str__(self):
        return f"Block {self.block_order} for {self.replica}"
