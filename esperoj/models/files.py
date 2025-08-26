from typing import TYPE_CHECKING

from django.conf import settings
from django.core.validators import MinValueValidator
from django.db import models
from django.db.models import Index, Q, UniqueConstraint

from .base import BaseModel

if TYPE_CHECKING:
    from .items import Item


class FileManager(models.Manager):
    """
    Custom manager for the File model, providing common query methods.
    """

    def by_path_prefix(self, prefix: str):
        """
        Returns a QuerySet of File objects whose 'path' field starts with the given prefix.
        """
        return self.filter(path__startswith=prefix)


class File(BaseModel):
    """Represents the intrinsic metadata of a single digital file.

    This model stores information that is inherent to the file itself,
    such as its size and checksums, independent of where it is stored.

    Attributes:
        name: A user-defined name for the file.
        path: A unique, logical path or identifier for the file within the system.
        size: The total size of the file in bytes.
        mime_type: The MIME type of the file.
        md5: The MD5 checksum of the file.
        sha1: The SHA1 checksum of the file.
        sha256: The SHA256 checksum of the file.
        replicas: A reverse relation to all physical copies of this file.
        items: A reverse relation to catalog items associated with this file.
    """

    # --- Core Information ---
    name = models.CharField(
        max_length=255,
        db_index=True,
        help_text="A user-defined, descriptive name for the file.",
    )
    path = models.CharField(
        max_length=1024,
        unique=True,
        help_text="The unique, logical path or identifier of the file within the system.",
    )
    size = models.PositiveBigIntegerField(
        validators=[MinValueValidator(0)],
        help_text="The total size of the file in bytes.",
    )
    mime_type = models.CharField(
        max_length=255, blank=True, null=True, default=None, help_text="The IANA media type (MIME type) of the file."
    )

    # --- Checksums ---
    md5 = models.CharField(
        max_length=32,
        blank=True,
        null=True,
        default=None,
        db_index=True,
        help_text="The MD5 hash of the file content.",
    )
    sha1 = models.CharField(
        max_length=40,
        blank=True,
        null=True,
        default=None,
        db_index=True,
        help_text="The SHA1 hash of the file content.",
    )
    sha256 = models.CharField(
        max_length=64,
        blank=True,
        null=True,
        default=None,
        db_index=True,
        help_text="The SHA256 hash of the file content.",
    )

    # --- Type hints for reverse relationships ---
    replicas: "models.Manager[FileReplica]"
    items: "models.Manager[Item]"

    objects = FileManager()  # Assign the custom manager

    class Meta:
        db_table = "file"
        ordering = ["path"]  # Changed ordering to path, as it's a unique identifier
        verbose_name = "File"
        verbose_name_plural = "Files"
        constraints = [
            UniqueConstraint(fields=["md5"], condition=Q(md5__isnull=False), name="unique_md5_if_not_null"),
            UniqueConstraint(fields=["sha1"], condition=Q(sha1__isnull=False), name="unique_sha1_if_not_null"),
            UniqueConstraint(fields=["sha256"], condition=Q(sha256__isnull=False), name="unique_sha256_if_not_null"),
        ]
        # Adding an index for path for faster lookups, although unique=True implies an index.
        # Explicitly adding it here for clarity if unique wasn't desired later.
        indexes = [
            Index(fields=["path"]),
        ]

    def __str__(self) -> str:
        """Returns the logical path of the file."""
        return self.path


class FileReplica(BaseModel):
    """Represents a specific, complete physical copy of a File.

    This model tracks where a file is stored and its state within that
    storage. A single File can have multiple replicas across different
    storage backends.

    Attributes:
        file: A foreign key to the logical File this is a copy of.
        replica_type: The type of replica (e.g., original, access copy).
        storage_name: The name of the storage backend where this replica resides.
        blocks: A reverse relation to the blocks that constitute this replica.
    """

    # --- Relationships ---
    file = models.ForeignKey(
        File,
        on_delete=models.CASCADE,
        related_name="replicas",
        help_text="The logical file that this replica is a copy of.",
    )

    # --- Replica Details ---
    replica_type = models.CharField(
        max_length=50,
        choices=settings.REPLICA_TYPES,
        help_text="The role or type of this replica (e.g., 'original', 'access_copy').",
    )
    storage_name = models.CharField(
        max_length=50,
        choices=settings.STORAGE_CHOICES,
        help_text="The configured storage backend where this replica is located.",
    )

    class Meta:
        db_table = "file_replica"
        ordering = ["file", "replica_type"]
        verbose_name = "File Replica"
        verbose_name_plural = "File Replicas"
        indexes = [
            Index(fields=["file", "replica_type"]),
            Index(fields=["storage_name"]),
        ]

    def __str__(self):
        """Returns a string identifying the replica and its type."""
        return f"{self.file.path} ({self.replica_type})"


class FileBlock(BaseModel):
    """Represents an individual chunk or part of a FileReplica.

    This is useful for large files that are stored in multiple parts, such as
    with multipart uploads in cloud storage.

    Attributes:
        replica: The parent FileReplica this block belongs to.
        block_order: The sequential position of this block within the replica.
        file_path: The path to this block within its storage backend.
        size: The size of this block in bytes.
        mime_type: The MIME type of this block.
        md5: The MD5 checksum of this block.
        sha1: The SHA1 checksum of this block.
        sha256: The SHA256 checksum of this block.
    """

    # --- Relationships ---
    replica = models.ForeignKey(
        FileReplica,
        on_delete=models.CASCADE,
        related_name="blocks",
        help_text="The file replica that this block is a part of.",
    )

    # --- Block Details ---
    block_order = models.PositiveIntegerField(
        help_text="The sequential position of this block within the replica (0-indexed)."
    )
    file_path = models.CharField(
        max_length=1024,
        help_text="The full path or key of this block within the storage backend.",
    )
    size = models.PositiveBigIntegerField(
        validators=[MinValueValidator(0)],
        help_text="The size of this individual block in bytes.",
    )
    mime_type = models.CharField(
        max_length=255,
        blank=True,
        null=True,
        default=None,
        help_text="The MIME type of the block, if different from the parent file.",
    )

    # --- Checksums ---
    md5 = models.CharField(
        max_length=32,
        blank=True,
        null=True,
        default=None,
        db_index=True,
        help_text="The MD5 hash of the block's content.",
    )
    sha1 = models.CharField(
        max_length=40,
        blank=True,
        null=True,
        default=None,
        db_index=True,
        help_text="The SHA1 hash of the block's content.",
    )
    sha256 = models.CharField(
        max_length=64,
        blank=True,
        null=True,
        default=None,
        db_index=True,
        help_text="The SHA256 hash of the block's content.",
    )

    class Meta:
        db_table = "file_block"
        unique_together = ("replica", "block_order")
        ordering = ["replica", "block_order"]
        verbose_name = "File Block"
        verbose_name_plural = "File Blocks"

    def __str__(self):
        """Returns a string identifying the block and its parent replica."""
        return f"Block {self.block_order} for {self.replica}"
