"""
File models for the esperoj application.

This module contains models for managing digital files and their storage
in the digital preservation system. It handles file metadata, replicas
across different storage systems, and file blocks for large files.
"""

from typing import TYPE_CHECKING

from django.conf import settings
from django.core.validators import MinValueValidator
from django.db import models
from django.db.models import Index, Q, UniqueConstraint, Manager
from django.core.exceptions import ValidationError

from .base import BaseModel

if TYPE_CHECKING:
    from .items import Item


class FileManager(Manager):
    """Custom manager for the File model providing common query methods."""

    def by_path_prefix(self, prefix: str):
        """Returns a QuerySet of File objects whose 'path' field starts with the given prefix."""
        return self.filter(path__startswith=prefix)

    def by_mime_type(self, mime_type: str):
        """Returns files of a specific MIME type."""
        return self.filter(mime_type=mime_type)

    def by_size_range(self, min_size=None, max_size=None):
        """Returns files within a size range (in bytes)."""
        queryset = self.all()
        if min_size is not None:
            queryset = queryset.filter(size__gte=min_size)
        if max_size is not None:
            queryset = queryset.filter(size__lte=max_size)
        return queryset

    def large_files(self, threshold_mb=100):
        """Returns files larger than the specified threshold in MB."""
        threshold_bytes = threshold_mb * 1024 * 1024
        return self.filter(size__gte=threshold_bytes)

    def with_replicas(self):
        """Returns files that have at least one replica."""
        return self.filter(replicas__isnull=False).distinct()

    def without_replicas(self):
        """Returns files that have no replicas."""
        return self.filter(replicas__isnull=True)

    def by_checksum(self, checksum_type: str, checksum_value: str):
        """Returns files matching a specific checksum."""
        lookup = {f"{checksum_type}__exact": checksum_value}
        return self.filter(**lookup)


class File(BaseModel):
    """
    Represents the intrinsic metadata of a single digital file.

    This model stores information that is inherent to the file itself,
    such as its size and checksums, independent of where it is stored.
    It serves as the logical representation of a file that can have
    multiple physical copies (replicas) in different storage systems.

    Attributes:
        name: A user-defined name for the file.
        path: A unique, logical path or identifier for the file within the system.
        original_filename: The original filename when first ingested.
        size: The total size of the file in bytes.
        mime_type: The MIME type of the file.
        md5: The MD5 checksum of the file.
        sha1: The SHA1 checksum of the file.
        sha256: The SHA256 checksum of the file.
        file_format: Additional format information beyond MIME type.
        compression: Information about file compression if applicable.

    Reverse Relations:
        replicas: All physical copies of this file.
        items: Catalog items associated with this file.
    """

    # --- Core Information ---
    name = models.CharField(
        max_length=512,
        db_index=True,
        help_text="A user-defined, descriptive name for the file.",
    )
    path = models.CharField(
        max_length=1024,
        unique=True,
        db_index=True,
        help_text="The unique, logical path or identifier of the file within the system.",
    )
    original_filename = models.CharField(
        max_length=512,
        blank=True,
        default="",
        help_text="The original filename when first ingested into the system.",
    )
    size = models.PositiveBigIntegerField(
        validators=[MinValueValidator(0)],
        db_index=True,
        help_text="The total size of the file in bytes.",
    )
    mime_type = models.CharField(
        max_length=255,
        blank=True,
        default="",
        db_index=True,
        help_text="The IANA media type (MIME type) of the file.",
    )

    # --- Format Information ---
    file_format = models.CharField(
        max_length=100,
        blank=True,
        default="",
        help_text="Additional format information (e.g., 'FLAC', 'PDF/A-1b', 'TIFF').",
    )
    compression = models.CharField(
        max_length=100,
        blank=True,
        default="",
        help_text="Compression information if applicable (e.g., 'gzip', 'lossless', 'lossy').",
    )

    # --- Checksums ---
    md5 = models.CharField(
        max_length=32,
        blank=True,
        default="",
        db_index=True,
        help_text="The MD5 hash of the file content.",
    )
    sha1 = models.CharField(
        max_length=40,
        blank=True,
        default="",
        db_index=True,
        help_text="The SHA1 hash of the file content.",
    )
    sha256 = models.CharField(
        max_length=64,
        blank=True,
        default="",
        db_index=True,
        help_text="The SHA256 hash of the file content.",
    )

    # --- Type hints for reverse relationships ---
    replicas: "Manager[FileReplica]"
    items: "Manager[Item]"

    objects = FileManager()

    class Meta:
        db_table = "file"
        ordering = ["path", "name"]
        verbose_name = "File"
        verbose_name_plural = "Files"
        constraints = [
            UniqueConstraint(fields=["md5"], condition=Q(md5__gt=""), name="unique_md5_if_not_empty"),
            UniqueConstraint(fields=["sha1"], condition=Q(sha1__gt=""), name="unique_sha1_if_not_empty"),
            UniqueConstraint(fields=["sha256"], condition=Q(sha256__gt=""), name="unique_sha256_if_not_empty"),
        ]
        indexes = [
            Index(fields=["path"]),
            Index(fields=["name"]),
            Index(fields=["size"]),
            Index(fields=["mime_type"]),
            Index(fields=["created_at"]),
            Index(fields=["md5"]),
            Index(fields=["sha1"]),
            Index(fields=["sha256"]),
        ]

    def __str__(self) -> str:
        """Returns the logical path of the file."""
        return self.path

    def clean(self) -> None:
        """Performs model validation."""
        super().clean()

        # Validate checksums are properly formatted
        if self.md5 and len(self.md5) != 32:
            raise ValidationError({"md5": "MD5 hash must be exactly 32 characters."})

        if self.sha1 and len(self.sha1) != 40:
            raise ValidationError({"sha1": "SHA1 hash must be exactly 40 characters."})

        if self.sha256 and len(self.sha256) != 64:
            raise ValidationError({"sha256": "SHA256 hash must be exactly 64 characters."})

        # Validate that at least one checksum is provided
        if not any([self.md5, self.sha1, self.sha256]):
            raise ValidationError("At least one checksum (MD5, SHA1, or SHA256) must be provided.")

    @property
    def display_size(self) -> str:
        """Returns a human-readable file size."""
        size = self.size
        for unit in ["B", "KB", "MB", "GB", "TB"]:
            if size < 1024.0:
                return f"{size:.1f} {unit}"
            size /= 1024.0
        return f"{size:.1f} PB"

    @property
    def primary_checksum(self) -> str:
        """Returns the best available checksum (preferring SHA256, then SHA1, then MD5)."""
        return self.sha256 or self.sha1 or self.md5

    @property
    def has_replicas(self) -> bool:
        """Returns True if the file has at least one replica."""
        return self.replicas.exists()

    @property
    def replica_count(self) -> int:
        """Returns the number of replicas for this file."""
        return self.replicas.count()

    @property
    def is_image(self) -> bool:
        """Returns True if the file is an image based on MIME type."""
        return self.mime_type.startswith("image/")

    @property
    def is_audio(self) -> bool:
        """Returns True if the file is audio based on MIME type."""
        return self.mime_type.startswith("audio/")

    @property
    def is_video(self) -> bool:
        """Returns True if the file is video based on MIME type."""
        return self.mime_type.startswith("video/")

    @property
    def is_text(self) -> bool:
        """Returns True if the file is text based on MIME type."""
        return self.mime_type.startswith("text/")

    def get_replica_by_type(self, replica_type: str):
        """Returns a specific replica by type, if it exists."""
        return self.replicas.filter(replica_type=replica_type).first()


class FileReplicaManager(Manager):
    """Custom manager for the FileReplica model."""

    def by_storage(self, storage_name: str):
        """Returns replicas in a specific storage system."""
        return self.filter(storage_name=storage_name)

    def by_type(self, replica_type: str):
        """Returns replicas of a specific type."""
        return self.filter(replica_type=replica_type)

    def active(self):
        """Returns only active (non-deleted) replicas."""
        return self.filter(is_active=True)

    def verified_recently(self, days=30):
        """Returns replicas verified within the specified number of days."""
        from django.utils import timezone
        import datetime

        cutoff = timezone.now() - datetime.timedelta(days=days)
        return self.filter(last_verified__gte=cutoff)

    def needs_verification(self, days=30):
        """Returns replicas that need verification."""
        from django.utils import timezone
        import datetime

        cutoff = timezone.now() - datetime.timedelta(days=days)
        return self.filter(models.Q(last_verified__isnull=True) | models.Q(last_verified__lt=cutoff))


class FileReplica(BaseModel):
    """
    Represents a specific, complete physical copy of a File.

    This model tracks where a file is stored and its state within that
    storage. A single File can have multiple replicas across different
    storage backends for redundancy and access purposes.

    Attributes:
        file: A foreign key to the logical File this is a copy of.
        replica_type: The type of replica (e.g., original, access copy, preservation).
        storage_name: The name of the storage backend where this replica resides.
        storage_path: The path within the storage system.
        is_active: Whether this replica is currently available.
        last_verified: When this replica was last verified for integrity.
        verification_status: The result of the last verification check.

    Reverse Relations:
        blocks: The blocks that constitute this replica (for large files).
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
        choices=getattr(settings, "REPLICA_TYPES", []),
        help_text="The role or type of this replica (e.g., 'original', 'access_copy', 'preservation').",
    )
    storage_name = models.CharField(
        max_length=50,
        choices=getattr(settings, "STORAGE_CHOICES", []),
        help_text="The configured storage backend where this replica is located.",
    )
    storage_path = models.CharField(
        max_length=1024,
        blank=True,
        default="",
        help_text="The path or key of this replica within the storage system.",
    )

    # --- Status Information ---
    is_active = models.BooleanField(
        default=True,
        help_text="Whether this replica is currently available and accessible.",
    )
    last_verified = models.DateTimeField(
        null=True,
        blank=True,
        help_text="When this replica was last verified for integrity.",
    )
    verification_status = models.CharField(
        max_length=20,
        choices=[
            ("pending", "Verification Pending"),
            ("success", "Verification Successful"),
            ("failed", "Verification Failed"),
            ("error", "Verification Error"),
        ],
        default="pending",
        help_text="The result of the last verification check.",
    )

    objects = FileReplicaManager()

    class Meta:
        db_table = "file_replica"
        ordering = ["file", "replica_type", "storage_name"]
        verbose_name = "File Replica"
        verbose_name_plural = "File Replicas"
        unique_together = ("file", "replica_type", "storage_name")
        indexes = [
            Index(fields=["file", "replica_type"]),
            Index(fields=["storage_name"]),
            Index(fields=["replica_type"]),
            Index(fields=["is_active"]),
            Index(fields=["last_verified"]),
            Index(fields=["verification_status"]),
        ]

    def __str__(self) -> str:
        """Returns a string identifying the replica and its type."""
        return f"{self.file.path} ({self.replica_type} on {self.storage_name})"

    @property
    def needs_verification(self) -> bool:
        """Returns True if this replica needs verification."""
        if not self.last_verified:
            return True

        from django.utils import timezone
        import datetime

        # Consider verification needed after 30 days
        cutoff = timezone.now() - datetime.timedelta(days=30)
        return self.last_verified < cutoff

    @property
    def verification_overdue(self) -> bool:
        """Returns True if verification is significantly overdue."""
        if not self.last_verified:
            return True

        from django.utils import timezone
        import datetime

        # Consider overdue after 90 days
        cutoff = timezone.now() - datetime.timedelta(days=90)
        return self.last_verified < cutoff

    def mark_verified(self, status="success") -> None:
        """Mark this replica as verified with the given status."""
        from django.utils import timezone

        self.last_verified = timezone.now()
        self.verification_status = status
        self.save(update_fields=["last_verified", "verification_status"])

    def mark_inactive(self) -> None:
        """Mark this replica as inactive."""
        self.is_active = False
        self.verification_status = "failed"
        self.save(update_fields=["is_active", "verification_status"])


class FileBlockManager(Manager):
    """Custom manager for the FileBlock model."""

    def for_replica(self, replica):
        """Returns all blocks for a specific replica, ordered by block_order."""
        return self.filter(replica=replica).order_by("block_order")

    def incomplete_blocks(self):
        """Returns blocks that may be incomplete or problematic."""
        return self.filter(models.Q(size=0) | models.Q(sha256="") | models.Q(file_path=""))


class FileBlock(BaseModel):
    """
    Represents an individual chunk or part of a FileReplica.

    This is useful for large files that are stored in multiple parts, such as
    with multipart uploads in cloud storage, or for implementing more granular
    integrity checking and parallel processing.

    Attributes:
        replica: The parent FileReplica this block belongs to.
        block_order: The sequential position of this block within the replica.
        file_path: The path to this block within its storage backend.
        size: The size of this block in bytes.
        mime_type: The MIME type of this block (if different from parent).
        md5: The MD5 checksum of this block.
        sha1: The SHA1 checksum of this block.
        sha256: The SHA256 checksum of this block.
        is_last_block: Whether this is the final block in the sequence.
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
        help_text="The sequential position of this block within the replica (0-indexed).",
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
        default="",
        help_text="The MIME type of the block, if different from the parent file.",
    )
    is_last_block = models.BooleanField(
        default=False,
        help_text="Whether this is the final block in the sequence.",
    )

    # --- Checksums ---
    md5 = models.CharField(
        max_length=32,
        blank=True,
        default="",
        db_index=True,
        help_text="The MD5 hash of the block's content.",
    )
    sha1 = models.CharField(
        max_length=40,
        blank=True,
        default="",
        db_index=True,
        help_text="The SHA1 hash of the block's content.",
    )
    sha256 = models.CharField(
        max_length=64,
        blank=True,
        default="",
        db_index=True,
        help_text="The SHA256 hash of the block's content.",
    )

    objects = FileBlockManager()

    class Meta:
        db_table = "file_block"
        unique_together = ("replica", "block_order")
        ordering = ["replica", "block_order"]
        verbose_name = "File Block"
        verbose_name_plural = "File Blocks"
        indexes = [
            Index(fields=["replica", "block_order"]),
            Index(fields=["block_order"]),
            Index(fields=["size"]),
            Index(fields=["is_last_block"]),
        ]

    def __str__(self) -> str:
        """Returns a string identifying the block and its parent replica."""
        return f"Block {self.block_order} of {self.replica}"

    def clean(self) -> None:
        """Performs model validation."""
        super().clean()

        # Validate that block_order is non-negative
        if self.block_order < 0:
            raise ValidationError({"block_order": "Block order must be non-negative."})

        # Validate checksums format if provided
        if self.md5 and len(self.md5) != 32:
            raise ValidationError({"md5": "MD5 hash must be exactly 32 characters."})

        if self.sha1 and len(self.sha1) != 40:
            raise ValidationError({"sha1": "SHA1 hash must be exactly 40 characters."})

        if self.sha256 and len(self.sha256) != 64:
            raise ValidationError({"sha256": "SHA256 hash must be exactly 64 characters."})

    @property
    def display_size(self) -> str:
        """Returns a human-readable block size."""
        size = self.size
        for unit in ["B", "KB", "MB", "GB"]:
            if size < 1024.0:
                return f"{size:.1f} {unit}"
            size /= 1024.0
        return f"{size:.1f} TB"

    @property
    def primary_checksum(self) -> str:
        """Returns the best available checksum for this block."""
        return self.sha256 or self.sha1 or self.md5
