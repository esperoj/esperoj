from typing import TYPE_CHECKING

from django.core.validators import MinValueValidator
from django.db import models
from django.db.models import Q, UniqueConstraint

from .base import BaseModel

if TYPE_CHECKING:
    from .storage import BaseStorage
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
    storages: models.Manager["BaseStorage"]
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

    def get_primary_storage(self) -> "BaseStorage | None":
        """Retrieves the primary storage location for this file, if one exists."""
        return self.storages.filter(is_primary=True).first()

    def get_latest_storage(self) -> "BaseStorage | None":
        """Retrieves the most recently updated storage location for this file."""
        return self.storages.order_by("-updated_at").first()
