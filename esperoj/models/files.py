from django.conf import settings
from django.core.validators import MinValueValidator
from django.db import models
from django.db.models import Q, Index, UniqueConstraint

from .base import BaseModel
from .entities import BaseName


class File(BaseModel):
    """Represents a digital file, independent of its storage location."""

    path = models.CharField(max_length=1024)
    size = models.PositiveBigIntegerField(validators=[MinValueValidator(0)])
    mime_type = models.CharField(max_length=255, blank=True, null=True, default=None)
    sha1 = models.CharField(
        max_length=40, blank=True, null=True, default=None, db_index=True
    )
    sha256 = models.CharField(
        max_length=64, blank=True, null=True, default=None, db_index=True
    )

    class Meta:
        ordering = ["path"]
        verbose_name = "File"
        verbose_name_plural = "Files"
        constraints = [
            UniqueConstraint(
                fields=["sha256"],
                condition=Q(sha256__isnull=False),
                name="unique_sha256_if_not_null",
            ),
            UniqueConstraint(
                fields=["sha1"],
                condition=Q(sha1__isnull=False),
                name="unique_sha1_if_not_null",
            ),
        ]
        db_table = "file"

    def get_primary_storage(self):
        return self.storages.filter(is_primary=True).first()

    def get_latest_storage(self):
        return self.storages.order_by("-updated_at").first()

    def __str__(self):
        primary_name = self.names.filter(language=settings.LANGUAGE_CODE).first()
        return (
            primary_name.name
            if primary_name
            else (self.names.first().name if self.names.exists() else self.path)
        )


class FileName(BaseName):
    file = models.ForeignKey(File, on_delete=models.CASCADE, related_name="names")

    class Meta(BaseName.Meta):
        verbose_name = "File Name"
        verbose_name_plural = "File Names"
        constraints = [
            UniqueConstraint(
                fields=["file", "language"], name="unique_name_per_lang_for_file"
            )
        ]
        db_table = "file_name"
