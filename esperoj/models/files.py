"""
File and Bitstream models for digital preservation.
"""

from django.core.validators import MinLengthValidator, MinValueValidator
from django.db import models

from .base import BaseModel


class FixityMixin(models.Model):
    """
    Mixin for cryptographic checksum attributes.

    Attributes:
        md5 (str): The MD5 message digest.
        sha1 (str): The SHA1 message digest.
        sha256 (str): The SHA256 message digest.
    """

    md5 = models.CharField(max_length=32, validators=[MinLengthValidator(32)], blank=True, default="")
    sha1 = models.CharField(max_length=40, validators=[MinLengthValidator(40)], blank=True, default="")
    sha256 = models.CharField(max_length=64, validators=[MinLengthValidator(64)], blank=True, default="")

    class Meta:
        abstract = True
        indexes = [
            models.Index(fields=["md5"]),
            models.Index(fields=["sha1"]),
            models.Index(fields=["sha256"]),
        ]
        constraints = [
            # SQL Generated: CHECK ((md5 = '') OR (LENGTH(md5) = 32))
            models.CheckConstraint(
                check=models.Q(md5="") | models.Q(md5__length=32),
                name="%(app_label)s_%(class)s_md5_len",
            ),
            # SQL Generated: CHECK ((sha1 = '') OR (LENGTH(sha1) = 40))
            models.CheckConstraint(
                check=models.Q(sha1="") | models.Q(sha1__length=40),
                name="%(app_label)s_%(class)s_sha1_len",
            ),
            # SQL Generated: CHECK ((sha256 = '') OR (LENGTH(sha256) = 64))
            models.CheckConstraint(
                check=models.Q(sha256="") | models.Q(sha256__length=64),
                name="%(app_label)s_%(class)s_sha256_len",
            ),
        ]


class SizeMixin(models.Model):
    """
    Mixin for byte size tracking.

    Attributes:
        size (int): The size of the object in bytes.
    """

    size = models.PositiveBigIntegerField(
        validators=[MinValueValidator(0)],
        db_index=True,
    )

    class Meta:
        abstract = True


class File(BaseModel, SizeMixin, FixityMixin):
    """
    A logical digital file.

    This describes the file conceptually (e.g., 'video.mkv'), its format,
    and its expected logical characteristics. This remains constant even if
    the file is physically split or stored in multiple locations.

    Attributes:
        representation (Representation): The set this file belongs to.
        path (str): The logical relative path/filename (e.g. 'data/video.mkv').
        mime_type (str): The IANA media type.
        format_puid (str): The format registry ID (e.g., 'fmt/11').
        composition_level (str): 'atomic' (whole) or 'composite' (split).
    """

    COMPOSITION_ATOMIC = "atomic"
    COMPOSITION_COMPOSITE = "composite"

    representation = models.ForeignKey(Representation, on_delete=models.CASCADE, related_name="files")
    # This path acts as the "folder" structure INSIDE the package
    path = models.CharField(max_length=1024, help_text="Logical path (e.g., 'videos/season1/ep01.mkv').")
    mime_type = models.CharField(max_length=255, blank=True)
    format_puid = models.CharField(max_length=50, blank=True)

    composition_level = models.CharField(
        max_length=20,
        choices=[
            (COMPOSITION_ATOMIC, "Atomic"),
            (COMPOSITION_COMPOSITE, "Composite"),
        ],
        default=COMPOSITION_ATOMIC,
    )

    class Meta:
        db_table = "file"
        ordering = ["path"]

    def __str__(self) -> str:
        return self.path


class Bitstream(BaseModel, SizeMixin, FixityMixin):
    """
    A specific physical data stream stored on a medium.

    If a File is 'atomic' and stored in two places (S3 and Local), there are
    two Bitstream records (both sequence 1).
    If a File is 'composite' (split in 3) and stored in one place, there are
    three Bitstream records (sequence 1, 2, 3).

    Attributes:
        file (File): The logical file this stream supports.
        sequence (int): The structural order (1-based).
        storage_id (str): Identifier for the storage system (e.g., 's3-main').
        location (str): The specific path/key on the storage medium.
    """

    file = models.ForeignKey(File, on_delete=models.CASCADE, related_name="bitstreams")
    sequence = models.PositiveIntegerField(default=1, help_text="Order of this part. If atomic, always 1.")

    # Identifier for where this is stored (e.g., 'aws_bucket_1', 'nas_drive_2')
    storage_id = models.CharField(max_length=50, db_index=True)

    # Actual path on disk / key in S3
    location = models.CharField(max_length=1024)

    class Meta:
        db_table = "bitstream"
        ordering = ["sequence", "storage_id"]
        # Allow multiple sequences (split parts) but unique per storage + file + sequence
        unique_together = ("file", "sequence", "storage_id")

    def __str__(self) -> str:
        return f"{self.file.path} [Part {self.sequence} @ {self.storage_id}]"
