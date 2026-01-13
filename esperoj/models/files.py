"""
File, Manifestation, and Bitstream models for digital preservation.
"""

from django.core.validators import MinLengthValidator, MinValueValidator
from django.db import models
from django.utils.translation import gettext_lazy as _

from .base import BaseModel


class AbstractFile(BaseModel):
    """
    Abstract base class for logical files and physical bitstreams.

    This model provides a shared structure for describing digital objects, including
    their names, sizes, and media types. It defines standard fields for fixity
    checking using cryptographic hashes (MD5, SHA1, and SHA256) and includes
    database-level constraints to ensure digest lengths are valid.

    Attributes:
        filename (str): The name of the file (e.g. 'video.mkv').
        size (int): The size of the file in bytes.
        mime_type (str): The IANA media type.
        md5 (str): The MD5 message digest (32 chars).
        sha1 (str): The SHA1 message digest (40 chars).
        sha256 (str): The SHA256 message digest (64 chars).
    """

    filename = models.CharField(
        max_length=1024,
        help_text="The name of the file (e.g. 'video.mkv').",
    )

    size = models.PositiveBigIntegerField(
        validators=[MinValueValidator(0)],
        db_index=True,
        help_text="The size of the file in bytes.",
    )

    mime_type = models.CharField(
        max_length=255,
        blank=True,
        help_text="The IANA media type (e.g. 'video/mp4').",
    )

    md5 = models.CharField(
        max_length=32,
        validators=[MinLengthValidator(32)],
        blank=True,
        default="",
        help_text="The MD5 message digest (32 chars).",
    )
    sha1 = models.CharField(
        max_length=40,
        validators=[MinLengthValidator(40)],
        blank=True,
        default="",
        help_text="The SHA1 message digest (40 chars).",
    )
    sha256 = models.CharField(
        max_length=64,
        validators=[MinLengthValidator(64)],
        blank=True,
        default="",
        help_text="The SHA256 message digest (64 chars).",
    )

    class Meta:
        abstract = True
        indexes = [
            models.Index(fields=["filename"]),
            models.Index(fields=["md5"]),
            models.Index(fields=["sha1"]),
            models.Index(fields=["sha256"]),
        ]
        constraints = [
            models.CheckConstraint(
                check=models.Q(md5="") | models.Q(md5__length=32),
                name="%(app_label)s_%(class)s_md5_len",
            ),
            models.CheckConstraint(
                check=models.Q(sha1="") | models.Q(sha1__length=40),
                name="%(app_label)s_%(class)s_sha1_len",
            ),
            models.CheckConstraint(
                check=models.Q(sha256="") | models.Q(sha256__length=64),
                name="%(app_label)s_%(class)s_sha256_len",
            ),
        ]

    def __str__(self) -> str:
        return self.filename


class File(AbstractFile):
    """A logical digital file.

    Describes the file conceptually (e.g., 'video.mkv') as it appears
    to the end user after retrieval and re-assembly. This model represents
    the ideal state of the file, regardless of how it is physically stored.
    """

    class Meta(AbstractFile.Meta):
        abstract = False
        db_table = "file"
        verbose_name = "File"
        verbose_name_plural = "Files"
        ordering = ["filename"]


class FileManifestation(BaseModel):
    """A specific physical arrangement (copy) of the logical file.

    This intermediary layer allows a single logical `File` to exist in multiple
    physical structures simultaneously. For example, one manifestation might be
    atomic (a single object on S3), while another is composite (split into
    chunks on Tape).

    Attributes:
        file (File): Reference to the logical file.
        combination_method (str): The algorithm required to reconstruct the
            logical file from its bitstreams (e.g., 'single', 'concat').
        label (str): Optional user-defined label for this copy.
    """

    class CombinationMethod(models.TextChoices):
        """Enumeration of method types for combining bitstreams."""

        # Atomic: The bitstream IS the file.
        SINGLE = "single", _("Single (Atomic)")

        # Composite: Parts must be concatenated (cat part1 part2 > full).
        CONCAT = "concat", _("Concatenation")

        # Composite: Parts are segments of a multipart zip.
        ZIP_MULTIPART = "zip_multipart", _("Zip Multipart")

        # Composite: Parts are segments created by 'split' or 'tar -M'.
        TAR_MULTIPART = "tar_multipart", _("Tar Multipart")

    file = models.ForeignKey(
        File,
        on_delete=models.CASCADE,
        related_name="manifestations",
        help_text="The logical preservation file this manifestation represents.",
    )

    combination_method = models.CharField(
        max_length=32,
        choices=CombinationMethod.choices,
        default=CombinationMethod.SINGLE,
        help_text="Algorithm required to reconstruct the logical file.",
    )

    label = models.CharField(
        max_length=255,
        blank=True,
        help_text="Optional label (e.g., 'S3 Deep Archive', 'Local NAS Copy').",
    )

    class Meta:
        db_table = "file_manifestation"
        verbose_name = "File Manifestation"
        verbose_name_plural = "File Manifestations"
        ordering = ["file", "label"]

    def __str__(self) -> str:
        # Use the Enum class to look up the label directly.
        # This avoids the 'get_combination_method_display' linter error.
        try:
            method_label = self.CombinationMethod(self.combination_method).label
        except ValueError:
            method_label = self.combination_method
        return f"{self.file.filename} [{method_label}]"

    @property
    def is_composite(self) -> bool:
        """Returns True if the manifestation consists of multiple bitstreams."""
        return self.combination_method != self.CombinationMethod.SINGLE


class Bitstream(AbstractFile):
    """A specific physical data stream stored on a medium.

    Linked to a Manifestation. If the Manifestation is atomic (SINGLE),
    this table contains one record per storage location. If the Manifestation
    is composite (e.g., CONCAT), this table contains N records representing
    the sequence parts.

    Attributes:
        manifestation (FileManifestation): The structural copy this stream belongs to.
        sequence (int): Order of this part (1-based).
        storage_id (str): Identifier for the storage system (e.g., 's3-main').
        location (str): Actual path/key on the storage medium.
    """

    manifestation = models.ForeignKey(
        FileManifestation,
        on_delete=models.CASCADE,
        related_name="bitstreams",
        help_text="The physical manifestation this bitstream belongs to.",
    )

    sequence = models.PositiveIntegerField(
        default=1,
        help_text="Order of this part. If Method is Single, always 1.",
    )

    storage_id = models.CharField(
        max_length=50,
        db_index=True,
        help_text="Identifier for the storage system (e.g., 's3-main').",
    )

    location = models.CharField(
        max_length=1024,
        help_text="Actual path/key on the storage medium.",
    )

    class Meta(AbstractFile.Meta):
        abstract = False
        db_table = "bitstream"
        verbose_name = "Bitstream"
        verbose_name_plural = "Bitstreams"
        ordering = ["manifestation", "sequence"]
        constraints = AbstractFile.Meta.constraints + [
            models.UniqueConstraint(
                fields=["manifestation", "sequence"],
                name="unique_bitstream_manifestation_sequence",
            )
        ]

    def __str__(self) -> str:
        return f"Part {self.sequence} of {self.manifestation} @ {self.storage_id}"
