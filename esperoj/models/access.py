"""
Access models for the esperoj application.

This module manages the 'Dissemination Layer'. It allows you to construct
user-facing bundles (AccessPackages) by mapping preservation files to
virtual directory structures.

This decouples the messy reality of storage (S3 buckets, UUIDs) from the
clean presentation required for users (folders, readable filenames).
"""

from pathlib import PurePosixPath

from django.db import models

from .base import BaseModel
from .files import File
from .items import Item


class AccessPackage(BaseModel):
    """
    A distinct, public-facing collection of content.

    This represents the 'DIP' (Dissemination Information Package). It acts as
    the catalog entry that users browse, search for, and view. Unlike the
    internal 'Item', which organizes preservation data, the AccessPackage
    organizes presentation data.

    Example:
        - Internal Item: "Project Gutenberg Dump 2023 (UUID: ...)"
        - AccessPackage 1: "Alice in Wonderland (EPUB Edition)"
        - AccessPackage 2: "Alice in Wonderland (HTML Online Read)"

    Attributes:
        item (Item): The internal preservation item this derives from.
        title (str): The public display title.
        identifier (str): Unique URL identifier.
        description (str): HTML/Markdown content describing this package.
    """

    # Link back to your internal preservation record
    item = models.ForeignKey(
        Item,
        on_delete=models.CASCADE,
        related_name="access_packages",
        help_text="The internal preservation item being disseminated.",
    )

    title = models.CharField(max_length=255, db_index=True, help_text="The public title of this package.")
    identifier = models.SlugField(max_length=255, unique=True, help_text="URL slug for the public page.")
    description = models.TextField(blank=True, help_text="Public description (supports Markdown/HTML).")

    class Meta:
        db_table = "access_package"
        verbose_name = "Access Package"
        verbose_name_plural = "Access Packages"
        ordering = ["title"]

    def __str__(self) -> str:
        return self.title


class PackageEntry(BaseModel):
    """
    A file mapped to a virtual path within an AccessPackage.

    This allows you to rename files and organize them into folders for the
    user without changing the original file on disk.

    Example:
        Preservation File: s3://bucket/a9f1-22b1.dat
        PackageEntry Path: "manuals/english/user_guide.pdf"

    Attributes:
        package (AccessPackage): The parent bundle.
        file_source (File): The actual preservation file.
        virtual_path (str): The logical path/filename shown to the user.
    """

    package = models.ForeignKey(
        AccessPackage, on_delete=models.CASCADE, related_name="entries", help_text="The package this file belongs to."
    )

    file_source = models.ForeignKey(
        File, on_delete=models.PROTECT, related_name="package_usages", help_text="The preservation file being exposed."
    )

    # This is the "Archive.org" style file listing magic
    virtual_path = models.CharField(
        max_length=1024, help_text="The relative path/filename shown to the user (e.g. 'extras/map.jpg')."
    )

    class Meta:
        db_table = "access_package_entry"
        verbose_name = "Package Entry"
        verbose_name_plural = "Package Entries"
        ordering = ["virtual_path"]
        # Ensure two files don't have the same path in the same package
        constraints = [models.UniqueConstraint(fields=["package", "virtual_path"], name="unique_package_virtual_path")]

    def __str__(self) -> str:
        return f"{self.package.identifier}: {self.virtual_path}"

    @property
    def filename(self) -> str:
        """Extracts just the filename from the virtual path."""
        return PurePosixPath(self.virtual_path).name

    @property
    def folder(self) -> str:
        """Extracts the folder structure from the virtual path."""
        parent = PurePosixPath(self.virtual_path).parent
        return str(parent) if str(parent) != "." else ""
