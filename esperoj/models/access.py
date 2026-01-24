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

from .entity import Entity
from .file import File


class AccessPackage(Entity):
    """
    A distinct, public-facing collection of content.

    This represents the 'DIP' (Dissemination Information Package). It acts as
    the catalog entry that users browse, search for, and view. Unlike the
    internal preservation record (Entity), the AccessPackage organizes
    presentation data.

    Example:
        - Internal Entity: "Project Gutenberg Dump 2023 (UUID: ...)"
        - AccessPackage 1: "Alice in Wonderland (EPUB Edition)"
        - AccessPackage 2: "Alice in Wonderland (HTML Online Read)"

    Attributes:
        entity (Entity): The internal preservation entity being disseminated.
        title (str): The public display title.
        identifier (str): Unique URL identifier.
        description (str): HTML/Markdown content describing this package.
    """

    # Link back to your internal preservation record
    entity = models.ForeignKey(
        Entity,
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


class PackageEntry(Entity):
    """
    A file mapped to a virtual path within an AccessPackage.

    This allows you to rename files and organize them into folders for the
    user without changing the original file on disk.

    Example:
        Preservation File: s3://bucket/a9f1-22b1.dat
        PackageEntry Path: "manuals/english/user_guide.pdf"

    Attributes:
        package (AccessPackage): The parent bundle.
        file (File): The actual preservation file.
        path (str): The logical path/filename shown to the user.
    """

    package = models.ForeignKey(
        AccessPackage, on_delete=models.CASCADE, related_name="entries", help_text="The package this file belongs to."
    )

    file = models.ForeignKey(
        File, on_delete=models.PROTECT, related_name="package_usages", help_text="The preservation file being exposed."
    )

    path = models.CharField(
        max_length=1024, help_text="The relative path/filename shown to the user (e.g. 'extras/map.jpg')."
    )

    class Meta:
        db_table = "access_package_entry"
        verbose_name = "Package Entry"
        verbose_name_plural = "Package Entries"
        ordering = ["path"]
        constraints = [models.UniqueConstraint(fields=["package", "path"], name="unique_package_path")]

    def __str__(self) -> str:
        return f"{self.package.identifier}: {self.path}"

    @property
    def filename(self) -> str:
        """Extracts just the filename from the virtual path."""
        return PurePosixPath(self.path).name

    @property
    def folder(self) -> str:
        """Extracts the folder structure from the virtual path."""
        parent = PurePosixPath(self.path).parent
        return str(parent) if str(parent) != "." else ""
