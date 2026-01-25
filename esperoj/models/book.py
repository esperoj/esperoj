from typing import TYPE_CHECKING

from django.core.exceptions import ValidationError
from django.db import models

from .entity import Entity

if TYPE_CHECKING:
    from django.db.models import QuerySet

    from .agent import Agent


class Book(Entity):
    """A book or written publication.

    Attributes:
        isbn_10: The 10-digit International Standard Book Number.
        isbn_13: The 13-digit International Standard Book Number.
        publisher: The publisher of the book.
        edition: The edition information.
        format: The physical format (hardcover, paperback, etc.).
        extent: The physical extent (e.g., page count, dimensions).
        table_of_contents: Table of contents or chapter listing.
    """

    # --- Book-specific fields ---
    isbn_10 = models.CharField(
        max_length=10,
        blank=True,
        default="",
        help_text="The 10-digit ISBN (without hyphens).",
    )
    isbn_13 = models.CharField(
        max_length=13,
        blank=True,
        default="",
        help_text="The 13-digit ISBN (without hyphens).",
    )
    publisher = models.CharField(
        max_length=255,
        blank=True,
        default="",
        help_text="The publisher of the book.",
    )
    edition = models.CharField(
        max_length=100,
        blank=True,
        default="",
        help_text="Edition information (e.g., '2nd Edition', 'Revised').",
    )
    format = models.CharField(
        max_length=50,
        blank=True,
        default="",
        help_text="Physical format (e.g., 'Hardcover', 'Paperback', 'Ebook').",
    )
    extent = models.CharField(
        max_length=255,
        blank=True,
        default="",
        help_text="Extent of the resource (e.g., 'xv, 320 pages').",
    )
    table_of_contents = models.TextField(
        blank=True,
        default="",
        help_text="Table of contents or chapter listing for the book.",
    )

    class Meta:
        db_table = "book"
        verbose_name = "Book"
        verbose_name_plural = "Books"
        indexes = [
            models.Index(fields=["isbn_10"]),
            models.Index(fields=["isbn_13"]),
            models.Index(fields=["publisher"]),
        ]

    @staticmethod
    def format_isbn(isbn: str) -> str:
        """Formats an ISBN string with hyphens.

        Args:
            isbn: A 10 or 13 digit ISBN string without formatting.

        Returns:
            The formatted ISBN string.
        """
        if not isbn:
            return ""

        clean_isbn = isbn.replace("-", "").replace(" ", "")
        match len(clean_isbn):
            case 13:
                return f"{clean_isbn[:3]}-{clean_isbn[3:4]}-{clean_isbn[4:6]}-{clean_isbn[6:12]}-{clean_isbn[12:]}"
            case 10:
                return f"{clean_isbn[:1]}-{clean_isbn[1:4]}-{clean_isbn[4:9]}-{clean_isbn[9:]}"
            case _:
                return isbn

    def save(self, *args, **kwargs) -> None:
        """Sets the type before saving.

        Args:
            *args: Variable length argument list.
            **kwargs: Arbitrary keyword arguments.
        """
        self.type = Item.ItemType.BOOK
        super().save(*args, **kwargs)

    def clean(self) -> None:
        """Performs model validation.

        Raises:
            ValidationError: If ISBN formats are invalid.
        """
        super().clean()

        if self.isbn_10:
            clean_isbn_10 = self.isbn_10.replace("-", "").replace(" ", "")
            if len(clean_isbn_10) != 10 or not clean_isbn_10.replace("X", "").isdigit():
                raise ValidationError({"isbn_10": "ISBN-10 must be 10 digits (last digit can be X)."})
            self.isbn_10 = clean_isbn_10

        if self.isbn_13:
            clean_isbn_13 = self.isbn_13.replace("-", "").replace(" ", "")
            if len(clean_isbn_13) != 13 or not clean_isbn_13.isdigit():
                raise ValidationError({"isbn_13": "ISBN-13 must be 13 digits."})
            self.isbn_13 = clean_isbn_13

    @property
    def creators(self) -> "QuerySet[Agent]":
        """For a Book, the primary creators are the Authors.

        Returns:
            A queryset of Agent instances.
        """
        from .relationships import ItemRoleName

        return self.get_agents_by_role(ItemRoleName.AUTHOR)

    @property
    def authors(self) -> "QuerySet[Agent]":
        """Returns all authors for this book.

        Returns:
            A queryset of Agent instances.
        """
        return self.creators

    @property
    def display_authors(self) -> str:
        """Returns a semicolon-separated string of authors.

        Returns:
            A string for display.
        """
        return self._get_agents_display_string(self.authors)

    @property
    def editors(self) -> "QuerySet[Agent]":
        """Returns all editors for this book.

        Returns:
            A queryset of Agent instances.
        """
        from .relationships import ItemRoleName

        return self.get_agents_by_role(ItemRoleName.EDITOR)

    @property
    def display_editors(self) -> str:
        """Returns a semicolon-separated string of editors.

        Returns:
            A string for display.
        """
        return self._get_agents_display_string(self.editors)

    @property
    def translators(self) -> "QuerySet[Agent]":
        """Returns all translators for this book.

        Returns:
            A queryset of Agent instances.
        """
        from .relationships import ItemRoleName

        return self.get_agents_by_role(ItemRoleName.TRANSLATOR)

    @property
    def display_translators(self) -> str:
        """Returns a semicolon-separated string of translators.

        Returns:
            A string for display.
        """
        return self._get_agents_display_string(self.translators)

    @property
    def display_isbn(self) -> str:
        """Returns a formatted ISBN for display.

        Returns:
            A string for display.
        """
        return self.format_isbn(self.primary_isbn)
