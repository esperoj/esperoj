"""Item models for the esperoj application.

This module contains models for catalogued items in the digital preservation
system, including the base Item model and specific item types like Song and Book.
"""

import datetime
from typing import TYPE_CHECKING, Union

from django.core.exceptions import ValidationError
from django.core.validators import MaxValueValidator, MinValueValidator
from django.db import models
from django.db.models import CheckConstraint, Index, Manager, Q
from django.db.models.signals import pre_save
from django.dispatch import receiver
from django.template.defaultfilters import date as date_filter

from .base import BaseModel

if TYPE_CHECKING:
    from django.db.models import QuerySet

    from .core import Agent
    from .relationships import ItemExternalReference, ItemRelationship, ItemRoleName, Role


class ItemType(models.TextChoices):
    """Enumeration for the type of a cataloged Item."""

    SONG = "SONG", "Song"
    BOOK = "BOOK", "Book"
    AUDIOBOOK = "AUDIOBOOK", "Audiobook"
    COMIC = "COMIC", "Comic"
    TEXT = "TEXT", "Text"
    MOVIE = "MOVIE", "Movie"
    GAME = "GAME", "Game"
    IMAGE = "IMAGE", "Image"
    VIDEO = "VIDEO", "Video"
    AUDIO = "AUDIO", "Audio"


class Item(BaseModel):
    """The concrete base model for all cataloged objects in the system.

    This model uses multi-table inheritance, where each subclass (like Book or
    Song) gets its own table with a one-to-one link to this base Item table.
    It represents any cataloged item in the digital preservation system.

    Attributes:
        title: The main title or name of the item.
        alternative_titles: A list of alternative titles for the item.
        identifier: A unique, human-readable identifier for the item.
        item_type: The type of the item (e.g., Book, Song).
        description: A free-text description of the item.
        languages: A list of language codes associated with the item.
        notes: Internal notes about the item.
        year: The year of the item's creation or publication.
        month: The month of the item's creation or publication.
        day: The day of the item's creation or publication.
        date: A denormalized DateField for sorting and filtering.
        people: People who contributed to this item.
        subjects: Topics/keywords associated with this item.
        collections: Collections this item belongs to.
    """

    # --- Core Information ---
    title = models.CharField(
        max_length=512,
        db_index=True,
        help_text="The main title or name of the item.",
    )
    alternative_titles = models.JSONField(
        blank=True,
        default=list,
        help_text="A list of alternative titles for this item.",
    )
    identifier = models.SlugField(
        max_length=255,
        unique=True,
        help_text="A unique, human-readable identifier for this item.",
    )
    item_type = models.CharField(
        max_length=20,
        choices=ItemType.choices,
        editable=False,
        help_text="The type of this item, set automatically by the subclass.",
    )
    description = models.TextField(
        blank=True,
        default="",
        help_text="A free-text description of the item.",
    )
    languages = models.JSONField(
        blank=True,
        default=list,
        help_text="A list of language codes associated with this item (e.g., ['en', 'fr']).",
    )
    notes = models.TextField(
        blank=True,
        default="",
        help_text="Internal notes about the item.",
    )

    # --- Date Fields ---
    year = models.IntegerField(
        null=True,
        blank=True,
        help_text="The year of publication or creation. Use a negative number for BCE years.",
    )
    month = models.PositiveSmallIntegerField(
        null=True,
        blank=True,
        validators=[MinValueValidator(1), MaxValueValidator(12)],
        help_text="The month of publication or creation (1-12).",
    )
    day = models.PositiveSmallIntegerField(
        null=True,
        blank=True,
        validators=[MinValueValidator(1), MaxValueValidator(31)],
        help_text="The day of publication or creation (1-31).",
    )
    date = models.DateField(
        null=True,
        blank=True,
        editable=False,
        help_text="A denormalized date field, automatically set from year, month, and day for sorting.",
    )

    # --- Relationships ---
    people = models.ManyToManyField(
        "esperoj.Person",
        through="esperoj.Role",
        related_name="items",
        blank=True,
        help_text="All people who contributed to this item.",
    )
    subjects = models.ManyToManyField(
        "esperoj.Subject",
        related_name="items",
        blank=True,
        help_text="Topics or keywords associated with this item.",
    )
    collections = models.ManyToManyField(
        "esperoj.Collection",
        related_name="items",
        blank=True,
        help_text="Collections this item belongs to.",
    )

    # --- Type hints for reverse relationships ---
    roles: "Manager[Role]"
    external_references: "Manager[ItemExternalReference]"
    outgoing_relationships: "Manager[ItemRelationship]"
    incoming_relationships: "Manager[ItemRelationship]"

    class Meta:
        db_table = "item"
        ordering = ["-year", "-month", "-day", "identifier"]
        verbose_name = "Item"
        verbose_name_plural = "Items"
        indexes = [
            Index(fields=["-year", "-month", "-day", "identifier"]),
            Index(fields=["identifier"]),
            Index(fields=["title"]),
            Index(fields=["item_type"]),
            Index(fields=["year"]),
            Index(fields=["date"]),
        ]
        constraints = [
            CheckConstraint(condition=Q(month__isnull=True) | Q(year__isnull=False), name="month_requires_year"),  # type: ignore
            CheckConstraint(condition=Q(day__isnull=True) | Q(month__isnull=False), name="day_requires_month"),  # type: ignore
        ]

    def __str__(self) -> str:
        """Returns the item's title."""
        return self.title

    def clean(self) -> None:
        """Performs model validation that cannot be handled by the database."""
        super().clean()

        # Validate that the date components form a valid date, only if the year is non-negative.
        # Negative years are allowed for historical or speculative contexts and do not form a 'valid'
        # datetime.date object, so this validation is skipped for them.
        if self.year is not None and self.month is not None and self.day is not None and self.year >= 0:
            try:
                datetime.date(self.year, self.month, self.day)
            except ValueError as e:
                raise ValidationError({"day": f"Invalid date: {e}"})

        # Validate languages are valid ISO codes (basic check)
        if self.languages:
            if not isinstance(self.languages, list):
                raise ValidationError({"languages": "Languages must be a list of language codes."})

            for lang in self.languages:
                if not isinstance(lang, str) or len(lang) < 2 or len(lang) > 10:
                    raise ValidationError(
                        {"languages": f"Invalid language code: {lang}. Must be 2-10 character strings."}
                    )

        # Validate alternative titles
        if self.alternative_titles:
            if not isinstance(self.alternative_titles, list):
                raise ValidationError({"alternative_titles": "Alternative titles must be a list of strings."})

            for alt_title in self.alternative_titles:
                if not isinstance(alt_title, str):
                    raise ValidationError(
                        {"alternative_titles": f"Invalid alternative title: {alt_title}. Must be a string."}
                    )

    def get_people_by_role(self, role: Union[str, "ItemRoleName"]) -> "QuerySet[Person]":
        """Returns a queryset of people with a specific role for this item.

        Uses select_related to prevent N+1 queries when accessing person data.

        Args:
            role: The role name or ItemRoleName instance to filter by.

        Returns:
            A queryset of Person instances.
        """
        return self.people.filter(roles__name=role, roles__item=self).select_related().order_by("roles__order")

    @property
    def creators(self) -> "QuerySet[Person]":
        """Abstract property for primary creators.

        Subclasses MUST override this property to define which roles
        are considered primary creators for that item type.

        Raises:
            NotImplementedError: If the subclass does not implement this property.
        """
        raise NotImplementedError(f"{self.__class__.__name__} must implement the 'creators' property.")

    @property
    def display_creators(self) -> str:
        """Returns a comma-separated string of primary creators."""
        return self._get_people_display_string(self.creators)

    @property
    def contributors(self) -> "QuerySet[Person]":
        """Returns all people who contributed but are not primary creators."""
        creator_pks = self.creators.values_list("pk", flat=True)
        return self.people.exclude(pk__in=creator_pks).select_related()

    @property
    def display_contributors(self) -> str:
        """Returns a comma-separated string of secondary contributors."""
        return self._get_people_display_string(self.contributors)

    @property
    def display_languages(self) -> str:
        """Returns a comma-separated string of languages."""
        if self.languages:
            return ", ".join(self.languages)
        return ""

    @property
    def display_alternative_titles(self) -> str:
        """Returns a comma-separated string of alternative titles."""
        if self.alternative_titles:
            return ", ".join(self.alternative_titles)
        return ""

    @property
    def display_date(self) -> str:
        """Returns a formatted date string for display."""
        if self.year is None:
            return ""

        year_str = f"{abs(self.year)}"
        if self.year < 0:
            year_str += " BCE"

        if self.month is None:
            return year_str

        try:
            # Use a dummy date to leverage Django's date template helper for localized formatting
            dummy_date = datetime.date(2000, self.month, self.day or 1)
            if self.day is None:
                return f"{date_filter(dummy_date, 'F')} {year_str}"
            return f"{date_filter(dummy_date, 'F j')}, {year_str}"
        except (ValueError, TypeError):
            return year_str

    def get_related_items(self, relationship_type: str | None = None) -> "QuerySet[Item]":
        """Returns items related to this item through any relationship.

        Args:
            relationship_type: Optional filter by relationship type.

        Returns:
            A queryset of related Item instances.
        """
        filters = models.Q()

        if relationship_type:
            filters = models.Q(
                models.Q(
                    incoming_relationships__from_item=self, incoming_relationships__relationship_type=relationship_type
                )
                | models.Q(
                    outgoing_relationships__to_item=self, outgoing_relationships__relationship_type=relationship_type
                )
            )
        else:
            filters = models.Q(
                models.Q(incoming_relationships__from_item=self) | models.Q(outgoing_relationships__to_item=self)
            )

        related_items = Item.objects.filter(filters).distinct()
        return related_items

    def get_parent_items(self) -> "QuerySet[Item]":
        """Returns items that this item is part of."""
        from .relationships import ItemRelationshipType

        return Item.objects.filter(
            incoming_relationships__from_item=self,
            incoming_relationships__relationship_type=ItemRelationshipType.PART_OF,
        ).distinct()

    def get_child_items(self) -> "QuerySet[Item]":
        """Returns items that are part of this item."""
        from .relationships import ItemRelationshipType

        return Item.objects.filter(
            incoming_relationships__from_item=self,
            incoming_relationships__relationship_type=ItemRelationshipType.CONTAINS,
        ).distinct()

    def get_sequential_items(self, direction: str = "both") -> "QuerySet[Item]":
        """Returns items in a sequence with this item.

        Args:
            direction: Direction of sequence ("both", "next", or "previous").

        Returns:
            A queryset of sequential Item instances.
        """
        from .relationships import ItemRelationshipType

        if direction == "next":
            return Item.objects.filter(
                incoming_relationships__from_item=self,
                incoming_relationships__relationship_type__in=[
                    ItemRelationshipType.SEQUEL_TO,
                    ItemRelationshipType.FOLLOWS,
                ],
            ).distinct()
        elif direction == "previous":
            return Item.objects.filter(
                outgoing_relationships__to_item=self,
                outgoing_relationships__relationship_type__in=[
                    ItemRelationshipType.SEQUEL_TO,
                    ItemRelationshipType.FOLLOWS,
                ],
            ).distinct()
        else:  # both
            return Item.objects.filter(
                models.Q(
                    incoming_relationships__from_item=self,
                    incoming_relationships__relationship_type__in=[
                        ItemRelationshipType.SEQUEL_TO,
                        ItemRelationshipType.FOLLOWS,
                        ItemRelationshipType.PREQUEL_TO,
                        ItemRelationshipType.PRECEDES,
                    ],
                )
                | models.Q(
                    outgoing_relationships__to_item=self,
                    outgoing_relationships__relationship_type__in=[
                        ItemRelationshipType.SEQUEL_TO,
                        ItemRelationshipType.FOLLOWS,
                        ItemRelationshipType.PREQUEL_TO,
                        ItemRelationshipType.PRECEDES,
                    ],
                )
            ).distinct()


@receiver(pre_save, sender=Item)
def update_item_date(_sender, instance, **_kwargs) -> None:
    """Signal to automatically set the 'date' field before an Item is saved."""
    if isinstance(instance, Item):
        if instance.year and instance.year > 0:
            month = instance.month or 1
            day = instance.day or 1
            try:
                instance.date = datetime.date(instance.year, month, day)
            except ValueError:
                # Handles invalid dates like Feb 30 by leaving the date null
                instance.date = None
        else:
            # Handles BCE years or cases where no date should be set
            instance.date = None


class Representation(BaseModel):
    """
    A specific digital embodiment of an Intellectual Entity.

    A single entity (e.g., "Episode 1") may have multiple representations,
    such as a "High Quality Preservation Master" and a "Low Res Access Copy".

    Attributes:
        item (Item): The content this represents.
        name (str): Label for this version (e.g., 'Preservation Master').
    """

    item = models.ForeignKey(Item, on_delete=models.CASCADE, related_name="representations")
    name = models.CharField(max_length=255)

    class Meta:
        db_table = "representation"

    def __str__(self) -> str:
        return f"{self.name} of {self.item.title}"


class Song(Item):
    """A musical composition and/or recording.

    This model merges the concepts of a musical work and a recording into a single
    entity. It represents both the abstract song (music and lyrics) and its
    recorded performance.

    Attributes:
        album: The album or release this song belongs to.
    """

    album = models.CharField(
        max_length=255,
        blank=True,
        default="",
        help_text="The album or release this song belongs to.",
    )

    class Meta:
        db_table = "song"
        verbose_name = "Song"
        verbose_name_plural = "Songs"
        ordering = ["title"]
        indexes = [
            Index(fields=["album"]),
        ]

    def save(self, *args, **kwargs) -> None:
        """Sets the item_type before saving."""
        self.item_type = ItemType.SONG
        super().save(*args, **kwargs)

    @property
    def creators(self) -> "QuerySet[Person]":
        """For a Song, primary creators are Artists."""
        from .relationships import ItemRoleName

        return self.get_people_by_role(ItemRoleName.ARTIST)

    @property
    def composers(self) -> "QuerySet[Person]":
        """Returns all people credited as composers for this song."""
        from .relationships import ItemRoleName

        return self.get_people_by_role(ItemRoleName.COMPOSER)

    @property
    def display_composers(self) -> str:
        """Returns a comma-separated string of composers."""
        return self._get_people_display_string(self.composers)

    @property
    def lyricists(self) -> "QuerySet[Person]":
        """Returns all people credited as lyricists for this song."""
        from .relationships import ItemRoleName

        return self.get_people_by_role(ItemRoleName.LYRICIST)

    @property
    def display_lyricists(self) -> str:
        """Returns a comma-separated string of lyricists."""
        return self._get_people_display_string(self.lyricists)

    @property
    def artists(self) -> "QuerySet[Person]":
        """Returns all performing artists for this song."""
        from .relationships import ItemRoleName

        return self.get_people_by_role(ItemRoleName.ARTIST)

    @property
    def display_artists(self) -> str:
        """Returns a comma-separated string of artists."""
        return self._get_people_display_string(self.artists)


class Book(Item):
    """A book or written publication.

    Attributes:
        isbn_10: The 10-digit International Standard Book Number.
        isbn_13: The 13-digit International Standard Book Number.
        page_count: The number of pages in the book.
        publisher: The publisher of the book.
        edition: The edition information.
        format: The physical format (hardcover, paperback, etc.).
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
    page_count = models.PositiveIntegerField(
        null=True,
        blank=True,
        help_text="The number of pages in the book.",
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

    class Meta:
        db_table = "book"
        verbose_name = "Book"
        verbose_name_plural = "Books"
        indexes = [
            Index(fields=["isbn_10"]),
            Index(fields=["isbn_13"]),
            Index(fields=["publisher"]),
            Index(fields=["page_count"]),
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
        if len(clean_isbn) == 13:
            # Standard ISBN-13 format: 3-1-2-6-1
            return f"{clean_isbn[:3]}-{clean_isbn[3:4]}-{clean_isbn[4:6]}-{clean_isbn[6:12]}-{clean_isbn[12:]}"
        if len(clean_isbn) == 10:
            # Standard ISBN-10 format: 1-3-5-1
            return f"{clean_isbn[:1]}-{clean_isbn[1:4]}-{clean_isbn[4:9]}-{clean_isbn[9:]}"

        return isbn

    def save(self, *args, **kwargs) -> None:
        """Sets the item_type before saving."""
        self.item_type = ItemType.BOOK
        super().save(*args, **kwargs)

    def clean(self) -> None:
        """Performs model validation."""
        super().clean()

        # Basic ISBN validation
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
    def creators(self) -> "QuerySet[Person]":
        """For a Book, the primary creators are the Authors."""
        from .relationships import ItemRoleName

        return self.get_people_by_role(ItemRoleName.AUTHOR)

    @property
    def authors(self) -> "QuerySet[Person]":
        """Returns all authors for this book."""
        return self.creators

    @property
    def display_authors(self) -> str:
        """Returns a comma-separated string of authors."""
        return self._get_people_display_string(self.authors)

    @property
    def editors(self) -> "QuerySet[Person]":
        """Returns all editors for this book."""
        from .relationships import ItemRoleName

        return self.get_people_by_role(ItemRoleName.EDITOR)

    @property
    def display_editors(self) -> str:
        """Returns a comma-separated string of editors."""
        return self._get_people_display_string(self.editors)

    @property
    def translators(self) -> "QuerySet[Person]":
        """Returns all translators for this book."""
        from .relationships import ItemRoleName

        return self.get_people_by_role(ItemRoleName.TRANSLATOR)

    @property
    def display_translators(self) -> str:
        """Returns a comma-separated string of translators."""
        return self._get_people_display_string(self.translators)

    @property
    def primary_isbn(self) -> str:
        """Returns the primary ISBN (preferring ISBN-13)."""
        return self.isbn_13 or self.isbn_10

    @property
    def display_isbn(self) -> str:
        """Returns a formatted ISBN for display."""
        return self.format_isbn(self.primary_isbn)

    @property
    def has_isbn(self) -> bool:
        """Returns True if the book has any ISBN."""
        return bool(self.isbn_10 or self.isbn_13)
