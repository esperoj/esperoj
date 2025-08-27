"""
Item models for the esperoj application.

This module contains models for catalogued items in the digital preservation
system, including the base Item model and specific item types like Song and Book.
"""

import datetime
from typing import TYPE_CHECKING, Union

from django.core.exceptions import ValidationError
from django.core.validators import MaxValueValidator, MinValueValidator
from django.db import models
from django.db.models import CheckConstraint, Index, Q, Manager
from django.db.models.signals import pre_save
from django.dispatch import receiver

from .base import BaseModel

if TYPE_CHECKING:
    from django.db.models import QuerySet
    from .core import Person, Subject, Collection
    from .files import File
    from .relationships import Role, ItemExternalReference, ItemRoleName


class ItemType(models.TextChoices):
    """Enumeration for the type of a cataloged Item."""

    SONG = "SONG", "Song"
    BOOK = "BOOK", "Book"
    DOCUMENT = "DOCUMENT", "Document"
    IMAGE = "IMAGE", "Image"
    VIDEO = "VIDEO", "Video"
    AUDIO = "AUDIO", "Audio"


class ItemManager(Manager):
    """Custom manager for the Item model providing common query methods."""

    def get_by_type(self, item_type: ItemType) -> "QuerySet[Item]":
        """Returns all items of a specific type."""
        return self.filter(item_type=item_type)

    def with_files(self):
        """Returns items that have associated files."""
        return self.filter(files__isnull=False).distinct()

    def by_year(self, year: int):
        """Returns items from a specific year."""
        return self.filter(year=year)

    def by_date_range(self, start_date=None, end_date=None):
        """Returns items within a date range."""
        queryset = self.all()
        if start_date:
            queryset = queryset.filter(date__gte=start_date)
        if end_date:
            queryset = queryset.filter(date__lte=end_date)
        return queryset

    def with_people(self):
        """Returns items that have associated people."""
        return self.filter(people__isnull=False).distinct()

    def by_language(self, language_code: str):
        """Returns items in a specific language."""
        return self.filter(languages__contains=[language_code])


class Item(BaseModel):
    """
    The concrete base model for all cataloged objects in the system.

    This model uses multi-table inheritance, where each subclass (like Book or
    Song) gets its own table with a one-to-one link to this base Item table.
    It represents any cataloged item in the digital preservation system.

    Attributes:
        title: The main title or name of the item.
        subtitle: An optional subtitle for the item.
        identifier: A unique, URL-friendly slug for the item.
        item_type: The type of the item (e.g., Book, Song).
        description: A free-text description of the item.
        languages: A JSONField storing a list of languages associated with the item.
        notes: Internal notes about the item.

    Date Information:
        year: The year of the item's creation or publication.
        month: The month of the item's creation or publication.
        day: The day of the item's creation or publication.
        date: A denormalized DateField for sorting and filtering.

    Relationships:
        people: People who contributed to this item (through Role model).
        subjects: Topics/keywords associated with this item.
        collections: Collections this item belongs to.
        files: Digital files associated with this item.
        external_references: External links related to this item.
        roles: Specific roles people play for this item.
    """

    # --- Core Information ---
    title = models.CharField(
        max_length=512,
        db_index=True,
        help_text="The main title or name of the item.",
    )
    subtitle = models.CharField(
        max_length=512,
        blank=True,
        default="",
        help_text="An optional subtitle for the item.",
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
    files = models.ManyToManyField(
        "esperoj.File",
        related_name="items",
        blank=True,
        help_text="Digital files associated with this item.",
    )

    # --- Type hints for reverse relationships ---
    roles: "Manager[Role]"
    external_references: "Manager[ItemExternalReference]"

    objects = ItemManager()

    class Meta:
        db_table = "item"
        ordering = ["-date", "identifier"]
        verbose_name = "Item"
        verbose_name_plural = "Items"
        indexes = [
            Index(fields=["-date", "identifier"]),
            Index(fields=["identifier"]),
            Index(fields=["title"]),
            Index(fields=["item_type"]),
            Index(fields=["year"]),
            Index(fields=["date"]),
        ]
        constraints = [
            CheckConstraint(check=Q(month__isnull=True) | Q(year__isnull=False), name="month_requires_year"),
            CheckConstraint(check=Q(day__isnull=True) | Q(month__isnull=False), name="day_requires_month"),
        ]

    def __str__(self) -> str:
        """Returns the item's title."""
        return self.title

    def clean(self) -> None:
        """Performs model validation that cannot be handled by the database."""
        super().clean()

        # Validate that the date components form a valid date
        if self.year and self.month and self.day:
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

    def _get_people_display_string(self, queryset: "QuerySet[Person]") -> str:
        """Helper method to format a queryset of people into a display string."""
        return ", ".join(person.authorized_name for person in queryset.distinct())

    def get_people_by_role(self, role: Union[str, "ItemRoleName"]) -> "QuerySet[Person]":
        """
        Returns a queryset of people with a specific role for this item.

        Uses select_related to prevent N+1 queries when accessing person data.
        """
        return self.people.filter(roles__name=role, roles__item=self).select_related().order_by("roles__order")

    @property
    def creators(self) -> "QuerySet[Person]":
        """
        Abstract property for primary creators.

        Subclasses MUST override this property to define which roles
        are considered primary creators for that item type.
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
    def full_title(self) -> str:
        """Returns the full title including subtitle if present."""
        if self.subtitle:
            return f"{self.title}: {self.subtitle}"
        return self.title

    @property
    def display_languages(self) -> str:
        """Returns a comma-separated string of languages."""
        if self.languages:
            return ", ".join(self.languages)
        return ""

    @property
    def has_date(self) -> bool:
        """Returns True if the item has at least a year."""
        return self.year is not None

    @property
    def display_date(self) -> str:
        """Returns a formatted date string for display."""
        if not self.year:
            return "Unknown date"

        if self.year < 0:
            year_str = f"{abs(self.year)} BCE"
        else:
            year_str = str(self.year)

        if self.month and self.day:
            try:
                date_obj = datetime.date(abs(self.year), self.month, self.day)
                return f"{date_obj.strftime('%B %d')}, {year_str}"
            except ValueError:
                pass

        if self.month:
            try:
                month_name = datetime.date(2000, self.month, 1).strftime("%B")
                return f"{month_name} {year_str}"
            except ValueError:
                pass

        return year_str

    def get_primary_file(self):
        """Returns the primary file associated with this item, if any."""
        return self.files.first()

    def get_file_count(self) -> int:
        """Returns the number of files associated with this item."""
        return self.files.count()


@receiver(pre_save, sender=Item)
def update_item_date(sender, instance, **kwargs) -> None:
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


class SongManager(Manager):
    """Custom manager for the Song model."""

    def by_artist(self, artist_name: str):
        """Returns songs by a specific artist."""
        return self.filter(people__authorized_name__icontains=artist_name, roles__name="Artist").distinct()

    def by_composer(self, composer_name: str):
        """Returns songs by a specific composer."""
        return self.filter(people__authorized_name__icontains=composer_name, roles__name="Composer").distinct()


class Song(Item):
    """
    A musical composition and/or recording.

    This model merges the concepts of a musical work and a recording into a single
    entity. It represents both the abstract song (music and lyrics) and its
    recorded performance.

    Additional Attributes:
        duration_seconds: The duration of the recording in seconds.
        bpm: Beats per minute (tempo).
        key_signature: The musical key of the song.
        track_number: Track number if part of an album.
        disc_number: Disc number if part of a multi-disc release.
    """

    # --- Song-specific fields ---
    duration_seconds = models.PositiveIntegerField(
        null=True,
        blank=True,
        help_text="The duration of the recording in seconds.",
    )
    bpm = models.PositiveSmallIntegerField(
        null=True,
        blank=True,
        help_text="Beats per minute (tempo) of the song.",
    )
    key_signature = models.CharField(
        max_length=10,
        blank=True,
        default="",
        help_text="The musical key of the song (e.g., 'C major', 'A minor').",
    )
    track_number = models.PositiveSmallIntegerField(
        null=True,
        blank=True,
        help_text="Track number if part of an album or collection.",
    )
    disc_number = models.PositiveSmallIntegerField(
        null=True,
        blank=True,
        default=1,
        help_text="Disc number if part of a multi-disc release.",
    )

    objects = SongManager()

    class Meta:
        db_table = "song"
        verbose_name = "Song"
        verbose_name_plural = "Songs"
        ordering = ["track_number", "title"]
        indexes = [
            Index(fields=["track_number"]),
            Index(fields=["disc_number", "track_number"]),
            Index(fields=["duration_seconds"]),
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

    @property
    def display_duration(self) -> str:
        """Returns a formatted duration string (e.g., '3:45')."""
        if not self.duration_seconds:
            return ""

        minutes = self.duration_seconds // 60
        seconds = self.duration_seconds % 60
        return f"{minutes}:{seconds:02d}"

    @property
    def is_part_of_album(self) -> bool:
        """Returns True if this song has a track number."""
        return self.track_number is not None


class BookManager(Manager):
    """Custom manager for the Book model."""

    def by_author(self, author_name: str):
        """Returns books by a specific author."""
        return self.filter(people__authorized_name__icontains=author_name, roles__name="Author").distinct()

    def by_isbn(self, isbn: str):
        """Returns books matching an ISBN (10 or 13 digit)."""
        # Remove hyphens and spaces from ISBN
        clean_isbn = isbn.replace("-", "").replace(" ", "")
        return self.filter(models.Q(isbn_10=clean_isbn) | models.Q(isbn_13=clean_isbn))

    def published_in_year(self, year: int):
        """Returns books published in a specific year."""
        return self.filter(year=year)


class Book(Item):
    """
    A book or written publication.

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

    objects = BookManager()

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
        if self.isbn_13:
            # Format ISBN-13: 978-0-123-45678-9
            isbn = self.isbn_13
            return f"{isbn[:3]}-{isbn[3]}-{isbn[4:7]}-{isbn[7:12]}-{isbn[12]}"
        elif self.isbn_10:
            # Format ISBN-10: 0-123-45678-9
            isbn = self.isbn_10
            return f"{isbn[0]}-{isbn[1:4]}-{isbn[4:9]}-{isbn[9]}"
        return ""

    @property
    def has_isbn(self) -> bool:
        """Returns True if the book has any ISBN."""
        return bool(self.isbn_10 or self.isbn_13)
