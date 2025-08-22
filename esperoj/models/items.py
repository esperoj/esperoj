import datetime

from django.conf import settings
from django.core.exceptions import ValidationError
from django.core.validators import MaxValueValidator, MinValueValidator, URLValidator
from django.db import models
from django.db.models import Q, Index, CheckConstraint
from django.contrib.postgres.indexes import GinIndex

from .base import BaseModel
from .entities import Artist, Author, Collection, Subject
from .files import File


class LocalizedTitle(BaseModel):
    """
    A normalized, language-specific title for an Item.
    An item can have multiple titles in the same language.
    """

    item = models.ForeignKey("Item", on_delete=models.CASCADE, related_name="titles")
    language = models.CharField(
        max_length=10,
        default=settings.LANGUAGE_CODE,
        help_text="Language code (e.g., 'en', 'es', 'fr')",
    )
    title = models.CharField(max_length=255)

    class Meta:
        ordering = ["language", "title"]
        verbose_name = "Localized Title"
        verbose_name_plural = "Localized Titles"
        indexes = [Index(fields=["item", "language"])]
        db_table = "localized_title"

    def __str__(self):
        return f'"{self.title}" ({self.language})'


class WebLink(BaseModel):
    """A normalized URL associated with an Item."""

    item = models.ForeignKey("Item", on_delete=models.CASCADE, related_name="weblinks")
    url = models.URLField(max_length=2048, validators=[URLValidator()])
    description = models.CharField(max_length=255, blank=True, default="")

    class Meta:
        ordering = ["-created_at"]
        verbose_name = "Web Link"
        verbose_name_plural = "Web Links"
        db_table = "weblink"

    def __str__(self):
        return self.url


class ItemQuerySet(models.QuerySet):
    def latest(self):
        return self.order_by("-date", "-updated_at")


class Item(BaseModel):
    """An abstract base model for any content item, like a song, book, or video."""

    identifier = models.SlugField(max_length=255, unique=True)
    collections = models.ManyToManyField(Collection, related_name="items", blank=True)
    subjects = models.ManyToManyField(Subject, related_name="items", blank=True)
    languages = models.JSONField(
        default=list,
        blank=True,
        help_text="A cached list of language codes from associated titles.",
    )
    year = models.PositiveSmallIntegerField(null=True, blank=True, default=None)
    month = models.PositiveSmallIntegerField(
        null=True,
        blank=True,
        default=None,
        validators=[MinValueValidator(1), MaxValueValidator(12)],
    )
    day = models.PositiveSmallIntegerField(
        null=True,
        blank=True,
        default=None,
        validators=[MinValueValidator(1), MaxValueValidator(31)],
    )
    files = models.ManyToManyField(File, related_name="items", blank=True)
    date = models.DateField(null=True, blank=True, editable=False)

    objects = ItemQuerySet.as_manager()

    class Meta:
        ordering = ["-date"]
        verbose_name = "Item"
        verbose_name_plural = "Items"
        indexes = [
            Index(fields=["-date"]),
            Index(fields=["identifier"]),
            Index(fields=["updated_at"]),
            GinIndex(fields=["languages"]),
        ]
        constraints = [
            CheckConstraint(
                condition=Q(month__isnull=True) | Q(year__isnull=False),
                name="month_requires_year",
            ),
            CheckConstraint(
                condition=Q(day__isnull=True) | Q(month__isnull=False),
                name="day_requires_month",
            ),
        ]
        db_table = "item"

    def clean(self):
        if self.month and not self.year:
            raise ValidationError("Month cannot be set without a year.")
        if self.day and not self.month:
            raise ValidationError("Day cannot be set without a month.")
        if self.day and self.year and self.month:
            try:
                datetime.date(self.year, self.month, self.day)
            except ValueError:
                raise ValidationError(
                    f"Invalid day '{self.day}' for the given month and year."
                )

    def _update_date_field(self):
        """Constructs the 'date' field from year, month, and day."""
        if self.year:
            month = self.month or 1
            day = self.day or 1
            self.date = datetime.date(self.year, month, day)
        else:
            self.date = None

    def save(self, *args, **kwargs):
        self._update_date_field()
        self.full_clean()
        super().save(*args, **kwargs)

    def __str__(self):
        primary_title = self.titles.filter(language=settings.LANGUAGE_CODE).first()
        return (
            primary_title.title
            if primary_title
            else (self.titles.first().title if self.titles.exists() else self.identifier)
        )


class Song(Item):
    """A song item, with a specific relationship to Artists."""

    artists = models.ManyToManyField(Artist, related_name="songs", blank=True)

    @property
    def creators(self):
        """Returns the artists for this song, providing a consistent API with other item types."""
        return self.artists

    class Meta:
        verbose_name = "Song"
        verbose_name_plural = "Songs"
        db_table = "song"


class Book(Item):
    """A book item, with a specific relationship to Authors and ISBN fields."""

    authors = models.ManyToManyField(Author, related_name="books", blank=True)
    isbn_10 = models.CharField(max_length=10, null=True, blank=True)
    isbn_13 = models.CharField(max_length=13, null=True, blank=True)

    @property
    def creators(self):
        """Returns the authors for this book, providing a consistent API with other item types."""
        return self.authors

    class Meta:
        verbose_name = "Book"
        verbose_name_plural = "Books"
        db_table = "book"
