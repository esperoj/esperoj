import datetime
from typing import TYPE_CHECKING, Union

from django.core.exceptions import ValidationError
from django.core.validators import MaxValueValidator, MinValueValidator
from django.db import models
from django.db.models import CheckConstraint, Index, Q
from django.db.models.signals import pre_save
from django.dispatch import receiver

from .base import BaseModel
from .entities import Person, Role, ItemRoleName

if TYPE_CHECKING:
    from django.db.models import QuerySet
    from .files import File  # Added for File model type hinting


class ItemType(models.TextChoices):
    """Enumeration for the type of a cataloged Item."""

    SONG = "SONG", "Song"
    BOOK = "BOOK", "Book"


class ItemManager(models.Manager):
    """Custom manager for the Item model."""

    def get_by_type(self, item_type: ItemType) -> "QuerySet[Item]":
        """Returns all items of a specific type."""
        return self.filter(item_type=item_type)


class Item(BaseModel):
    """The concrete base model for all cataloged objects in the system.

    This model uses multi-table inheritance, where each subclass (like Book or
    Song) gets its own table with a one-to-one link to this base Item
    table.

    Attributes:
        title: The main title or name of the item.
        identifier: A unique, URL-friendly slug for the item.
        item_type: The type of the item (e.g., Book, Song).
        description: A free-text description of the item.
        languages: A JSONField storing a list of languages associated with the item.
        people: A many-to-many relationship to all people who contributed.
        files: A reverse many-to-many relationship to associated File objects.
        year: The year of the item's creation or publication.
        month: The month of the item's creation or publication.
        day: The day of the item's creation or publication.
        date: A denormalized DateField for sorting and filtering.
    """

    # --- Core Information ---
    title = models.CharField(max_length=255, help_text="The main title or name of the item.")
    identifier = models.SlugField(
        max_length=255, unique=True, help_text="A unique, human-readable identifier for this item."
    )
    item_type = models.CharField(
        max_length=20,
        choices=ItemType.choices,
        editable=False,
        help_text="The type of this item, set automatically by the subclass.",
    )
    description = models.TextField(blank=True, null=True, help_text="A free-text description of the item.")
    languages = models.JSONField(
        blank=True,
        default=list,
        help_text="A list of languages associated with this item (e.g., ['en', 'fr']).",
    )

    # --- Relationships ---
    # Using the Role model from entities.py as the through table
    people = models.ManyToManyField(
        Person, through=Role, related_name="items", blank=True, help_text="All people who contributed to this item."
    )
    # Type hint for the reverse relationship to File objects
    files: "models.Manager[File]"

    # --- Date Fields ---
    year = models.IntegerField(
        null=True, blank=True, help_text="The year of publication or creation. Use a negative number for BC years."
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

    # --- Manager ---
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
        ]
        constraints = [
            CheckConstraint(condition=Q(month__isnull=True) | Q(year__isnull=False), name="month_requires_year"),  # type: ignore
            CheckConstraint(condition=Q(day__isnull=True) | Q(month__isnull=False), name="day_requires_month"),  # type: ignore
        ]

    def __str__(self):
        """Returns the item's title."""
        return self.title

    def clean(self):
        """Performs model validation that cannot be handled by the database."""
        super().clean()
        if self.year and self.month and self.day:
            try:
                # This validates that the date is real (e.g., not February 30th)
                datetime.date(self.year, self.month, self.day)
            except ValueError as e:
                raise ValidationError({"day": f"Invalid date: {e}"})

    def _get_people_display_string(self, queryset: "QuerySet[Person]") -> str:
        """Helper method to format a queryset of people into a display string."""
        return ", ".join(person.authorized_name for person in queryset.distinct())

    def get_people_by_role(self, role: Union[ItemRoleName, str]) -> "QuerySet[Person]":
        """Returns a queryset of people with a specific role for this item.

        The 'roles' lookup refers to the related_name on the ForeignKey 'person'
        and 'item' in the Role model, allowing filtering through the
        Many-to-Many relationship.
        """
        return self.people.filter(roles__name=role, roles__item=self)

    @property
    def creators(self) -> "QuerySet[Person]":
        """Abstract property for primary creators.

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
        return self.people.exclude(pk__in=creator_pks)

    @property
    def display_contributors(self) -> str:
        """Returns a comma-separated string of secondary contributors."""
        return self._get_people_display_string(self.contributors)


@receiver(pre_save, sender=Item)
def update_item_date(sender, instance, **kwargs):
    """A signal to automatically set the 'date' field before an Item is saved."""
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
            # Handles BC years or cases where no date should be set
            instance.date = None


class Song(Item):
    """A musical composition and/or recording.

    This model merges the concepts of a MusicalWork and a Recording into a single
    entity. It represents both the abstract song (music and lyrics) and its
    recorded performance.
    """

    class Meta:
        db_table = "song"
        verbose_name = "Song"
        verbose_name_plural = "Songs"

    def save(self, *args, **kwargs):
        """Sets the item_type before saving."""
        self.item_type = ItemType.SONG
        super().save(*args, **kwargs)

    @property
    def creators(self) -> "QuerySet[Person]":
        """For a Song, primary creators are Composers, Lyricists, and Artists."""
        return self.people.filter(roles__item=self, roles__name__in=[ItemRoleName.ARTIST])

    @property
    def composers(self) -> "QuerySet[Person]":
        """Returns all people credited as composers for this song."""
        return self.get_people_by_role(ItemRoleName.COMPOSER)

    @property
    def display_composers(self) -> str:
        """Returns a comma-separated string of composers."""
        return self._get_people_display_string(self.composers)

    @property
    def lyricists(self) -> "QuerySet[Person]":
        """Returns all people credited as lyricists for this song."""
        return self.get_people_by_role(ItemRoleName.LYRICIST)

    @property
    def display_lyricists(self) -> str:
        """Returns a comma-separated string of lyricists."""
        return self._get_people_display_string(self.lyricists)

    @property
    def artists(self) -> "QuerySet[Person]":
        """Returns all performing artists for this song."""
        return self.get_people_by_role(ItemRoleName.ARTIST)

    @property
    def display_artists(self) -> str:
        """Returns a comma-separated string of artists."""
        return self._get_people_display_string(self.artists)


class Book(Item):
    """A book.

    Attributes:
        subtitle: The book's subtitle.
        isbn_10: The 10-digit International Standard Book Number.
        isbn_13: The 13-digit International Standard Book Number.
    """

    # --- Book Details ---
    subtitle = models.CharField(max_length=255, blank=True, null=True, help_text="The subtitle of the book, if any.")
    isbn_10 = models.CharField(max_length=10, blank=True, help_text="The 10-digit ISBN.")
    isbn_13 = models.CharField(max_length=13, blank=True, help_text="The 13-digit ISBN.")

    class Meta:
        db_table = "book"
        verbose_name = "Book"
        verbose_name_plural = "Books"

    def save(self, *args, **kwargs):
        """Sets the item_type before saving."""
        self.item_type = ItemType.BOOK
        super().save(*args, **kwargs)

    @property
    def creators(self) -> "QuerySet[Person]":
        """For a Book, the primary creators are the Authors."""
        return self.get_people_by_role(ItemRoleName.AUTHOR)

    @property
    def authors(self) -> "QuerySet[Person]":
        """Returns all authors for this book."""
        return self.creators

    @property
    def display_authors(self) -> str:
        """Returns a comma-separated string of authors."""
        return self._get_people_display_string(self.authors)
