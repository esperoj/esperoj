import datetime
from typing import TYPE_CHECKING

from django.core.exceptions import ValidationError
from django.core.validators import MaxValueValidator, MinValueValidator
from django.db import models
from django.db.models import CheckConstraint, Index, Q
from django.db.models.signals import pre_save
from django.dispatch import receiver

from .base import BaseModel
from .entities import Person

if TYPE_CHECKING:
    from django.db.models import QuerySet


class ItemType(models.TextChoices):
    """Enumeration for the type of a cataloged Item."""

    MUSICAL_WORK = "MUSICAL_WORK", "Musical Work"
    RECORDING = "RECORDING", "Recording"
    BOOK = "BOOK", "Book"


class ContributionRole(models.TextChoices):
    """A unified list of all possible roles a Person can have in relation to an Item."""

    # Musical Roles
    COMPOSER = "COMPOSER", "Composer"
    LYRICIST = "LYRICIST", "Lyricist"
    ARTIST = "ARTIST", "Artist"  # The performer of a recording
    PRODUCER = "PRODUCER", "Producer"
    ENGINEER = "ENGINEER", "Engineer"

    # Literary Roles
    AUTHOR = "AUTHOR", "Author"
    EDITOR = "EDITOR", "Editor"
    TRANSLATOR = "TRANSLATOR", "Translator"


class Contribution(BaseModel):
    """A through model connecting a Person to an Item with a specific role.

    This model represents a single contribution, defining who did what for a
    given catalog item.

    Attributes:
        person: The person who made the contribution.
        item: The item that the person contributed to.
        role: The role the person had in the contribution.
    """
    person = models.ForeignKey(
        Person, on_delete=models.CASCADE, related_name="contributions",
        help_text="The person making the contribution."
    )
    item = models.ForeignKey(
        "Item", on_delete=models.CASCADE, related_name="contributions",
        help_text="The item being contributed to."
    )
    role = models.CharField(
        max_length=20, choices=ContributionRole.choices,
        help_text="The role of the person in this contribution."
    )

    class Meta:
        db_table = "contribution"
        unique_together = [["person", "item", "role"]]
        ordering = ["item", "role"]
        verbose_name = "Contribution"
        verbose_name_plural = "Contributions"
        indexes = [
            Index(fields=["person", "item", "role"]),
        ]

    def __str__(self):
        return f"{self.person} as {self.role} for {self.item}"


class ItemManager(models.Manager):
    """Custom manager for the Item model."""

    def get_by_type(self, item_type: ItemType) -> "QuerySet[Item]":
        """Returns all items of a specific type."""
        return self.filter(item_type=item_type)


class Item(BaseModel):
    """The concrete base model for all cataloged objects in the system.

    This model uses multi-table inheritance, where each subclass (like Book or
    Recording) gets its own table with a one-to-one link to this base Item
    table.

    Attributes:
        title: The main title or name of the item.
        identifier: A unique, URL-friendly slug for the item.
        item_type: The type of the item (e.g., Book, Recording).
        description: A free-text description of the item.
        people: A many-to-many relationship to all people who contributed.
        year: The year of the item's creation or publication.
        month: The month of the item's creation or publication.
        day: The day of the item's creation or publication.
        date: A denormalized DateField for sorting and filtering.
    """
    # --- Core Information ---
    title = models.CharField(
        max_length=255, help_text="The main title or name of the item."
    )
    identifier = models.SlugField(
        max_length=255, unique=True, help_text="A unique, URL-friendly slug for the item."
    )
    item_type = models.CharField(
        max_length=20, choices=ItemType.choices, editable=False,
        help_text="The type of this item, set automatically by the subclass."
    )
    description = models.TextField(
        blank=True, null=True, help_text="A free-text description of the item."
    )

    # --- Relationships ---
    people = models.ManyToManyField(
        Person, through=Contribution, related_name="items", blank=True,
        help_text="All people who contributed to this item."
    )

    # --- Date Fields ---
    year = models.IntegerField(
        null=True, blank=True, help_text="The year of publication or creation. Use a negative number for BC years."
    )
    month = models.PositiveSmallIntegerField(
        null=True, blank=True, validators=[MinValueValidator(1), MaxValueValidator(12)],
        help_text="The month of publication or creation (1-12)."
    )
    day = models.PositiveSmallIntegerField(
        null=True, blank=True, validators=[MinValueValidator(1), MaxValueValidator(31)],
        help_text="The day of publication or creation (1-31)."
    )
    date = models.DateField(
        null=True, blank=True, editable=False,
        help_text="A denormalized date field, automatically set from year, month, and day for sorting."
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
            Index(fields=["title"]), # Added index for title
            Index(fields=["item_type"]),
        ]
        constraints = [
            CheckConstraint(condition=Q(month__isnull=True) | Q(year__isnull=False), name="month_requires_year"), # type: ignore
            CheckConstraint(condition=Q(day__isnull=True) | Q(month__isnull=False), name="day_requires_month"), # type: ignore
        ]

    def __str__(self):
        """Returns the item's title."""
        return self.title # Changed to return title

    def clean(self):
        """Performs model validation that cannot be handled by the database."""
        super().clean()
        if self.year and self.month and self.day:
            try:
                # This validates that the date is real (e.g., not February 30th)
                datetime.date(self.year, self.month, self.day)
            except ValueError as e:
                raise ValidationError({"day": f"Invalid date: {e}"})

    def get_people_by_role(self, role: ContributionRole) -> "QuerySet[Person]":
        """Returns a queryset of people with a specific contribution role."""
        return self.people.filter(contributions__role=role, contributions__item=self)

    @property
    def creators(self) -> "QuerySet[Person]":
        """Abstract property for primary creators.

        Subclasses MUST override this property to define which contribution
        roles are considered primary creators for that item type.
        """
        raise NotImplementedError(f"{self.__class__.__name__} must implement the 'creators' property.")

    @property
    def contributors(self) -> "QuerySet[Person]":
        """Returns all people who contributed but are not primary creators."""
        creator_pks = self.creators.values_list("pk", flat=True)
        return self.people.exclude(pk__in=creator_pks)


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


class MusicalWork(Item):
    """An abstract musical composition or work.

    This represents the song itself (music and lyrics), distinct from any
    particular performance or recording of it.
    """

    class Meta:
        db_table = "musical_work"
        verbose_name = "Musical Work"
        verbose_name_plural = "Musical Works"

    def save(self, *args, **kwargs):
        """Sets the item_type before saving."""
        self.item_type = ItemType.MUSICAL_WORK
        super().save(*args, **kwargs)

    @property
    def creators(self) -> "QuerySet[Person]":
        """For a MusicalWork, creators are Composers and Lyricists."""
        return self.people.filter(
            contributions__item=self,
            contributions__role__in=[ContributionRole.COMPOSER, ContributionRole.LYRICIST]
        )

    @property
    def composers(self) -> "QuerySet[Person]":
        """Returns all people credited as composers for this work."""
        return self.get_people_by_role(ContributionRole.COMPOSER)

    @property
    def lyricists(self) -> "QuerySet[Person]":
        """Returns all people credited as lyricists for this work."""
        return self.get_people_by_role(ContributionRole.LYRICIST)


class Recording(Item):
    """A specific recorded performance of a MusicalWork.

    Attributes:
        work: The MusicalWork that this is a recording of.
    """
    work = models.ForeignKey(
        MusicalWork, on_delete=models.PROTECT, related_name="recordings",
        help_text="The musical work that was performed in this recording."
    )

    class Meta:
        db_table = "recording"
        verbose_name = "Recording"
        verbose_name_plural = "Recordings"

    def save(self, *args, **kwargs):
        """Sets the item_type before saving."""
        self.item_type = ItemType.RECORDING
        super().save(*args, **kwargs)

    @property
    def creators(self) -> "QuerySet[Person]":
        """For a Recording, the primary creators are the performing Artists."""
        return self.get_people_by_role(ContributionRole.ARTIST)

    @property
    def artists(self) -> "QuerySet[Person]":
        """Returns all performing artists for this recording."""
        return self.creators


class Book(Item):
    """A book.

    Attributes:
        subtitle: The book's subtitle.
        isbn_10: The 10-digit International Standard Book Number.
        isbn_13: The 13-digit International Standard Book Number.
    """
    # --- Book Details ---
    subtitle = models.CharField(
        max_length=255, blank=True, null=True,
        help_text="The subtitle of the book, if any."
    )
    isbn_10 = models.CharField(
        max_length=10, blank=True, help_text="The 10-digit ISBN."
    )
    isbn_13 = models.CharField(
        max_length=13, blank=True, help_text="The 13-digit ISBN."
    )

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
        return self.get_people_by_role(ContributionRole.AUTHOR)

    @property
    def authors(self) -> "QuerySet[Person]":
        """Returns all authors for this book."""
        return self.creators
