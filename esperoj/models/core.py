"""
Core entities for the esperoj application.

This module contains the fundamental entities that other models reference:
Person, Subject, and Collection. These models are kept separate to avoid
circular dependencies and provide a clear foundation for the rest of the system.
"""

from typing import TYPE_CHECKING

from django.db import models
from django.db.models import Index, Manager

from esperoj.utils.dates import format_person_display_name_with_dates
from esperoj.utils.text import generate_sort_name

from .base import BaseModel

if TYPE_CHECKING:
    from .items import Item
    from .relationships import Role, PersonExternalReference


class PersonManager(Manager):
    """Custom manager for the Person model providing common query methods."""

    def by_authorized_name(self, name: str):
        """Returns persons matching the given authorized name."""
        return self.filter(authorized_name__icontains=name)

    def by_sort_name(self, sort_name: str):
        """Returns persons matching the given sort name."""
        return self.filter(sort_name__icontains=sort_name)

    def living(self):
        """Returns persons who are still living (no death date)."""
        return self.filter(death_date__isnull=True)

    def deceased(self):
        """Returns persons who have died."""
        return self.filter(death_date__isnull=False)


class Person(BaseModel):
    """
    Represents a person, such as an author, artist, or composer.

    This model is designed for flexibility, storing both a display-friendly
    authorized name and a separate, structured name for sorting and indexing,
    following archival standards.

    Attributes:
        authorized_name: The full, authoritative name for display.
        sort_name: The name in an inverted order for sorting.
        identifier: A unique, URL-friendly slug for the person.
        birth_date: The person's date of birth.
        death_date: The person's date of death.
        biographical_note: A brief description or biography.

    Reverse Relations:
        items: Items this person contributed to.
        roles: Roles this person performs for items.
        external_references: Associated external links.
    """

    # --- Core Information ---
    authorized_name = models.CharField(
        max_length=512,
        help_text="The full, authoritative name in direct order (e.g., 'Dr. Martin Luther King, Jr.').",
    )
    sort_name = models.CharField(
        max_length=512,
        blank=True,
        help_text="Name in inverted order for sorting (e.g., 'King, Martin Luther, Jr.'). "
        "Automatically generated if left blank.",
    )
    identifier = models.SlugField(
        max_length=255,
        unique=True,
        help_text="A unique, human-readable identifier for this person.",
    )

    # --- Biographical Details ---
    birth_date = models.DateField(
        null=True,
        blank=True,
        help_text="The person's date of birth.",
    )
    death_date = models.DateField(
        null=True,
        blank=True,
        help_text="The person's date of death.",
    )
    biographical_note = models.TextField(
        blank=True,
        default="",
        help_text="A brief description or biographical information about the person.",
    )

    # --- Type hints for reverse relationships ---
    items: "Manager[Item]"
    roles: "Manager[Role]"
    external_references: "Manager[PersonExternalReference]"

    objects = PersonManager()

    class Meta:
        db_table = "person"
        ordering = ["sort_name", "authorized_name"]
        verbose_name = "Person"
        verbose_name_plural = "People"
        indexes = [
            Index(fields=["identifier"]),
            Index(fields=["sort_name"]),
            Index(fields=["authorized_name"]),
            Index(fields=["birth_date"]),
            Index(fields=["death_date"]),
        ]

    def __str__(self) -> str:
        """Returns the person's authoritative name."""
        return self.authorized_name

    def clean(self) -> None:
        """Performs model validation."""
        super().clean()

        if self.birth_date and self.death_date and self.death_date < self.birth_date:
            from django.core.exceptions import ValidationError

            raise ValidationError({"death_date": "Death date cannot be before birth date."})

    def save(self, *args, **kwargs) -> None:
        """Automatically generates sort_name if it's not provided."""
        if not self.sort_name and self.authorized_name:
            self.sort_name = generate_sort_name(self.authorized_name)
        super().save(*args, **kwargs)

    @property
    def is_living(self) -> bool:
        """Returns True if the person is still living (no death date)."""
        return self.death_date is None

    @property
    def display_name_with_dates(self) -> str:
        """Returns the name with birth/death dates in parentheses."""
        return format_person_display_name_with_dates(self.authorized_name, self.birth_date, self.death_date)


class SubjectManager(Manager):
    """Custom manager for the Subject model providing common query methods."""

    def by_name(self, name: str):
        """Returns subjects matching the given name."""
        return self.filter(name__icontains=name)

    def with_items(self):
        """Returns subjects that have associated items."""
        return self.filter(items__isnull=False).distinct()


class Subject(BaseModel):
    """
    A subject, topic, or keyword used for categorization.

    Subjects are used to categorize and organize items in the collection.
    They represent topics, themes, or keywords that can be associated with
    multiple items.

    Attributes:
        name: The name of the subject.
        identifier: A unique, URL-friendly slug for the subject.
        description: A detailed description of the subject.

    Reverse Relations:
        items: Items categorized under this subject.
    """

    # --- Core Information ---
    name = models.CharField(
        max_length=512,
        help_text="The name of the subject or topic.",
    )
    identifier = models.SlugField(
        max_length=255,
        unique=True,
        help_text="A unique, URL-friendly slug for the subject.",
    )
    description = models.TextField(
        blank=True,
        default="",
        help_text="A detailed description of the subject.",
    )

    # --- Type hints for reverse relationships ---

    items: "Manager[Item]"

    objects = SubjectManager()

    class Meta:
        db_table = "subject"
        ordering = ["name"]
        verbose_name = "Subject"
        verbose_name_plural = "Subjects"
        indexes = [
            Index(fields=["identifier"]),
            Index(fields=["name"]),
        ]

    def __str__(self) -> str:
        """Returns the subject's name."""
        return self.name

    @property
    def item_count(self) -> int:
        """Returns the number of items associated with this subject."""
        return self.items.count()


class CollectionManager(Manager):
    """Custom manager for the Collection model providing common query methods."""

    def by_name(self, name: str):
        """Returns collections matching the given name."""
        return self.filter(name__icontains=name)

    def with_items(self):
        """Returns collections that have associated items."""
        return self.filter(items__isnull=False).distinct()


class Collection(BaseModel):
    """
    A collection that groups multiple related Items.

    Collections are used to organize items into logical groupings,
    such as albums, book series, or thematic collections.

    Attributes:
        name: The name of the collection.
        identifier: A unique, URL-friendly slug for the collection.
        description: A detailed description of the collection's scope.

    Reverse Relations:
        items: Items within this collection.
    """

    # --- Core Information ---
    name = models.CharField(
        max_length=512,
        help_text="The name of the collection.",
    )
    identifier = models.SlugField(
        max_length=255,
        unique=True,
        help_text="A unique, URL-friendly slug for the collection.",
    )
    description = models.TextField(
        blank=True,
        default="",
        help_text="A detailed description of the collection's scope and contents.",
    )

    # --- Type hints for reverse relationships ---
    items: "Manager[Item]"

    objects = CollectionManager()

    class Meta:
        db_table = "collection"
        ordering = ["name"]
        verbose_name = "Collection"
        verbose_name_plural = "Collections"
        indexes = [
            Index(fields=["identifier"]),
            Index(fields=["name"]),
        ]

    def __str__(self) -> str:
        """Returns the collection's name."""
        return self.name

    @property
    def item_count(self) -> int:
        """Returns the number of items in this collection."""
        return self.items.count()

    @property
    def latest_item_date(self):
        """Returns the date of the most recent item in this collection."""
        from .items import Item

        latest_item = Item.objects.filter(collections=self, date__isnull=False).order_by("-date").first()
        return latest_item.date if latest_item else None
