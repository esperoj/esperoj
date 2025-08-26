from typing import TYPE_CHECKING

from django.db import models
from django.db.models import Index

from .base import BaseModel

if TYPE_CHECKING:
    from django.db.models import Manager
    from .items import Item


class Person(BaseModel):
    """Represents a person, such as an author, artist, or composer.

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
        wikipedia_link: A URL to the person's Wikipedia page.
        items: A reverse relation to the items this person contributed to.
        roles: A reverse relation to the roles this person performs for items.
    """

    # --- Core Information ---
    authorized_name = models.CharField(
        max_length=512, help_text="The full, authoritative name in direct order (e.g., 'Dr. Martin Luther King, Jr.')."
    )
    sort_name = models.CharField(
        max_length=512,
        blank=True,
        help_text="Name in inverted order for sorting (e.g., 'King, Martin Luther, Jr.'). Automatically generated if left blank.",
    )
    identifier = models.SlugField(
        max_length=255, unique=True, help_text="A unique, human-readable identifier for this person."
    )

    # --- Biographical Details ---
    birth_date = models.DateField(null=True, blank=True, help_text="The person's date of birth.")
    death_date = models.DateField(null=True, blank=True, help_text="The person's date of death.")
    biographical_note = models.TextField(
        blank=True, default="", help_text="A brief description or biographical information about the person."
    )
    wikipedia_link = models.CharField(
        max_length=255, blank=True, null=True, help_text="A link to the person's Wikipedia page."
    )

    class Meta:
        db_table = "person"
        ordering = ["sort_name"]
        verbose_name = "Person"
        verbose_name_plural = "People"
        indexes = [
            Index(fields=["identifier"]),
            Index(fields=["sort_name"]),
            Index(fields=["authorized_name"]),
        ]

    def __str__(self):
        """Returns the person's authoritative name."""
        return self.authorized_name

    def save(self, *args, **kwargs):
        """Automatically generates sort_name if it's not provided."""
        if not self.sort_name and self.authorized_name:
            name_parts = self.authorized_name.split()
            if len(name_parts) > 1:
                last_part = name_parts[-1]
                first_parts = " ".join(name_parts[:-1])
                self.sort_name = f"{last_part}, {first_parts}"
            else:
                self.sort_name = self.authorized_name
        super().save(*args, **kwargs)


class Subject(BaseModel):
    """A subject, topic, or keyword used for categorization.

    Attributes:
        name: The name of the subject.
        identifier: A unique, URL-friendly slug for the subject.
        description: A detailed description of the subject.
        items: A reverse relation to items categorized under this subject.
    """

    name = models.CharField(max_length=512, help_text="The name of the subject or topic.")
    identifier = models.SlugField(max_length=255, unique=True, help_text="A unique, URL-friendly slug for the subject.")
    description = models.TextField(blank=True, default="", help_text="A detailed description of the subject.")

    # --- Type hints for reverse relationships ---
    items: "Manager[Item]"

    class Meta:
        db_table = "subject"
        ordering = ["name"]
        verbose_name = "Subject"
        verbose_name_plural = "Subjects"
        indexes = [
            Index(fields=["identifier"]),
            Index(fields=["name"]),
        ]

    def __str__(self):
        """Returns the subject's name."""
        return self.name


class Collection(BaseModel):
    """A collection that groups multiple related Items.

    Attributes:
        name: The name of the collection.
        identifier: A unique, URL-friendly slug for the collection.
        description: A detailed description of the collection's scope.
        items: A reverse relation to items within this collection.
    """

    name = models.CharField(max_length=512, help_text="The name of the collection.")
    identifier = models.SlugField(
        max_length=255, unique=True, help_text="A unique, URL-friendly slug for the collection."
    )
    description = models.TextField(
        blank=True, default="", help_text="A detailed description of the collection's scope and contents."
    )

    # --- Type hints for reverse relationships ---
    items: "Manager[Item]"

    class Meta:
        db_table = "collection"
        ordering = ["name"]
        verbose_name = "Collection"
        verbose_name_plural = "Collections"
        indexes = [
            Index(fields=["identifier"]),
            Index(fields=["name"]),
        ]

    def __str__(self):
        """Returns the collection's name."""
        return self.name


class ItemRoleName(models.TextChoices):  # Renamed from ContributionRole
    """A unified list of all possible roles a Person can have in relation to an Item."""

    # Musical Roles
    COMPOSER = "Composer", "Composer"
    LYRICIST = "Lyricist", "Lyricist"
    ARTIST = "Artist", "Artist"  # The performer of a recording
    PRODUCER = "Producer", "Producer"
    ENGINEER = "Engineer", "Engineer"

    # Literary Roles
    AUTHOR = "Author", "Author"
    EDITOR = "Editor", "Editor"
    TRANSLATOR = "Translator", "Translator"


class Role(BaseModel):
    """Represents the role a Person plays in relation to an Item.

    This model serves as the 'through' table for the many-to-many relationship
    between Person and Item, allowing us to specify the nature of a person's
    contribution.

    Attributes:
        person: The Person involved in the role.
        item: The Item to which the Person is contributing.
        name: The specific role (e.g., 'Author', 'Composer', 'Artist').
    """

    person = models.ForeignKey(
        Person, on_delete=models.CASCADE, related_name="roles", help_text="The person associated with this role."
    )
    item = models.ForeignKey(
        "esperoj.Item",
        on_delete=models.CASCADE,
        related_name="roles",
        help_text="The item to which this role pertains.",
    )
    name = models.CharField(
        max_length=50,
        choices=ItemRoleName.choices,
        help_text="The specific role performed by the person for this item.",
    )

    class Meta:
        db_table = "role"
        unique_together = ("person", "item", "name")
        ordering = ["item", "person", "name"]
        verbose_name = "Role"
        verbose_name_plural = "Roles"
        indexes = [
            Index(fields=["person"]),
            Index(fields=["item"]),
            Index(fields=["name"]),
        ]

    def __str__(self):
        """Returns a string representing the role."""
        return f"{self.person.authorized_name} as {self.name} for {self.item.title}"
