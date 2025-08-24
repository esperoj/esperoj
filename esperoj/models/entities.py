from django.conf import settings
from django.db import models
from django.db.models import UniqueConstraint, Index, When, Case
from .base import BaseModel

from typing import TYPE_CHECKING

if TYPE_CHECKING:
    from django.db.models import Manager


class Person(BaseModel):
    """
    Represents a person, modeled for flexibility and archival standards.
    Stores a full authoritative name for display and a separate, structured
    name for sorting and indexing.
    """

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
    birth_date = models.DateField(null=True, blank=True)
    death_date = models.DateField(null=True, blank=True)
    biographical_note = models.TextField(
        blank=True, default="", help_text="A brief description or biographical information about the person."
    )

    class Meta:
        ordering = ["sort_name"]
        verbose_name = "Person"
        verbose_name_plural = "People"
        indexes = [
            Index(fields=["identifier"]),
            Index(fields=["sort_name"]),
            Index(fields=["authorized_name"]),
        ]
        db_table = "person"

    def __str__(self):
        """The string representation is always the authoritative display name."""
        return self.authorized_name

    def save(self, *args, **kwargs):
        """
        Automatically generates the sort_name from the authorized_name
        if the sort_name is not provided manually.
        """
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
    """A subject, topic, or keyword used for categorization."""

    name = models.CharField(max_length=512)
    identifier = models.SlugField(max_length=255, unique=True)
    description = models.TextField(blank=True, default="")

    class Meta:
        ordering = ["identifier"]
        verbose_name = "Subject"
        verbose_name_plural = "Subjects"
        indexes = [
            Index(fields=["identifier"]),
            Index(fields=["name"]),
        ]
        db_table = "subject"

    def __str__(self):
        return self.name


class Collection(BaseModel):
    """A collection that groups multiple Items."""

    name = models.CharField(max_length=512)
    identifier = models.SlugField(max_length=255, unique=True)
    description = models.TextField(blank=True, default="")

    class Meta:
        ordering = ["identifier"]
        verbose_name = "Collection"
        verbose_name_plural = "Collections"
        indexes = [
            Index(fields=["identifier"]),
            Index(fields=["name"]),
        ]
        db_table = "collection"

    def __str__(self):
        return self.name
