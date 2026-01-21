"""Core entities for the esperoj application.

This module contains the fundamental entities: Agent, Subject, and Collection.
It implements archival standards including:
- PREMIS for Agent types.
- EDTF (ISO 8601-2) for fuzzy dating.
- SKOS for hierarchical subjects.
"""

from typing import TYPE_CHECKING

from django.db import models
from django.db.models import Index, Manager
from django.utils.translation import gettext_lazy as _

from .base import BaseModel

if TYPE_CHECKING:
    from .item import Item
    from .relationship import AgentExternalReference, Atribution


class Subject(BaseModel):
    """A subject, topic, or keyword. Aligns with SKOS (Simple Knowledge Organization System).

    Attributes:
        name: The name of the subject.
        identifier: A unique slug.
        description: Description of the subject.
        parent: Link to a broader subject (SKOS hierarchy).
    """

    name = models.CharField(
        max_length=512,
        help_text=_("The name of the subject or topic."),
    )
    identifier = models.SlugField(
        max_length=255,
        unique=True,
        help_text=_("A unique, URL-friendly slug."),
    )
    description = models.TextField(
        blank=True,
        default="",
        help_text=_("A detailed description of the subject."),
    )

    # --- Hierarchy (SKOS) ---
    parent = models.ForeignKey(
        "self",
        on_delete=models.SET_NULL,
        null=True,
        blank=True,
        related_name="children",
        help_text=_("The broader subject category this belongs to (SKOS Broader)."),
    )

    alternative_names = models.JSONField(
        blank=True,
        default=dict,
        help_text=_("Alternative names or translations."),
    )

    items: "Manager[Item]"

    class Meta:
        db_table = "subject"
        ordering = ["name"]
        verbose_name = _("Subject")
        verbose_name_plural = _("Subjects")
        indexes = [
            Index(fields=["identifier"]),
            Index(fields=["name"]),
        ]

    def __str__(self) -> str:
        return self.name


class Collection(BaseModel):
    """A collection grouping multiple related Items.

    Attributes:
        name: The name of the collection.
        identifier: A unique slug.
        type: The type of collection (Series, Anthology, etc.).
        description: Description of scope.
        parent: Link to a parent collection (Sub-collections/Series).
        alternative_names: Alternative names or translations.
    """

    class CollectionType(models.TextChoices):
        """Enumeration for the type of a Collection."""

        SERIES = "SERIES", _("Series")
        ANTHOLOGY = "ANTHOLOGY", _("Anthology")
        ARCHIVE = "ARCHIVE", _("Archive")
        OTHER = "OTHER", _("Other")

    name = models.CharField(
        max_length=512,
        help_text=_("The name of the collection."),
    )
    identifier = models.SlugField(
        max_length=255,
        unique=True,
        help_text=_("A unique, URL-friendly slug."),
    )
    type = models.CharField(
        max_length=20,
        choices=CollectionType.choices,
        default=CollectionType.OTHER,
        help_text=_("High-level classification of the collection."),
    )
    description = models.TextField(
        blank=True,
        default="",
        help_text=_("A detailed description of the collection's scope."),
    )

    # --- Hierarchy ---
    parent = models.ForeignKey(
        "self",
        on_delete=models.SET_NULL,
        null=True,
        blank=True,
        related_name="subcollections",
        help_text=_("The parent collection if this is a sub-collection or series."),
    )

    alternative_names = models.JSONField(
        blank=True,
        default=dict,
        help_text=_("Alternative names or translations."),
    )

    items: "Manager[Item]"

    class Meta:
        db_table = "collection"
        ordering = ["name"]
        verbose_name = _("Collection")
        verbose_name_plural = _("Collections")
        indexes = [
            Index(fields=["identifier"]),
            Index(fields=["name"]),
            Index(fields=["type"]),
        ]

    def __str__(self) -> str:
        return self.name
