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
    from .items import Item
    from .relationships import AgentExternalReference, Role


class Agent(BaseModel):
    """Represents an agent (PREMIS: Agent entity).

    An agent can be an author, artist, publisher, or any entity that performs
    a role. This model supports name parsing and fuzzy dates.

    Attributes:
        name (str): The full, authoritative name.
        sort_name (str): The name used for sorting.
        identifier (str): A unique, URL-friendly slug.
        type (str): The type of agent (Person, Organization, Family).
        birth_date (str): EDTF string for birth/founding (e.g., '1980', '1980?').
        death_date (str): EDTF string for death/dissolution.
        description (str): Public biographical info.
        note (str): Internal notes.
        alternative_names (dict): JSON of alias names.

    Reverse Relationships:
        items: Items related to this agent.
        roles: Roles this agent performs.
        external_references: External links (Wikidata, Websites) from relationships module.
    """

    class AgentType(models.TextChoices):
        """Defines the types of agents, aligned with PREMIS and ISAAR standards."""

        PERSON = "Person", _("Person")
        ORGANIZATION = "Organization", _("Organization")
        FAMILY = "Family", _("Family")
        SOFTWARE = "Software", _("Software")
        OTHER = "Other", _("Other")

    # --- Constants for Name Parsing ---
    _IGNORED_TITLES = {
        "dr",
        "doctor",
        "mr",
        "mrs",
        "ms",
        "miss",
        "prof",
        "professor",
        "sir",
        "madam",
        "dame",
        "rev",
        "reverend",
        "hon",
        "honorable",
    }
    _KNOWN_SUFFIXES = {"jr", "sr", "ii", "iii", "iv", "v", "esq", "phd", "md"}

    # --- Core Information ---
    name = models.CharField(
        max_length=512,
        help_text=_("The full, authoritative name (e.g., 'Dr. Martin Luther King, Jr.' or 'Penguin Books')."),
    )
    sort_name = models.CharField(
        max_length=512,
        blank=True,
        help_text=_("The name used for sorting. Auto-generated if blank."),
    )
    identifier = models.SlugField(
        max_length=255,
        unique=True,
        help_text=_("A unique, human-readable identifier (slug)."),
    )
    type = models.CharField(
        max_length=20,
        choices=AgentType.choices,
        default=AgentType.PERSON,
        help_text=_("High-level classification (matches PREMIS agentType)."),
    )

    # --- Temporal Details (EDTF Standard) ---
    # Using CharField to support "1990?", "1990~", "2020-05", etc.
    birth_date = models.CharField(
        max_length=50,
        blank=True,
        default="",
        help_text=_("Date of birth or founding in EDTF format (e.g., '1980', '1980-05~', '1980?')."),
    )
    death_date = models.CharField(
        max_length=50,
        blank=True,
        default="",
        help_text=_("Date of death or dissolution in EDTF format."),
    )

    # --- Description and Notes ---
    description = models.TextField(
        blank=True,
        default="",
        help_text=_("A public description or biographical information."),
    )
    note = models.TextField(
        blank=True,
        default="",
        help_text=_("Internal notes, not intended for public display."),
    )

    # --- Alternative Names (JSON) ---
    alternative_names = models.JSONField(
        blank=True,
        default=dict,
        help_text=_("Variant names. Keys typically are language codes or types."),
    )

    # --- Type hints for reverse relationships ---
    # These are defined via 'related_name' in other models
    items: "Manager[Item]"
    roles: "Manager[Role]"
    external_references: "Manager[AgentExternalReference]"

    class Meta:
        db_table = "agent"
        ordering = ["sort_name", "name"]
        verbose_name = _("Agent")
        verbose_name_plural = _("Agents")
        indexes = [
            Index(fields=["identifier"]),
            Index(fields=["sort_name"]),
            Index(fields=["name"]),
            Index(fields=["type"]),
        ]

    def __str__(self) -> str:
        return self.name

    def generate_sort_name(self, name: str) -> str:
        """Generates a sortable name string by handling suffixes and removing titles."""
        clean_name = name.strip()
        parts = clean_name.replace(",", "").split()

        if not parts:
            return ""

        # Extract Suffix
        suffix_part = ""
        if len(parts) > 1:
            last_word_clean = parts[-1].lower().replace(".", "")
            if last_word_clean in self._KNOWN_SUFFIXES:
                suffix_part = parts.pop()

        # Filter Titles
        filtered_parts = []
        for i, part in enumerate(parts):
            # Always keep the last word (Surname)
            if i == len(parts) - 1:
                filtered_parts.append(part)
                continue
            # Check if this part is a title
            if part.lower().replace(".", "") not in self._IGNORED_TITLES:
                filtered_parts.append(part)

        parts = filtered_parts

        # Construct Sort Name
        if len(parts) > 1:
            last_name = parts.pop()
            first_names = " ".join(parts)
            if suffix_part:
                return f"{last_name}, {first_names}, {suffix_part}"
            return f"{last_name}, {first_names}"

        # Fallback for single words
        return parts[0]

    def save(self, *args, **kwargs) -> None:
        """Saves the agent, auto-generating sort_name if missing."""
        if not self.sort_name and self.name:
            if self.type == self.AgentType.PERSON:
                self.sort_name = self.generate_sort_name(self.name)
            else:
                self.sort_name = self.name
        super().save(*args, **kwargs)


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
