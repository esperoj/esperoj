"""
Relationship models for the esperoj application.

This module contains models that define relationships between core entities,
including roles that people play in items, external references, and related
enumerations. This separation helps avoid circular dependencies.
"""

from typing import TYPE_CHECKING

from django.db import models
from django.db.models import Index, Manager

from .base import BaseModel

if TYPE_CHECKING:
    from .core import Person
    from .items import Item


class ItemRoleName(models.TextChoices):
    """
    A unified list of all possible roles a Person can have in relation to an Item.

    This enum defines the vocabulary of roles that can be assigned to people
    for different types of items in the collection.
    """

    # --- Musical Roles ---
    COMPOSER = "Composer", "Composer"
    LYRICIST = "Lyricist", "Lyricist"
    ARTIST = "Artist", "Artist"  # The performer of a recording
    PRODUCER = "Producer", "Producer"
    ENGINEER = "Engineer", "Engineer"
    SONGWRITER = "Songwriter", "Songwriter"
    MUSICIAN = "Musician", "Musician"

    # --- Literary Roles ---
    AUTHOR = "Author", "Author"
    EDITOR = "Editor", "Editor"
    TRANSLATOR = "Translator", "Translator"
    ILLUSTRATOR = "Illustrator", "Illustrator"
    PUBLISHER = "Publisher", "Publisher"

    # --- General Creative Roles ---
    CREATOR = "Creator", "Creator"
    CONTRIBUTOR = "Contributor", "Contributor"
    COLLABORATOR = "Collaborator", "Collaborator"


class RoleManager(Manager):
    """Custom manager for the Role model providing common query methods."""

    def for_person(self, person):
        """Returns all roles for a specific person."""
        return self.filter(person=person).select_related("item", "person")

    def for_item(self, item):
        """Returns all roles for a specific item."""
        return self.filter(item=item).select_related("item", "person")

    def by_role_name(self, role_name):
        """Returns all roles of a specific type."""
        return self.filter(name=role_name).select_related("item", "person")

    def creators_for_item(self, item):
        """Returns roles that are considered 'creator' roles for an item."""
        creator_roles = [
            ItemRoleName.AUTHOR,
            ItemRoleName.COMPOSER,
            ItemRoleName.ARTIST,
            ItemRoleName.CREATOR,
        ]
        return self.filter(item=item, name__in=creator_roles).select_related("person")


class Role(BaseModel):
    """
    Represents the role a Person plays in relation to an Item.

    This model serves as the 'through' table for the many-to-many relationship
    between Person and Item, allowing us to specify the nature of a person's
    contribution to a specific item.

    Attributes:
        person: The Person involved in the role.
        item: The Item to which the Person is contributing.
        name: The specific role (e.g., 'Author', 'Composer', 'Artist').
        order: The order of this person for this role (for multiple people with same role).
        notes: Optional notes about this specific role assignment.
    """

    # --- Relationships ---
    person = models.ForeignKey(
        "esperoj.Person",
        on_delete=models.CASCADE,
        related_name="roles",
        help_text="The person associated with this role.",
    )
    item = models.ForeignKey(
        "esperoj.Item",
        on_delete=models.CASCADE,
        related_name="roles",
        help_text="The item to which this role pertains.",
    )

    # --- Role Details ---
    name = models.CharField(
        max_length=50,
        choices=ItemRoleName.choices,
        help_text="The specific role performed by the person for this item.",
    )
    order = models.PositiveSmallIntegerField(
        default=1,
        help_text="The order of this person for this role (1 = primary, 2 = secondary, etc.).",
    )
    notes = models.TextField(
        blank=True,
        default="",
        help_text="Optional notes about this specific role assignment.",
    )

    objects = RoleManager()

    class Meta:
        db_table = "role"
        unique_together = ("person", "item", "name", "order")
        ordering = ["item", "name", "order", "person"]
        verbose_name = "Role"
        verbose_name_plural = "Roles"
        indexes = [
            Index(fields=["person"]),
            Index(fields=["item"]),
            Index(fields=["name"]),
            Index(fields=["item", "name", "order"]),
        ]

    def __str__(self) -> str:
        """Returns a string representing the role."""
        return f"{self.person.authorized_name} as {self.name} for {self.item.title}"

    def clean(self) -> None:
        """Performs model validation."""
        super().clean()

        # Ensure order is positive
        if self.order < 1:
            from django.core.exceptions import ValidationError

            raise ValidationError({"order": "Order must be 1 or greater."})

    @property
    def is_primary(self) -> bool:
        """Returns True if this is the primary person for this role."""
        return self.order == 1


class ExternalReferenceType(models.TextChoices):
    """Defines the type of an external reference link."""

    # --- General Web Presence ---
    WEBSITE = "WEBSITE", "Official Website"
    SOCIAL_MEDIA = "SOCIAL_MEDIA", "Social Media"
    BLOG = "BLOG", "Blog"

    # --- Knowledge Bases ---
    WIKIPEDIA = "WIKIPEDIA", "Wikipedia"
    WIKIDATA = "WIKIDATA", "Wikidata"

    # --- Music Databases ---
    MUSICBRAINZ = "MUSICBRAINZ", "MusicBrainz"
    DISCOGS = "DISCOGS", "Discogs"
    ALLMUSIC = "ALLMUSIC", "AllMusic"
    LASTFM = "LASTFM", "Last.fm"

    # --- Book Databases ---
    GOODREADS = "GOODREADS", "Goodreads"
    OPENLIBRARY = "OPENLIBRARY", "Open Library"
    WORLDCAT = "WORLDCAT", "WorldCat"

    # --- Media Databases ---
    IMDB = "IMDB", "IMDb"
    TMDB = "TMDB", "The Movie Database"

    # --- Academic ---
    ORCID = "ORCID", "ORCID"
    SCHOLAR = "SCHOLAR", "Google Scholar"

    # --- Commercial ---
    AMAZON = "AMAZON", "Amazon"
    ITUNES = "ITUNES", "iTunes"
    SPOTIFY = "SPOTIFY", "Spotify"

    # --- Archival ---
    FINDING_AID = "FINDING_AID", "Finding Aid"
    ARCHIVE = "ARCHIVE", "Archive"

    # --- Media Links ---
    DOWNLOAD = "DOWNLOAD", "Download Link"
    STREAMING = "STREAMING", "Streaming Link"
    VIDEO = "VIDEO", "Video"

    # --- Other ---
    OTHER = "OTHER", "Other"


class AbstractExternalReference(BaseModel):
    """
    An abstract model for storing external references (URLs) related to other models.

    This model is not intended to be used directly but to be inherited by
    concrete models that link a URL to a specific parent object (e.g., a Person or an Item).

    Attributes:
        url: The full URL of the external resource.
        type: The category of the link (e.g., 'Website', 'Social Media').
        label: An optional, user-friendly label for the link.
        notes: Optional internal notes about the reference.
        verified_at: When this link was last verified as working.
        is_active: Whether this link is currently active/working.
    """

    # --- Reference Details ---
    url = models.URLField(
        max_length=2048,
        help_text="The full URL of the external resource.",
    )
    type = models.CharField(
        max_length=50,
        choices=ExternalReferenceType.choices,
        default=ExternalReferenceType.OTHER,
        help_text="The category of the link.",
    )
    label = models.CharField(
        max_length=255,
        blank=True,
        help_text="An optional, user-friendly label for the link (e.g., 'Facebook Profile').",
    )
    notes = models.TextField(
        blank=True,
        help_text="Optional internal notes about this reference.",
    )

    # --- Status Tracking ---
    verified_at = models.DateTimeField(
        null=True,
        blank=True,
        help_text="When this link was last verified as working.",
    )
    is_active = models.BooleanField(
        default=True,
        help_text="Whether this link is currently active/working.",
    )

    class Meta:
        abstract = True
        ordering = ["type", "label", "url"]
        indexes = [
            Index(fields=["type"]),
            Index(fields=["is_active"]),
        ]

    def __str__(self) -> str:
        """Returns a string representation of the external reference."""
        if self.label:
            return f"{self.label}: {self.url}"

        # Get the display name for the type from the choices
        type_display = self.type
        for choice_value, choice_label in ExternalReferenceType.choices:
            if choice_value == self.type:
                type_display = choice_label
                break
        return f"{type_display}: {self.url}"

    @property
    def domain(self) -> str:
        """Returns the domain name from the URL."""
        from urllib.parse import urlparse

        return urlparse(self.url).netloc

    def mark_verified(self) -> None:
        """Mark this reference as verified and update the timestamp."""
        from django.utils import timezone

        self.verified_at = timezone.now()
        self.is_active = True
        self.save(update_fields=["verified_at", "is_active"])

    def mark_inactive(self) -> None:
        """Mark this reference as inactive."""
        self.is_active = False
        self.save(update_fields=["is_active"])


class PersonExternalReference(AbstractExternalReference):
    """An external reference link associated with a Person."""

    person = models.ForeignKey(
        "esperoj.Person",
        on_delete=models.CASCADE,
        related_name="external_references",
        help_text="The person this link refers to.",
    )

    class Meta(AbstractExternalReference.Meta):
        db_table = "person_external_reference"
        verbose_name = "Person External Reference"
        verbose_name_plural = "Person External References"
        unique_together = ("person", "url")
        indexes = AbstractExternalReference.Meta.indexes + [
            Index(fields=["person", "type"]),
        ]


class ItemExternalReference(AbstractExternalReference):
    """An external reference link associated with an Item."""

    item = models.ForeignKey(
        "esperoj.Item",
        on_delete=models.CASCADE,
        related_name="external_references",
        help_text="The item this link refers to.",
    )

    class Meta(AbstractExternalReference.Meta):
        db_table = "item_external_reference"
        verbose_name = "Item External Reference"
        verbose_name_plural = "Item External References"
        unique_together = ("item", "url")
        indexes = AbstractExternalReference.Meta.indexes + [
            Index(fields=["item", "type"]),
        ]
