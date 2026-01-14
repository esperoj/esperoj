"""Models for representing relationships between items, agents, and external resources.

This module defines the structures for linking items to the people and organizations
involved in their creation (Roles), linking items to each other (ItemRelationships),
and connecting internal records to external databases (ExternalReferences).
"""

from typing import TYPE_CHECKING

from django.db import models
from django.db.models import Index, Q, QuerySet

from .base import BaseModel

if TYPE_CHECKING:
    from .core import Agent
    from .items import Item


class ItemRelationshipType(models.TextChoices):
    """Defines the nature of a relationship between two items."""

    # --- Hierarchical ---
    PARENT = "Parent", "is parent of"
    CHILD = "Child", "is child of"

    # --- Sequential ---
    PRECEDES = "Precedes", "precedes"
    FOLLOWS = "Follows", "follows"

    # --- Derivative ---
    VERSION_OF = "VersionOf", "is version of"
    ADAPTATION_OF = "AdaptationOf", "is adaptation of"
    TRANSLATION_OF = "TranslationOf", "is translation of"

    # --- Creative/Structural ---
    INCLUDES = "Includes", "includes"
    PART_OF = "PartOf", "is part of"

    # --- General ---
    RELATED_TO = "RelatedTo", "is related to"


class ItemRoleName(models.TextChoices):
    """
    A unified list of all possible roles an Agent can have in relation to an Item.

    This enum defines the vocabulary of roles that can be assigned to agents
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


class RoleManager(models.Manager):
    """Custom manager for the Role model."""

    def for_agent(self, agent: "Agent") -> QuerySet:
        """Returns all roles for a specific agent."""
        return self.filter(agent=agent)

    def for_item(self, item: "Item") -> QuerySet:
        """Returns all roles for a specific item."""
        return self.filter(item=item)

    def by_role_name(self, role_name: str) -> QuerySet:
        """Returns all roles of a certain type."""
        return self.filter(name=role_name)

    def creators_for_item(self, item: "Item") -> QuerySet:
        """Returns primary creators for an item.

        Determined by roles traditionally considered 'creators'
        (Author, Composer, Artist, Creator).
        """
        creative_roles = [
            ItemRoleName.AUTHOR,
            ItemRoleName.COMPOSER,
            ItemRoleName.ARTIST,
            ItemRoleName.CREATOR,
        ]
        return self.filter(item=item, name__in=creative_roles).order_by("order")


class Role(BaseModel):
    """
    Represents the role an Agent plays in relation to an Item.

    This model serves as the 'through' table for the many-to-many relationship
    between Agent and Item, allowing us to specify the nature of an agent's
    contribution to a specific item.

    Attributes:
        agent: The Agent involved in the role.
        item: The Item to which the Agent is contributing.
        name: The specific role (e.g., 'Author', 'Composer', 'Artist', 'Publisher').
        order: The order of this agent for this role (for multiple agents with same role).
        notes: Optional notes about this specific role assignment.
    """

    # --- Relationships ---
    agent = models.ForeignKey(
        "esperoj.Agent",
        on_delete=models.CASCADE,
        related_name="roles",
        help_text="The agent associated with this role.",
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
        help_text="The specific role performed by the agent for this item.",
    )
    order = models.PositiveSmallIntegerField(
        default=1,
        help_text="The order of this agent for this role (1 = primary, 2 = secondary, etc.).",
    )
    notes = models.TextField(
        blank=True,
        default="",
        help_text="Optional notes about this specific role assignment.",
    )

    objects = RoleManager()

    class Meta:
        db_table = "role"
        unique_together = ("agent", "item", "name", "order")
        ordering = ["item", "name", "order", "agent"]
        verbose_name = "Role"
        verbose_name_plural = "Roles"
        indexes = [
            Index(fields=["agent"]),
            Index(fields=["item"]),
            Index(fields=["name"]),
            Index(fields=["item", "name", "order"]),
        ]

    def __str__(self) -> str:
        """Returns a string representing the role."""
        return f"{self.agent.authorized_name} as {self.name} for {self.item.title}"

    def clean(self) -> None:
        """Performs model validation."""
        super().clean()

        # Ensure order is positive
        if self.order < 1:
            from django.core.exceptions import ValidationError

            raise ValidationError({"order": "Order must be 1 or greater."})

    @property
    def is_primary(self) -> bool:
        """Returns True if this is the primary agent for this role."""
        return self.order == 1


class ExternalReferenceType(models.TextChoices):
    """Defines the source or type of an external reference."""

    # --- General ---
    WIKIPEDIA = "Wikipedia", "Wikipedia"
    WIKIDATA = "Wikidata", "Wikidata"
    OFFICIAL_WEBSITE = "OfficialWebsite", "Official Website"

    # --- Music ---
    MUSICBRAINZ = "MusicBrainz", "MusicBrainz"
    DISCOGS = "Discogs", "Discogs"
    SPOTIFY = "Spotify", "Spotify"

    # --- Literature ---
    GOODREADS = "Goodreads", "Goodreads"
    OPEN_LIBRARY = "OpenLibrary", "Open Library"
    ISFDB = "ISFDB", "ISFDB"

    # --- Authority Control ---
    LCNAF = "LCNAF", "Library of Congress Name Authority File"
    VIAF = "VIAF", "VIAF"
    ISNI = "ISNI", "ISNI"


class AbstractExternalReference(BaseModel):
    """Base class for external identifiers and links."""

    type = models.CharField(
        max_length=50,
        choices=ExternalReferenceType.choices,
        help_text="The source or type of the external reference.",
    )
    url = models.URLField(
        max_length=1024,
        help_text="The full URL to the external resource.",
    )
    label = models.CharField(
        max_length=255,
        blank=True,
        help_text="A human-readable label for the link.",
    )
    is_active = models.BooleanField(
        default=True,
        help_text="Whether this link is currently valid.",
    )
    verified_at = models.DateTimeField(
        null=True,
        blank=True,
        help_text="The date and time this link was last verified.",
    )
    notes = models.TextField(
        blank=True,
        default="",
        help_text="Additional information about this reference.",
    )

    class Meta:
        abstract = True
        db_table = "external_reference"
        ordering = ["type", "label"]

    def __str__(self) -> str:
        """Returns a string representation of the reference."""
        if self.label:
            return f"{self.type}: {self.label}"
        return f"{self.type} ({self.url})"

    @property
    def domain(self) -> str:
        """Extracts the domain from the URL."""
        from urllib.parse import urlparse

        return urlparse(self.url).netloc

    def mark_verified(self) -> None:
        """Marks the reference as verified."""
        from django.utils import timezone

        self.verified_at = timezone.now()
        self.is_active = True
        self.save(update_fields=["verified_at", "is_active"])

    def mark_inactive(self) -> None:
        """Marks the reference as inactive/broken."""
        self.is_active = False
        self.save(update_fields=["is_active"])


class AgentExternalReference(AbstractExternalReference):
    """An external reference link associated with an Agent."""

    agent = models.ForeignKey(
        "esperoj.Agent",
        on_delete=models.CASCADE,
        related_name="external_references",
        help_text="The agent this link refers to.",
    )

    class Meta(AbstractExternalReference.Meta):
        db_table = "agent_external_reference"
        verbose_name = "Agent External Reference"
        verbose_name_plural = "Agent External References"


class ItemRelationshipManager(models.Manager):
    """Custom manager for ItemRelationship model."""

    def for_item(self, item: "Item") -> QuerySet:
        """Returns all relationships involving a specific item."""
        return self.filter(Q(from_item=item) | Q(to_item=item))

    def outgoing_for_item(self, item: "Item") -> QuerySet:
        """Returns relationships where the item is the source."""
        return self.filter(from_item=item)

    def incoming_for_item(self, item: "Item") -> QuerySet:
        """Returns relationships where the item is the target."""
        return self.filter(to_item=item)

    def by_type(self, relationship_type: str) -> QuerySet:
        """Returns relationships of a certain type."""
        return self.filter(type=relationship_type)

    def hierarchical(self) -> QuerySet:
        """Returns only hierarchical relationships."""
        return self.filter(type__in=[ItemRelationshipType.PARENT, ItemRelationshipType.CHILD])


class ItemRelationship(BaseModel):
    """
    Represents a relationship between two items.

    This model allows for complex graphs of items, including hierarchies
    (e.g., chapters in a book), sequences (e.g., songs in an album),
    and semantic links (e.g., adaptations, translations).

    Attributes:
        from_item: The source item in the relationship.
        to_item: The target item in the relationship.
        type: The nature of the relationship (from ItemRelationshipType).
        order: The sequence order (e.g., track number or chapter number).
        notes: Optional context or explanation for the relationship.
    """

    # --- Relationship participants ---
    from_item = models.ForeignKey(
        "esperoj.Item",
        on_delete=models.CASCADE,
        related_name="outgoing_relationships",
        help_text="The source item of the relationship.",
    )
    to_item = models.ForeignKey(
        "esperoj.Item",
        on_delete=models.CASCADE,
        related_name="incoming_relationships",
        help_text="The target item of the relationship.",
    )

    # --- Relationship details ---
    type = models.CharField(
        max_length=50,
        choices=ItemRelationshipType.choices,
        help_text="The nature of the relationship.",
    )
    order = models.PositiveSmallIntegerField(
        default=1,
        help_text="The sequence order for this relationship (e.g., track or chapter number).",
    )
    notes = models.TextField(
        blank=True,
        default="",
        help_text="Optional context about this relationship.",
    )

    objects = ItemRelationshipManager()

    class Meta:
        db_table = "item_relationship"
        unique_together = ("from_item", "to_item", "type", "order")
        ordering = ["from_item", "type", "order"]
        verbose_name = "Item Relationship"
        verbose_name_plural = "Item Relationships"
        indexes = [
            Index(fields=["from_item", "type"]),
            Index(fields=["to_item", "type"]),
            Index(fields=["type"]),
        ]

    def __str__(self) -> str:
        """Returns a string representing the relationship."""
        type_display = self.get_type_display()
        return f"'{self.from_item.title}' {type_display} '{self.to_item.title}'"

    def clean(self) -> None:
        """Ensures the relationship is valid."""
        super().clean()

        # Prevent items from relating to themselves
        if self.from_item_id and self.to_item_id and self.from_item_id == self.to_item_id:
            from django.core.exceptions import ValidationError

            raise ValidationError("An item cannot have a relationship with itself.")

    @property
    def is_hierarchical(self) -> bool:
        """Returns True if the relationship is hierarchical (Parent/Child)."""
        return self.type in [
            ItemRelationshipType.PARENT,
            ItemRelationshipType.CHILD,
        ]

    @property
    def is_sequential(self) -> bool:
        """Returns True if the relationship is sequential (Precedes/Follows)."""
        return self.type in [
            ItemRelationshipType.PRECEDES,
            ItemRelationshipType.FOLLOWS,
        ]

    @property
    def is_creative(self) -> bool:
        """Returns True if the relationship is derivative (Version/Adaptation/Translation)."""
        return self.type in [
            ItemRelationshipType.VERSION_OF,
            ItemRelationshipType.ADAPTATION_OF,
            ItemRelationshipType.TRANSLATION_OF,
        ]

    def get_inverse_relationship_type(self) -> str | None:
        """Returns the logical opposite relationship type, if one exists."""
        mapping = {
            ItemRelationshipType.PARENT: ItemRelationshipType.CHILD,
            ItemRelationshipType.CHILD: ItemRelationshipType.PARENT,
            ItemRelationshipType.PRECEDES: ItemRelationshipType.FOLLOWS,
            ItemRelationshipType.FOLLOWS: ItemRelationshipType.PRECEDES,
            ItemRelationshipType.PART_OF: ItemRelationshipType.INCLUDES,
            ItemRelationshipType.INCLUDES: ItemRelationshipType.PART_OF,
        }
        return mapping.get(self.type)


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
