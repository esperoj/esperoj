"""Models for representing relationships between items, agents, and external resources.

This module defines the structures for linking items to the people and organizations
involved in their creation (AgentItems), linking items to each other (ItemRelationships),
and connecting internal records to external databases (References).
"""

from typing import Optional

from django.db import models
from django.db.models import Index

from .base import Entity
from .core import Collection
from .item import Item


class AgentItemManager(models.Manager):
    """Manager for Agent-Item relationships."""

    pass


class ItemRelationshipManager(models.Manager):
    """Manager for Item-to-Item relationships."""

    pass


class CollectionItem(models.Model):
    """Through model for Item-Collection relationships with sequence ordering.

    Attributes:
        item: The item belonging to the collection.
        collection: The collection the item belongs to.
        relation_type: The nature of the item's inclusion in this collection.
        order: The numeric order of the item within the collection.
    """

    class RelationType(models.TextChoices):
        """Nature of an item's inclusion in a collection."""

        MEMBER = "Member", "Member"
        FEATURED = "Featured", "Featured"
        SUPPLEMENTARY = "Supplementary", "Supplementary"

    item = models.ForeignKey(
        Item,
        on_delete=models.CASCADE,
        related_name="collection_items",
    )
    collection = models.ForeignKey(
        Collection,
        on_delete=models.CASCADE,
        related_name="collection_items",
    )
    relation_type = models.CharField(
        max_length=50,
        choices=RelationType.choices,
        default=RelationType.MEMBER,
        help_text="The nature of the item's inclusion in this collection.",
    )
    order = models.PositiveIntegerField(
        null=True,
        blank=True,
        default=None,
        help_text="The sequence order of the item within this collection (optional).",
    )

    class Meta:
        db_table = "collection_item"
        ordering = ["order"]
        unique_together = ("item", "collection", "relation_type")


class AgentItem(Entity):
    """
    Represents the relationship (role) an Agent plays in relation to an Item.

    This model serves as the 'through' table for the many-to-many relationship
    between Agent and Item, allowing us to specify the nature of an agent's
    contribution to a specific item.

    Attributes:
        agent: The Agent involved in the relationship.
        item: The Item to which the Agent is contributing.
        relation_type: The specific role (e.g., 'Author', 'Composer', 'Artist', 'Publisher').
        notes: Optional notes about this specific relationship assignment.
    """

    class RelationType(models.TextChoices):
        """
        A unified list of all possible roles an Agent can have in relation to an Item.
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

    # --- Relationships ---
    agent = models.ForeignKey(
        "esperoj.Agent",
        on_delete=models.CASCADE,
        related_name="agent_items",
        help_text="The agent associated with this relationship.",
    )
    item = models.ForeignKey(
        "esperoj.Item",
        on_delete=models.CASCADE,
        related_name="agent_items",
        help_text="The item to which this relationship pertains.",
    )

    # --- Role Details ---
    relation_type = models.CharField(
        max_length=50,
        choices=RelationType.choices,
        help_text="The specific role performed by the agent for this item.",
    )
    notes = models.TextField(
        blank=True,
        default="",
        help_text="Optional notes about this specific relationship assignment.",
    )

    objects = AgentItemManager()

    class Meta:
        db_table = "agent_item"
        unique_together = ("agent", "item", "relation_type")
        ordering = ["item", "relation_type", "agent"]
        verbose_name = "Agent Item"
        verbose_name_plural = "Agent Items"
        indexes = [
            Index(fields=["agent"]),
            Index(fields=["item"]),
            Index(fields=["relation_type"]),
            Index(fields=["item", "relation_type"]),
        ]

    def __str__(self) -> str:
        """Returns a string representing the relationship."""
        return f"{self.agent.authorized_name} as {self.relation_type} for {self.item.title}"


class AbstractReference(Entity):
    """Base class for external identifiers and links."""

    class ReferenceType(models.TextChoices):
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

    type = models.CharField(
        max_length=50,
        choices=ReferenceType.choices,
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


class AgentReference(AbstractReference):
    """An external reference link associated with an Agent."""

    agent = models.ForeignKey(
        "esperoj.Agent",
        on_delete=models.CASCADE,
        related_name="external_references",
        help_text="The agent this link refers to.",
    )

    class Meta(AbstractReference.Meta):
        db_table = "agent_external_reference"
        verbose_name = "Agent External Reference"
        verbose_name_plural = "Agent External References"


class ItemReference(AbstractReference):
    """An external reference link associated with an Item."""

    item = models.ForeignKey(
        "esperoj.Item",
        on_delete=models.CASCADE,
        related_name="external_references",
        help_text="The item this link refers to.",
    )

    class Meta(AbstractReference.Meta):
        db_table = "item_external_reference"
        verbose_name = "Item External Reference"
        verbose_name_plural = "Item External References"


class ItemRelation(Entity):
    """
    Represents a relationship between two items.

    This model allows for complex graphs of items, including hierarchies
    (e.g., chapters in a book), sequences (e.g., songs in an album),
    and semantic links (e.g., adaptations, translations).

    Attributes:
        from_item: The source item in the relationship.
        to_item: The target item in the relationship.
        type: The nature of the relationship (from RelationType).
        order: The sequence order (e.g., track number or chapter number).
        notes: Optional context or explanation for the relationship.
    """

    class RelationType(models.TextChoices):
        """
        Defines the nature of a relationship between two items.
        Satisfies Dublin Core Metadata Initiative (DCMI) relation terms.
        """

        # --- Dublin Core (DCMI) Terms ---
        IS_VERSION_OF = "isVersionOf", "Is Version Of"
        HAS_VERSION = "hasVersion", "Has Version"
        IS_REPLACED_BY = "isReplacedBy", "Is Replaced By"
        REPLACES = "replaces", "Replaces"
        IS_REQUIRED_BY = "isRequiredBy", "Is Required By"
        REQUIRES = "requires", "Requires"
        IS_PART_OF = "isPartOf", "Is Part Of"
        HAS_PART = "hasPart", "Has Part"
        IS_REFERENCED_BY = "isReferencedBy", "Is Referenced By"
        REFERENCES = "references", "References"
        IS_FORMAT_OF = "isFormatOf", "Is Format Of"
        HAS_FORMAT = "hasFormat", "Has Format"
        CONFORMS_TO = "conformsTo", "Conforms To"

        # --- Contextual and Structural Roles ---
        PARENT = "Parent", "Parent"
        CHILD = "Child", "Child"
        PRECEDES = "Precedes", "Precedes"
        FOLLOWS = "Follows", "Follows"
        ADAPTATION_OF = "AdaptationOf", "Adaptation Of"
        TRANSLATION_OF = "TranslationOf", "Translation Of"

        # --- Compatibility Aliases ---
        VERSION_OF = IS_VERSION_OF
        PART_OF = IS_PART_OF
        INCLUDES = HAS_PART

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
        choices=RelationType.choices,
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
        # Using getattr to satisfy static analysis checks for dynamic get_FOO_display methods
        type_display = getattr(self, "get_type_display")()
        return f"'{self.from_item.title}' {type_display} '{self.to_item.title}'"

    def clean(self) -> None:
        """Ensures the relationship is valid."""
        super().clean()

        # Prevent items from relating to themselves
        # Using primary keys directly to avoid loading full objects if possible
        if self.from_item and self.to_item and self.from_item.pk == self.to_item.pk:
            from django.core.exceptions import ValidationError

            raise ValidationError("An item cannot have a relationship with itself.")

    @property
    def is_hierarchical(self) -> bool:
        """Returns True if the relationship is hierarchical (Parent/Child or Part/Includes)."""
        return self.type in [
            self.RelationType.PARENT,
            self.RelationType.CHILD,
            self.RelationType.IS_PART_OF,
            self.RelationType.HAS_PART,
        ]

    @property
    def is_sequential(self) -> bool:
        """Returns True if the relationship is sequential (Precedes/Follows)."""
        return self.type in [
            self.RelationType.PRECEDES,
            self.RelationType.FOLLOWS,
        ]

    @property
    def is_creative(self) -> bool:
        """Returns True if the relationship is derivative (Version/Adaptation/Translation)."""
        return self.type in [
            self.RelationType.IS_VERSION_OF,
            self.RelationType.HAS_VERSION,
            self.RelationType.ADAPTATION_OF,
            self.RelationType.TRANSLATION_OF,
        ]

    def get_inverse_relationship_type(self) -> Optional["ItemRelation.RelationType"]:
        """Returns the logical opposite relationship type, if one exists."""
        mapping = {
            self.RelationType.PARENT: self.RelationType.CHILD,
            self.RelationType.CHILD: self.RelationType.PARENT,
            self.RelationType.PRECEDES: self.RelationType.FOLLOWS,
            self.RelationType.FOLLOWS: self.RelationType.PRECEDES,
            self.RelationType.IS_PART_OF: self.RelationType.HAS_PART,
            self.RelationType.HAS_PART: self.RelationType.IS_PART_OF,
            self.RelationType.IS_VERSION_OF: self.RelationType.HAS_VERSION,
            self.RelationType.HAS_VERSION: self.RelationType.IS_VERSION_OF,
            self.RelationType.IS_REPLACED_BY: self.RelationType.REPLACES,
            self.RelationType.REPLACES: self.RelationType.IS_REPLACED_BY,
            self.RelationType.IS_REQUIRED_BY: self.RelationType.REQUIRES,
            self.RelationType.REQUIRES: self.RelationType.IS_REQUIRED_BY,
            self.RelationType.IS_REFERENCED_BY: self.RelationType.REFERENCES,
            self.RelationType.REFERENCES: self.RelationType.IS_REFERENCED_BY,
            self.RelationType.IS_FORMAT_OF: self.RelationType.HAS_FORMAT,
            self.RelationType.HAS_FORMAT: self.RelationType.IS_FORMAT_OF,
        }
        # Cast the string field value to the enum member for dictionary lookup
        try:
            enum_type = self.RelationType(self.type)
            return mapping.get(enum_type)
        except ValueError:
            return None
