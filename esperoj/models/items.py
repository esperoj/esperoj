"""Item models for the esperoj application.

This module contains models for catalogued items in the digital preservation
system, including the base Item model and specific item types like Song and Book.
"""

from typing import TYPE_CHECKING, Union

from django.core.exceptions import ValidationError
from django.db import models
from django.db.models import Index, Manager

from .base import BaseModel

if TYPE_CHECKING:
    from django.db.models import QuerySet

    from .core import Agent
    from .relationships import Attribution, ItemExternalReference, ItemRelationship, ItemRoleName


class ItemType(models.TextChoices):
    """Enumeration for the type of a cataloged Item."""

    SONG = "SONG", "Song"
    BOOK = "BOOK", "Book"
    AUDIOBOOK = "AUDIOBOOK", "Audiobook"
    COMIC = "COMIC", "Comic"
    TEXT = "TEXT", "Text"
    MOVIE = "MOVIE", "Movie"
    GAME = "GAME", "Game"
    IMAGE = "IMAGE", "Image"
    VIDEO = "VIDEO", "Video"
    AUDIO = "AUDIO", "Audio"


class CollectionType(models.TextChoices):
    """Enumeration for the type of a Collection."""

    SERIES = "SERIES", "Series"
    ANTHOLOGY = "ANTHOLOGY", "Anthology"
    ARCHIVE = "ARCHIVE", "Archive"
    OTHER = "OTHER", "Other"


class Collection(BaseModel):
    """A collection of items, such as a book series or music anthology.

    Attributes:
        title: The title or name of the collection.
        identifier: A unique, human-readable identifier for the collection.
        type: The type of collection (e.g., series, anthology).
        description: A free-text description of the collection.
    """

    title = models.CharField(
        max_length=512,
        db_index=True,
        help_text="The title or name of the collection.",
    )
    identifier = models.SlugField(
        max_length=255,
        unique=True,
        help_text="A unique, human-readable identifier for this collection.",
    )
    type = models.CharField(
        max_length=20,
        choices=CollectionType.choices,
        default=CollectionType.SERIES,
        help_text="The type of this collection.",
    )
    description = models.TextField(
        blank=True,
        default="",
        help_text="A description of the collection.",
    )

    class Meta:
        db_table = "collection"
        verbose_name = "Collection"
        verbose_name_plural = "Collections"

    def __str__(self) -> str:
        """Returns the collection's title.

        Returns:
            The title of the collection.
        """
        return self.title

    def get_ordered_items(self) -> "QuerySet[Item]":
        """Returns the items in this collection ordered by their position.

        Returns:
            A queryset of Item instances.
        """
        return self.items.all().order_by("memberships__order")


class CollectionMembership(models.Model):
    """Through model for Item-Collection relationships with sequence ordering.

    Attributes:
        item: The item belonging to the collection.
        collection: The collection the item belongs to.
        order: The numeric order of the item within the collection.
    """

    item = models.ForeignKey(
        "Item",
        on_delete=models.CASCADE,
        related_name="memberships",
    )
    collection = models.ForeignKey(
        "Collection",
        on_delete=models.CASCADE,
        related_name="memberships",
    )
    order = models.PositiveIntegerField(
        default=0,
        help_text="The sequence order of the item within this collection.",
    )

    class Meta:
        db_table = "collection_membership"
        ordering = ["order"]
        unique_together = ("item", "collection")


class Item(BaseModel):
    """The concrete base model for all cataloged objects in the system.

    This model uses multi-table inheritance, where each subclass (like Book or
    Song) gets its own table with a one-to-one link to this base Item table.
    It represents any cataloged item in the digital preservation system.

    Notes about implemented todos:
        - Replaced 'Role' with 'Attribution' and updated references.
        - Added 'Collection' and 'CollectionMembership' for series ordering.
        - Updated '_get_agents_display_string' to use semicolon separator.
        - Replaced the 'people' relationship with 'agents'.
        - Replaced year/month/day fields with a single string 'date' field (EDTF).
        - Replaced 'item_type' field name with simply 'type'.
    """

    # --- Core Information ---
    title = models.CharField(
        max_length=512,
        db_index=True,
        help_text="The main title or name of the item.",
    )
    alternative_titles = models.JSONField(
        blank=True,
        default=list,
        help_text="A list of alternative titles for this item.",
    )
    identifier = models.SlugField(
        max_length=255,
        unique=True,
        help_text="A unique, human-readable identifier for this item.",
    )
    # short name for the item's type (keeps previous enum values)
    type = models.CharField(
        max_length=20,
        choices=ItemType.choices,
        editable=False,
        help_text="The type of this item, set automatically by the subclass.",
    )
    description = models.TextField(
        blank=True,
        default="",
        help_text="A free-text description of the item.",
    )
    languages = models.JSONField(
        blank=True,
        default=list,
        help_text="A list of language codes associated with this item (e.g., ['en', 'fr']).",
    )
    notes = models.TextField(
        blank=True,
        default="",
        help_text="Internal notes about the item.",
    )

    # --- Date ---
    date = models.CharField(
        max_length=64,
        blank=True,
        default="",
        help_text="The date of the item, ideally in EDTF (ISO 8601-2) format (e.g. '1984', '1984-06', '1984-06-12', '200X', '1984~').",
    )

    # --- Relationships ---
    agents = models.ManyToManyField(
        "esperoj.Agent",
        through="esperoj.Attribution",
        related_name="items",
        blank=True,
        help_text="All agents (people/organizations) who contributed to this item.",
    )
    subjects = models.ManyToManyField(
        "esperoj.Subject",
        related_name="items",
        blank=True,
        help_text="Topics or keywords associated with this item.",
    )
    collections = models.ManyToManyField(
        "Collection",
        through="CollectionMembership",
        related_name="items",
        blank=True,
        help_text="Collections this item belongs to.",
    )

    # --- Type hints for reverse relationships ---
    attributions: "Manager[Attribution]"
    external_references: "Manager[ItemExternalReference]"
    outgoing_relationships: "Manager[ItemRelationship]"
    incoming_relationships: "Manager[ItemRelationship]"

    class Meta:
        db_table = "item"
        ordering = ["-date", "identifier"]
        verbose_name = "Item"
        verbose_name_plural = "Items"
        indexes = [
            Index(fields=["identifier"]),
            Index(fields=["title"]),
            Index(fields=["type"]),
            Index(fields=["date"]),
        ]
        constraints = []

    def __str__(self) -> str:
        """Returns the item's title.

        Returns:
            The title of the item.
        """
        return self.title

    def clean(self) -> None:
        """Performs model validation that cannot be handled by the database.

        Raises:
            ValidationError: If date, languages, or alternative titles are invalid.
        """
        super().clean()

        # Validate date field
        if self.date:
            if not isinstance(self.date, str):
                raise ValidationError({"date": "Date must be a string."})
            if len(self.date) > 64:
                raise ValidationError({"date": "Date string is too long."})

        # Validate languages are valid ISO codes (basic check)
        if self.languages:
            if not isinstance(self.languages, list):
                raise ValidationError({"languages": "Languages must be a list of language codes."})

            for lang in self.languages:
                if not isinstance(lang, str) or len(lang) < 2 or len(lang) > 10:
                    raise ValidationError(
                        {"languages": f"Invalid language code: {lang}. Must be 2-10 character strings."}
                    )

        # Validate alternative titles
        if self.alternative_titles:
            if not isinstance(self.alternative_titles, list):
                raise ValidationError({"alternative_titles": "Alternative titles must be a list of strings."})

            for alt_title in self.alternative_titles:
                if not isinstance(alt_title, str):
                    raise ValidationError(
                        {"alternative_titles": f"Invalid alternative title: {alt_title}. Must be a string."}
                    )

    def get_agents_by_role(self, role: Union[str, "ItemRoleName"]) -> "QuerySet[Agent]":
        """Returns a queryset of agents with a specific role for this item.

        Uses select_related to prevent N+1 queries when accessing agent data.

        Args:
            role: The role name or ItemRoleName instance to filter by.

        Returns:
            A queryset of Agent instances.
        """
        return (
            self.agents.filter(attributions__name=role, attributions__item=self)
            .select_related()
            .order_by("attributions__order")
        )

    def _get_agents_display_string(self, qs: "QuerySet[Agent]") -> str:
        """Utility to render agent querysets as a semicolon-separated string.

        Args:
            qs: A queryset of Agent instances.

        Returns:
            A string of agent names separated by semicolons.
        """
        if not qs:
            return ""
        # Convert to list of strings to avoid lazy evaluation surprises in templates
        return "; ".join(str(a) for a in qs)

    @property
    def creators(self) -> "QuerySet[Agent]":
        """Abstract property for primary creators.

        Subclasses MUST override this property to define which roles
        are considered primary creators for that item type.

        Returns:
            A queryset of Agent instances.

        Raises:
            NotImplementedError: If the subclass does not implement this property.
        """
        raise NotImplementedError(f"{self.__class__.__name__} must implement the 'creators' property.")

    @property
    def display_creators(self) -> str:
        """Returns a semicolon-separated string of primary creators.

        Returns:
            A string for display.
        """
        return self._get_agents_display_string(self.creators)

    @property
    def contributors(self) -> "QuerySet[Agent]":
        """Returns all agents who contributed but are not primary creators.

        Returns:
            A queryset of Agent instances.
        """
        creator_pks = self.creators.values_list("pk", flat=True)
        return self.agents.exclude(pk__in=creator_pks).select_related()

    @property
    def display_contributors(self) -> str:
        """Returns a semicolon-separated string of secondary contributors.

        Returns:
            A string for display.
        """
        return self._get_agents_display_string(self.contributors)

    @property
    def display_languages(self) -> str:
        """Returns a comma-separated string of languages.

        Returns:
            A string for display.
        """
        if self.languages:
            return ", ".join(self.languages)
        return ""

    @property
    def display_alternative_titles(self) -> str:
        """Returns a comma-separated string of alternative titles.

        Returns:
            A string for display.
        """
        if self.alternative_titles:
            return ", ".join(self.alternative_titles)
        return ""

    @property
    def display_date(self) -> str:
        """Returns the date string for display.

        Returns:
            The raw date string.
        """
        return self.date

    def get_related_items(self, relationship_type: str | None = None) -> "QuerySet[Item]":
        """Returns items related to this item through any relationship.

        Args:
            relationship_type: Optional filter by relationship type.

        Returns:
            A queryset of related Item instances.
        """
        filters = models.Q()

        if relationship_type:
            filters = models.Q(
                models.Q(
                    incoming_relationships__from_item=self, incoming_relationships__relationship_type=relationship_type
                )
                | models.Q(
                    outgoing_relationships__to_item=self, outgoing_relationships__relationship_type=relationship_type
                )
            )
        else:
            filters = models.Q(
                models.Q(incoming_relationships__from_item=self) | models.Q(outgoing_relationships__to_item=self)
            )

        related_items = Item.objects.filter(filters).distinct()
        return related_items

    def get_parent_items(self) -> "QuerySet[Item]":
        """Returns items that this item is part of.

        Returns:
            A queryset of Item instances.
        """
        from .relationships import ItemRelationshipType

        return Item.objects.filter(
            incoming_relationships__from_item=self,
            incoming_relationships__relationship_type=ItemRelationshipType.PART_OF,
        ).distinct()

    def get_child_items(self) -> "QuerySet[Item]":
        """Returns items that are part of this item.

        Returns:
            A queryset of Item instances.
        """
        from .relationships import ItemRelationshipType

        return Item.objects.filter(
            outgoing_relationships__to_item=self,
            outgoing_relationships__relationship_type=ItemRelationshipType.PART_OF,
        ).distinct()

    def get_sequential_items(self, direction: str = "both") -> "QuerySet[Item]":
        """Returns items in a sequence with this item.

        Args:
            direction: Direction of sequence ("both", "next", or "previous").

        Returns:
            A queryset of sequential Item instances.
        """
        from .relationships import ItemRelationshipType

        if direction == "next":
            return Item.objects.filter(
                models.Q(
                    outgoing_relationships__to_item=self,
                    outgoing_relationships__relationship_type=ItemRelationshipType.FOLLOWS,
                )
                | models.Q(
                    incoming_relationships__from_item=self,
                    incoming_relationships__relationship_type=ItemRelationshipType.PRECEDES,
                )
            ).distinct()
        elif direction == "previous":
            return Item.objects.filter(
                models.Q(
                    incoming_relationships__from_item=self,
                    incoming_relationships__relationship_type=ItemRelationshipType.FOLLOWS,
                )
                | models.Q(
                    outgoing_relationships__to_item=self,
                    outgoing_relationships__relationship_type=ItemRelationshipType.PRECEDES,
                )
            ).distinct()
        else:  # both
            return Item.objects.filter(
                models.Q(
                    incoming_relationships__from_item=self,
                    incoming_relationships__relationship_type__in=[
                        ItemRelationshipType.FOLLOWS,
                        ItemRelationshipType.PRECEDES,
                    ],
                )
                | models.Q(
                    outgoing_relationships__to_item=self,
                    outgoing_relationships__relationship_type__in=[
                        ItemRelationshipType.FOLLOWS,
                        ItemRelationshipType.PRECEDES,
                    ],
                )
            ).distinct()

    def get_next_in_collection(self, collection: "Collection") -> Union["Item", None]:
        """Returns the next item in the given collection according to the series order.

        Args:
            collection: The collection to look within.

        Returns:
            The next Item instance or None if this is the last item or not in the collection.
        """
        current_membership = self.memberships.filter(collection=collection).first()
        if not current_membership:
            return None

        next_membership = (
            self.collections.through.objects.filter(collection=collection, order__gt=current_membership.order)
            .order_by("order")
            .first()
        )
        return next_membership.item if next_membership else None

    def get_previous_in_collection(self, collection: "Collection") -> Union["Item", None]:
        """Returns the previous item in the given collection according to the series order.

        Args:
            collection: The collection to look within.

        Returns:
            The previous Item instance or None if this is the first item or not in the collection.
        """
        current_membership = self.memberships.filter(collection=collection).first()
        if not current_membership:
            return None

        prev_membership = (
            self.collections.through.objects.filter(collection=collection, order__lt=current_membership.order)
            .order_by("-order")
            .first()
        )
        return prev_membership.item if prev_membership else None


class Song(Item):
    """A musical composition and/or recording.

    This model merges the concepts of a musical work and a recording into a single
    entity. It represents both the abstract song (music and lyrics) and its
    recorded performance.

    Attributes:
        album: The album or release this song belongs to.
    """

    album = models.CharField(
        max_length=255,
        blank=True,
        default="",
        help_text="The album or release this song belongs to.",
    )

    class Meta:
        db_table = "song"
        verbose_name = "Song"
        verbose_name_plural = "Songs"
        ordering = ["title"]
        indexes = [
            Index(fields=["album"]),
        ]

    def save(self, *args, **kwargs) -> None:
        """Sets the type before saving.

        Args:
            *args: Variable length argument list.
            **kwargs: Arbitrary keyword arguments.
        """
        self.type = ItemType.SONG
        super().save(*args, **kwargs)

    @property
    def creators(self) -> "QuerySet[Agent]":
        """For a Song, primary creators are Artists.

        Returns:
            A queryset of Agent instances.
        """
        from .relationships import ItemRoleName

        return self.get_agents_by_role(ItemRoleName.ARTIST)

    @property
    def composers(self) -> "QuerySet[Agent]":
        """Returns all agents credited as composers for this song.

        Returns:
            A queryset of Agent instances.
        """
        from .relationships import ItemRoleName

        return self.get_agents_by_role(ItemRoleName.COMPOSER)

    @property
    def display_composers(self) -> str:
        """Returns a semicolon-separated string of composers.

        Returns:
            A string for display.
        """
        return self._get_agents_display_string(self.composers)

    @property
    def lyricists(self) -> "QuerySet[Agent]":
        """Returns all agents credited as lyricists for this song.

        Returns:
            A queryset of Agent instances.
        """
        from .relationships import ItemRoleName

        return self.get_agents_by_role(ItemRoleName.LYRICIST)

    @property
    def display_lyricists(self) -> str:
        """Returns a semicolon-separated string of lyricists.

        Returns:
            A string for display.
        """
        return self._get_agents_display_string(self.lyricists)

    @property
    def artists(self) -> "QuerySet[Agent]":
        """Returns all performing artists for this song.

        Returns:
            A queryset of Agent instances.
        """
        from .relationships import ItemRoleName

        return self.get_agents_by_role(ItemRoleName.ARTIST)

    @property
    def display_artists(self) -> str:
        """Returns a semicolon-separated string of artists.

        Returns:
            A string for display.
        """
        return self._get_agents_display_string(self.artists)


class Book(Item):
    """A book or written publication.

    Attributes:
        isbn_10: The 10-digit International Standard Book Number.
        isbn_13: The 13-digit International Standard Book Number.
        page_count: The number of pages in the book.
        publisher: The publisher of the book.
        edition: The edition information.
        format: The physical format (hardcover, paperback, etc.).
    """

    # --- Book-specific fields ---
    isbn_10 = models.CharField(
        max_length=10,
        blank=True,
        default="",
        help_text="The 10-digit ISBN (without hyphens).",
    )
    isbn_13 = models.CharField(
        max_length=13,
        blank=True,
        default="",
        help_text="The 13-digit ISBN (without hyphens).",
    )
    page_count = models.PositiveIntegerField(
        null=True,
        blank=True,
        help_text="The number of pages in the book.",
    )
    publisher = models.CharField(
        max_length=255,
        blank=True,
        default="",
        help_text="The publisher of the book.",
    )
    edition = models.CharField(
        max_length=100,
        blank=True,
        default="",
        help_text="Edition information (e.g., '2nd Edition', 'Revised').",
    )
    format = models.CharField(
        max_length=50,
        blank=True,
        default="",
        help_text="Physical format (e.g., 'Hardcover', 'Paperback', 'Ebook').",
    )
    extent = models.CharField(
        max_length=255,
        blank=True,
        default="",
        help_text="Extent of the resource (e.g., 'xv, 320 pages').",
    )
    table_of_contents = models.TextField(
        blank=True,
        default="",
        help_text="Table of contents or chapter listing for the book.",
    )

    class Meta:
        db_table = "book"
        verbose_name = "Book"
        verbose_name_plural = "Books"
        indexes = [
            Index(fields=["isbn_10"]),
            Index(fields=["isbn_13"]),
            Index(fields=["publisher"]),
            Index(fields=["page_count"]),
        ]

    @staticmethod
    def format_isbn(isbn: str) -> str:
        """Formats an ISBN string with hyphens.

        Args:
            isbn: A 10 or 13 digit ISBN string without formatting.

        Returns:
            The formatted ISBN string.
        """
        if not isbn:
            return ""

        clean_isbn = isbn.replace("-", "").replace(" ", "")
        if len(clean_isbn) == 13:
            return f"{clean_isbn[:3]}-{clean_isbn[3:4]}-{clean_isbn[4:6]}-{clean_isbn[6:12]}-{clean_isbn[12:]}"
        if len(clean_isbn) == 10:
            return f"{clean_isbn[:1]}-{clean_isbn[1:4]}-{clean_isbn[4:9]}-{clean_isbn[9:]}"

        return isbn

    def save(self, *args, **kwargs) -> None:
        """Sets the type before saving.

        Args:
            *args: Variable length argument list.
            **kwargs: Arbitrary keyword arguments.
        """
        self.type = ItemType.BOOK
        super().save(*args, **kwargs)

    def clean(self) -> None:
        """Performs model validation.

        Raises:
            ValidationError: If ISBN formats are invalid.
        """
        super().clean()

        if self.isbn_10:
            clean_isbn_10 = self.isbn_10.replace("-", "").replace(" ", "")
            if len(clean_isbn_10) != 10 or not clean_isbn_10.replace("X", "").isdigit():
                raise ValidationError({"isbn_10": "ISBN-10 must be 10 digits (last digit can be X)."})
            self.isbn_10 = clean_isbn_10

        if self.isbn_13:
            clean_isbn_13 = self.isbn_13.replace("-", "").replace(" ", "")
            if len(clean_isbn_13) != 13 or not clean_isbn_13.isdigit():
                raise ValidationError({"isbn_13": "ISBN-13 must be 13 digits."})
            self.isbn_13 = clean_isbn_13

    @property
    def creators(self) -> "QuerySet[Agent]":
        """For a Book, the primary creators are the Authors.

        Returns:
            A queryset of Agent instances.
        """
        from .relationships import ItemRoleName

        return self.get_agents_by_role(ItemRoleName.AUTHOR)

    @property
    def authors(self) -> "QuerySet[Agent]":
        """Returns all authors for this book.

        Returns:
            A queryset of Agent instances.
        """
        return self.creators

    @property
    def display_authors(self) -> str:
        """Returns a semicolon-separated string of authors.

        Returns:
            A string for display.
        """
        return self._get_agents_display_string(self.authors)

    @property
    def editors(self) -> "QuerySet[Agent]":
        """Returns all editors for this book.

        Returns:
            A queryset of Agent instances.
        """
        from .relationships import ItemRoleName

        return self.get_agents_by_role(ItemRoleName.EDITOR)

    @property
    def display_editors(self) -> str:
        """Returns a semicolon-separated string of editors.

        Returns:
            A string for display.
        """
        return self._get_agents_display_string(self.editors)

    @property
    def translators(self) -> "QuerySet[Agent]":
        """Returns all translators for this book.

        Returns:
            A queryset of Agent instances.
        """
        from .relationships import ItemRoleName

        return self.get_agents_by_role(ItemRoleName.TRANSLATOR)

    @property
    def display_translators(self) -> str:
        """Returns a semicolon-separated string of translators.

        Returns:
            A string for display.
        """
        return self._get_agents_display_string(self.translators)

    @property
    def primary_isbn(self) -> str:
        """Returns the primary ISBN (preferring ISBN-13).

        Returns:
            The raw ISBN string.
        """
        return self.isbn_13 or self.isbn_10

    @property
    def display_isbn(self) -> str:
        """Returns a formatted ISBN for display.

        Returns:
            A string for display.
        """
        return self.format_isbn(self.primary_isbn)

    @property
    def has_isbn(self) -> bool:
        """Returns True if the book has any ISBN.

        Returns:
            True if an ISBN is present.
        """
        return bool(self.isbn_10 or self.isbn_13)
