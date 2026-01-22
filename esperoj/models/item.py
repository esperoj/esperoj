"""Item models for the esperoj application.

This module contains models for catalogued items in the digital preservation
system, including the base Item model and specific item types like Song and Book.
"""

from typing import TYPE_CHECKING, Union

from django.core.exceptions import ValidationError
from django.db import models
from django.db.models import Index, Manager

from .base import Entity

if TYPE_CHECKING:
    from django.db.models import QuerySet

    from .core import Agent
    from .relationship import Attribution, ItemExternalReference, ItemRoleName


class Collection(Entity):
    """A collection of items, such as a some series or music anthology.

    Attributes:
        title: The title or name of the collection.
        identifier: A unique, human-readable identifier for the collection.
        type: The type of collection (e.g., series, anthology).
        description: A free-text description of the collection.
    """

    class CollectionType(models.TextChoices):
        """Enumeration for the type of a collection."""

        SERIES = "SERIES", "Series"
        ANTHOLOGY = "ANTHOLOGY", "Anthology"

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


class Item(Entity):
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

    subjects = models.ManyToManyField(
        "Subject",
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
