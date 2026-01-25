from django.db import models

from .entity import Entity
from .relation import EntityRelation


class Collection(Entity):
    """A collection grouping multiple related Items.

    Attributes:
        name: The name of the collection.
        collection_type: High-level classification (Series, Anthology, etc.).
        description: Description of the collection's scope.
    """

    class CollectionType(models.TextChoices):
        """Enumeration for the type of a Collection."""

        SERIES = "SERIES", "Series"
        ANTHOLOGY = "ANTHOLOGY", "Anthology"
        ARCHIVE = "ARCHIVE", "Archive"
        OTHER = "OTHER", "Other"

    name = models.CharField(
        max_length=512,
        help_text="The name of the collection.",
    )
    collection_type = models.CharField(
        max_length=20,
        choices=CollectionType.choices,
        default=CollectionType.OTHER,
        help_text="High-level classification of the collection.",
    )
    description = models.TextField(
        blank=True,
        default="",
        help_text="A detailed description of the collection's scope.",
    )

    relations: models.Manager["EntityRelation"]

    class Meta:
        db_table = "collection"
        ordering = ["name"]
        verbose_name = "Collection"
        verbose_name_plural = "Collections"
        indexes = [
            models.Index(fields=["name"]),
            models.Index(fields=["collection_type"]),
        ]

    def __str__(self) -> str:
        return self.name
