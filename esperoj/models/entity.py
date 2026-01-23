"""Core models module for the Esperoj project."""

import uuid_utils.compat as uuid
from django.apps import apps
from django.core.exceptions import ObjectDoesNotExist
from django.db import models
from django.db.models.functions import Length
from simple_history.models import HistoricalRecords

models.CharField.register_lookup(Length, "length")


class Entity(models.Model):
    """
    Base model for all entities in the Esperoj project.

    Provides core fields including a unique slug identifier, a UUID primary key,
    polymorphic type tracking, and automatic timestamping.

    Attributes:
        id (UUID): Primary key, generated via UUIDv7 for time-sorted uniqueness.
        identifier (str): A unique, human-readable slug identifier.
        type (str): The specific subclass type of the entity (e.g., 'BOOK').
        created_at (datetime): Timestamp when the entity was first created.
        updated_at (datetime): Timestamp when the entity was last modified.
        history (HistoricalRecords): Audit log of changes to the entity.
    """

    class EntityType(models.TextChoices):
        """Enumeration of available entity types."""

        BOOK = "BOOK", "Book"

    # Primary Key
    id = models.UUIDField(
        primary_key=True,
        default=uuid.uuid7,
        editable=False,
        unique=True,
        help_text="Unique UUIDv7 identifier.",
    )

    # Human-Readable Identifier
    identifier = models.SlugField(
        max_length=255,
        unique=True,
        help_text="A unique, human-readable identifier (slug).",
    )

    # Discriminator
    type = models.CharField(
        max_length=255,
        choices=EntityType.choices,
        default=EntityType.BOOK,
        editable=False,
        help_text="The polymorphic type of this entity.",
    )

    # Timestamps
    created_at = models.DateTimeField(
        auto_now_add=True,
        editable=False,
        help_text="The time when the entity was created.",
    )
    updated_at = models.DateTimeField(
        auto_now=True,
        editable=False,
        help_text="The time when the entity was last updated.",
    )

    # History / Meta
    history = HistoricalRecords(inherit=True)

    class Meta:
        """Meta options for the Entity model."""

        db_table = "entity"
        app_label = "esperoj"
        ordering = ["-created_at"]
        indexes = [
            models.Index(fields=["type"], name="idx_%(app_label)s_%(class)s_type"),
            models.Index(fields=["created_at"], name="idx_%(app_label)s_%(class)s_created_at"),
            models.Index(fields=["updated_at"], name="idx_%(app_label)s_%(class)s_updated_at"),
        ]

    def get_real_instance(self) -> models.Model:
        """
        Retrieve the specialized model instance (e.g., Book) for this entity.

        Attempts to load the child model based on the stored `type`. If the
        specialized model cannot be found or loaded, returns the current
        Entity instance.

        Returns:
            models.Model: The specific subclass instance if found, otherwise self.
        """
        try:
            if model_class := apps.get_model(self._meta.app_label, self.type):
                return model_class.objects.get(id=self.id)
        except (LookupError, ValueError, ObjectDoesNotExist):
            pass
        return self

    def __str__(self) -> str:
        """Return the string representation of the entity."""
        return f"Entity {self.identifier} ({self.id})"
