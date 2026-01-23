"""Core models module for the Esperoj project.

This module defines the base `Entity` model, which acts as the foundational
table for content types within the application.
"""

import uuid_utils.compat as uuid
from django.apps import apps
from django.db import models
from django.db.models.functions import Length
from simple_history.models import HistoricalRecords

models.CharField.register_lookup(Length, "length")


class Entity(models.Model):
    """Represents the base abstract entity for the project.

    This model serves as a parent class for specific content types (such as Books)
    and provides a consistent ID strategy, type discrimination, and audit timestamps.

    Attributes:
        id (uuid.UUID): The primary key for the entity, generated using UUIDv7.
            This ensures time-ordered uniqueness.
        type (str): The discriminator field indicating the specific subclass
            or category of the entity (e.g., 'BOOK'). Choices are defined
            in `Entity.EntityType`.
        created_at (datetime.datetime): The date and time when the entity was
            created.
        updated_at (datetime.datetime): The date and time when the entity was
            last modified.
        history (simple_history.manager.HistoryManager): An interface for viewing
            historical changes made to this record.
    """

    class EntityType(models.TextChoices):
        """Enumeration of valid entity types supported by the system."""

        BOOK = "BOOK", "Book"

    id = models.UUIDField(primary_key=True, default=uuid.uuid7, editable=False, unique=True)
    type = models.CharField(
        max_length=255,
        choices=EntityType.choices,
        default=EntityType.BOOK,
        editable=False,
    )
    created_at = models.DateTimeField(auto_now_add=True, editable=False)
    updated_at = models.DateTimeField(auto_now=True, editable=False)
    history = HistoricalRecords(inherit=True)

    class Meta:
        db_table = "entity"
        app_label = "esperoj"
        indexes = [
            models.Index(fields=["created_at"], name="idx_%(app_label)s_%(class)s_created_at"),
            models.Index(fields=["updated_at"], name="idx_%(app_label)s_%(class)s_updated_at"),
            # Note: 'kind' is not defined in the fields above; assuming this refers to 'type'
            models.Index(fields=["type"], name="idx_%(app_label)s_%(class)s_type"),
        ]

    def get_real_instance(self):
        """Retrieves the concrete model instance based on the entity type.

        This method uses the `type` field to dynamically resolve the subclass
        model and fetch the complete instance.

        Returns:
            models.Model: The specific subclass instance (e.g., a `Book` object)
            if the app registry lookup and database query succeed. Returns `self`
            (the generic `Entity` instance) if the specific model cannot be found.
        """
        try:
            if model_class := apps.get_model(self._meta.app_label, self.type):
                return model_class.objects.get(id=self.id)
        except (LookupError, ValueError):
            pass
        return self

    def __str__(self):
        """Returns the human-readable string representation of the entity.

        Returns:
            str: A string formatted as "Entity <uuid>".
        """
        return f"Entity {self.id}"
