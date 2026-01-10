"""Core models module for the Esperoj project.

This module defines abstract base models, such as BaseModel, which provide
common fields, functionality, and historical tracking for all other models
in the application. It aims to ensure consistency and reduce redundancy
across the database schema.
"""

import uuid_utils.compat as uuid
from django.db import models
from django.db.models.functions import Length
from simple_history.models import HistoricalRecords

models.CharField.register_lookup(Length, "length")


class BaseModel(models.Model):
    """Abstract base model providing common fields and functionality for all models.

    This model provides a UUID primary key, creation and update timestamps, and
    historical records tracking. Concrete models inheriting from BaseModel
    should explicitly define their `db_table` in their `Meta` class to ensure
    consistent naming and prevent unexpected behavior.

    Attributes:
        id (models.UUIDField): Unique identifier for the record, automatically
            generated as a UUID v7.
        created_at (models.DateTimeField): Timestamp indicating when the record
            was first created.
        updated_at (models.DateTimeField): Timestamp indicating when the record
            was last updated.
        history (HistoricalRecords): Historical records tracking changes using
            simple_history.
    """

    # Core Fields
    id = models.UUIDField(
        primary_key=True,
        default=uuid.uuid7,
        editable=False,
        unique=True,
        help_text="Unique identifier for the record, automatically generated as a UUID v7.",
    )

    # Timestamp Fields
    created_at = models.DateTimeField(
        auto_now_add=True,
        editable=False,
        help_text="Timestamp indicating when the record was first created.",
    )
    updated_at = models.DateTimeField(
        auto_now=True,
        editable=False,
        help_text="Timestamp indicating when the record was last updated.",
    )

    # History Tracking
    history = HistoricalRecords(inherit=True)

    class Meta:
        abstract = True
        app_label = "esperoj"
        indexes = [
            models.Index(fields=["created_at"], name="idx_%(app_label)s_%(class)s_created_at"),
            models.Index(fields=["updated_at"], name="idx_%(app_label)s_%(class)s_updated_at"),
        ]
