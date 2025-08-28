from django.db import models
from simple_history.models import HistoricalRecords
import uuid_utils.compat as uuid


class BaseModel(models.Model):
    """
    An abstract base model providing common fields and functionality for all models.

    This model includes:
    - A UUID primary key (`id`).
    - `created_at` and `updated_at` timestamps for record creation and last update.
    - Historical records tracking changes using `simple_history`.

    Concrete models inheriting from BaseModel should explicitly define their `db_table`
    in their `Meta` class to ensure consistent naming and prevent unexpected behavior.
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
