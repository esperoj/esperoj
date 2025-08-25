from django.db import models
from simple_history.models import HistoricalRecords
import uuid_utils.compat as uuid

class BaseModel(models.Model):
    """An abstract base model."""
    id = models.UUIDField(
            primary_key=True,
            default=uuid.uuid7,
            editable=False,
            unique=True
        )
    created_at = models.DateTimeField(auto_now_add=True, editable=False)
    updated_at = models.DateTimeField(auto_now=True, editable=False)
    history = HistoricalRecords(inherit=True)

    class Meta:
        abstract = True
        app_label = "esperoj"
