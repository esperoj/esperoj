from django.db import models
from simple_history.models import HistoricalRecords


class BaseModel(models.Model):
    """An abstract base model providing self-updating created_at and updated_at fields."""

    created_at = models.DateTimeField(auto_now_add=True, editable=False)
    updated_at = models.DateTimeField(auto_now=True, editable=False)
    history = HistoricalRecords(inherit=True)

    class Meta:
        abstract = True
        app_label = "esperoj"
