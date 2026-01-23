"""Core models module for the Esperoj project."""

import uuid_utils.compat as uuid
from django.apps import apps
from django.db import models
from django.db.models.functions import Length
from simple_history.models import HistoricalRecords

models.CharField.register_lookup(Length, "length")


class Entity(models.Model):
    class Kind(models.TextChoices):
        BOOK = "BOOK", "Book"

    id = models.UUIDField(primary_key=True, default=uuid.uuid7, editable=False, unique=True)
    kind = models.CharField(
        max_length=255,
        choices=Kind.choices,
        default=Kind.BOOK,
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
            models.Index(fields=["kind"], name="idx_%(app_label)s_%(class)s_kind"),
        ]

    def save(self, *args, **kwargs):
        if self.kind == self.Kind.ENTITY and self.__class__ != Entity:
            try:
                self.kind = self.Kind[self.__class__.__name__.upper()]
            except KeyError:
                pass
        super().save(*args, **kwargs)

    def get_real_instance(self):
        if self.kind and self.kind != self.Kind.ENTITY:
            try:
                model_class = apps.get_model(self._meta.app_label, self.kind)
                if model_class and model_class != Entity:
                    return model_class.objects.get(id=self.id)
            except (LookupError, ValueError):
                pass
        return self

    def __str__(self):
        return f"Entity {self.id}"
