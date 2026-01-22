"""Core models module for the Esperoj project."""

import uuid_utils.compat as uuid
from django.contrib.contenttypes.models import ContentType
from django.db import models
from django.db.models.functions import Length
from simple_history.models import HistoricalRecords

models.CharField.register_lookup(Length, "length")


class RelationType(models.Model):
    id = models.UUIDField(primary_key=True, default=uuid.uuid7, editable=False, unique=True)
    name = models.CharField(max_length=100, unique=True)
    slug = models.SlugField(max_length=100, unique=True)
    description = models.TextField(blank=True)
    forward_verb = models.CharField(max_length=100, blank=True)
    reverse_verb = models.CharField(max_length=100, blank=True)

    class Meta:
        db_table = "relation_type"
        app_label = "esperoj"
        ordering = ["name"]

    def __str__(self):
        return self.name


class Entity(models.Model):
    id = models.UUIDField(primary_key=True, default=uuid.uuid7, editable=False, unique=True)
    polymorphic_ctype = models.ForeignKey(
        ContentType, null=True, editable=False, on_delete=models.CASCADE, related_name="polymorphic_%(class)s"
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
            models.Index(fields=["polymorphic_ctype"], name="idx_%(app_label)s_%(class)s_ctype"),
        ]

    def save(self, *args, **kwargs):
        if not self.polymorphic_ctype:
            self.polymorphic_ctype = ContentType.objects.get_for_model(self.__class__)
        super().save(*args, **kwargs)

    def get_real_instance(self):
        if self.polymorphic_ctype:
            model_class = self.polymorphic_ctype.model_class()
            if model_class and model_class != Entity:
                return model_class.objects.get(id=self.id)
        return self

    def __str__(self):
        return f"Entity {self.id}"


class EntityRelation(models.Model):
    source = models.ForeignKey(Entity, on_delete=models.CASCADE, related_name="outgoing_relations")
    target = models.ForeignKey(Entity, on_delete=models.CASCADE, related_name="incoming_relations")
    relation_type = models.ForeignKey(RelationType, on_delete=models.PROTECT, related_name="relations")
    created_at = models.DateTimeField(auto_now_add=True)

    class Meta:
        db_table = "entity_relation"
        app_label = "esperoj"
        unique_together = ("source", "target", "relation_type")

    def __str__(self):
        # We use .pk here. It satisfies type checkers.
        # Note: This might trigger a DB query if 'relation_type' isn't pre-fetched.
        return f"{self.source.pk} -> {self.relation_type.slug} -> {self.target.pk}"
