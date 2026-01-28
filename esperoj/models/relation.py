import uuid_utils.compat as uuid
from django.db import models

from .entity import Entity


class RelationType(models.Model):
    id = models.UUIDField(primary_key=True, default=uuid.uuid7, editable=False, unique=True)
    name = models.CharField(max_length=100, unique=True)
    identifier = models.SlugField(max_length=100, unique=True)
    description = models.TextField(blank=True)
    forward_verb = models.CharField(max_length=100, blank=True)
    reverse_verb = models.CharField(max_length=100, blank=True)
    created_at = models.DateTimeField(auto_now_add=True)
    updated_at = models.DateTimeField(auto_now=True, editable=False)

    class Meta:
        db_table = "relation_type"
        app_label = "esperoj"
        ordering = ["name"]

    def __str__(self):
        return self.name


class EntityRelation(models.Model):
    source = models.ForeignKey(Entity, on_delete=models.CASCADE, related_name="outgoing_relations")
    target = models.ForeignKey(Entity, on_delete=models.CASCADE, related_name="incoming_relations")
    relation_type = models.ForeignKey(RelationType, on_delete=models.PROTECT, related_name="relations")
    created_at = models.DateTimeField(auto_now_add=True)
    updated_at = models.DateTimeField(auto_now=True, editable=False)

    class Meta:
        db_table = "entity_relation"
        app_label = "esperoj"
        unique_together = ("source", "target", "relation_type")

    def __str__(self):
        return f"{self.source.pk} -> {self.relation_type.identifier} -> {self.target.pk}"
