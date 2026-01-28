from typing import TYPE_CHECKING

from django.db import models

from .entity import Entity

if TYPE_CHECKING:
    from django.db.models.manager import RelatedManager

    from .relation import EntityRelation


class EntityItemMixin(models.Model):
    """
    Mixin for Entity subclasses providing convenience properties for related items.

    This mixin adds properties like `subjects` and `collections` which utilize the
    `EntityRelation` system. It is designed to be efficient by leveraging Django's
    prefetching mechanism to avoid N+1 query problems.
    """

    if TYPE_CHECKING:
        incoming_relations: RelatedManager["EntityRelation"]
        outgoing_relations: RelatedManager["EntityRelation"]

    class Meta:
        abstract = True

    @property
    def subjects(self) -> list[Entity]:
        """
        Returns entities linked as subjects via outgoing relations.

        To avoid N+1 queries, ensure that relations are prefetched:
        .prefetch_related('outgoing_relations__target', 'outgoing_relations__relation_type')
        """
        # Using .all() ensures we use the prefetch cache if available, keeping the filter in-memory.
        return [rel.target for rel in self.outgoing_relations.all() if rel.relation_type.identifier == "subject"]

    @property
    def collections(self) -> list[Entity]:
        """
        Returns entities (collections) that this entity belongs to via incoming relations.

        To avoid N+1 queries, ensure that relations are prefetched:
        .prefetch_related('incoming_relations__source', 'incoming_relations__relation_type')
        """
        # Using .all() ensures we use the prefetch cache if available, keeping the filter in-memory.
        return [rel.source for rel in self.incoming_relations.all() if rel.relation_type.identifier == "collection"]
