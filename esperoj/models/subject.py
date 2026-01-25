from django.db import models

from .entity import Entity
from .relation import EntityRelation


class Subject(Entity):
    """A subject, topic, or keyword. Aligns with SKOS (Simple Knowledge Organization System).

    Attributes:
        name: The name of the subject.
        description: Description of the subject.
    """

    name = models.CharField(
        max_length=512,
        help_text="The name of the subject or topic.",
    )
    description = models.TextField(
        blank=True,
        default="",
        help_text="A detailed description of the subject.",
    )

    relations: models.Manager["EntityRelation"]

    class Meta:
        db_table = "subject"
        ordering = ["name"]
        verbose_name = "Subject"
        verbose_name_plural = "Subjects"
        indexes = [
            models.Index(fields=["name"]),
        ]

    def __str__(self) -> str:
        return self.name
