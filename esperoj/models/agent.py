from django.db import models

from .entity import Entity
from .relation import EntityRelation


class Agent(Entity):
    """Represents an agent (PREMIS: Agent entity).

    An agent can be an author, artist, publisher, or any entity responsible
    for or associated with an item. This model stores basic descriptive
    information and fuzzy dates using the EDTF standard.

    Attributes:
        agent_type (str): High-level classification (Person, Organization, etc.).
        name (str): The full, authoritative name.
        sort_name (str): The name used for sorting. Managed manually.
        identifier (str): A unique, URL-friendly slug (inherited from Entity).
        birth_date (str): EDTF string for birth/founding (e.g., '1980', '1980?').
        death_date (str): EDTF string for death/dissolution.
        description (str): Public biographical information.

    Reverse relationships:
        relations (Manager): Manager for EntityRelation objects associated with this agent.
    """

    class AgentType(models.TextChoices):
        """Defines the types of agents, aligned with PREMIS and ISAAR standards."""

        PERSON = "Person", "Person"
        ORGANIZATION = "Organization", "Organization"
        FAMILY = "Family", "Family"
        SOFTWARE = "Software", "Software"
        OTHER = "Other", "Other"

    # --- Classification ---
    agent_type = models.CharField(
        max_length=20,
        choices=AgentType.choices,
        default=AgentType.PERSON,
        help_text="High-level classification (matches PREMIS agentType).",
    )

    # --- Identity ---
    name = models.CharField(
        max_length=512,
        help_text="The full, authoritative name (e.g., 'Dr. Martin Luther King, Jr.' or 'Penguin Books').",
    )
    sort_name = models.CharField(
        max_length=512,
        blank=True,
        help_text="The name used for sorting. This should be provided/maintained manually.",
    )

    # --- Temporal Details (EDTF Standard) ---
    # Using CharField to support "1990?", "1990~", "2020-05", etc.
    birth_date = models.CharField(
        max_length=50,
        blank=True,
        default="",
        help_text="Date of birth or founding in EDTF format (e.g., '1980', '1980-05~', '1980?').",
    )
    death_date = models.CharField(
        max_length=50,
        blank=True,
        default="",
        help_text="Date of death or dissolution in EDTF format.",
    )

    # --- Description and Notes ---
    description = models.TextField(
        blank=True,
        default="",
        help_text="A public description or biographical information.",
    )

    # --- Managers and Relationships ---
    relations: models.Manager[EntityRelation]

    class Meta:
        db_table = "agent"
        ordering = ["sort_name", "name"]
        verbose_name = "Agent"
        verbose_name_plural = "Agents"
        indexes = [
            models.Index(fields=["identifier"]),
            models.Index(fields=["agent_type"]),
            models.Index(fields=["name"]),
            models.Index(fields=["sort_name"]),
        ]

    def __str__(self) -> str:
        return self.name
