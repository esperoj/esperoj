from django.db import models
from django.db.models import Index, Manager
from django.utils.translation import gettext_lazy as _

from .entity import Entity
from .relation import EntityRelation


# todo: update docstrings
class Agent(Entity):
    """Represents an agent (PREMIS: Agent entity).

    An agent can be an author, artist, publisher, or any entity that is
    responsible for or associated with an item. This model stores basic
    descriptive information, fuzzy dates (EDTF), and alternative names.

    Attributes:
        name (str): The full, authoritative name.
        sort_name (str): The name used for sorting. Managed manually.
        identifier (str): A unique, URL-friendly slug.
        type (str): The type of agent (Person, Organization, Family, etc.).
        birth_date (str): EDTF string for birth/founding (e.g., '1980', '1980?').
        death_date (str): EDTF string for death/dissolution.
        description (str): Public biographical info.
        note (str): Internal notes.
        alternative_names (dict): JSON of alias names.

    Reverse relationships (defined on other models via related_name):
        items: Items related to this agent.
        relations: AgentItems to items.
        references: External references associated with this agent (uses AgentReference model).
    """

    class AgentType(models.TextChoices):
        """Defines the types of agents, aligned with PREMIS and ISAAR standards."""

        PERSON = "Person", _("Person")
        ORGANIZATION = "Organization", _("Organization")
        FAMILY = "Family", _("Family")
        SOFTWARE = "Software", _("Software")
        OTHER = "Other", _("Other")

    # --- Core Information ---
    name = models.CharField(
        max_length=512,
        help_text=_("The full, authoritative name (e.g., 'Dr. Martin Luther King, Jr.' or 'Penguin Books')."),
    )
    sort_name = models.CharField(
        max_length=512,
        blank=True,
        help_text=_("The name used for sorting. This should be provided/maintained manually."),
    )

    agent_type = models.CharField(
        max_length=20,
        choices=AgentType.choices,
        default=AgentType.PERSON,
        help_text=_("High-level classification (matches PREMIS agentType)."),
    )

    # --- Temporal Details (EDTF Standard) ---
    # Using CharField to support "1990?", "1990~", "2020-05", etc.
    birth_date = models.CharField(
        max_length=50,
        blank=True,
        default="",
        help_text=_("Date of birth or founding in EDTF format (e.g., '1980', '1980-05~', '1980?')."),
    )
    death_date = models.CharField(
        max_length=50,
        blank=True,
        default="",
        help_text=_("Date of death or dissolution in EDTF format."),
    )

    # --- Description and Notes ---
    description = models.TextField(
        blank=True,
        default="",
        help_text=_("A public description or biographical information."),
    )
    note = models.TextField(
        blank=True,
        default="",
        help_text=_("Internal notes, not intended for public display."),
    )

    # --- Alternative Names (JSON) ---
    alternative_names = models.JSONField(
        blank=True,
        default=dict,
        help_text=_("Variant names. Keys typically are language codes or types."),
    )

    relations: "Manager[EntityRelation]"

    class Meta:
        db_table = "agent"
        ordering = ["sort_name", "name"]
        verbose_name = _("Agent")
        verbose_name_plural = _("Agents")
        indexes = [
            Index(fields=["identifier"]),
            Index(fields=["sort_name"]),
            Index(fields=["name"]),
            Index(fields=["type"]),
        ]

    def __str__(self) -> str:
        return self.name
