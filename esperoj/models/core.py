"""Core entities for the esperoj application.

This module contains the fundamental entities that other models reference:
Agent, Subject, and Collection. These models are kept separate to avoid
circular dependencies and provide a clear foundation for the rest of the system.
"""

from typing import TYPE_CHECKING

from django.db import models
from django.db.models import Index, Manager

from .base import BaseModel

if TYPE_CHECKING:
    from .items import Item
    from .relationships import AgentExternalReference, Role


class AgentType(models.TextChoices):
    """Defines the types of agents in the system."""

    PERSON = "Person", "Person"
    ORGANIZATION = "Organization", "Organization"
    GROUP = "Group", "Group"
    SOFTWARE = "Software", "Software"
    OTHER = "Other", "Other"


class Agent(BaseModel):
    """Represents an agent, such as a person, organization, or group.

    An agent can be an author, artist, publisher, or any entity that performs
    a role in the creation or distribution of items.

    Attributes:
        name (str): The full, authoritative name for display.
        alternative_names (dict): A JSON object of alternative names (e.g., translations,
            transliterations, or aliases) keyed by language code or type.
        sort_name (str): The name used for sorting (e.g., "King, Martin Luther, Jr.").
        identifier (str): A unique, URL-friendly slug for the agent.
        agent_type (str): The type of agent (e.g., Person, Organization).
        birth_date (date): The agent's date of birth or founding date.
        death_date (date): The agent's date of death or dissolution date.
        description (str): A public description or biographical information.
        note (str): Internal notes about the agent.
        items (Manager): Items related to this agent.
        roles (Manager): Roles this agent performs for items.
        external_references (Manager): Associated external links.
    """

    # --- Constants for Name Parsing ---
    # Titles to strip from the sort name entirely
    _IGNORED_TITLES = {
        "dr",
        "doctor",
        "mr",
        "mrs",
        "ms",
        "miss",
        "prof",
        "professor",
        "sir",
        "madam",
        "dame",
        "rev",
        "reverend",
        "hon",
        "honorable",
    }
    # Suffixes to preserve but move to the end
    _KNOWN_SUFFIXES = {"jr", "sr", "ii", "iii", "iv", "v", "esq", "phd", "md"}

    # --- Core Information ---
    name = models.CharField(
        max_length=512,
        help_text="The full, authoritative name in direct order (e.g., 'Dr. Martin Luther King, Jr.' or 'Penguin Books').",
    )
    sort_name = models.CharField(
        max_length=512,
        blank=True,
        help_text="The name used for sorting.",
    )
    identifier = models.SlugField(
        max_length=255,
        unique=True,
        help_text="A unique, human-readable identifier for this agent.",
    )
    agent_type = models.CharField(
        max_length=20,
        choices=AgentType.choices,
        default=AgentType.PERSON,
        help_text="The type of agent (e.g., Person, Organization).",
    )

    # --- Temporal Details ---
    birth_date = models.DateField(
        null=True,
        blank=True,
        help_text="The agent's date of birth or founding date.",
    )
    death_date = models.DateField(
        null=True,
        blank=True,
        help_text="The agent's date of death or dissolution date.",
    )

    # --- Description and Notes ---
    description = models.TextField(
        blank=True,
        default="",
        help_text="A public description or biographical information about the agent.",
    )
    note = models.TextField(
        blank=True,
        default="",
        help_text="Internal notes about the agent, not intended for public display.",
    )

    # --- Alternative Names (JSON) ---
    alternative_names = models.JSONField(
        blank=True,
        default=dict,
        help_text=(
            "A JSON object containing alternative names for this agent. "
            "Keys typically are language codes or types (e.g., {'ja': '名前', 'alt': 'Alias Name'})."
        ),
    )

    # --- Type hints for reverse relationships ---
    items: "Manager[Item]"
    roles: "Manager[Role]"
    external_references: "Manager[AgentExternalReference]"

    class Meta:
        db_table = "agent"
        ordering = ["sort_name", "name"]
        verbose_name = "Agent"
        verbose_name_plural = "Agents"
        indexes = [
            Index(fields=["identifier"]),
            Index(fields=["sort_name"]),
            Index(fields=["name"]),
            Index(fields=["birth_date"]),
            Index(fields=["death_date"]),
            Index(fields=["agent_type"]),
        ]

    def __str__(self) -> str:
        """Returns the agent's authoritative name.

        Returns:
            str: The name of the agent.
        """
        return self.name

    def generate_sort_name(self, name: str) -> str:
        """Generates a sortable name string by handling suffixes and removing titles.

        This method applies the following logic:
        1. Tokens matching common titles (e.g., "Dr.", "Sir") are removed.
        2. Tokens matching common suffixes (e.g., "Jr.", "III") are moved to the end.
        3. The last remaining word is treated as the surname.
        4. Format becomes: "Surname, First Middle, Suffix".

        Examples:
            "Dr. Martin Luther King, Jr." -> "King, Martin Luther, Jr."
            "Sir Elton John" -> "John, Elton"
            "Penguin Books" -> "Penguin Books" (if passed as non-person)

        Args:
            name (str): The full name to process.

        Returns:
            str: The formatted sort name.
        """
        # 1. Basic cleanup
        clean_name = name.strip()
        parts = clean_name.replace(",", "").split()

        if not parts:
            return ""

        # 2. Extract Suffix (Check the last word)
        # We strip dots and lowercase to check against our constant set
        suffix_part = ""
        if len(parts) > 1:
            last_word_clean = parts[-1].lower().replace(".", "")
            if last_word_clean in self._KNOWN_SUFFIXES:
                suffix_part = parts.pop()  # Remove suffix from main parts

        # 3. Filter out Titles (Dr, Sir, etc)
        # We assume titles appear at the start, but we filter all non-last-names just in case
        filtered_parts = []
        for i, part in enumerate(parts):
            # Always keep the last word (Surname), even if it looks like a title (rare edge case)
            if i == len(parts) - 1:
                filtered_parts.append(part)
                continue

            # Check if this part is a title
            if part.lower().replace(".", "") not in self._IGNORED_TITLES:
                filtered_parts.append(part)

        parts = filtered_parts

        # 4. Construct the Sort Name
        if len(parts) > 1:
            last_name = parts.pop()
            first_names = " ".join(parts)

            if suffix_part:
                return f"{last_name}, {first_names}, {suffix_part}"
            return f"{last_name}, {first_names}"

        # Fallback for single words (e.g., "Madonna", "Prince")
        return parts[0]

    def save(self, *args, **kwargs) -> None:
        """Saves the agent instance.

        Automatically generates a `sort_name` if one is not provided.
        If the agent is a person, it applies name parsing logic (stripping titles,
        handling suffixes). For organizations, the sort name defaults to the
        regular name.

        Args:
            *args: Variable length argument list.
            **kwargs: Arbitrary keyword arguments.
        """
        if not self.sort_name and self.name:
            if self.agent_type == AgentType.PERSON:
                self.sort_name = self.generate_sort_name(self.name)
            else:
                self.sort_name = self.name
        super().save(*args, **kwargs)


class Subject(BaseModel):
    """A subject, topic, or keyword used for categorization.

    Subjects are used to categorize and organize items in the collection.
    They represent topics, themes, or keywords that can be associated with
    multiple items.

    Attributes:
        name: The name of the subject.
        alternative_names (dict): A JSON object of alternative names (e.g., translations or aliases).
        identifier: A unique, URL-friendly slug for the subject.
        description: A detailed description of the subject.

    Reverse Relations:
        items: Items categorized under this subject.
    """

    # --- Core Information ---
    name = models.CharField(
        max_length=512,
        help_text="The name of the subject or topic.",
    )
    identifier = models.SlugField(
        max_length=255,
        unique=True,
        help_text="A unique, URL-friendly slug for the subject.",
    )
    description = models.TextField(
        blank=True,
        default="",
        help_text="A detailed description of the subject.",
    )

    # --- Alternative Names (JSON) ---
    alternative_names = models.JSONField(
        blank=True,
        default=dict,
        help_text=(
            "A JSON object containing alternative names for this subject. "
            "Keys typically are language codes or types (e.g., {'fr': 'Sujet', 'alt': 'Alternate'})."
        ),
    )

    # --- Type hints for reverse relationships ---
    items: "Manager[Item]"

    class Meta:
        db_table = "subject"
        ordering = ["name"]
        verbose_name = "Subject"
        verbose_name_plural = "Subjects"
        indexes = [
            Index(fields=["identifier"]),
            Index(fields=["name"]),
        ]

    def __str__(self) -> str:
        """Returns the subject's name."""
        return self.name


class Collection(BaseModel):
    """A collection that groups multiple related Items.

    Collections are used to organize items into logical groupings,
    such as albums, book series, or thematic collections.

    Attributes:
        name: The name of the collection.
        alternative_names (dict): A JSON object of alternative names (e.g., translations or alternate titles).
        identifier: A unique, URL-friendly slug for the collection.
        description: A detailed description of the collection's scope.

    Reverse Relations:
        items: Items within this collection.
    """

    # --- Core Information ---
    name = models.CharField(
        max_length=512,
        help_text="The name of the collection.",
    )
    identifier = models.SlugField(
        max_length=255,
        unique=True,
        help_text="A unique, URL-friendly slug for the collection.",
    )
    description = models.TextField(
        blank=True,
        default="",
        help_text="A detailed description of the collection's scope and contents.",
    )

    # --- Alternative Names (JSON) ---
    alternative_names = models.JSONField(
        blank=True,
        default=dict,
        help_text=(
            "A JSON object containing alternative names for this collection. "
            "Keys typically are language codes or types (e.g., {'es': 'Colección', 'alt': 'Alternate Title'})."
        ),
    )

    # --- Type hints for reverse relationships ---
    items: "Manager[Item]"

    class Meta:
        db_table = "collection"
        ordering = ["name"]
        verbose_name = "Collection"
        verbose_name_plural = "Collections"
        indexes = [
            Index(fields=["identifier"]),
            Index(fields=["name"]),
        ]

    def __str__(self) -> str:
        """Returns the collection's name."""
        return self.name
