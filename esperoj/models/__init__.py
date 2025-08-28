"""
Models for the esperoj application.

This module provides a clean interface for importing all models while
organizing them into logical groups to avoid circular dependencies.
"""

from .base import BaseModel
from .core import Person, Subject, Collection

from .relationships import (
    Role,
    ItemRoleName,
    ItemRelationship,
    ItemRelationshipType,
    ExternalReferenceType,
    PersonExternalReference,
    ItemExternalReference,
)

from .files import File, FileReplica, FileBlock

from .items import Item, ItemType, Song, Book

__all__ = [
    # Base
    "BaseModel",
    # Core Entities
    "Person",
    "Subject",
    "Collection",
    # Relationships
    "Role",
    "ItemRoleName",
    "ItemRelationship",
    "ItemRelationshipType",
    "ExternalReferenceType",
    "PersonExternalReference",
    "ItemExternalReference",
    # Files
    "File",
    "FileReplica",
    "FileBlock",
    # Items
    "Item",
    "ItemType",
    "Song",
    "Book",
]
