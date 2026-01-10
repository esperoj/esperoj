"""
Models for the esperoj application.

This module provides a clean interface for importing all models while
organizing them into logical groups to avoid circular dependencies.
"""

from .base import BaseModel
from .core import Collection, Person, Subject
from .files import File
from .items import Book, Item, ItemType, Song
from .relationships import (
    ExternalReferenceType,
    ItemExternalReference,
    ItemRelationship,
    ItemRelationshipType,
    ItemRoleName,
    PersonExternalReference,
    Role,
)

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
