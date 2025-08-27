"""
Models for the esperoj application.

This module provides a clean interface for importing all models while
organizing them into logical groups to avoid circular dependencies.
"""

# Import from core entities first (no dependencies)
from .base import BaseModel
from .core import Person, Subject, Collection

# Import relationships (depends on core)
from .relationships import (
    Role,
    ItemRoleName,
    ExternalReferenceType,
    PersonExternalReference,
    ItemExternalReference,
)

# Import files (standalone)
from .files import File, FileReplica, FileBlock

# Import items (depends on core and relationships)
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
