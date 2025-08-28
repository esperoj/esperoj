"""
Admin configuration for the esperoj application.

This module imports and registers all admin classes from the organized
admin submodules, providing a clean interface for Django's admin system.
"""

# Import all admin classes to register them with Django admin
from .core import PersonAdmin, SubjectAdmin, CollectionAdmin
from .relationships import RoleAdmin, ItemRelationshipAdmin, PersonExternalReferenceAdmin, ItemExternalReferenceAdmin
from .files import FileAdmin, FileReplicaAdmin, FileBlockAdmin
from .items import ItemAdmin, SongAdmin, BookAdmin

# All admin classes are automatically registered via the @admin.register decorators
# in their respective modules. This __init__.py file serves as the entry point
# for Django to discover and load all admin configurations.

__all__ = [
    # Core entities
    "PersonAdmin",
    "SubjectAdmin",
    "CollectionAdmin",
    # Relationships
    "RoleAdmin",
    "ItemRelationshipAdmin",
    "PersonExternalReferenceAdmin",
    "ItemExternalReferenceAdmin",
    # Files
    "FileAdmin",
    "FileReplicaAdmin",
    "FileBlockAdmin",
    # Items
    "ItemAdmin",
    "SongAdmin",
    "BookAdmin",
]
