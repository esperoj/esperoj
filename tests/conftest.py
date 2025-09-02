"""
Pytest configuration and shared fixtures for the esperoj application tests.

This file is automatically discovered by pytest and is used to define
global fixtures, hooks, and plugins for the entire test suite.

Fixtures defined here are available to all tests without needing to be
imported. The `pytest-django` plugin is configured here to ensure that
the Django application is properly initialized and that tests have
access to a clean database.
"""

import pytest
from django.contrib.auth.models import User

from esperoj.models import (
    Person,
    Subject,
    Collection,
    Item,
    Song,
    Book,
    File,
    FileReplica,
    Role,
    ItemRoleName,
)
from esperoj.constants import ReplicaType, StorageName


# --- User & Authentication Fixtures ---


@pytest.fixture
def user(db) -> User:
    """Fixture to create a standard user."""
    return User.objects.create_user(username="testuser", password="password")


@pytest.fixture
def admin_user(db) -> User:
    """Fixture to create a superuser."""
    return User.objects.create_superuser(username="admin", password="password", email="admin@example.com")


# --- Core Model Fixtures ---


@pytest.fixture
def person(db) -> Person:
    """Fixture to create a sample Person."""
    return Person.objects.create(
        authorized_name="John Doe",
        sort_name="Doe, John",
        identifier="john-doe",
    )


@pytest.fixture
def subject(db) -> Subject:
    """Fixture to create a sample Subject."""
    return Subject.objects.create(name="Software Engineering", identifier="software-engineering")


@pytest.fixture
def collection(db) -> Collection:
    """Fixture to create a sample Collection."""
    return Collection.objects.create(name="My Favorite Songs", identifier="my-favorite-songs")


# --- File Model Fixtures ---


@pytest.fixture
def file_instance(db) -> File:
    """Fixture to create a sample File."""
    return File.objects.create(
        name="test_file.txt",
        path="/test/test_file.txt",
        size=1024,
        mime_type="text/plain",
        sha256="a" * 64,  # Dummy checksum
    )


@pytest.fixture
def file_replica(db, file_instance: File) -> FileReplica:
    """Fixture to create a sample FileReplica."""
    return FileReplica.objects.create(
        file=file_instance,
        replica_type=ReplicaType.ORIGINAL,
        storage_name=StorageName.LOCAL_DEFAULT,
        storage_path="/replicas/test_file.txt",
    )


# --- Item Model Fixtures ---


@pytest.fixture
def book_item(db) -> Book:
    """Fixture to create a sample Book item."""
    return Book.objects.create(
        title="The Art of Software",
        identifier="the-art-of-software",
        year=2024,
        isbn_13="9780123456789",
        publisher="Tech Press",
    )


@pytest.fixture
def song_item(db) -> Song:
    """Fixture to create a sample Song item."""
    return Song.objects.create(
        title="Code Rhapsody",
        identifier="code-rhapsody",
        year=2024,
        duration_seconds=180,
    )


# --- Relationship Fixtures ---


@pytest.fixture
def role(db, person: Person, song_item: Song) -> Role:
    """Fixture to create a sample Role."""
    return Role.objects.create(
        person=person,
        item=song_item,
        name=ItemRoleName.ARTIST,
    )
