"""Tests for esperoj."""
from pathlib import Path

import pytest

from esperoj.database import Record


def test_archive_nonexistent_file(esperoj):
    """Test that archiving a nonexistent file raises FileNotFoundError."""
    with pytest.raises(FileNotFoundError):
        esperoj.archive("nonexistent_file.txt")


def test_ingest_valid_file(esperoj, tmp_path):
    """Test that ingesting a valid file creates a file record in the database and the storage."""
    file_path = tmp_path / "test_file.txt"
    file_path.write_text("Test content")

    file_record = esperoj.ingest(file_path)

    assert isinstance(esperoj.db.table("Files").get(file_record.record_id), Record)
    assert esperoj.storage.file_exists("test_file.txt")


def test_ingest_invalid_file(esperoj):
    """Test that ingesting an invalid file (nonexistent file) raises FileNotFoundError."""
    with pytest.raises(FileNotFoundError):
        esperoj.ingest(Path("nonexistent_file.txt"))


def test_ingest_existing_file(esperoj, tmp_path):
    """Test that ingesting an existing file (already in the database) raises FileExistsError."""
    file_path = tmp_path / "test_file.txt"
    file_path.write_text("Test content")

    esperoj.db.table("Files").create({"Name": "test_file.txt"})

    with pytest.raises(FileExistsError):
        esperoj.ingest(file_path)


def test_ingest_existing_file_in_storage(esperoj, tmp_path):
    """Test that ingesting an existing file (already in the storage) raises FileExistsError."""
    file_path = tmp_path / "test_file.txt"
    file_path.write_text("Test content")

    esperoj.storage.upload_file(str(file_path), "test_file.txt")
    with pytest.raises(FileExistsError):
        esperoj.ingest(file_path)


def test_ingest_invalid_path(esperoj):
    """Test that ingesting an invalid path (not a file) raises FileNotFoundError."""
    with pytest.raises(FileNotFoundError):
        esperoj.ingest(Path("path/to/directory"))
