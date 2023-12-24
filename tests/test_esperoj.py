"""Tests for esperoj module."""

import pytest

from esperoj.database.memory import MemoryRecord


def test_ingest_file_not_found(esperoj, tmp_path):
    """Test that FileNotFoundError is raised when the file does not exist."""
    non_existent_file = tmp_path / "nonexistent.txt"
    with pytest.raises(FileNotFoundError):
        esperoj.ingest(non_existent_file)


def test_ingest_file_exists_in_database(esperoj, memory_table, tmp_file):
    """Test that FileExistsError is raised when the file already exists in the database or storage."""
    memory_table.create({"Name": tmp_file.name, "Size": 123, "SHA256": "dummyhash"})
    with pytest.raises(FileExistsError):
        esperoj.ingest(tmp_file)


def test_ingest_file_exists_in_storage(esperoj, s3_storage, tmp_file):
    """Test that FileExistsError is raised when the file already exists in the database or storage."""
    s3_storage.upload_file(tmp_file, tmp_file.name)
    with pytest.raises(FileExistsError):
        esperoj.ingest(tmp_file)


def test_ingest_success(esperoj, memory_table, s3_storage, tmp_file):
    """Test successful ingestion of a file."""
    record = esperoj.ingest(tmp_file)
    assert isinstance(record, MemoryRecord)
    assert record.fields["Name"] == tmp_file.name
    assert record.fields["Size"] == tmp_file.stat().st_size
    assert "SHA256" in record.fields
    assert s3_storage.file_exists(tmp_file.name)
