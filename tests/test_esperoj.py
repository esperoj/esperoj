"""Tests for Esperoj class."""

import pytest

from esperoj.database.memory import MemoryRecord
from esperoj.exceptions import RecordNotFoundError


def test_archive_success(mocker, esperoj, memory_table, s3_storage):
    """Test successful archiving of a record."""
    record = memory_table.create({"Name": "test_file.txt"})
    mocker.patch.object(s3_storage, "get_link", return_value="http://example.com/test_file.txt")
    mocker.patch("esperoj.esperoj.archive", return_value="http://archive.org/test_file.txt")
    archive_url = esperoj.archive(record.record_id)
    assert archive_url == "http://archive.org/test_file.txt"
    assert record.fields["Internet Archive"] == archive_url


def test_archive_record_not_found(esperoj, memory_table):
    """Test archiving with a non-existent record ID."""
    with pytest.raises(RecordNotFoundError):
        esperoj.archive("nonexistent_record_id")


def test_archive_url_failure(mocker, esperoj, memory_table, s3_storage):
    """Test archiving failure due to a RuntimeError."""
    record = memory_table.create({"Name": "test_file.txt"})
    mocker.patch.object(s3_storage, "get_link", return_value="http://example.com/test_file.txt")
    mocker.patch("esperoj.esperoj.archive", side_effect=RuntimeError("Archiving failed"))
    with pytest.raises(RuntimeError) as excinfo:
        esperoj.archive(record.record_id)
    assert "Archiving failed" in str(excinfo.value)


def test_ingest_file_not_found(esperoj, tmp_path):
    """Test ingesting a non-existent file."""
    non_existent_file = tmp_path / "nonexistent.txt"
    with pytest.raises(FileNotFoundError):
        esperoj.ingest(non_existent_file)


def test_ingest_file_exists_in_database(esperoj, memory_table, tmp_file):
    """Test ingesting a file that already exists in the database."""
    memory_table.create({"Name": tmp_file.name, "Size": 123, "SHA256": "dummyhash"})
    with pytest.raises(FileExistsError):
        esperoj.ingest(tmp_file)


def test_ingest_file_exists_in_storage(esperoj, s3_storage, tmp_file):
    """Test ingesting a file that already exists in the storage."""
    s3_storage.upload_file(str(tmp_file), tmp_file.name)
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


def test_verify_integrity_success(mocker, esperoj, memory_table, s3_storage):
    """Test successful integrity verification of a record."""
    record = memory_table.create({"Name": "test_file.txt", "SHA256": "validhash"})
    mocker.patch.object(s3_storage, "get_link", return_value="http://example.com/test_file.txt")
    mocker.patch("esperoj.esperoj.archive", return_value="http://archive.org/test_file.txt")
    mocker.patch("esperoj.esperoj.calculate_hash_from_url", return_value="validhash")
    assert esperoj.verify(record.record_id) is True


def test_verify_integrity_failure(mocker, esperoj, memory_table, s3_storage):
    """Test integrity verification failure due to an invalid hash."""
    record = memory_table.create({"Name": "test_file.txt", "SHA256": "validhash"})
    mocker.patch.object(s3_storage, "get_link", return_value="http://example.com/test_file.txt")
    mocker.patch("esperoj.esperoj.archive", return_value="http://archive.org/test_file.txt")
    mocker.patch(
        "esperoj.esperoj.calculate_hash_from_url", side_effect=["invalidhash", "validhash"]
    )
    assert esperoj.verify(record.record_id) is False


def test_verify_record_not_found(esperoj, memory_table):
    """Test integrity verification with a non-existent record ID."""
    with pytest.raises(RecordNotFoundError):
        esperoj.verify("nonexistent_record_id")


def test_verify_storage_error(mocker, esperoj, memory_table, s3_storage):
    """Test integrity verification failure due to a storage error."""
    record = memory_table.create({"Name": "test_file.txt", "SHA256": "validhash"})
    mocker.patch.object(s3_storage, "get_link", side_effect=FileNotFoundError)
    mocker.patch("esperoj.esperoj.archive", return_value="http://archive.org/test_file.txt")
    with pytest.raises(FileNotFoundError):
        esperoj.verify(record.record_id)
