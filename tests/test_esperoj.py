"""Tests for esperoj module."""

from unittest.mock import patch

import pytest

from esperoj import Esperoj
from esperoj.database.memory import MemoryRecord
from esperoj.exceptions import RecordNotFoundError


def test__archive_success():
    """Test the _archive method with a successful response."""
    with patch("requests.post") as mock_post, patch("requests.get") as mock_get:
        mock_post.return_value.status_code = 200
        mock_post.return_value.json.return_value = {"job_id": "123"}
        mock_get.return_value.status_code = 200
        mock_get.return_value.json.return_value = {
            "status": "success",
            "timestamp": "20230101123456",
            "original_url": "http://example.com/test_file.txt",
        }
        archive_url = Esperoj._archive("http://example.com/test_file.txt")
        assert (
            archive_url
            == "https://web.archive.org/web/20230101123456/http://example.com/test_file.txt"
        )


def test__archive_failure():
    """Test the _archive method with a failure response."""
    with patch("requests.post") as mock_post:
        mock_post.return_value.status_code = 500
        mock_post.return_value.text = "Internal Server Error"
        with pytest.raises(RuntimeError) as excinfo:
            Esperoj._archive("http://example.com/test_file.txt")
        assert "Error: Internal Server Error" in str(excinfo.value)


def test__archive_timeout():
    """Test the _archive method with a timeout."""
    with patch("requests.post") as mock_post, patch("requests.get") as mock_get, patch(
        "time.time", side_effect=[0, 301]
    ):
        mock_post.return_value.status_code = 200
        mock_post.return_value.json.return_value = {"job_id": "123"}
        mock_get.return_value.status_code = 200
        mock_get.return_value.json.return_value = {"status": "pending"}
        with pytest.raises(RuntimeError) as excinfo:
            Esperoj._archive("http://example.com/test_file.txt")
        assert "Error: Archiving process timed out." in str(excinfo.value)


def test_calculate_hash_from_url_success():
    """Test the _calculate_hash_from_url method with a successful response."""
    with patch("requests.get") as mock_get:
        mock_get.return_value.status_code = 200
        mock_get.return_value.iter_content.return_value = [b"test data"]
        hash_result = Esperoj._calculate_hash_from_url("http://example.com/test_file.txt")
        expected_hash = "916f0027a575074ce72a331777c3478d6513f786a591bd892da1a577bf2335f9"
        assert hash_result == expected_hash


def test_calculate_hash_from_url_failure():
    """Test the _calculate_hash_from_url method with a failure response."""
    with patch("requests.get") as mock_get:
        mock_get.return_value.status_code = 404
        mock_get.return_value.text = "Not Found"
        with pytest.raises(RuntimeError) as excinfo:
            Esperoj._calculate_hash_from_url("http://example.com/nonexistent_file.txt")
        assert "Error: Not Found" in str(excinfo.value)


def test_archive_success(esperoj, memory_table, s3_storage):
    """Test successful archiving of a record."""
    record = memory_table.create({"Name": "test_file.txt"})
    with patch.object(
        s3_storage, "get_link", return_value="http://example.com/test_file.txt"
    ), patch("esperoj.Esperoj._archive", return_value="http://archive.org/test_file.txt"):
        archive_url = esperoj.archive(record.record_id)
        assert archive_url == "http://archive.org/test_file.txt"
        assert record.fields["Internet Archive"] == archive_url


def test_archive_record_not_found(esperoj, memory_table):
    """Test that RecordNotFoundError is raised when trying to archive a non-existent record."""
    with pytest.raises(RecordNotFoundError):
        esperoj.archive("nonexistent_record_id")


def test_archive_url_failure(esperoj, memory_table, s3_storage):
    """Test that RuntimeError is raised when archiving fails due to an error in obtaining the URL."""
    record = memory_table.create({"Name": "test_file.txt"})
    with patch.object(
        s3_storage, "get_link", return_value="http://example.com/test_file.txt"
    ), patch("esperoj.Esperoj._archive", side_effect=RuntimeError("Archiving failed")):
        with pytest.raises(RuntimeError) as excinfo:
            esperoj.archive(record.record_id)
        assert "Archiving failed" in str(excinfo.value)


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


def test_verify_integrity_success(esperoj, memory_table, s3_storage):
    """Test successful verification of a record's integrity."""
    record = memory_table.create({"Name": "test_file.txt", "SHA256": "validhash"})
    with patch.object(
        s3_storage, "get_link", return_value="http://example.com/test_file.txt"
    ), patch("esperoj.Esperoj._archive", return_value="http://archive.org/test_file.txt"), patch(
        "esperoj.esperoj.Esperoj._calculate_hash_from_url", return_value="validhash"
    ):
        assert esperoj.verify(record.record_id) is True


def test_verify_integrity_failure(esperoj, memory_table, s3_storage):
    """Test that integrity verification fails when the calculated hash does not match the stored hash."""
    record = memory_table.create({"Name": "test_file.txt", "SHA256": "validhash"})
    with patch.object(
        s3_storage, "get_link", return_value="http://example.com/test_file.txt"
    ), patch("esperoj.Esperoj._archive", return_value="http://archive.org/test_file.txt"), patch(
        "esperoj.esperoj.Esperoj._calculate_hash_from_url", side_effect=["invalidhash", "validhash"]
    ):
        assert esperoj.verify(record.record_id) is False


def test_verify_record_not_found(esperoj, memory_table):
    """Test that RecordNotFoundError is raised when trying to verify a non-existent record."""
    with pytest.raises(RecordNotFoundError):
        esperoj.verify("nonexistent_record_id")


def test_verify_storage_error(esperoj, memory_table, s3_storage):
    """Test that FileNotFoundError is raised when S3 storage raises a FileNotFoundError."""
    record = memory_table.create({"Name": "test_file.txt", "SHA256": "validhash"})
    with patch.object(s3_storage, "get_link", side_effect=FileNotFoundError), patch(
        "esperoj.Esperoj._archive", return_value="http://archive.org/test_file.txt"
    ), pytest.raises(FileNotFoundError):
        esperoj.verify(record.record_id)
