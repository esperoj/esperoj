"""Tests for S3 storage."""

import pytest

from esperoj.storage.s3 import default_config


def test_file_exists(s3_storage):
    """Test file existence check in S3 storage."""
    s3_storage.s3.put_object(
        Bucket=default_config["bucket_name"], Key="test.txt", Body="test content"
    )
    assert s3_storage.file_exists("test.txt") is True
    assert s3_storage.file_exists("nonexistent.txt") is False


def test_upload_file(s3_storage, tmp_path):
    """Test file upload to S3 storage."""
    tmp_file = tmp_path / "test.txt"
    tmp_file.write_text("test content")
    s3_storage.upload_file(str(tmp_file), "uploaded.txt")
    assert s3_storage.file_exists("uploaded.txt") is True


def test_upload_file_not_found(s3_storage, tmp_path):
    """Test uploading a non-existent file to S3 storage raises FileNotFoundError."""
    tmp_file = tmp_path / "nonexistent.txt"
    with pytest.raises(FileNotFoundError):
        s3_storage.upload_file(str(tmp_file), "uploaded.txt")


def test_download_file(s3_storage, tmp_path):
    """Test file download from S3 storage."""
    s3_storage.s3.put_object(
        Bucket=default_config["bucket_name"], Key="test.txt", Body="test content"
    )
    tmp_file = tmp_path / "downloaded.txt"
    s3_storage.download_file("test.txt", str(tmp_file))
    assert tmp_file.read_text() == "test content"


def test_download_file_not_found(s3_storage, tmp_path):
    """Test downloading a non-existent file from S3 storage raises FileNotFoundError."""
    tmp_file = tmp_path / "nonexistent.txt"
    with pytest.raises(FileNotFoundError):
        s3_storage.download_file("nonexistent.txt", str(tmp_file))


def test_delete_file(s3_storage):
    """Test file deletion in S3 storage."""
    s3_storage.s3.put_object(
        Bucket=default_config["bucket_name"], Key="test.txt", Body="test content"
    )
    assert s3_storage.delete_file("test.txt") is True
    assert s3_storage.file_exists("test.txt") is False


def test_list_files(s3_storage):
    """Test listing files in S3 storage."""
    s3_storage.s3.put_object(
        Bucket=default_config["bucket_name"], Key="test1.txt", Body="test content"
    )
    s3_storage.s3.put_object(
        Bucket=default_config["bucket_name"], Key="test2.txt", Body="test content"
    )
    files = s3_storage.list_files("")
    assert len(files) == 2
    assert "test1.txt" in files
    assert "test2.txt" in files


def test_list_files_not_found(s3_storage):
    """Test listing files in a non-existent directory in S3 storage raises FileNotFoundError."""
    with pytest.raises(FileNotFoundError):
        s3_storage.list_files("nonexistent")
