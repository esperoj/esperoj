"""Tests for S3 storage."""

import pytest
import requests


def test_file_exists(config, s3_storage):
    """Test file existence check in S3 storage."""
    s3_storage.client.put_object(
        Bucket=config["storages"][0]["bucket_name"], Key="test.txt", Body="test content"
    )
    assert s3_storage.file_exists("test.txt") is True
    assert s3_storage.file_exists("nonexistent.txt") is False


def test_upload_file(s3_storage, tmp_path):
    """Test file upload to S3 storage."""
    tmp_file = tmp_path / "test.txt"
    tmp_file.write_text("test content")
    download_file = tmp_path / "uploaded.txt"
    s3_storage.upload_file(str(tmp_file), "uploaded.txt")
    assert s3_storage.file_exists("uploaded.txt") is True
    s3_storage.download_file("uploaded.txt", str(download_file))
    assert download_file.read_text() == "test content"


def test_upload_file_not_found(s3_storage, tmp_path):
    """Test uploading a non-existent file to S3 storage raises FileNotFoundError."""
    tmp_file = tmp_path / "nonexistent.txt"
    with pytest.raises(FileNotFoundError):
        s3_storage.upload_file(str(tmp_file), "uploaded.txt")


def test_download_file(config, s3_storage, tmp_path):
    """Test file download from S3 storage."""
    s3_storage.client.put_object(
        Bucket=config["storages"][0]["bucket_name"], Key="test.txt", Body="test content"
    )
    tmp_file = tmp_path / "downloaded.txt"
    s3_storage.download_file("test.txt", str(tmp_file))
    assert tmp_file.read_text() == "test content"


def test_download_file_not_found(s3_storage, tmp_path):
    """Test downloading a non-existent file from S3 storage raises FileNotFoundError."""
    tmp_file = tmp_path / "nonexistent.txt"
    with pytest.raises(FileNotFoundError):
        s3_storage.download_file("nonexistent.txt", str(tmp_file))


def test_delete_files(config, s3_storage):
    """Test file deletion in S3 storage."""
    s3_storage.client.put_object(
        Bucket=config["storages"][0]["bucket_name"], Key="test.txt", Body="test content"
    )
    assert s3_storage.delete_files(["test.txt"])["errors"] == []
    assert s3_storage.file_exists("test.txt") is False


def test_get_link_existing_file(s3_storage):
    """Test get_link method for an existing file."""
    s3_storage.client.put_object(
        Bucket=s3_storage.config["bucket_name"], Key="path/to/file.txt", Body=b"Test content"
    )
    url = s3_storage.get_link("path/to/file.txt")
    response = requests.get(url)
    assert response.status_code == 200
    assert response.content == b"Test content"


def test_get_link_nonexistent_file(s3_storage):
    """Test get_link method for a non-existent file."""
    with pytest.raises(FileNotFoundError):
        s3_storage.get_link("path/to/nonexistent_file.txt")


def test_get_link_error(s3_storage, monkeypatch):
    """Test get_link method for an error during URL generation."""

    def mock_file_exists(path):
        raise RuntimeError("Something went wrong!")

    monkeypatch.setattr(s3_storage, "file_exists", mock_file_exists)
    with pytest.raises(RuntimeError):
        s3_storage.get_link("path/to/file.txt")


def test_get_link_empty_bucket(s3_storage):
    """Test get_link method for a file in an empty bucket."""
    with pytest.raises(FileNotFoundError):
        s3_storage.get_link("path/to/file.txt")


def test_list_files(config, s3_storage):
    """Test listing files in S3 storage."""
    s3_storage.client.put_object(
        Bucket=config["storages"][0]["bucket_name"], Key="test1.txt", Body="test content"
    )
    s3_storage.client.put_object(
        Bucket=config["storages"][0]["bucket_name"], Key="test2.txt", Body="test content"
    )
    files = s3_storage.list_files("")
    assert len(files) == 2
    assert "test1.txt" in files
    assert "test2.txt" in files


def test_list_files_not_found(s3_storage):
    """Test listing files in a non-existent directory in S3 storage raises FileNotFoundError."""
    with pytest.raises(FileNotFoundError):
        s3_storage.list_files("nonexistent")
