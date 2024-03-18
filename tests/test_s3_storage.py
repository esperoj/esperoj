"""Tests for S3 storage."""

from unittest.mock import MagicMock, patch

import pytest
import requests
from botocore.exceptions import ClientError

from esperoj.storage.s3 import S3Storage
from esperoj.storage.storage import StorageFactory


def upload_test_file(s3_storage, tmp_path):
    """Function to upload test file."""
    tmp_file = tmp_path / "test.txt"
    tmp_file.write_text("test content")
    s3_storage.upload_file(str(tmp_file), tmp_file.name)


def test_create_storage(s3_storage):
    """Test create storage."""
    assert type(StorageFactory.create({"type": "s3"})) is type(s3_storage)


def test_create_storage_with_invalid_type():
    """Test create storage with invalid type."""
    with pytest.raises(ValueError, match=r"Unknown storage type: invalid_type"):
        StorageFactory.create({"type": "invalid_type"})


def test_delete_files(s3_storage, tmp_path):
    """Test file deletion in S3 storage."""
    upload_test_file(s3_storage, tmp_path)
    assert s3_storage.delete_files(["test.txt"])["errors"] == []
    assert s3_storage.file_exists("test.txt") is False


def test_delete_files_errors():
    """Test the delete_files method when there are errors."""
    s3_storage = S3Storage({})
    mock_response = {"Errors": [{"Key": "file1.txt", "Message": "Access Denied"}]}
    s3_storage.client.delete_objects = MagicMock(return_value=mock_response)
    response = s3_storage.delete_files(["file1.txt"])
    assert response["errors"] == [{"path": "file1.txt", "message": "Access Denied"}]


def test_download_file(s3_storage, tmp_path):
    """Test file download from S3 storage."""
    s3_storage.client.put_object(
        Bucket=s3_storage.config["bucket_name"], Key="test.txt", Body="test content"
    )
    tmp_file = tmp_path / "downloaded.txt"
    s3_storage.download_file("test.txt", str(tmp_file))
    assert tmp_file.read_text() == "test content"


def test_file_exists(s3_storage, tmp_path):
    """Test file existence check in S3 storage."""
    upload_test_file(s3_storage, tmp_path)
    assert s3_storage.file_exists("test.txt") is True
    assert s3_storage.file_exists("nonexistent.txt") is False


def test_file_exists_raises_client_error(s3_storage, monkeypatch):
    """Test that file_exists raises ClientError for errors other than 404."""

    def mock_head_object(*args, **kwargs):
        raise ClientError(
            {
                "Error": {
                    "Code": "InternalError",
                    "Message": "Internal Server Error",
                }
            },
            "head_object",
        )

    monkeypatch.setattr(s3_storage.client, "head_object", mock_head_object)

    with pytest.raises(ClientError) as exc_info:
        s3_storage.file_exists("test.txt")

    assert exc_info.value.response["Error"]["Code"] == "InternalError"


def test_get_link_empty_bucket(s3_storage):
    """Test get_link method for a file in an empty bucket."""
    with pytest.raises(FileNotFoundError):
        s3_storage.get_link("path/to/file.txt")


def test_get_link_error(s3_storage, monkeypatch):
    """Test get_link method for an error during URL generation."""

    def mock_file_exists(path):
        raise RuntimeError("Something went wrong!")

    monkeypatch.setattr(s3_storage, "file_exists", mock_file_exists)
    with pytest.raises(RuntimeError):
        s3_storage.get_link("path/to/file.txt")


def test_get_link_existing_file(s3_storage, tmp_path):
    """Test get_link method for an existing file."""
    upload_test_file(s3_storage, tmp_path)
    url = s3_storage.get_link("test.txt")
    response = requests.get(url)
    assert response.status_code == 200
    assert response.content == b"test content"


def test_get_link_nonexistent_file(s3_storage):
    """Test get_link method for a non-existent file."""
    with pytest.raises(FileNotFoundError):
        s3_storage.get_link("nonexistent.txt")


def test_get_file(s3_storage, tmp_path):
    """Test get_file method for downloading a file from S3 storage."""
    upload_test_file(s3_storage, tmp_path)
    downloaded_file = tmp_path / "downloaded.txt"
    with downloaded_file.open("wb") as f:
        for chunk in s3_storage.get_file("test.txt"):
            f.write(chunk)
    assert downloaded_file.read_text() == "test content"


def test_list_files(s3_storage, tmp_path):
    """Test listing files in S3 storage."""
    upload_test_file(s3_storage, tmp_path)
    second_file = tmp_path / "test2.txt"
    second_file.write_text("test content")
    s3_storage.upload_file(str(second_file), "test2.txt")
    files = s3_storage.list_files("")
    assert len(files) == 2
    assert "test.txt" in files
    assert "test2.txt" in files


def test_list_files_not_found(s3_storage):
    """Test listing files in a non-existent directory in S3 storage raises FileNotFoundError."""
    with pytest.raises(FileNotFoundError):
        s3_storage.list_files("nonexistent")


def test_upload_file(s3_storage, tmp_path):
    """Test file upload to S3 storage."""
    upload_test_file(s3_storage, tmp_path)
    download_file = tmp_path / "uploaded.txt"
    assert s3_storage.file_exists("test.txt") is True
    s3_storage.download_file("test.txt", str(download_file))
    assert download_file.read_text() == "test content"


@patch("esperoj.storage.s3.boto3.client")
def test_upload_file_client_error(mock_client):
    """Test the upload_file method when there is a client error."""
    mock_client.return_value.upload_file.side_effect = ClientError(
        {"Error": {"Code": "InternalError", "Message": "Internal Server Error"}},
        "upload_file",
    )
    s3_storage = S3Storage({})
    with pytest.raises(ClientError) as exc_info:
        s3_storage.upload_file("source.txt", "destination.txt")
    assert exc_info.value.response["Error"]["Code"] == "InternalError"


def test_upload_file_not_found(s3_storage, tmp_path):
    """Test uploading a non-existent file to S3 storage raises FileNotFoundError."""
    tmp_file = tmp_path / "nonexistent.txt"
    with pytest.raises(FileNotFoundError):
        s3_storage.upload_file(str(tmp_file), "uploaded.txt")
