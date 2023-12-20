"""Tests for s3."""

import pytest


def test_delete_file(s3_storage, s3_client, bucket_name):
    """Test deleting a file."""
    s3_client.put_object(Bucket=bucket_name, Key="test_file", Body=b"")
    assert s3_storage.file_exists("test_file")
    s3_storage.delete_file("test_file")
    assert not s3_storage.file_exists("test_file")


def test_delete_file_not_found(s3_storage):
    """Test deleting a file that doesn't exist."""
    with pytest.raises(FileNotFoundError):
        s3_storage.delete_file("non_existent_file")


def test_download_file(s3_storage, s3_client, bucket_name, tmp_path):
    """Test downloading a file."""
    s3_client.put_object(Bucket=bucket_name, Key="test_file", Body=b"test content")
    dst_file = tmp_path / "downloaded_file"
    s3_storage.download_file("test_file", str(dst_file))
    assert dst_file.read_text() == "test content"


def test_download_file_not_found(s3_storage, tmp_path):
    """Test downloading a file that doesn't exist."""
    with pytest.raises(FileNotFoundError):
        s3_storage.download_file("non_existent_file", str(tmp_path / "downloaded_file"))


def test_file_exists(s3_storage, s3_client, bucket_name):
    """Test checking if a file exists."""
    assert not s3_storage.file_exists("test_file")
    s3_client.put_object(Bucket=bucket_name, Key="test_file", Body=b"")
    assert s3_storage.file_exists("test_file")


def test_list_files(s3_storage, s3_client, bucket_name):
    """Test listing files in a directory."""
    s3_client.put_object(Bucket=bucket_name, Key="dir/test_file", Body=b"")
    assert s3_storage.list_files("dir") == ["dir/test_file"]


def test_list_files_directory_not_found(s3_storage):
    """Test listing files in a directory that doesn't exist."""
    with pytest.raises(FileNotFoundError):
        s3_storage.list_files("non_existent_dir")


def test_upload_file(s3_storage, s3_client, bucket_name, tmp_path):
    """Test uploading a file."""
    src_file = tmp_path / "test_file"
    src_file.write_text("test content")
    s3_storage.upload_file(str(src_file), "uploaded_file")
    obj = s3_client.get_object(Bucket=bucket_name, Key="uploaded_file")
    assert obj["Body"].read().decode() == "test content"


def test_upload_file_not_found(s3_storage):
    """Test uploading a file that doesn't exist."""
    with pytest.raises(FileNotFoundError):
        s3_storage.upload_file("non_existent_file", "uploaded_file")
