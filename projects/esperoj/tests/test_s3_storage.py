"""Tests for S3 storage."""

import pytest
from botocore.exceptions import ClientError

from esperoj.storage.s3 import S3Storage
from esperoj.storage.storage import StorageFactory

def upload_test_file(s3_storage, tmp_file):
    """Function to upload test file."""
    s3_storage.upload(str(tmp_file), tmp_file.name)

def test_upload_and_download(s3_storage, tmp_file, tmp_path):
    upload_test_file(s3_storage, tmp_file)
    download_path = tmp_path / "downloaded.txt"
    s3_storage.download("tmp.txt", str(download_path))
    assert download_path.read_text() == "Test content"

def test_exists(s3_storage, tmp_file):
    upload_test_file(s3_storage, tmp_file)
    assert s3_storage.exists("tmp.txt") is True
    assert s3_storage.exists("non_existing.txt") is False
