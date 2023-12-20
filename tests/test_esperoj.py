"""Test for esperoj."""

from pathlib import Path

import pytest


def test_calculate_hash_content(esperoj):
    """Test the calculate_hash method with content input."""
    content = "test content"
    sha256_hash = esperoj.calculate_hash(content=content)
    assert sha256_hash == "6ae8a75555209fd6c44157c0aed8016e763ff435a19cf186f76863140143ff72"


def test_calculate_hash_path(tmp_path, esperoj):
    """Test the calculate_hash method with path input."""
    test_file = tmp_path / "test.txt"
    test_file.write_text("test content")
    sha256_hash = esperoj.calculate_hash(path=test_file)
    assert sha256_hash == "6ae8a75555209fd6c44157c0aed8016e763ff435a19cf186f76863140143ff72"


def test_calculate_hash_invalid_algorithm(esperoj):
    """Test the calculate_hash method with an invalid algorithm."""
    with pytest.raises(ValueError, match="Unsupported.*"):
        esperoj.calculate_hash(content="test content", algorithm="invalid_algorithm")


def test_ingest_success(tmp_path, esperoj):
    """Test the ingest method with a successful file ingestion."""
    test_file = tmp_path / "test.txt"
    test_file.write_text("test content")
    result = esperoj.ingest(test_file)
    assert "Name" in result["fields"]
    assert "Size" in result["fields"]
    assert "SHA256" in result["fields"]
    assert "test_storage" in result["fields"]


def test_ingest_invalid_path(esperoj):
    """Test the ingest method with an invalid file path."""
    with pytest.raises(FileNotFoundError):
        esperoj.ingest(Path("non_existent_file.txt"))


def test_ingest_file_exists(tmp_path, esperoj):
    """Test the ingest method when the file already exists in storage."""
    test_file = tmp_path / "test.txt"
    test_file.write_text("test content")
    esperoj.storage.upload_file(str(test_file), test_file.name)
    with pytest.raises(FileExistsError):
        esperoj.ingest(test_file)
