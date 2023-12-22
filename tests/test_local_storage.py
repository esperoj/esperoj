"""Tests for local storage."""

import pytest


def test_local_storage_init(local_storage, tmp_path):
    """Test the initialization of the local storage with a given name and base path."""
    assert local_storage.name == "test_storage"
    assert local_storage.base_path == tmp_path


def test_get_full_path(local_storage, tmp_path):
    """Test getting the full path of a file in the local storage."""
    full_path = local_storage._get_full_path("test.txt")
    assert full_path == tmp_path / "test.txt"


def test_delete_file(local_storage, tmp_path):
    """Test deleting a file from the local storage."""
    test_file = tmp_path / "test.txt"
    test_file.touch()
    local_storage.delete_file("test.txt")
    assert not test_file.exists()


def test_delete_file_not_found(local_storage):
    """Test that deleting a non-existent file raises a FileNotFoundError."""
    with pytest.raises(FileNotFoundError):
        local_storage.delete_file("test.txt")


def test_download_file(local_storage, tmp_path):
    """Test downloading a file from the local storage."""
    src_file_path = tmp_path / "test.txt"
    src_file_path.write_text("test content")
    dst_file_path = tmp_path / "downloaded_test.txt"
    local_storage.download_file("test.txt", str(dst_file_path))
    assert dst_file_path.exists()
    assert dst_file_path.read_text() == "test content"


def test_download_file_not_found(local_storage, tmp_path):
    """Test that downloading a non-existent file raises a FileNotFoundError."""
    with pytest.raises(FileNotFoundError):
        local_storage.download_file("test.txt", str(tmp_path / "downloaded_test.txt"))


def test_file_exists(local_storage, tmp_path):
    """Test checking the existence of a file in the local storage."""
    test_file = tmp_path / "test.txt"
    test_file.touch()
    assert local_storage.file_exists("test.txt")
    assert not local_storage.file_exists("non_existent.txt")


def test_list_files(local_storage, tmp_path):
    """Test listing all files in a directory within the local storage."""
    test_file1 = tmp_path / "test1.txt"
    test_file2 = tmp_path / "test2.txt"
    test_dir = tmp_path / "test_dir"
    test_file1.touch()
    test_file2.touch()
    test_dir.mkdir()
    files = local_storage.list_files(".")
    assert len(files) == 2
    assert "test1.txt" in files
    assert "test2.txt" in files
    assert "test_dir" not in files


def test_list_files_not_found(local_storage):
    """Test that listing files in a non-existent directory raises a FileNotFoundError."""
    with pytest.raises(FileNotFoundError):
        local_storage.list_files("non_existent")


def test_upload_file(local_storage, tmp_path):
    """Test uploading a file to the local storage."""
    src_file_path = tmp_path / "test.txt"
    src_file_path.write_text("test content")
    local_storage.upload_file(str(src_file_path), "uploaded_test.txt")
    dst_file_path = tmp_path / "uploaded_test.txt"
    assert dst_file_path.exists()
    assert dst_file_path.read_text() == "test content"


def test_upload_file_not_found(local_storage, tmp_path):
    """Test that uploading a non-existent file raises a FileNotFoundError."""
    non_existent_file = tmp_path / "non_existent.txt"
    with pytest.raises(FileNotFoundError):
        local_storage.upload_file(str(non_existent_file), "uploaded_test.txt")
