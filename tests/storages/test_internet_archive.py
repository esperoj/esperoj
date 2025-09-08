"""
Tests for the InternetArchiveFileSystem custom fsspec backend.

This test suite verifies the functionality of the `InternetArchiveFileSystem`,
including file uploads, downloads, existence checks, and the non-operational
'rm' method. It uses `unittest.mock` to patch the `internetarchive` and `requests`
libraries, simulating API interactions without making actual network calls.
"""

import io
import pytest
import requests
from unittest.mock import patch, MagicMock

from esperoj.storages.internet_archive import InternetArchiveFile, InternetArchiveFileSystem


@pytest.fixture
def ia_fs() -> InternetArchiveFileSystem:
    """Fixture for an InternetArchiveFileSystem instance."""
    return InternetArchiveFileSystem(access_key="test_access_key", secret_key="test_secret_key")


def test_ia_filesystem_init(ia_fs: InternetArchiveFileSystem) -> None:
    """Tests that the InternetArchiveFileSystem initializes correctly."""
    assert ia_fs.access_key == "test_access_key"
    assert ia_fs.secret_key == "test_secret_key"


def test_ia_filesystem_init_missing_keys() -> None:
    """Tests that initialization fails without required keys."""
    with pytest.raises(ValueError, match="requires 'access_key' and 'secret_key'"):
        InternetArchiveFileSystem(access_key="", secret_key="")
    with pytest.raises(ValueError, match="requires 'access_key' and 'secret_key'"):
        InternetArchiveFileSystem(access_key="key", secret_key="")
    with pytest.raises(ValueError, match="requires 'access_key' and 'secret_key'"):
        InternetArchiveFileSystem(access_key="", secret_key="key")


def test_ia_file_init_unsupported_mode(ia_fs: InternetArchiveFileSystem) -> None:
    """Tests that InternetArchiveFile raises an error for unsupported modes."""
    with pytest.raises(ValueError, match="InternetArchiveFile only supports write-binary"):
        InternetArchiveFile(fs=ia_fs, path="my-item/file.txt", mode="rb")


@patch("esperoj.storages.internet_archive.internetarchive.upload")
def test_ia_file_close_is_idempotent(mock_upload: MagicMock, ia_fs: InternetArchiveFileSystem) -> None:
    """Tests that calling close() multiple times does not cause errors."""
    f = ia_fs._open("my-item/idempotent.txt", mode="wb")
    f.write(b"some data")
    f.close()  # First call, should trigger upload
    f.close()  # Second call, should do nothing

    mock_upload.assert_called_once()


@pytest.mark.parametrize(
    "path, expected_identifier, expected_path_in_item",
    [
        ("my-item/file.txt", "my-item", "file.txt"),
        ("my-item/path/to/file.txt", "my-item", "path/to/file.txt"),
        ("another-item/data.csv", "another-item", "data.csv"),
    ],
)
def test_parse_ia_path_success(
    ia_fs: InternetArchiveFileSystem, path: str, expected_identifier: str, expected_path_in_item: str
) -> None:
    """Tests successful parsing of valid Internet Archive paths."""
    identifier, path_in_item = ia_fs._parse_ia_path(path)
    assert identifier == expected_identifier
    assert path_in_item == expected_path_in_item


@pytest.mark.parametrize(
    "invalid_path",
    [
        "just-an-identifier",  # Missing path part
        "identifier/",  # Empty path part
        "/path/to/file.txt",  # Empty identifier part
        "",  # Empty path
    ],
)
def test_parse_ia_path_failure(ia_fs: InternetArchiveFileSystem, invalid_path: str) -> None:
    """Tests that parsing invalid paths raises ValueError."""
    with pytest.raises(ValueError):
        ia_fs._parse_ia_path(invalid_path)


@patch("esperoj.storages.internet_archive.internetarchive.upload")
def test_ia_file_open_write_success(mock_upload: MagicMock, ia_fs: InternetArchiveFileSystem) -> None:
    """Tests a successful file upload using 'wb' mode."""
    file_content = b"This is a test for IA."
    fsspec_path = "test-item/remote/path/file.txt"
    metadata = {"title": "Test Title", "mediatype": "data"}

    with ia_fs._open(fsspec_path, mode="wb", metadata=metadata) as f:
        f.write(file_content)
        # Upload happens on close()

    mock_upload.assert_called_once()
    call_kwargs = mock_upload.call_args.kwargs
    assert call_kwargs["identifier"] == "test-item"
    assert "files" in call_kwargs
    assert "remote/path/file.txt" in call_kwargs["files"]
    uploaded_file_obj = call_kwargs["files"]["remote/path/file.txt"]
    assert uploaded_file_obj.read() == file_content
    assert call_kwargs["metadata"] == metadata
    assert call_kwargs["access_key"] == ia_fs.access_key
    assert call_kwargs["secret_key"] == ia_fs.secret_key


@patch("esperoj.storages.internet_archive.internetarchive.upload")
def test_ia_file_open_write_empty_file(mock_upload: MagicMock, ia_fs: InternetArchiveFileSystem) -> None:
    """Tests that writing an empty file does not trigger an upload."""
    with ia_fs._open("my-item/empty.txt", mode="wb") as f:
        f.write(b"")

    mock_upload.assert_not_called()


@patch("esperoj.storages.internet_archive.internetarchive.upload")
def test_ia_file_open_write_failure(mock_upload: MagicMock, ia_fs: InternetArchiveFileSystem) -> None:
    """Tests a failed file upload."""
    mock_upload.side_effect = Exception("IA API error")

    with pytest.raises(IOError, match="Internet Archive upload failed"):
        with ia_fs._open("my-item/fail.txt", mode="wb") as f:
            f.write(b"this upload will fail")


@patch("esperoj.storages.internet_archive.requests.get")
def test_ia_filesystem_open_read_success(mock_get: MagicMock, ia_fs: InternetArchiveFileSystem) -> None:
    """Tests a successful file read using 'rb' mode."""
    mock_response = MagicMock()
    mock_response.raise_for_status.return_value = None
    mock_response.raw = io.BytesIO(b"remote file content")
    mock_get.return_value = mock_response

    fsspec_path = "read-item/data/file.zip"
    expected_url = "https://archive.org/download/read-item/data/file.zip"

    with ia_fs._open(fsspec_path, mode="rb") as f:
        content = f.read()

    assert content == b"remote file content"
    mock_get.assert_called_once_with(expected_url, stream=True, timeout=60)


@patch("esperoj.storages.internet_archive.requests.get")
def test_ia_filesystem_open_read_failure(mock_get: MagicMock, ia_fs: InternetArchiveFileSystem) -> None:
    """Tests a failed file read."""
    fsspec_path = "bad-item/nonexistent.txt"
    expected_url = "https://archive.org/download/bad-item/nonexistent.txt"
    mock_get.side_effect = requests.RequestException("Network error")

    with pytest.raises(IOError, match=f"Failed to stream file from Internet Archive URL {expected_url}"):
        ia_fs._open(fsspec_path, mode="rb")


def test_ia_filesystem_open_unsupported_mode(ia_fs: InternetArchiveFileSystem) -> None:
    """Tests that opening in an unsupported mode raises an error."""
    with pytest.raises(NotImplementedError):
        ia_fs._open("my-item/file.txt", mode="w")


@patch("esperoj.storages.internet_archive.requests.head")
def test_ia_filesystem_exists_success(mock_head: MagicMock, ia_fs: InternetArchiveFileSystem) -> None:
    """Tests `exists` for a file that is present."""
    mock_response = MagicMock()
    mock_response.status_code = 200
    mock_head.return_value = mock_response
    assert ia_fs.exists("my-item/exists.txt") is True


@patch("esperoj.storages.internet_archive.requests.head")
def test_ia_filesystem_exists_failure(mock_head: MagicMock, ia_fs: InternetArchiveFileSystem) -> None:
    """Tests `exists` for a file that is not present."""
    mock_response = MagicMock()
    mock_response.status_code = 404
    mock_head.return_value = mock_response
    assert ia_fs.exists("my-item/not-exists.txt") is False


@patch("esperoj.storages.internet_archive.requests.head")
def test_ia_filesystem_exists_request_exception(mock_head: MagicMock, ia_fs: InternetArchiveFileSystem) -> None:
    """Tests `exists` when the HEAD request fails."""
    mock_head.side_effect = requests.RequestException("Connection timed out")
    assert ia_fs.exists("my-item/timeout.txt") is False


def test_ia_filesystem_exists_invalid_path(ia_fs: InternetArchiveFileSystem) -> None:
    """Tests `exists` with an invalid path format returns False."""
    assert ia_fs.exists("invalid-path-format") is False


def test_ia_filesystem_rm(ia_fs: InternetArchiveFileSystem, caplog: pytest.LogCaptureFixture) -> None:
    """Tests that `rm` is a no-op and logs a warning."""
    fsspec_path = "my-item/some-file.txt"
    ia_fs.rm(fsspec_path)  # Should not raise an error

    assert "Direct file deletion (rm) is not supported" in caplog.text
    assert fsspec_path in caplog.text
