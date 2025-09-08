"""
Tests for the CatboxFileSystem custom fsspec backend.

This test suite verifies the functionality of the `CatboxFileSystem`, including
file uploads, downloads, existence checks, and deletions. It uses `unittest.mock`
to patch the `requests` library, allowing for the simulation of API interactions
without making actual network calls.
"""

import io
import pytest
import requests
from unittest.mock import patch, MagicMock

from esperoj.storages.catbox import CatboxFileSystem, CatboxFile


@pytest.fixture
def catbox_fs() -> CatboxFileSystem:
    """Fixture for a CatboxFileSystem instance with a userhash."""
    return CatboxFileSystem(userhash="test_user_hash")


@pytest.fixture
def catbox_fs_anonymous() -> CatboxFileSystem:
    """Fixture for a CatboxFileSystem instance without a userhash."""
    return CatboxFileSystem()


def test_catbox_filesystem_init(catbox_fs: CatboxFileSystem, catbox_fs_anonymous: CatboxFileSystem) -> None:
    """Tests that the CatboxFileSystem initializes correctly."""
    assert catbox_fs.userhash == "test_user_hash"
    assert catbox_fs.api_url == "https://catbox.moe/user/api.php"
    assert catbox_fs_anonymous.userhash is None

    # Test with custom api_url
    fs = CatboxFileSystem(api_url="http://custom.local/api")
    assert fs.api_url == "http://custom.local/api"


def test_catbox_file_init_unsupported_mode(catbox_fs: CatboxFileSystem) -> None:
    """Tests that CatboxFile raises an error for unsupported modes."""
    with pytest.raises(ValueError, match="CatboxFile only supports write-binary"):
        CatboxFile(fs=catbox_fs, path="test.txt", mode="rb")


@patch("esperoj.storages.catbox.requests.post")
def test_catbox_file_close_is_idempotent(mock_post: MagicMock, catbox_fs: CatboxFileSystem) -> None:
    """Tests that calling close() multiple times does not cause errors."""
    mock_response = MagicMock()
    mock_response.status_code = 200
    mock_response.text = "https://files.catbox.moe/output.txt"
    mock_post.return_value = mock_response

    f = catbox_fs._open("test/idempotent.txt", mode="wb")
    f.write(b"some data")
    f.close()  # First call, should trigger upload
    f.close()  # Second call, should do nothing

    mock_post.assert_called_once()


@patch("esperoj.storages.catbox.requests.post")
def test_catbox_file_open_write_success(mock_post: MagicMock, catbox_fs: CatboxFileSystem) -> None:
    """Tests a successful file upload using the 'wb' mode."""
    mock_response = MagicMock()
    mock_response.status_code = 200
    mock_response.text = "https://files.catbox.moe/output.txt"
    mock_post.return_value = mock_response

    file_content = b"This is a test file."
    file_path = "test/output.txt"

    with catbox_fs._open(file_path, mode="wb") as f:
        f.write(file_content)
        # The upload happens on close()

    mock_post.assert_called_once()
    # Check the 'data' part of the call
    assert mock_post.call_args.kwargs["data"] == {"reqtype": "fileupload", "userhash": "test_user_hash"}
    # Check the 'files' part
    files_arg = mock_post.call_args.kwargs["files"]
    assert "fileToUpload" in files_arg
    filename, uploaded_content = files_arg["fileToUpload"]
    assert filename == "output.txt"
    assert uploaded_content == file_content


@patch("esperoj.storages.catbox.requests.post")
def test_catbox_file_open_write_empty_file(mock_post: MagicMock, catbox_fs: CatboxFileSystem) -> None:
    """Tests that writing an empty file does not trigger an upload."""
    with catbox_fs._open("test/empty.txt", mode="wb") as f:
        f.write(b"")

    mock_post.assert_not_called()


@patch("esperoj.storages.catbox.requests.post")
def test_catbox_file_open_write_failure(mock_post: MagicMock, catbox_fs: CatboxFileSystem) -> None:
    """Tests a failed file upload."""
    mock_post.side_effect = requests.RequestException("API is down")

    with pytest.raises(IOError, match="File upload failed: API is down"):
        with catbox_fs._open("test/fail.txt", mode="wb") as f:
            f.write(b"this will fail")


@patch("esperoj.storages.catbox.requests.get")
def test_catbox_filesystem_open_read_success(mock_get: MagicMock, catbox_fs: CatboxFileSystem) -> None:
    """Tests a successful file read using 'rb' mode."""
    mock_response = MagicMock()
    mock_response.raise_for_status.return_value = None
    mock_response.raw = io.BytesIO(b"remote file content")
    mock_get.return_value = mock_response

    file_url = "https://files.catbox.moe/remote.txt"
    with catbox_fs._open(file_url, mode="rb") as f:
        content = f.read()

    assert content == b"remote file content"
    mock_get.assert_called_once_with(file_url, stream=True, timeout=60)


@patch("esperoj.storages.catbox.requests.get")
def test_catbox_filesystem_open_read_failure(mock_get: MagicMock, catbox_fs: CatboxFileSystem) -> None:
    """Tests a failed file read."""
    mock_get.side_effect = requests.RequestException("Network error")

    file_url = "https://files.catbox.moe/nonexistent.txt"
    with pytest.raises(IOError, match=f"Failed to stream file from Catbox URL {file_url}: Network error"):
        catbox_fs._open(file_url, mode="rb")


def test_catbox_filesystem_open_unsupported_mode(catbox_fs: CatboxFileSystem) -> None:
    """Tests that opening in an unsupported mode raises an error."""
    with pytest.raises(NotImplementedError):
        catbox_fs._open("test.txt", mode="w")


@patch("esperoj.storages.catbox.requests.head")
def test_catbox_filesystem_exists_success(mock_head: MagicMock, catbox_fs: CatboxFileSystem) -> None:
    """Tests `exists` for a file that is present."""
    mock_response = MagicMock()
    mock_response.status_code = 200
    mock_head.return_value = mock_response
    assert catbox_fs.exists("https://files.catbox.moe/exists.txt") is True


@patch("esperoj.storages.catbox.requests.head")
def test_catbox_filesystem_exists_failure(mock_head: MagicMock, catbox_fs: CatboxFileSystem) -> None:
    """Tests `exists` for a file that is not present."""
    mock_response = MagicMock()
    mock_response.status_code = 404
    mock_head.return_value = mock_response
    assert catbox_fs.exists("https://files.catbox.moe/not-exists.txt") is False


@patch("esperoj.storages.catbox.requests.head")
def test_catbox_filesystem_exists_request_exception(mock_head: MagicMock, catbox_fs: CatboxFileSystem) -> None:
    """Tests `exists` when the HEAD request fails."""
    mock_head.side_effect = requests.RequestException("Connection timed out")
    assert catbox_fs.exists("https://files.catbox.moe/timeout.txt") is False


@patch("esperoj.storages.catbox.requests.post")
def test_catbox_filesystem_rm_success(mock_post: MagicMock, catbox_fs: CatboxFileSystem) -> None:
    """Tests a successful file removal."""
    mock_response = MagicMock()
    mock_response.raise_for_status.return_value = None
    mock_post.return_value = mock_response

    file_url = "https://files.catbox.moe/todelete.txt"
    catbox_fs.rm(file_url)

    mock_post.assert_called_once_with(
        catbox_fs.api_url,
        data={"reqtype": "deletefiles", "userhash": "test_user_hash", "files": "todelete.txt"},
        timeout=60,
    )


@patch("esperoj.storages.catbox.requests.post")
def test_catbox_filesystem_rm_no_userhash(
    mock_post: MagicMock, catbox_fs_anonymous: CatboxFileSystem, caplog: pytest.LogCaptureFixture
) -> None:
    """Tests that `rm` does not make an API call without a userhash."""
    file_url = "https://files.catbox.moe/orphan.txt"
    catbox_fs_anonymous.rm(file_url)

    mock_post.assert_not_called()
    assert "File deletion is not supported without a userhash" in caplog.text


@patch("esperoj.storages.catbox.requests.post")
def test_catbox_filesystem_rm_failure(mock_post: MagicMock, catbox_fs: CatboxFileSystem) -> None:
    """Tests a failed file removal."""
    mock_post.side_effect = requests.RequestException("Deletion failed")

    with pytest.raises(IOError, match="File deletion failed: Deletion failed"):
        catbox_fs.rm("https://files.catbox.moe/faildelete.txt")
