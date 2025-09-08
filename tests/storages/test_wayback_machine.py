"""
Tests for the WaybackMachineFileSystem custom fsspec backend.

This test suite verifies the functionality of the `WaybackMachineFileSystem`,
primarily its ability to initiate URL captures via the SPN2 API and stream
content from existing Wayback Machine URLs. It uses `unittest.mock` to patch
the `requests` library and `time.sleep`, simulating API interactions without
making actual network calls.
"""

import io
import pytest
import requests
from unittest.mock import patch, MagicMock, call

from esperoj.storages.wayback_machine import WaybackMachineFile, WaybackMachineFileSystem


@pytest.fixture
def wayback_fs() -> WaybackMachineFileSystem:
    """Fixture for a WaybackMachineFileSystem instance with mock credentials."""
    return WaybackMachineFileSystem(access_key="test_access", secret_key="test_secret")


def test_wayback_filesystem_init(wayback_fs: WaybackMachineFileSystem) -> None:
    """Tests that the WaybackMachineFileSystem initializes correctly."""
    assert wayback_fs.access_key == "test_access"
    assert wayback_fs.secret_key == "test_secret"
    assert wayback_fs.api_url_save == "https://web.archive.org/save"
    assert wayback_fs.api_url_status == "https://web.archive.org/save/status"


def test_wayback_filesystem_init_missing_keys() -> None:
    """Tests that initialization fails if API keys are not provided."""
    with pytest.raises(ValueError, match="`access_key` and `secret_key` must be provided."):
        WaybackMachineFileSystem(access_key=None, secret_key="secret")
    with pytest.raises(ValueError, match="`access_key` and `secret_key` must be provided."):
        WaybackMachineFileSystem(access_key="access", secret_key=None)


def test_wayback_file_init_unsupported_mode(wayback_fs: WaybackMachineFileSystem) -> None:
    """Tests that WaybackMachineFile raises an error for unsupported modes."""
    with pytest.raises(ValueError, match="WaybackFile only supports write-binary"):
        WaybackMachineFile(fs=wayback_fs, path="test.txt", mode="rb")


@patch("esperoj.storages.wayback_machine.requests.post")
@patch("esperoj.storages.wayback_machine.requests.get")
@patch("esperoj.storages.wayback_machine.time.sleep", return_value=None)
def test_wayback_file_open_write_success(
    mock_sleep: MagicMock, mock_get: MagicMock, mock_post: MagicMock, wayback_fs: WaybackMachineFileSystem
) -> None:
    """Tests a successful URL capture process."""
    # Mock the initial save request
    mock_post.return_value = MagicMock(json=lambda: {"job_id": "test-job-123"})
    # Mock the status polling
    mock_get.side_effect = [
        MagicMock(json=lambda: {"status": "pending"}),  # First poll
        MagicMock(
            json=lambda: {  # Second poll
                "status": "success",
                "timestamp": "20240101000000",
                "original_url": "https://example.com",
            }
        ),
    ]

    target_url = "https://example.com"
    with wayback_fs._open("archive/capture.txt", mode="wb") as f:
        f.write(target_url.encode("utf-8"))
        # Capture and polling happen on close()

    # Verify initial POST call
    expected_headers = {
        "Accept": "application/json",
        "Authorization": "LOW test_access:test_secret",
    }
    expected_data = {
        "url": target_url,
        "skip_first_archive": "1",
        "js_behavior_timeout": "0",
    }
    mock_post.assert_called_once_with(wayback_fs.api_url_save, headers=expected_headers, data=expected_data, timeout=60)

    # Verify status GET calls
    status_url = f"{wayback_fs.api_url_status}/test-job-123"
    assert mock_get.call_count == 2
    mock_get.assert_has_calls([call(status_url, headers=expected_headers, timeout=30)] * 2)


@patch("esperoj.storages.wayback_machine.requests.post")
def test_wayback_file_start_capture_missing_job_id(mock_post: MagicMock, wayback_fs: WaybackMachineFileSystem) -> None:
    """Tests that _start_capture raises an error if job_id is missing from response."""
    mock_post.return_value = MagicMock(json=lambda: {"status": "success", "message": "no job_id"})

    with pytest.raises(
        IOError, match="Invalid API response received: 'job_id' not found in capture initiation response."
    ):
        with wayback_fs._open("archive/no-job-id.txt", mode="wb") as f:
            f.write(b"https://example.com/no-job")


@patch("esperoj.storages.wayback_machine.requests.post")
@patch("esperoj.storages.wayback_machine.requests.get")
@patch("esperoj.storages.wayback_machine.time.sleep", return_value=None)
def test_wayback_file_open_write_capture_failure(
    mock_sleep: MagicMock, mock_get: MagicMock, mock_post: MagicMock, wayback_fs: WaybackMachineFileSystem
) -> None:
    """Tests a failed URL capture process."""
    mock_post.return_value = MagicMock(json=lambda: {"job_id": "fail-job-456"})
    mock_get.return_value = MagicMock(json=lambda: {"status": "error", "message": "Capture failed"})

    with pytest.raises(IOError, match="Failed to capture https://example.com/fail: Capture failed"):
        with wayback_fs._open("archive/fail.txt", mode="wb") as f:
            f.write(b"https://example.com/fail")


@patch("esperoj.storages.wayback_machine.requests.post")
@patch("esperoj.storages.wayback_machine.requests.get")
@patch("esperoj.storages.wayback_machine.time.sleep", return_value=None)
@patch("esperoj.storages.wayback_machine.time.time")
def test_wayback_file_open_write_timeout(
    mock_time: MagicMock,
    mock_sleep: MagicMock,
    mock_get: MagicMock,
    mock_post: MagicMock,
    wayback_fs: WaybackMachineFileSystem,
) -> None:
    """Tests the capture process timing out."""
    mock_post.return_value = MagicMock(json=lambda: {"job_id": "timeout-job-789"})
    # Always return pending to simulate a timeout
    mock_get.return_value = MagicMock(json=lambda: {"status": "pending"})

    # Set a sequence of time values that will exceed the 150s timeout
    # First call sets the timeout, subsequent calls are in the while loop.
    mock_time.side_effect = [
        1000,  # Initial time to set timeout = 1150
        1001,  # First check in loop, continues
        1151,  # Second check, > 1150, causes loop to exit
    ]

    with pytest.raises(IOError, match="Capture timed out after 150 seconds."):
        with wayback_fs._open("archive/timeout.txt", mode="wb") as f:
            f.write(b"https://example.com/timeout")


@patch("esperoj.storages.wayback_machine.requests.post")
@patch("esperoj.storages.wayback_machine.requests.get")
def test_wayback_file_close_is_idempotent(
    mock_get: MagicMock, mock_post: MagicMock, wayback_fs: WaybackMachineFileSystem
) -> None:
    """Tests that calling close() multiple times does not cause errors and only uploads once."""
    # Mock the initial save request
    mock_post.return_value = MagicMock(json=lambda: {"job_id": "idempotent-job"})
    # Mock the status polling
    mock_get.side_effect = [
        MagicMock(json=lambda: {"status": "pending"}),  # First poll
        MagicMock(
            json=lambda: {  # Second poll
                "status": "success",
                "timestamp": "20240101000000",
                "original_url": "https://example.com/idempotent",
            }
        ),
    ]

    f = wayback_fs._open("archive/idempotent.txt", mode="wb")
    f.write(b"https://example.com/idempotent")
    f.close()  # First call, should trigger post and polling
    f.close()  # Second call, should do nothing

    mock_post.assert_called_once()
    assert mock_get.call_count == 2


def test_wayback_file_open_write_empty_url(
    wayback_fs: WaybackMachineFileSystem, caplog: pytest.LogCaptureFixture
) -> None:
    """Tests that writing an empty URL does not trigger a capture."""
    with wayback_fs._open("archive/empty.txt", mode="wb") as f:
        f.write(b"")  # Empty content

    assert "No URL provided to capture for path archive/empty.txt. Aborting." in caplog.text


@patch("esperoj.storages.wayback_machine.requests.post")
@patch("esperoj.storages.wayback_machine.requests.get")
def test_wayback_file_open_write_malformed_response(
    mock_get: MagicMock, mock_post: MagicMock, wayback_fs: WaybackMachineFileSystem
) -> None:
    """Tests that a malformed API response during polling raises an IOError (KeyError case)."""
    mock_post.return_value = MagicMock(json=lambda: {"job_id": "malformed-job"})
    # Simulate a successful status, but missing the 'timestamp' key, triggering KeyError in close()
    mock_get.side_effect = [
        MagicMock(json=lambda: {"status": "pending"}),
        MagicMock(json=lambda: {"status": "success", "original_url": "https://example.com/malformed"}),
    ]

    target_url = "https://example.com/malformed"
    with pytest.raises(IOError, match="Invalid API response received: 'timestamp'") as excinfo:
        with wayback_fs._open("archive/malformed.txt", mode="wb") as f:
            f.write(target_url.encode("utf-8"))

    # Assert that the underlying KeyError is the cause
    assert isinstance(excinfo.value.__cause__, KeyError)


@patch("esperoj.storages.wayback_machine.requests.post")
@patch("esperoj.storages.wayback_machine.requests.get")
def test_wayback_file_open_write_invalid_json_response(
    mock_get: MagicMock, mock_post: MagicMock, wayback_fs: WaybackMachineFileSystem, caplog: pytest.LogCaptureFixture
) -> None:
    """Tests that an invalid JSON response from the API raises an IOError (ValueError case)."""
    mock_post.return_value = MagicMock(json=MagicMock(side_effect=ValueError("Invalid JSON")))
    # Ensure GET is not called if POST fails at JSON parsing
    mock_get.return_value = MagicMock(json=lambda: {"status": "success"})

    target_url = "https://example.com/invalid-json-post"
    with pytest.raises(IOError, match="Invalid API response received: Invalid JSON") as excinfo:
        with wayback_fs._open("archive/invalid-json-post.txt", mode="wb") as f:
            f.write(target_url.encode("utf-8"))

    assert isinstance(excinfo.value.__cause__, ValueError)
    assert "Unexpected API response for https://example.com/invalid-json-post: Invalid JSON" in caplog.text
    caplog.clear()  # Clear logs for the next assertion

    mock_post.return_value = MagicMock(json=lambda: {"job_id": "valid-job-for-get"})
    mock_get.return_value = MagicMock(json=MagicMock(side_effect=ValueError("Invalid JSON Status")))
    target_url = "https://example.com/invalid-json-get"
    with pytest.raises(IOError, match="Invalid API response received: Invalid JSON Status") as excinfo:
        with wayback_fs._open("archive/invalid-json-get.txt", mode="wb") as f:
            f.write(target_url.encode("utf-8"))

    assert isinstance(excinfo.value.__cause__, ValueError)
    assert "Unexpected API response for https://example.com/invalid-json-get: Invalid JSON Status" in caplog.text


def test_wayback_file_open_write_invalid_url(wayback_fs: WaybackMachineFileSystem) -> None:
    """Tests writing an invalid URL (not http/https)."""
    with pytest.raises(ValueError, match="The provided input must be a valid HTTP or HTTPS URL."):
        with wayback_fs._open("archive/invalid.txt", mode="wb") as f:
            f.write(b"ftp://example.com")


@patch("esperoj.storages.wayback_machine.requests.get")
def test_wayback_filesystem_open_read_success(mock_get: MagicMock, wayback_fs: WaybackMachineFileSystem) -> None:
    """Tests a successful file read from a Wayback URL."""
    mock_response = MagicMock()
    mock_response.raise_for_status.return_value = None
    mock_response.raw = io.BytesIO(b"archived page content")
    mock_get.return_value = mock_response

    wayback_url = "https://web.archive.org/web/20240101000000/https://example.com"
    with wayback_fs._open(wayback_url, mode="rb") as f:
        content = f.read()

    assert content == b"archived page content"
    mock_get.assert_called_once_with(wayback_url, stream=True, timeout=60)


@patch("esperoj.storages.wayback_machine.requests.get")
def test_wayback_filesystem_open_read_failure(mock_get: MagicMock, wayback_fs: WaybackMachineFileSystem) -> None:
    """Tests a failed file read from a Wayback URL."""
    mock_get.side_effect = requests.RequestException("Network error")
    url = "https://web.archive.org/web/invalid"
    with pytest.raises(IOError, match=f"Failed to stream file from Wayback URL {url}"):
        wayback_fs._open(url, mode="rb")


def test_wayback_filesystem_open_unsupported_mode(wayback_fs: WaybackMachineFileSystem) -> None:
    """Tests that opening in an unsupported mode raises an error."""
    with pytest.raises(NotImplementedError):
        wayback_fs._open("http://example.com", mode="w")


@patch("esperoj.storages.wayback_machine.requests.head")
def test_wayback_filesystem_exists_success(mock_head: MagicMock, wayback_fs: WaybackMachineFileSystem) -> None:
    """Tests `exists` for a URL that is present."""
    mock_head.return_value = MagicMock(status_code=200)
    assert wayback_fs.exists("http://example.com/exists") is True


@patch("esperoj.storages.wayback_machine.requests.head")
def test_wayback_filesystem_exists_failure(mock_head: MagicMock, wayback_fs: WaybackMachineFileSystem) -> None:
    """Tests `exists` for a URL that is not present."""
    mock_head.return_value = MagicMock(status_code=404)
    assert wayback_fs.exists("http://example.com/not-exists") is False


@patch("esperoj.storages.wayback_machine.requests.head")
def test_wayback_filesystem_exists_request_exception(
    mock_head: MagicMock, wayback_fs: WaybackMachineFileSystem
) -> None:
    """Tests `exists` when the HEAD request fails."""
    mock_head.side_effect = requests.RequestException("Connection error")
    assert wayback_fs.exists("http://example.com/error") is False


def test_wayback_filesystem_rm(wayback_fs: WaybackMachineFileSystem, caplog: pytest.LogCaptureFixture) -> None:
    """Tests that `rm` is a no-op and logs a warning."""
    path = "http://example.com/to-delete"
    wayback_fs.rm(path)  # Should not raise an error

    assert "File deletion is not supported by the Wayback Machine API" in caplog.text
    assert path in caplog.text
