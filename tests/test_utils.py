"""Test for the utils module."""
import pytest

from esperoj.utils import archive, calculate_hash, calculate_hash_from_url


def test_archive(mocker):
    """Test that a URL can be archived successfully."""
    mocker.patch(
        "requests.Session.post",
        return_value=mocker.Mock(status_code=200, json=lambda: {"job_id": "123"}),
    )
    mocker.patch(
        "requests.Session.get",
        return_value=mocker.Mock(
            status_code=200,
            json=lambda: {
                "status": "success",
                "timestamp": "20231228",
                "original_url": "http://example.com",
            },
        ),
    )
    assert (
        archive("http://example.com") == "https://web.archive.org/web/20231228/http://example.com"
    )


def test_archive_error(mocker):
    """Test that an error is raised when the archive request fails."""
    mocker.patch("requests.Session.post", return_value=mocker.Mock(status_code=400, text="Error"))
    with pytest.raises(RuntimeError, match="Error: Error"):
        archive("http://example.com")


def test_archive_non_200_status(mocker):
    """Test that an error is raised when the archive response is not 200."""
    mocker.patch(
        "requests.Session.post",
        return_value=mocker.Mock(status_code=200, json=lambda: {"job_id": "123"}),
    )
    mock_response = mocker.Mock(status_code=400, text="Error")
    mocker.patch("requests.Session.get", return_value=mock_response)
    with pytest.raises(RuntimeError, match="Error: Error"):
        archive("http://example.com")


def test_archive_timeout(mocker):
    """Test that an error is raised when the archiving process times out."""
    mocker.patch(
        "requests.Session.post",
        return_value=mocker.Mock(status_code=200, json=lambda: {"job_id": "123"}),
    )
    mocker.patch(
        "requests.Session.get",
        return_value=mocker.Mock(status_code=200, json=lambda: {"status": "pending"}),
    )
    mocker.patch("time.sleep", return_value=None)
    mocker.patch("time.time", side_effect=[0, 30, 301])
    with pytest.raises(RuntimeError, match="Error: Archiving process timed out."):
        archive("http://example.com")


def test_archive_unknown_status(mocker):
    """Test that an error is raised when the archive status is unknown."""
    mocker.patch(
        "requests.Session.post",
        return_value=mocker.Mock(status_code=200, json=lambda: {"job_id": "123"}),
    )
    mock_response = mocker.Mock(status_code=200)
    mock_response.json.side_effect = [{"status": "unknown"}]
    mocker.patch("requests.Session.get", return_value=mock_response)
    with pytest.raises(RuntimeError, match="Error: .*"):
        archive("http://example.com")


def test_calculate_hash():
    """Test that a hash can be calculated from a byte string."""
    assert (
        calculate_hash(iter([b"test"]))
        == "9f86d081884c7d659a2feaa0c55ad015a3bf4f1b2b0b822cd15d6c15b0f00a08"
    )


def test_calculate_hash_from_url(mocker):
    """Test that a hash can be calculated from a URL."""
    mocker.patch(
        "requests.get",
        return_value=mocker.Mock(status_code=200, iter_content=lambda chunk_size: iter([b"test"])),
    )
    assert (
        calculate_hash_from_url("http://example.com")
        == "9f86d081884c7d659a2feaa0c55ad015a3bf4f1b2b0b822cd15d6c15b0f00a08"
    )


def test_calculate_hash_from_url_error(mocker):
    """Test that an error is raised when the URL request fails."""
    mocker.patch("requests.get", return_value=mocker.Mock(status_code=400, text="Error"))
    with pytest.raises(RuntimeError, match="Error: Error"):
        calculate_hash_from_url("http://example.com")
