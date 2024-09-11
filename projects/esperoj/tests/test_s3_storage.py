"""Tests for S3 storage."""


def upload_test_file(s3_storage, tmp_file):
    """Function to upload test file."""
    s3_storage.upload(str(tmp_file), tmp_file.name)


def test_delete(s3_storage, tmp_file):
    upload_test_file(s3_storage, tmp_file)
    assert s3_storage.delete(["tmp.txt"]) is True
    assert s3_storage.exists("tmp.txt") is False


def test_exists(s3_storage, tmp_file):
    upload_test_file(s3_storage, tmp_file)
    assert s3_storage.exists("tmp.txt") is True
    assert s3_storage.exists("non_existing.txt") is False


# TODO: Use download utils from utils to download and check
def test_link(s3_storage, tmp_file):
    upload_test_file(s3_storage, tmp_file)
    url = s3_storage.link("tmp.txt")
    assert isinstance(url, str)


def test_list(s3_storage, tmp_path):
    file_path_1 = tmp_path / "test1.txt"
    file_path_1.write_text("This is test file 1")
    file_path_2 = tmp_path / "test2.txt"
    file_path_2.write_text("This is test file 2")
    s3_storage.upload(str(file_path_1), "test/test1.txt")
    s3_storage.upload(str(file_path_2), "test/test2.txt")
    files = s3_storage.list("test/")
    assert "test/test1.txt" in files
    assert "test/test2.txt" in files


def test_size(s3_storage, tmp_file):
    upload_test_file(s3_storage, tmp_file)
    size = s3_storage.size("tmp.txt")
    assert size == tmp_file.stat().st_size


def test_stream(s3_storage, tmp_file):
    upload_test_file(s3_storage, tmp_file)
    content = b"".join(s3_storage.stream("tmp.txt"))
    assert content == b"Test content"


def test_upload_and_download(s3_storage, tmp_file, tmp_path):
    upload_test_file(s3_storage, tmp_file)
    download_path = tmp_path / "downloaded.txt"
    s3_storage.download("tmp.txt", str(download_path))
    assert download_path.read_text() == "Test content"
