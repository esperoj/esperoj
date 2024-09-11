"""Fixtures for testing."""

from datetime import datetime
import tomllib
from pathlib import Path

import pytest
from moto import mock_aws

from esperoj.database.database import DatabaseFactory
from esperoj.database.models import table_models
from esperoj.storage.storage import StorageFactory


@pytest.fixture(autouse=True)
def _mock_env(mocker):
    """Mock the environment variables for Internet Archive access."""
    mocker.patch.dict(
        "os.environ",
        {
            "INTERNET_ARCHIVE_ACCESS_KEY": "test_key",
            "INTERNET_ARCHIVE_SECRET_KEY": "test_secret",
        },
    )


@pytest.fixture()
def config():
    """Return a config."""
    p = Path(__file__).with_name("esperoj.toml")
    return tomllib.loads(p.read_text())


@pytest.fixture()
def tmp_file(tmp_path):
    """Return a test file."""
    file = tmp_path / "tmp.txt"
    file.write_text("Test content")
    return file


@pytest.fixture()
def s3_storage(config):
    """Return a mocked instance of S3Storage."""
    with mock_aws():
        s3 = StorageFactory.create(config["storages"][0])
        s3.client.create_bucket(Bucket=config["storages"][0]["bucket_name"])
        yield s3


@pytest.fixture
def memory_db(config):
    db = DatabaseFactory.create(config["databases"][0], table_models)
    for table_name in table_models:
        db.create_table(table_name)
    db.create_table("test")
    fields_list = [{"name": "Alice"}, {"name": "Bob"}]
    db.batch_create("test", fields_list)
    mirror_info_dict = {
    "sources": ["http://example.com/source1", "http://example.com/source2"],
    "encrypted": True
}
    music_dict = {
    "title": "My Favorite Song",
    "comment": "This is a great track!",
    "files": [],
    "modified": datetime.now(),
    "created": datetime.now()
}
    file_dict = {
    "name": "example_file.mp3",
    "sha256": "a" * 64,  # Replace with a valid SHA256 hash
    "size": 1024,
    "mirrors": {"mirror1": mirror_info_dict},
    "musics": [],
    "modified": datetime.now(),
    "created": datetime.now(),
    "metadata": {"genre": "Pop", "artist": "Artist Name"},
    "verified": True
}
    db.create("files", file_dict)
    db.create("musics", music_dict)
    return db