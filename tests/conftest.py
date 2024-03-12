"""Fixtures for testing."""

import tomllib
from pathlib import Path

import pytest
from moto import mock_aws

from esperoj.database.memory import MemoryDatabase
from esperoj.esperoj import Esperoj
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
    with p.open("rb") as f:
        yield tomllib.load(f)


@pytest.fixture()
def tmp_file(tmp_path):
    """Return a test file."""
    file = tmp_path / "tmp_file.txt"
    file.write_text("Test content")
    return file


@pytest.fixture()
def memory_db():
    """Return a test memory database."""
    db = MemoryDatabase("test_db")
    yield db
    db.close()


@pytest.fixture()
def s3_storage(config):
    """Return a mocked instance of S3Storage."""
    with mock_aws():
        s3 = StorageFactory.create("s3", config["storages"][0])
        s3.client.create_bucket(Bucket=config["storages"][0]["bucket_name"])
        yield s3


@pytest.fixture()
def esperoj(memory_db, s3_storage):
    """Return an Esperoj object."""
    return Esperoj(db=memory_db, storage=s3_storage)


@pytest.fixture()
def memory_table(memory_db):
    """Return a test memory table."""
    return memory_db.table("Files")
