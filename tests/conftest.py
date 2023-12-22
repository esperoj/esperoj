"""Fixtures for testing."""


import pytest
from moto import mock_s3

from esperoj import Esperoj
from esperoj.database.memory import MemoryDatabase
from esperoj.storage.local import LocalStorage
from esperoj.storage.s3 import S3Storage, default_config


@pytest.fixture()
def bucket_name():
    """Return a test bucket name."""
    return "test_bucket"


@pytest.fixture()
def memory_db():
    """Return a test memory database."""
    db = MemoryDatabase("test_db")
    yield db
    db.close()


@pytest.fixture()
def s3_storage():
    """Return a mocked instance of S3Storage."""
    with mock_s3():
        s3 = S3Storage("test_storage", default_config)
        s3.s3.create_bucket(Bucket=default_config["bucket_name"])
        yield s3


@pytest.fixture()
def local_storage(tmp_path):
    """Return a local storage object."""
    return LocalStorage("test_storage", str(tmp_path))


@pytest.fixture()
def esperoj(db, local_storage):
    """Return an Esperoj object."""
    return Esperoj(db, local_storage)


@pytest.fixture()
def memory_table(memory_db):
    """Return a test memory table."""
    return memory_db.table("test_table")
