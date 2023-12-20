"""Contain list of fixtures."""
import os

import boto3
import pytest
from moto import mock_s3

from esperoj.database import Database
from esperoj.esperoj import Esperoj
from esperoj.s3 import S3Storage
from esperoj.storage import LocalStorage


@pytest.fixture()
def bucket_name():
    """Fixture that provides a test bucket name.

    Returns
    -------
        str: Name of the test bucket.
    """
    return "test_bucket"


@pytest.fixture()
def s3_storage(s3_client, bucket_name):
    """Fixture that creates a test S3 storage bucket.

    Args:
        s3_client (boto3.client): The boto3 S3 client.
        bucket_name (str): The name of the bucket to create.

    Returns
    -------
        S3Storage: An instance of S3Storage with the created bucket.
    """
    s3_client.create_bucket(Bucket=bucket_name)
    return S3Storage(name="test_storage", config={"bucket_name": bucket_name})


@pytest.fixture()
def _aws_credentials():
    """Fixture that mocks AWS credentials for moto.

    This sets environment variables for AWS credentials.
    """
    os.environ["AWS_ACCESS_KEY_ID"] = "testing"
    os.environ["AWS_SECRET_ACCESS_KEY"] = "testing"
    os.environ["AWS_SECURITY_TOKEN"] = "testing"
    os.environ["AWS_SESSION_TOKEN"] = "testing"


@pytest.fixture()
def s3_client(_aws_credentials):
    """Fixture that provides a mocked S3 client.

    This uses the moto library to mock an S3 client.

    Yields
    ------
        boto3.client: A mocked S3 client.
    """
    with mock_s3():
        yield boto3.client("s3", region_name="us-east-1")


@pytest.fixture()
def db():
    """Fixture that provides a temporary database.

    This creates a temporary database, yields it for use in tests, and then closes it.

    Yields
    ------
        Database: A temporary database.
    """
    db_ = Database()
    yield db_
    db_.close()


@pytest.fixture()
def local_storage(tmp_path):
    """Fixture that provides a local storage instance.

    Args:
        tmp_path (pathlib.Path): A temporary path provided by pytest.

    Returns
    -------
        LocalStorage: An instance of LocalStorage with the provided path.
    """
    return LocalStorage("test_storage", config={"base_path": str(tmp_path / "test_storage")})


@pytest.fixture()
def esperoj(db, local_storage):
    """Fixture that provides an Esperoj instance.

    Args:
        db (Database): A database instance.
        local_storage (LocalStorage): A local storage instance.

    Returns
    -------
        Esperoj: An instance of Esperoj with the provided database and local storage.
    """
    return Esperoj(db, local_storage)


@pytest.fixture()
def table(db):
    """Fixture that provides an empty table.

    Args:
        db (Database): A database instance.

    Returns
    -------
        Table: An empty table from the provided database.
    """
    return db.table("test")
