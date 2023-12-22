"""Contain list of fixtures."""
import os
from uuid import uuid4

import boto3
import pytest
from moto import mock_s3

from esperoj import Esperoj
from esperoj.database.memory import MemoryDatabase, MemoryRecord
from esperoj.s3 import S3Storage
from esperoj.storage import LocalStorage


@pytest.fixture()
def bucket_name():
    return "test_bucket"


@pytest.fixture()
def memory_db():
    db = MemoryDatabase("test_db")
    yield db
    db.close()


@pytest.fixture()
def s3_storage(s3_client, bucket_name):
    s3_client.create_bucket(Bucket=bucket_name)
    return S3Storage(name="test_storage", config={"bucket_name": bucket_name})


@pytest.fixture()
def _aws_credentials():
    os.environ["AWS_ACCESS_KEY_ID"] = "testing"
    os.environ["AWS_SECRET_ACCESS_KEY"] = "testing"
    os.environ["AWS_SECURITY_TOKEN"] = "testing"
    os.environ["AWS_SESSION_TOKEN"] = "testing"


@pytest.fixture()
def s3_client(_aws_credentials):
    with mock_s3():
        yield boto3.client("s3", region_name="us-east-1")


@pytest.fixture()
def local_storage(tmp_path):
    return LocalStorage("test_storage", config={"base_path": str(tmp_path / "test_storage")})


@pytest.fixture()
def esperoj(db, local_storage):
    return Esperoj(db, local_storage)


@pytest.fixture()
def memory_table(memory_db):
    return memory_db.table("test_table")


@pytest.fixture()
def mock_memory_records():
    records = [
        MemoryRecord(
            uuid4(),
            {
                "SHA256": "ec7dbedc9b0fb7832fb210964e53d4989d969cc245fa37e07a8bcb5e75056b53",
                "Name": "Next to you (Parasyte Ost).flac",
                "Musics": [uuid4()],
                "Size": 7131586,
                "Storj": "Next to you (Parasyte Ost).flac",
                "Created": "2023-12-18T02:55:57.000Z",
                "Direct Link": "https://link.storjshare.io/raw/jvknve7x2wrg3rqqnfuelbegwkgq/esperoj/Next to you (Parasyte Ost).flac",
            },
        ),
        MemoryRecord(
            uuid4(),
            {
                "SHA256": "e028945c62520815529879e8bfc0df6fbc6b90c0a88ba56442380a914d1bdc67",
                "Name": "Next To You.flac",
                "Musics": [uuid4()],
                "Size": 32952433,
                "Storj": "Next To You.flac",
                "Created": "2023-12-18T02:52:27.000Z",
                "Direct Link": "https://link.storjshare.io/raw/jvknve7x2wrg3rqqnfuelbegwkgq/esperoj/Next To You.flac",
            },
        ),
    ]
    return records
