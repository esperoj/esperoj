"""Fixtures for testing."""

import json
import logging
import os
import tomllib
from pathlib import Path

import pytest
from moto import mock_aws

from esperoj.database.database import DatabaseFactory
from esperoj.database.models import table_models
from esperoj.esperoj import Esperoj
from esperoj.storage.file_host import FileHostFactory
from esperoj.storage.storage import StorageFactory


@pytest.fixture()
def aws_credentials():
    """Mocked AWS Credentials for moto."""
    os.environ["AWS_ACCESS_KEY_ID"] = "testing"
    os.environ["AWS_SECRET_ACCESS_KEY"] = "testing"
    os.environ["AWS_SECURITY_TOKEN"] = "testing"
    os.environ["AWS_SESSION_TOKEN"] = "testing"
    os.environ["AWS_DEFAULT_REGION"] = "us-east-1"


@pytest.fixture()
def config():
    """Return a config."""
    p = Path(__file__).parent / "test_data" / "esperoj.toml"
    return tomllib.loads(p.read_text())


@pytest.fixture()
def tmp_file(tmp_path):
    """Return a test file."""
    file = tmp_path / "tmp.txt"
    file.write_text("Test content")
    return file


@pytest.fixture()
def s3_storage(config, aws_credentials):
    """Return a mocked instance of S3Storage."""
    with mock_aws():
        s3 = StorageFactory.create(config["storages"][0])
        s3.client.create_bucket(Bucket=config["storages"][0]["bucket_name"])
        yield s3


@pytest.fixture
def memory_db(config):
    db = DatabaseFactory.create(config["databases"][0], table_models)
    for table_name in ["files", "musics"]:
        p = Path(__file__).parent / "test_data" / "json" / f"{table_name}.json"
        db.create_table(table_name)
        db.batch_create(table_name, json.loads(p.read_text()))
    db.create_table("test")
    fields_list = [{"name": "Alice"}, {"name": "Bob"}]
    db.batch_create("test", fields_list)
    return db


@pytest.fixture
def local_file_host(config, tmp_path):
    local_file_host_config = config["file_hosts"][0]
    local_file_host_config["base_src"] = str(tmp_path)
    return FileHostFactory.create(local_file_host_config)


@pytest.fixture
def logger(scope="session"):
    logger = logging.getLogger("tests")
    logger.setLevel(logging.INFO)
    handler = logging.StreamHandler()
    formatter = logging.Formatter("%(asctime)s - %(name)s - %(levelname)s - %(message)s")
    handler.setFormatter(formatter)
    logger.addHandler(handler)
    return logger


@pytest.fixture
def esperoj(config, memory_db, s3_storage, local_file_host, logger):
    loggers = {"primary": logger}
    databases = {"primary": memory_db}
    storages = {"s3_storage": s3_storage}
    file_hosts = {"local_file_host": local_file_host}
    for file in memory_db.query("files"):
        src = Path(__file__).parent / "test_data" / "samples" / f"{file.name}"
        s3_storage.upload(str(src), src.name)
        local_file_host.upload(src)
    return Esperoj(config=config, databases=databases, file_hosts=file_hosts, storages=storages, loggers=loggers)
