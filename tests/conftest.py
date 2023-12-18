"""Contain list of fixtures."""
import pytest

from esperoj.database import Database


@pytest.fixture(scope="function")
def db():
    """A temporary database."""
    db_ = Database()
    yield db_
    db_.close()


@pytest.fixture(scope="function")
def table(db):
    """An empty table."""
    return db.table("table")
