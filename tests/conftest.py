"""Contain list of fixtures."""
import pytest

from esperoj.database import Database


@pytest.fixture()
def db():
    """Return a temporary database."""
    db_ = Database()
    yield db_
    db_.close()


@pytest.fixture()
def table(db):
    """Return an empty table."""
    return db.table("table")
