"""Tests for database table functionality in Esperoj."""

import pytest

from esperoj.exceptions import InvalidRecordError, RecordNotFoundError


@pytest.fixture()
def record(table):
    """Create a test record in the table."""
    return table.create({"name": "test"})


def test_database_table_creation(db):
    """Test creating a table using the database instance."""
    table = db.table("test_table")
    assert table.name == "test_table"


def test_database_table_creation_error(db):
    """Test error handling for invalid table names."""
    with pytest.raises(ValueError, match="Table name must be a string."):
        db.table(123)


def test_table_create(table):
    """Test creating a record in a table."""
    record = table.create({"name": "test"})
    assert "id" in record
    assert record["fields"] == {"name": "test"}


@pytest.mark.parametrize("invalid_input", [123, "string", None])
def test_table_create_error(table, invalid_input):
    """Test error handling for invalid record data."""
    with pytest.raises(InvalidRecordError):
        table.create(invalid_input)


def test_table_delete(table, record):
    """Test deleting a record from a table."""
    record_id = record["id"]
    assert table.delete(record_id) == record_id


def test_table_delete_error(table):
    """Test error handling for deleting a non-existent record."""
    with pytest.raises(RecordNotFoundError):
        table.delete("non_existent_id")


def test_table_get(table, record):
    """Test retrieving a record by ID."""
    record_id = record["id"]
    assert table.get(record_id) == record


def test_table_get_error(table):
    """Test error handling for retrieving a non-existent record."""
    with pytest.raises(RecordNotFoundError):
        table.get("non_existent_id")


def test_table_get_all(table, record):
    """Test retrieving all records from a table."""
    assert table.get_all() == [record]


def test_table_match(table, record):
    """Test retrieving records matching specific criteria."""
    assert table.match({"name": "test"}) == [record]


def test_table_update(table, record):
    """Test updating an existing record."""
    updated_record = table.update({"id": record["id"], "fields": {"name": "updated"}})
    assert updated_record["fields"]["name"] == "updated"


def test_table_update_error(table):
    """Test error handling for updating a non-existent record."""
    with pytest.raises(RecordNotFoundError):
        table.update({"id": "non_existent_id", "fields": {"name": "updated"}})


def test_base_table_create_many(table):
    """Test creating multiple records in a single call."""
    count = 2
    records = table.create_many([{"name": "test1"}, {"name": "test2"}])
    assert len(records) == count


def test_base_table_delete_many(table, record):
    """Test deleting multiple records by ID."""
    record_id = record["id"]
    assert table.delete_many([record_id]) == [record_id]


def test_base_table_get_many(table, record):
    """Test retrieving multiple records by ID."""
    record_id = record["id"]
    assert table.get_many([record_id]) == [record]


def test_base_table_update_many(table, record):
    """Test updating multiple records in a single call."""
    updated_records = table.update_many([{"id": record["id"], "fields": {"name": "updated"}}])
    assert updated_records[0]["fields"]["name"] == "updated"
