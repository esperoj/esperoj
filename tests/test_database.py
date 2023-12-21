from collections.abc import Iterable
from uuid import UUID

import pytest

from esperoj.database.memory import MemoryDatabase, MemoryRecord, MemoryTable
from esperoj.exceptions import RecordNotFoundError


@pytest.fixture()
def db():
    return MemoryDatabase("test_db")

@pytest.fixture()
def table(db):
    return db.table("test_table")

def test_memory_database_creation(db):
    assert isinstance(db, MemoryDatabase)
    assert db.name == "test_db"

def test_memory_table_creation(table):
    assert isinstance(table, MemoryTable)
    assert table.name == "test_table"

@pytest.mark.parametrize("fields", [
    {"name": "John", "age": 30},
    {"name": "Jane", "age": 28, "city": "New York"},
])
def test_memory_record_creation(table, fields):
    record = table.create(fields)
    assert isinstance(record, MemoryRecord)
    assert isinstance(record.record_id, UUID)
    assert record.fields == fields

@pytest.mark.parametrize("fields, update_fields, expected_fields", [
    ({"name": "John", "age": 30}, {"age": 31}, {"name": "John", "age": 31}),
    ({"name": "Jane", "age": 28, "city": "New York"}, {"city": "Los Angeles"}, {"name": "Jane", "age": 28, "city": "Los Angeles"}),
])
def test_memory_record_update(table, fields, update_fields, expected_fields):
    record = table.create(fields)
    updated_record = record.update(update_fields)
    assert isinstance(updated_record, MemoryRecord)
    assert updated_record.fields == expected_fields

def test_memory_table_get(table):
    fields = {"name": "John", "age": 30}
    record = table.create(fields)
    fetched_record = table.get(record.record_id)
    assert fetched_record == record

def test_memory_table_get_not_found(table):
    with pytest.raises(RecordNotFoundError):
        table.get(UUID(int=0))

def test_memory_table_delete(table):
    fields = {"name": "John", "age": 30}
    record = table.create(fields)
    deleted_id = table.delete(record.record_id)
    assert deleted_id == record.record_id
    with pytest.raises(RecordNotFoundError):
        table.get(record.record_id)

def test_memory_table_delete_not_found(table):
    with pytest.raises(RecordNotFoundError):
        table.delete(UUID(int=0))

@pytest.mark.parametrize("records, formulas, expected_records", [
    ([{"name": "John", "age": 30}, {"name": "Jane", "age": 28}], {"age": 30}, [{"name": "John", "age": 30}]),
    ([{"name": "John", "age": 30}, {"name": "Jane", "age": 28}], {"age": 40}, []),
])
def test_memory_table_get_all(table, records, formulas, expected_records):
    created_records = [table.create(record) for record in records]
    fetched_records = list(table.get_all(formulas))
    assert len(fetched_records) == len(expected_records)
    for record in fetched_records:
        assert record.fields in expected_records

def test_memory_table_update(table):
    fields = {"name": "John", "age": 30}
    update_fields = {"age": 31}
    record = table.create(fields)
    updated_record = table.update(record.record_id, update_fields)
    assert isinstance(updated_record, MemoryRecord)
    assert updated_record.fields == {**fields, **update_fields}

def test_memory_table_update_not_found(table):
    with pytest.raises(RecordNotFoundError):
        table.update(UUID(int=0), {"age": 31})

def test_memory_database_table(db):
    table1 = db.table("table1")
    table2 = db.table("table2")
    assert table1 != table2
    assert table1.name == "table1"
    assert table2.name == "table2"

def test_memory_database_close(db):
    table = db.table("test_table")
    table.create({"name": "John", "age": 30})
    assert db.close()
    assert not db.tables
