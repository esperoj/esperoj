from uuid import UUID

import pytest

from esperoj.database.memory import MemoryDatabase, MemoryRecord, MemoryTable
from esperoj.exceptions import RecordNotFoundError


def test_memory_database_creation(memory_db):
    assert isinstance(memory_db, MemoryDatabase)
    assert memory_db.name == "test_db"


def test_memory_table_creation(memory_db, memory_table):
    name = memory_table.name
    assert isinstance(memory_table, MemoryTable)
    assert name == "test_table"
    assert memory_db.table(name) is memory_table


@pytest.mark.parametrize(
    "index", range(2)
)
def test_memory_record_creation(memory_table, mock_memory_records, index):
    fields = mock_memory_records[index].fields
    record = memory_table.create(fields)
    assert isinstance(record, MemoryRecord)
    assert isinstance(record.record_id, UUID)
    assert record.fields == fields

@pytest.mark.parametrize(
    ("fields", "update_fields", "expected_fields"),
    [
        ({"name": "John", "age": 30}, {"age": 31}, {"name": "John", "age": 31}),
        (
            {"name": "Jane", "age": 28, "city": "New York"},
            {"city": "Los Angeles"},
            {"name": "Jane", "age": 28, "city": "Los Angeles"},
        ),
    ],
)
def test_memory_record_update(memory_table, fields, update_fields, expected_fields):
    record = memory_table.create(fields)
    updated_record = record.update(update_fields)
    assert isinstance(updated_record, MemoryRecord)
    assert updated_record.fields == expected_fields
def test_memory_table_delete(memory_table):
    fields = {"name": "John", "age": 30}
    record = memory_table.create(fields)
    deleted_id = memory_table.delete(record.record_id)
    assert deleted_id == record.record_id
    with pytest.raises(RecordNotFoundError):
        memory_table.get(record.record_id)


def test_memory_table_delete_not_found(memory_table):
    with pytest.raises(RecordNotFoundError):
        memory_table.delete(UUID(int=0))




def test_memory_table_get(memory_table):
    fields = {"name": "John", "age": 30}
    record = memory_table.create(fields)
    fetched_record = memory_table.get(record.record_id)
    assert fetched_record == record


def test_memory_table_get_not_found(memory_table):
    with pytest.raises(RecordNotFoundError):
        memory_table.get(UUID(int=0))
@pytest.mark.parametrize(
        ("records", "formulas", "expected_records"),
    [
        (
            [{"name": "John", "age": 30}, {"name": "Jane", "age": 28}],
            {"age": 30},
            [{"name": "John", "age": 30}],
        ),
        ([{"name": "John", "age": 30}], None, [{"name": "John", "age": 30}]),
        ([{"name": "John", "age": 30}, {"name": "Jane", "age": 28}], {"age": 40}, []),
    ],
)
def test_memory_table_get_all(memory_table, records, formulas, expected_records):
    if formulas:
        fetched_records = list(memory_table.get_all(formulas))
    else:
        fetched_records = list(memory_table.get_all())
    assert len(fetched_records) == len(expected_records)
    for record in fetched_records:
        assert record.fields in expected_records




def test_memory_table_update(memory_table):
    fields = {"name": "John", "age": 30}
    update_fields = {"age": 31}
    record = memory_table.create(fields)
    updated_record = memory_table.update(record.record_id, update_fields)
    assert isinstance(updated_record, MemoryRecord)
    assert updated_record.fields == {**fields, **update_fields}


def test_memory_table_update_not_found(memory_table):
    with pytest.raises(RecordNotFoundError):
        memory_table.update(UUID(int=0), {"age": 31})


def test_memory_database_memory_table(memory_db):
    memory_table1 = memory_db.table("memory_table1")
    memory_table2 = memory_db.table("memory_table2")
    assert memory_table1 != memory_table2
    assert memory_table1.name == "memory_table1"
    assert memory_table2.name == "memory_table2"


def test_memory_database_close(memory_db):
    memory_table = memory_db.table("test_memory_table")
    memory_table.create({"name": "John", "age": 30})
    assert memory_db.close()
    assert not memory_db.tables
