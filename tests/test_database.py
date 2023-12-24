"""Test memory database."""
from uuid import UUID

import pytest

from esperoj.database.memory import MemoryDatabase, MemoryRecord, MemoryTable
from esperoj.exceptions import RecordNotFoundError


@pytest.fixture()
def sample_memory_table() -> MemoryTable:
    """Sample memory table with data to work with."""
    db = MemoryDatabase(name="TestDB")
    table = db.table(name="TestTable")
    table.create({"name": "Alice", "age": 30})
    table.create({"name": "Charlie", "age": 22})
    table.create({"name": "Bob", "age": 25})
    return table


def test_memory_database_creation(memory_db):
    """Test the creation of a MemoryDatabase instance."""
    assert isinstance(memory_db, MemoryDatabase)
    assert memory_db.name == "test_db"


def test_memory_table_creation(memory_db, memory_table):
    """Test the creation of a MemoryTable instance."""
    name = memory_table.name
    assert isinstance(memory_table, MemoryTable)
    assert name == "Files"
    assert memory_db.table(name) is memory_table


@pytest.mark.parametrize("fields", [{"Name": "Esperoj"}, {"Age": 12}])
def test_memory_record_creation(memory_table, fields):
    """Test the creation of a MemoryRecord instance."""
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
    """Test updating fields of a MemoryRecord instance."""
    record = memory_table.create(fields)
    updated_record = record.update(update_fields)
    assert isinstance(updated_record, MemoryRecord)
    assert updated_record.fields == expected_fields


def test_memory_table_delete(memory_table):
    """Test deleting a record from a MemoryTable instance."""
    fields = {"name": "John", "age": 30}
    record = memory_table.create(fields)
    deleted_id = memory_table.delete(record.record_id)
    assert deleted_id == record.record_id
    with pytest.raises(RecordNotFoundError):
        memory_table.get(record.record_id)


def test_memory_table_delete_not_found(memory_table):
    """Test attempting to delete a non-existent record from a MemoryTable instance."""
    with pytest.raises(RecordNotFoundError):
        memory_table.delete(UUID(int=0))


def test_memory_table_get(memory_table):
    """Test fetching a record from a MemoryTable instance."""
    fields = {"name": "John", "age": 30}
    record = memory_table.create(fields)
    fetched_record = memory_table.get(record.record_id)
    assert fetched_record == record


def test_memory_table_get_not_found(memory_table):
    """Test attempting to fetch a non-existent record from a MemoryTable instance."""
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
    """Test fetching all records from a MemoryTable instance based on given formulas."""
    list(memory_table.create_many(records))
    fetched_records = list(memory_table.get_all(formulas))
    assert len(fetched_records) == len(expected_records)
    assert all(record.fields in expected_records for record in fetched_records)


def test_get_all_sort_numeric_ascending(sample_memory_table):
    """Test sorting records by a numeric field in ascending order."""
    sorted_records = list(sample_memory_table.get_all(sort=["age"]))
    ages = [record.fields["age"] for record in sorted_records]
    assert ages == [22, 25, 30], "Records should be sorted by age in ascending order"


def test_get_all_sort_numeric_descending(sample_memory_table):
    """Test sorting records by a numeric field in descending order."""
    sorted_records = list(sample_memory_table.get_all(sort=["-age"]))
    ages = [record.fields["age"] for record in sorted_records]
    assert ages == [30, 25, 22], "Records should be sorted by age in descending order"


def test_get_all_sort_string_ascending(sample_memory_table):
    """Test sorting records by a string field in ascending order."""
    sorted_records = list(sample_memory_table.get_all(sort=["name"]))
    names = [record.fields["name"] for record in sorted_records]
    assert names == [
        "Alice",
        "Bob",
        "Charlie",
    ], "Records should be sorted by name in ascending order"


def test_get_all_sort_string_descending(sample_memory_table):
    """Test sorting records by a string field in descending order."""
    sorted_records = list(sample_memory_table.get_all(sort=["-name"]))
    names = [record.fields["name"] for record in sorted_records]
    assert names == [
        "Charlie",
        "Bob",
        "Alice",
    ], "Records should be sorted by name in descending order"


def test_memory_table_update(memory_table):
    """Test updating a record in a MemoryTable instance."""
    fields = {"name": "John", "age": 30}
    update_fields = {"age": 31}
    record = memory_table.create(fields)
    updated_record = memory_table.update(record.record_id, update_fields)
    assert isinstance(updated_record, MemoryRecord)
    assert updated_record.fields == fields | update_fields


def test_memory_table_update_not_found(memory_table):
    """Test attempting to update a non-existent record in a MemoryTable instance."""
    with pytest.raises(RecordNotFoundError):
        memory_table.update(UUID(int=0), {"age": 31})


def test_memory_database_memory_table(memory_db):
    """Test creating multiple MemoryTable instances in a MemoryDatabase."""
    memory_table1 = memory_db.table("memory_table1")
    memory_table2 = memory_db.table("memory_table2")
    assert memory_table1 != memory_table2
    assert memory_table1.name == "memory_table1"
    assert memory_table2.name == "memory_table2"


def test_memory_database_close(memory_db):
    """Test closing a MemoryDatabase instance."""
    memory_table = memory_db.table("test_memory_table")
    memory_table.create({"name": "John", "age": 30})
    assert memory_db.close()
    assert not memory_db.tables


def test_create_many(memory_table):
    """Test creating multiple records in a MemoryTable instance."""
    records_to_create = [{"name": "Alice"}, {"name": "Bob"}]
    created_records = memory_table.create_many(records_to_create)
    assert len(list(created_records)) == len(records_to_create)
    assert all(isinstance(record, MemoryRecord) for record in created_records)


def test_delete_many(memory_table):
    """Test deleting multiple records from a MemoryTable instance."""
    records_to_create = [{"name": "Alice"}, {"name": "Bob"}]
    created_records = list(memory_table.create_many(records_to_create))
    record_ids = [record.record_id for record in created_records]
    deleted_record_ids = memory_table.delete_many(record_ids)
    assert list(deleted_record_ids) == record_ids
    assert not list(memory_table.get_all())


def test_get_many(memory_table):
    """Test fetching multiple records from a MemoryTable instance."""
    records_to_create = [{"name": "Alice"}, {"name": "Bob"}]
    created_records = list(memory_table.create_many(records_to_create))
    record_ids = [record.record_id for record in created_records]
    retrieved_records = list(memory_table.get_many(record_ids))
    assert retrieved_records == created_records
    assert len(list(retrieved_records)) == len(records_to_create)


def test_update_many(memory_table):
    """Test updating multiple records in a MemoryTable instance."""
    records_to_create = [{"name": "Alice"}, {"name": "Bob"}]
    created_records = list(memory_table.create_many(records_to_create))
    updates = [
        {"record_id": record.record_id, "fields": {"name": "Updated"}} for record in created_records
    ]
    updated_records = list(memory_table.update_many(updates))
    assert len(list(updated_records)) == len(updates)
    assert updated_records[0].fields["name"] == "Updated"
    assert updated_records[1].fields["name"] == "Updated"
