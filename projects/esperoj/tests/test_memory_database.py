import pytest

from esperoj.database.database import Table


def test_batch_create(memory_db):
    records = memory_db.query("test")
    assert len(records) == 2
    assert records[0].name == "Alice"
    assert records[1].name == "Bob"


def test_batch_delete(memory_db):
    records = memory_db.query("test")
    result = memory_db.batch_delete("test", [records[0].id, records[1].id])
    assert result is True
    remaining_records = memory_db.query("test")
    assert len(remaining_records) == 0


def test_batch_delete_non_existent(memory_db):
    result = memory_db.batch_delete("test", ["non_existent_id"])
    assert result is False


def test_batch_get(memory_db):
    records = memory_db.query("test")
    records = memory_db.batch_get("test", [record.id for record in records])
    assert len(records) == 2
    assert records[0].name == "Alice"


def test_batch_update(memory_db):
    records = memory_db.query("test")
    updates = [
        {"id": records[0].id, "name": "Charlie"},
        {"id": records[1].id, "age": 30},
        {"id": "non_existent_id", "name": "Invalid"},
    ]
    updated_records = memory_db.batch_update("test", updates)
    assert len(updated_records) == 2
    assert updated_records[0].name == "Charlie"
    assert updated_records[1].age == 30
    assert records[1].name == updated_records[1].name


def test_batch_update_links(memory_db):
    files = memory_db.query("files")
    musics = memory_db.query("musics")
    result = memory_db.batch_update_links("files", "musics", {files[0].id: [musics[0].id]})
    assert result is True
    linked_records = memory_db._get_linked_records("files", "musics", [files[0].id])
    assert linked_records[files[0].id][0] == musics[0].id


def test_query(memory_db):
    result = memory_db.query("test")
    assert len(result) == 2


def test_create_table(memory_db):
    with pytest.raises(ValueError):
        memory_db.create_table("test")


def test_get_table(memory_db):
    table = memory_db.get_table("files")
    assert isinstance(table, Table)
    with pytest.raises(ValueError):
        memory_db.get_table("non_existent_table")
