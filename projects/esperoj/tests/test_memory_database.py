import pytest
from typing import Any
from esperoj.database.database import ID, Fields, Record, Table


def test_batch_create(memory_db):
    records = memory_db.query("test")
    assert len(records) == 2
    assert records[0].name == "Alice"
    assert records[1].name == "Bob"

def test_batch_delete(memory_db):
    records = memory_db.query("test")
    result = memory_db.batch_delete("test", records[0].id)
    assert result is True
    remaining_records = memory_db.query("test")
    assert len(remaining_records) == 1
    assert remaining_records[0].name == "Bob"

def test_batch_get(memory_db):
    records = memory_db.query("test")
    records = memory_db.batch_get("test", [record.id for record in records])
    assert len(records) == 2
    assert records[0].name == "Alice"

def test_batch_update(memory_db):
    records = memory_db.query("test")
    updated_records = memory_db.batch_update("test", [(records[0].id, {"name": "Charlie"})])
    assert len(updated_records) == 1
    assert updated_records[0].name == "Charlie"

def test_batch_update_links(memory_db):
    files = memory_db.query("files")
    musics = memory_db.query("musics")
    result = memory_db.batch_update_links("files", "musics", {files[0].id: [musics[0].id]})
    assert result is True
    linked_records = memory_db.get_linked_records("files", "musics", [musics[0].id])
    assert linked_records[0].id == files[0].id
