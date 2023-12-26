import pytest
from src.esperoj.database.airtable import (Airtable, AirtableRecord,
                                           AirtableTable)

from tests.conftest import esperoj


def test_airtable_record_update(esperoj):
    record = AirtableRecord("test_id", {"field": "value"}, esperoj)
    updated_record = record.update({"field": "new_value"})
    assert updated_record.fields["field"] == "new_value"

def test_airtable_table_init(esperoj):
    table = AirtableTable("test_table", esperoj)
    assert table.name == "test_table"

def test_airtable_table_record_from_dict(esperoj):
    table = AirtableTable("test_table", esperoj)
    record = table._record_from_dict({"id": "test_id", "fields": {"field": "value"}})
    assert record.record_id == "test_id"
    assert record.fields["field"] == "value"

# Continue writing test functions for the rest of the methods in AirtableTable and Airtable...

def test_airtable_init(esperoj):
    airtable = Airtable("test_db")
    assert airtable.name == "test_db"

def test_airtable_table(esperoj):
    airtable = Airtable("test_db")
    table = airtable.table("test_table")
    assert table.name == "test_table"

def test_airtable_close(esperoj):
    airtable = Airtable("test_db")
    assert airtable.close() is True
