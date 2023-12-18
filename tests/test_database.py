"""Test database module."""


def test_get(table):
    """Create a record and test if we can get it."""
    fields = {"name": "esperoj"}
    record = table.create(fields)
    assert table.get(record["id"]) == record
