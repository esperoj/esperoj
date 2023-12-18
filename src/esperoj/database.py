"""Module contain Database class."""

import uuid


class Table:
    """Table class."""

    def __init__(self, name):
        self.name = name
        self.records = []

    def get_all(self):
        """List all records."""
        return self.records

    def create(self, fields):
        """Create a new record."""
        record = {"id": str(uuid.uuid4()), "fields": fields}
        self.records.append(record)
        return record

    def get(self, record_id):
        """Get record data."""
        return next((record for record in self.records if record["id"] == record_id), None)


class Database:
    """Database class."""

    def __init__(self):
        pass

    def table(self, name: str):
        """Return the table."""
        return Table(name=name)

    def close(self):
        """Close the database."""
