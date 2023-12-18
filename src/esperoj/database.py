"""Module contain Database class."""

import uuid


class Table:
    def __init__(self, name):
        self.name = name
        self.records = []

    def all(self):
        return self.records

    def create(self, fields):
        record = {"id": str(uuid.uuid4()), "fields": fields}
        self.records.append(record)
        return record

    def get(self, record_id):
        return next((record for record in self.records if record["id"] == record_id), None)


class Database:
    def __init__(self):
        pass

    def table(self, name: str):
        """Return the table."""
        return Table(name=name)

    def close(self):
        pass
