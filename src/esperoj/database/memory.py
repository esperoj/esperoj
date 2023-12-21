from collections.abc import Iterable
from typing import Any, Self
from uuid import uuid4

from esperoj.database import Database, Record, Table
from esperoj.exceptions import RecordNotFoundError


class MemoryRecord(Record):
    def update(self, fields: dict[str, Any]) -> Self:
        self.fields.update(fields)
        return self

class MemoryTable(Table):
    def __init__(self, name: str):
        self.records: dict[Any, dict[str, Any]] = {}
        self.name = name

    def create(self, fields: dict[str, Any]) -> MemoryRecord:
        record_id = uuid4()
        self.records[record_id] = fields
        return MemoryRecord(record_id, fields)

    def delete(self, record_id: Any) -> Any:
        if self.records.pop(record_id, None): return record_id

    def get(self, record_id: Any) -> MemoryRecord:
        fields = self.records.get(record_id)
        if fields:
            return MemoryRecord(record_id, fields)
        raise RecordNotFoundError()

    def get_all(self, formulas: dict[str, Any] = {}) -> Iterable[MemoryRecord]:
        if not formulas:
            return [MemoryRecord(record_id, fields) for record_id, fields in self.records.items()]

        filtered_records: list[MemoryRecord] = []
        for record_id, fields in self.records.items():
            if all(fields.get(key) == value for key, value in formulas.items()):
                filtered_records.append(MemoryRecord(record_id, fields))
        return filtered_records

    def update(self, record_id: Any, fields: dict[str, Any]) -> MemoryRecord:
        record = self.get(record_id)
        record.fields.update(fields)
        return MemoryRecord(record_id, record.fields)

class MemoryDatabase(Database):
    def __init__(self, name: str):
        self.tables: dict[str, MemoryTable] = {}
        self.name = name

    def table(self, name: str) -> MemoryTable:
        if name not in self.tables:
            self.tables[name] = MemoryTable(name)
        return self.tables[name]

    def close(self) -> bool:
        self.tables.clear()
        return True
