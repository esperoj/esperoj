"""Module contains Airtable class."""

import os
from collections.abc import Iterable
from dataclasses import dataclass
from typing import Any, Self

from pyairtable import Api
from pyairtable.formulas import match

from esperoj.database import Database, Record, Table
from esperoj.exceptions import InvalidRecordError, RecordNotFoundError


@dataclass
class AirtableRecord(Record):
    table: type["AirtableTable"]

    def update(self, fields: dict[str, Any]) -> Self:
        return self.table.update(self.record_id, fields)


class AirtableTable(Table):
    def __init__(self, name: str, client):
        self.name = name
        self.client = client

    def _record_from_dict(self, record_dict: dict[str, Any]) -> AirtableRecord:
        return AirtableRecord(record_dict["id"], record_dict["fields"], self)

    def create(self, fields: dict[str, Any]) -> dict[str, Any]:
        if not isinstance(fields, dict):
            raise InvalidRecordError("fields must be a dictionary.")
        return self._record_from_dict(self.client.create(fields))

    def create_many(self, records: Iterable[dict[str, Any]]) -> Iterable[AirtableRecord]:
        return (self._record_from_dict(record) for record in self.client.batch_create(records))

    def delete(self, record_id: str) -> str:
        if not isinstance(record_id, str):
            raise InvalidRecordError("record_id must be a string.")

        record = self.get(record_id)
        result = self.client.delete(record.record_id)
        if result["deleted"] is True:
            return record_id
        raise Exception(f"Deletion failed for id: {result['id']}")

    def delete_many(self, record_ids: Iterable[str]) -> Iterable[str]:
        results = self.client.batch_delete(record_ids)
        for result in results:
            if result["deleted"] is False:
                raise ValueError(f"Deletion failed for id: {result['id']}")
        return record_ids

    def get(self, record_id: str) -> AirtableRecord:
        if not isinstance(record_id, str):
            raise InvalidRecordError("record_id must be a string.")

        record = self.client.get(record_id)
        if record is None:
            raise RecordNotFoundError(f"Record with id '{record_id}' not found.")

        return self._record_from_dict(record)

    def get_all(self, formulas: dict[str, Any] | None = None) -> Iterable[AirtableRecord]:
        if formulas is not None:
            formulas = match(formulas)
        return (self._record_from_dict(record) for record in self.client.all(formula=formulas))

    def get_many(self, record_ids: Iterable[str]) -> Iterable[AirtableRecord]:
        return (self.get(record_id) for record_id in record_ids)

    def update(self, record_id: str, fields: dict[str, Any]) -> AirtableRecord:
        return self._record_from_dict(self.client.update(record_id, fields))

    def update_many(self, records: Iterable[dict[str, Any]]) -> Iterable[AirtableRecord]:
        return (
            self._record_from_dict(record)
            for record in self.client.batch_update(
                {"id": r["record_id"], "fields": r["fields"]} for r in records
            )
        )


class Airtable(Database):
    def __init__(self, name: str, api_key: str = "", base_id: str = ""):
        self.name = name
        self.api_key = api_key or str(os.getenv("AIRTABLE_API_KEY"))
        self.base_id = base_id or str(os.getenv("AIRTABLE_BASE_ID"))
        self.client = Api(self.api_key)

    def table(self, name: str) -> AirtableTable:
        return AirtableTable(name, self.client.table(self.base_id, name))

    def close(self) -> bool:
        return True
