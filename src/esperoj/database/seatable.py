"""Module contains Seatable class."""
import uuid
import os
from collections.abc import Iterator
from typing import Any, Self
from seatable_api import Base, context
from esperoj.database.database import Database, Record, RecordId, FieldKey, FieldValue, Fields, Table
from esperoj.exceptions import InvalidRecordError, RecordDeletionError, RecordNotFoundError

class SeatableRecord(Record):
    table: "SeatableTable"

class SeatableTable(Table):
    def __init__(self, name: str, database: "SeatableDatabase"):
        self.name = name
        self.database = database
        self.client = database.client

    def _record_from_dict(self, record_dict: dict[str, Any]) -> SeatableRecord:
        record_id = record_dict.pop("_id")
        for key in list(record_dict.keys()):
            if key.startswith("_"):
                record_dict.pop(key)
        return SeatableRecord(record_id=record_id, fields=record_dict, table=self)

    def batch_create(self, fields_list: list[Fields]) -> list[Record]:
        fields_list = [{"_id": str(uuid.uuid4())[:22]} | fields for fields in fields_list]
        if self.client.batch_append_rows(self.name, fields_list)["inserted_row_count"] is not len(fields_list):
            raise RuntimeError("Failed to insert all rows")
        return [self._record_from_dict(fields) for fields in fields_list]

    def batch_delete(self, record_ids: list[RecordId]) -> list[RecordId]:
        """Delete the records with the given record_ids."""
        return self.client.batch_delete_rows(self.name, record_ids)

    def batch_get(self, record_ids: list[RecordId]) -> dict[RecordId, Record]:
        """Get the records with the given record_ids."""

    def batch_get_link_id(self, field_keys: list[FieldKey]) -> dict[FieldKey, str]:
        """Get the link ids for the given field keys."""

    def batch_update(self, records: list[tuple[RecordId, Fields]]) -> dict[RecordId, Record]:
        """Update the records with the given record_ids with the given fields."""

    def batch_update_links(
        self,
        field_key: FieldKey,
        other_table_name: str,
        record_ids_map: dict[RecordId, list[RecordId]],
    ):
        """Update the links between the records with the given record_ids and the records with the given other_record_ids in the other table."""

    def get_linked_records(
        self, field_key: FieldKey, record_ids: list[RecordId]
    ) -> dict[RecordId, list[RecordId]]:
        """Get the linked records for the given record_ids."""

    def query(self, query: str, params: tuple = ()) -> list[Record]:
        """Query the table with the given query."""
        return self.client.query("select * from demo")

class SeatableDatabase(Database):

    def __init__(self, config: dict[Any, Any]):
        self.name = config["name"]
        self.config = config
        self.server_url = config.get("server_url", "") or context.server_url or "https://cloud.seatable.io"
        self.api_token = config.get("api_token", "") or context.api_token or os.getenv("SEATABLE_API_TOKEN")
        self.client = Base(self.api_token, self.server_url)
        self.client.auth()

    def create_table(self, name: str) -> SeatableTable:
        return SeatableTable(name, self)

    def close(self) -> bool:
        """Close the database."""
        return