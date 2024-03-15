"""Module contains Seatable class."""
import os
import uuid
from collections.abc import Iterator
from typing import Any, Self
from operator import itemgetter
from seatable_api import Base, context

from esperoj.database.database import (
    Database,
    FieldKey,
    Fields,
    FieldValue,
    Record,
    RecordId,
    Table,
)
from esperoj.exceptions import InvalidRecordError, RecordDeletionError, RecordNotFoundError


class SeatableRecord(Record):
    table: "SeatableTable"

class SeatableTable(Table):
    def __init__(self, name: str, database: "SeatableDatabase"):
        self.name = name
        self.database = database
        self.client = database.client
        self.metadata = next((table for table in database.metadata["tables"] if table["name"] == self.name), {})
        self.links = next(({link["name"]: link["data"]} for link in self.metadata["columns"] if link["type"] == "link"), {})

    def _record_from_dict(self, record_dict: dict[str, Any]) -> SeatableRecord:
        record_id = record_dict.pop("_id")
        for key in list(record_dict.keys()):
            if key.startswith("_"):
                record_dict.pop(key)
            if key in self.links:
                record_dict[key] = [item["row_id"] if isinstance(item, dict) else item for item in record_dict[key]]
        return SeatableRecord(record_id=record_id, fields=record_dict, table=self)

    def _update_links_from_fields_list(self, fields_list: list[Fields]) -> bool:
        links = {key: {} for key in self.links}
        for fields in fields_list:
            for key, value in fields.items():
                if key in links:
                    links[key][fields["_id"]] = value
        return all(self.batch_update_links(key, value) for key, value in links.items() if value != {})

    def batch_create(self, fields_list: list[Fields]) -> list[Record]:
        # BUG: The batch_delete yield error for different id format.
        fields_list = [{"_id": str(uuid.uuid4())[:22]} | fields for fields in fields_list]
        if self.client.batch_append_rows(self.name, fields_list)["inserted_row_count"] is not len(fields_list):
            raise RuntimeError("Failed to create all rows")
        if not self._update_links_from_fields_list(fields_list):
            raise RuntimeError("Failed to link all records")
        return [self._record_from_dict(fields) for fields in fields_list]

    def batch_delete(self, record_ids: list[RecordId]) -> list[RecordId]:
        """Delete the records with the given record_ids."""
        if self.client.batch_delete_rows(self.name, record_ids)["deleted_rows"] is not len(record_ids):
            raise RuntimeError("Failed to delete all records")
        return record_ids


    def batch_get(self, record_ids: list[RecordId]) -> list[Record]:
        """Get the records with the given record_ids."""
        query = f"""SELECT * from `{self.name}` WHERE `_id` IN ({','.join([f"'{record_id}'" for record_id in record_ids])})"""
        return [self._record_from_dict(record) for record in self.client.query(query)]

    def batch_get_link_id(self, field_keys: list[FieldKey]) -> dict[FieldKey, str]:
        """Get the link ids for the given field keys."""
        return {key : self.links[key]["link_id"] for key in field_keys}

    def batch_update(self, records: list[tuple[RecordId, Fields]]) -> dict[RecordId, Record]:
        """Update the records with the given record_ids with the given fields."""

    def batch_update_links(
        self,
        field_key: FieldKey,
        record_ids_map: dict[RecordId, list[RecordId]],
    ) -> bool:
        """Update the links between the records with the given record_ids and the records with the given other_record_ids in the other table."""
        link_id = self.get_link_id(field_key)
        other_table_id = self.links[field_key]["other_table_id"]
        return self.client.batch_update_links(link_id, self.name, other_table_id, list(record_ids_map.keys()), record_ids_map)["success"]

    def get_linked_records(
        self, field_key: FieldKey, record_ids: list[RecordId]
    ) -> dict[RecordId, list[RecordId]]:
        """Get the linked records for the given record_ids."""

    def query(self, query: str, params: tuple = ()) -> list[Record]:
        """Query the table with the given query."""
        return [self._record_from_dict(record) for record in self.client.query("select * from demo")]

class SeatableDatabase(Database):

    def __init__(self, config: dict[Any, Any]):
        self.name = config["name"]
        self.config = config
        self.server_url = config.get("server_url", "") or context.server_url or "https://cloud.seatable.io"
        self.api_token = config.get("api_token", "") or context.api_token or os.getenv("SEATABLE_API_TOKEN")
        self.client = Base(self.api_token, self.server_url)
        self.client.auth()
        self.metadata = self.client.get_metadata()

    def create_table(self, name: str) -> SeatableTable:
        return SeatableTable(name, self)

    def close(self) -> bool:
        """Close the database."""
        return
