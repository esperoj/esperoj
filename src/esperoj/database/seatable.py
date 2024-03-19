"""Module contains Seatable class."""

import os
import uuid
from typing import Any

from jsonpath_ng.ext import parse
from seatable_api import Base

from esperoj.database.database import (
    Database,
    FieldKey,
    Fields,
    FieldValue,
    Record,
    RecordId,
    Table,
)


class SeatableRecord(Record):
    pass


class SeatableTable(Table):
    def __init__(self, name: str, database: "SeatableDatabase"):
        self.name = name
        self.database = database
        self.client = database.client
        self.metadata = next(
            (table for table in database.metadata["tables"] if table["name"] == self.name), {}
        )
        self.links = next(
            (
                {link["name"]: link["data"] | {"key": link["key"]}}
                for link in self.metadata["columns"]
                if link["type"] == "link"
            ),
            {},
        )

    def _record_from_dict(self, record_dict: dict[FieldKey, FieldValue]) -> Record:
        record_id = record_dict["_id"]
        fields = {}
        for key, value in record_dict.items():
            if not key.startswith("_"):
                fields[key] = value
            if key in self.links:
                fields[key] = [
                    item["row_id"] if isinstance(item, dict) else item for item in record_dict[key]
                ]
        return SeatableRecord(record_id=record_id, fields=fields, table=self)

    def _update_links(self, records: list[Record]) -> bool:
        links = {key: {} for key in self.links}
        for record in records:
            for key, value in record.fields.items():
                if key in links:
                    links[key][record.record_id] = value
        return all(
            self.batch_update_links(key, value) for key, value in links.items() if value != {}
        )

    def batch_create(self, fields_list: list[Fields]) -> list[Record]:
        # BUG: The batch_delete yield error for different id format.
        records = []
        for chunk in [fields_list[i : i + 1000] for i in range(0, len(fields_list), 1000)]:
            chunk_fields = [{**{"_id": str(uuid.uuid4())[:22]}, **fields} for fields in chunk]
            chunk_records = [self._record_from_dict(fields) for fields in chunk_fields]
            if self.client.batch_append_rows(self.name, chunk_fields)["inserted_row_count"] != len(
                chunk_fields
            ):
                raise RuntimeError("Failed to create all rows")
            if not self._update_links(chunk_records):
                raise RuntimeError("Failed to link all records")
            records.extend(chunk_records)
        return records

    def batch_delete(self, record_ids: list[RecordId]) -> bool:
        """Delete the records with the given record_ids."""
        for chunk in [record_ids[i : i + 1000] for i in range(0, len(record_ids), 1000)]:
            if self.client.batch_delete_rows(self.name, chunk)["deleted_rows"] != len(chunk):
                raise RuntimeError("Failed to delete all records")
        return True

    def batch_get(self, record_ids: list[RecordId]) -> list[Record]:
        """Get the records with the given record_ids."""
        query = f"""SELECT * from `{self.name}` WHERE `_id` IN ({','.join([f"'{record_id}'" for record_id in record_ids])})"""
        return [self._record_from_dict(record) for record in self.client.query(query)]

    def batch_get_link_id(self, field_keys: list[FieldKey]) -> dict[FieldKey, str]:
        """Get the link ids for the given field keys."""
        return {key: self.links[key]["link_id"] for key in field_keys}

    def batch_update(self, records: list[tuple[RecordId, Fields]]) -> bool:
        """Update the records with the given record_ids with the given fields."""
        for chunk in [records[i : i + 1000] for i in range(0, len(records), 1000)]:
            chunk_records = [
                self._record_from_dict({"_id": record_id, **fields}) for record_id, fields in chunk
            ]
            if not self._update_links(chunk_records):
                raise RuntimeError("Failed to link all records")
            if not self.client.batch_update_rows(
                self.name, [{"row_id": record_id, "row": fields} for record_id, fields in chunk]
            )["success"]:
                raise RuntimeError("Failed to update all records")
        return True

    def batch_update_links(
        self,
        field_key: FieldKey,
        record_ids_map: dict[RecordId, list[RecordId]],
    ) -> bool:
        """Update the links between the records with the given record_ids and the records with the given other_record_ids in the other table."""
        link_id = self.get_link_id(field_key)
        other_table_id = self.links[field_key]["other_table_id"]
        return self.client.batch_update_links(
            link_id, self.name, other_table_id, list(record_ids_map.keys()), record_ids_map
        )["success"]

    def get_linked_records(
        self, field_key: FieldKey, record_ids: list[RecordId]
    ) -> dict[RecordId, list[RecordId]]:
        """Get the linked records for the given record_ids."""
        return {
            record_id: [item["row_id"] for item in record_ids]
            for record_id, record_ids in self.client.get_linked_records(
                self.links[field_key]["table_id"],
                self.links[field_key]["key"],
                [{"row_id": item} for item in record_ids],
            ).items()
        }

    def query(self, query: str) -> list[Record]:
        r"""Query the table with the given query.

        Example:
        table.query("$[\@.name][?name='Esperoj']")
        table.query("$[\@._id][*]")
        """
        data = self.client.query(f"SELECT * FROM `{self.name}` LIMIT 10000")
        jsonpath_expr = parse(query)
        return [self._record_from_dict(matched.value) for matched in jsonpath_expr.find(data)]


class SeatableDatabase(Database):
    def __init__(self, config: dict[Any, Any]):
        self.config = config
        self.name = config["name"]
        self.aliases = config["aliases"] or []
        self.server_url = config.get("server_url", "") or "https://cloud.seatable.io"
        self.api_token = config.get("api_token", "") or os.getenv("SEATABLE_API_TOKEN")
        self.client = Base(self.api_token, self.server_url)
        self.client.auth()
        self.metadata = self.client.get_metadata()

    def create_table(self, name: str) -> SeatableTable:
        return SeatableTable(name, self)

    def get_table(self, name: str) -> SeatableTable:
        return SeatableTable(name, self)

    def close(self) -> bool:
        """Close the database."""
        return True
