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
    """Represents a record in a Seatable table."""

    def __init__(self, record_id: RecordId, fields: Fields, table: "SeatableTable"):
        """Initializes a SeatableRecord instance.

        Args:
            record_id (RecordId): The unique identifier of the record.
            fields (Fields): A dictionary mapping field keys to field values.
            table (SeatableTable): The table that the record belongs to.
        """
        super().__init__(record_id, fields, table)


class SeatableTable(Table):
    """Represents a table in a Seatable database."""

    def __init__(self, name: str, database: "SeatableDatabase"):
        """Initializes a SeatableTable instance.

        Args:
            name (str): The name of the table.
            database (SeatableDatabase): The database that the table belongs to.
        """
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
        """Converts a dictionary representing a record to a SeatableRecord instance.

        Args:
            record_dict (dict[FieldKey, FieldValue]): A dictionary representing a record.

        Returns:
            SeatableRecord: A SeatableRecord instance representing the record.
        """
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
        """Updates the links for a list of records.

        Args:
            records (list[Record]): A list of records to update the links for.

        Returns:
            bool: True if all links were updated successfully, False otherwise.
        """
        links = {key: {} for key in self.links}
        for record in records:
            for key, value in record.fields.items():
                if key in links:
                    links[key][record.record_id] = value
        return all(
            self.batch_update_links(key, value) for key, value in links.items() if value != {}
        )

    def batch_create(self, fields_list: list[Fields]) -> list[Record]:
        """Creates multiple records in the table.

        Args:
            fields_list (list[Fields]): A list of dictionaries representing the fields for the new records.

        Returns:
            list[Record]: A list of SeatableRecord instances representing the created records.
        """
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
        """Deletes multiple records from the table.

        Args:
            record_ids (list[RecordId]): A list of record identifiers to delete.

        Returns:
            bool: True if all records were deleted successfully, False otherwise.
        """
        for chunk in [record_ids[i : i + 1000] for i in range(0, len(record_ids), 1000)]:
            if self.client.batch_delete_rows(self.name, chunk)["deleted_rows"] != len(chunk):
                raise RuntimeError("Failed to delete all records")
        return True

    def batch_get(self, record_ids: list[RecordId]) -> list[Record]:
        """Retrieves multiple records from the table.

        Args:
            record_ids (list[RecordId]): A list of record identifiers to retrieve.

        Returns:
            list[Record]: A list of SeatableRecord instances representing the retrieved records.
        """
        query = f"""SELECT * from `{self.name}` WHERE `_id` IN ({','.join([f"'{record_id}'" for record_id in record_ids])})"""
        return [self._record_from_dict(record) for record in self.client.query(query)]

    def batch_get_link_id(self, field_keys: list[FieldKey]) -> dict[FieldKey, str]:
        """Retrieves the link identifiers for the given field keys.

        Args:
            field_keys (list[FieldKey]): A list of field keys to retrieve the link identifiers for.

        Returns:
            dict[FieldKey, str]: A dictionary mapping field keys to their corresponding link identifiers.
        """
        return {key: self.links[key]["link_id"] for key in field_keys}

    def batch_update(self, records: list[tuple[RecordId, Fields]]) -> list[Record]:
        """Updates multiple records in the table.

        Args:
            records (list[tuple[RecordId, Fields]]): A list of tuples containing record identifiers and dictionaries of fields to update.

        Returns:
            list[Record]: A list of SeatableRecord instances representing the updated records.
        """
        results = []
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
            results += chunk_records
        return results

    def batch_update_links(
        self,
        field_key: FieldKey,
        record_ids_map: dict[RecordId, list[RecordId]],
    ) -> bool:
        """Updates the links between records in the table and records in another table.

        Args:
            field_key (FieldKey): The key of the field representing the link.
            record_ids_map (dict[RecordId, list[RecordId]]): A dictionary mapping record identifiers in this table to lists of record identifiers in the linked table.

        Returns:
            bool: True if all links were updated successfully, False otherwise.
        """
        link_id = self.get_link_id(field_key)
        other_table_id = self.links[field_key]["other_table_id"]
        return self.client.batch_update_links(
            link_id, self.name, other_table_id, list(record_ids_map.keys()), record_ids_map
        )["success"]

    def get_linked_records(
        self, field_key: FieldKey, record_ids: list[RecordId]
    ) -> dict[RecordId, list[RecordId]]:
        """Retrieves the records linked to the given records through the specified field.

        Args:
            field_key (FieldKey): The key of the field representing the link.
            record_ids (list[RecordId]): A list of record identifiers to retrieve the linked records for.

        Returns:
            dict[RecordId, list[RecordId]]: A dictionary mapping record identifiers to lists of linked record identifiers.
        """
        return {
            record_id: [item["row_id"] for item in record_ids]
            for record_id, record_ids in self.client.get_linked_records(
                self.links[field_key]["table_id"],
                self.links[field_key]["key"],
                [{"row_id": item} for item in record_ids],
            ).items()
        }

    def query(self, query: str) -> list[Record]:
        """Executes a query on the table and returns the resulting records.

        Args:
            query (str): The query string.

        Returns:
            list[Record]: A list of SeatableRecord instances representing the resulting records.

        Example:
            table.query("$[/@.name][?name='Esperoj']")
            table.query("$[/@._id][*]")
        """
        data = self.client.query(f"SELECT * FROM `{self.name}` LIMIT 10000")
        jsonpath_expr = parse(query)
        return [self._record_from_dict(matched.value) for matched in jsonpath_expr.find(data)]


class SeatableDatabase(Database):
    """Represents a Seatable database."""

    def __init__(self, config: dict[Any, Any]):
        """Initializes a SeatableDatabase instance.

        Args:
            config (dict[Any, Any]): A dictionary containing the configuration for the database.
        """
        self.config = config
        self.name = config["name"]
        self.aliases = config["aliases"] or []
        self.server_url = config.get("server_url", "") or "https://cloud.seatable.io"
        self.api_token = config.get("api_token", "") or os.getenv("SEATABLE_API_TOKEN")
        self.client = Base(self.api_token, self.server_url)
        self.client.auth()
        self.metadata = self.client.get_metadata()

    def create_table(self, name: str) -> SeatableTable:
        """Creates a new table in the database.

        Args:
            name (str): The name of the new table.

        Returns:
            SeatableTable: A SeatableTable instance representing the new table.
        """
        return SeatableTable(name, self)

    def get_table(self, name: str) -> SeatableTable:
        """Retrieves an existing table from the database.

        Args:
            name (str): The name of the table to retrieve.

        Returns:
            SeatableTable: A SeatableTable instance representing the retrieved table.
        """
        return SeatableTable(name, self)
