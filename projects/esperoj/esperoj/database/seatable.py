"""Module contains SeatableDatabase class."""

import uuid
from typing import Any

from seatable_api import Base

from esperoj.database.database import (
    ID,
    Database,
    FieldKey,
    Fields,
    Record,
    Table,
)
from esperoj.database.query import Query


class SeatableDatabase(Database):
    """Represents a Seatable database."""

    def __init__(self, name: str, config: dict[Any, Any], models: dict[str, type[Record]] | None = None):
        """Initializes a SeatableDatabase instance.

        Args:
            name (str): The name of the database.
            config (dict[Any, Any]): Configuration for the Seatable database.
            models (dict[str, type[Record]] | None): Models for the database tables.
        """
        super().__init__(name, config, models)
        self.client = Base(config["api_token"], config["server_url"])
        self.client.auth()
        self.metadata = self.client.get_metadata()
        self.links = {}
        for table in self.metadata["tables"]:
            table_name = table["name"]
            table_links = {
                link["name"]: link["data"] | {"key": link["key"]} for link in table["columns"] if link["type"] == "link"
            }
            self.links[table_name] = table_links

    def _seatable_record_to_record(self, table_name: str, fields_list: list[dict[FieldKey, Any]]) -> list[Record]:
        """Converts dictionaries representing a record to a Record instance."""
        model_class = self.models.get(table_name, Record)
        records = []
        for fields in fields_list:
            record_fields = {"id": fields["_id"]}
            for key, value in fields.items():
                if not key.startswith("_"):
                    record_fields[key] = value
                if key in self.links[table_name]:
                    if isinstance(record_fields[key], list):
                        record_fields[key] = [
                            item["row_id"] if isinstance(item, dict) else item for item in record_fields[key]
                        ]
                    else:
                        record_fields[key] = []
            records.append(model_class(**record_fields))
        return records

    def _fields_to_seatable_record(
        self, table_name: str, fields_list: list[dict[FieldKey, Any]]
    ) -> list[dict[FieldKey, Any]]:
        """Converts dictionaries representing a record to a Record instance.

        Args:
            table_name (str): The name of the table.
            fields_list (list[dict[FieldKey, Any]]): A list of dictionaries representing a record.

        Returns:
            Records (list[Record]): Records instance representing the records.
        """
        model_class = self.models.get(table_name, Record)
        return [
            {
                **{key: value for key, value in fields.items() if key != "id"},
                "_id": fields.get("id", str(uuid.uuid4())[:22]),
            }
            for fields in (model_class(**raw_fields).model_dump() for raw_fields in fields_list)
        ]

    def _batch_get_link_id(self, table_name: str, field_keys: list[FieldKey]) -> dict[FieldKey, str]:
        """Retrieves the link identifiers for the given field keys.

        Args:
            table_name (str): The name of the table.
            field_keys (list[FieldKey]): A list of field keys to retrieve the link identifiers for.

        Returns:
            dict[FieldKey, str]: A dictionary mapping field keys to their corresponding link identifiers.
        """
        return {key: self.links[table_name][key]["link_id"] for key in field_keys}

    def _get_link_id(self, table_name: str, field_key: FieldKey) -> str:
        """Get the link id for the given field key.

        Args:
            table_name (str): The name of the table.
            field_key (FieldKey): The key of the field representing the link.

        Returns:
            str: The link ID for the given field key.
        """
        return self._batch_get_link_id(table_name, [field_key])[field_key]

    def _get_linked_records(self, table_name: str, field_key: FieldKey, record_ids: list[ID]) -> dict[ID, list[ID]]:
        """Retrieves linked records for the given record IDs.

        Args:
            table_name (str): The name of the table.
            field_key (FieldKey): The key of the link field.
            record_ids (list[ID]): A list of record IDs.

        Returns:
            dict[ID, list[ID]]: A dictionary mapping record IDs to lists of linked record IDs.
        """
        return {
            record_id: [item["row_id"] for item in record_ids]
            for record_id, record_ids in self.client.get_linked_records(
                self.links[table_name][field_key]["table_id"],
                self.links[table_name][field_key]["key"],
                [{"row_id": item} for item in record_ids],
            ).items()
        }

    def _update_links(self, table_name: str, records: list[Record]) -> bool:
        """Updates the links for a list of records.

        Args:
            table_name (str): The name of the table.
            records (list[Record]): A list of records to update the links for.

        Returns:
            bool: True if all links were updated successfully, False otherwise.
        """
        links = {key: {} for key in self.links[table_name]}
        for record in records:
            for key, value in record.__dict__.items():
                if key in links:
                    links[key][record.id] = value
        return all(
            self.batch_update_links(table_name, key, record_ids_map)
            for key, record_ids_map in links.items()
            if record_ids_map != {}
        )

    def batch_create(self, table_name: str, fields_list: list[Fields]) -> list[Record]:
        """Creates multiple records in the table.

        Args:
            table_name (str): The name of the table.
            fields_list (list[Fields]): A list of dictionaries representing the fields for the new records.

        Returns:
            list[Record]: A list of Record instances representing the created records.
        """
        records = []
        for chunk in [fields_list[i : i + 1000] for i in range(0, len(fields_list), 1000)]:
            chunk_fields = self._fields_to_seatable_record(table_name, chunk)
            chunk_records = self._seatable_record_to_record(table_name, chunk_fields)
            if self.client.batch_append_rows(table_name, chunk_fields)["inserted_row_count"] != len(chunk_fields):
                raise RuntimeError("Failed to create all rows")
            if not self._update_links(table_name, chunk_records):
                raise RuntimeError("Failed to link all records")
            records.extend(chunk_records)
        return records

    def batch_delete(self, table_name: str, record_ids: list[ID]) -> bool:
        """Deletes multiple records from the table.

        Args:
            table_name (str): The name of the table.
            record_ids (list[ID]): A list of record IDs to delete.

        Returns:
            bool: True if the records were successfully deleted, False otherwise.
        """
        for chunk in [record_ids[i : i + 10000] for i in range(0, len(record_ids), 10000)]:
            if self.client.batch_delete_rows(table_name, chunk)["deleted_rows"] != len(chunk):
                raise RuntimeError("Failed to delete all records")
        return True

    def batch_get(self, table_name: str, record_ids: list[ID]) -> list[Record]:
        """Retrieves multiple records from the table.

        Args:
            table_name (str): The name of the table.
            record_ids (list[ID]): A list of record IDs to retrieve.

        Returns:
            list[Record]: A list of Record instances representing the retrieved records.
        """
        # TODO: This line risks sql injection. Find alternatives.
        query = f"""SELECT * from `{table_name}` WHERE `_id` IN ({','.join([f"'{record_id}'" for record_id in record_ids])}) LIMIT 10000"""
        return self._seatable_record_to_record(table_name, self.client.query(query))

    def batch_update(self, table_name: str, fields_list: list[Fields]) -> list[Record]:
        """Updates multiple records in the table.

        Args:
            table_name (str): The name of the table.
            fields_list (list[Fields]): A list of fields to update.

        Returns:
            list[Record]: A list of Record instances representing the updated records.
        """
        records = []
        seatable_records = []
        model_class = self.models.get(table_name, Record)
        old_records = self.batch_get(table_name, [fields["id"] for fields in fields_list])
        for old_record, fields in zip(old_records, fields_list, strict=False):
            record = model_class(**(old_record.__dict__ | fields))
            seatable_record = self._fields_to_seatable_record(table_name, [record.__dict__])[0]
            records.append(record)
            seatable_records.append(seatable_record)
        for chunk_records, chunk_seatable_records in zip(
            [records[i : i + 1000] for i in range(0, len(records), 1000)],
            [seatable_records[i : i + 1000] for i in range(0, len(seatable_records), 1000)],
            strict=False,
        ):
            if not self._update_links(table_name, chunk_records):
                raise RuntimeError("Failed to link all records")
            if not self.client.batch_update_rows(
                table_name,
                [
                    {"row_id": fields["_id"], "row": {key: value for key, value in fields.items() if key != "_id"}}
                    for fields in chunk_seatable_records
                ],
            )["success"]:
                raise RuntimeError("Failed to update all records")
        return records

    def batch_update_links(self, table_name: str, field_key: FieldKey, record_ids_map: dict[ID, list[ID]]) -> bool:
        """Updates links for multiple records in the table.

        Args:
            table_name (str): The name of the table.
            field_key (FieldKey): The key of the link field.
            record_ids_map (dict[ID, list[ID]]): A dictionary mapping record IDs to lists of linked record IDs.

        Returns:
            bool: True if the links were successfully updated, False otherwise.
        """
        link_id = self._get_link_id(table_name, field_key)
        other_table_id = self.links[table_name][field_key]["other_table_id"]
        self.client.batch_update_links(link_id, table_name, other_table_id, list(record_ids_map.keys()), record_ids_map)
        return True

    def query(self, table_name: str, query: Query | None = None) -> list[Record]:
        """Queries the table with the given query.

        Args:
            table_name (str): The name of the table.
            query (Query | None): The query object to execute.

        Returns:
            list[Record]: A list of Record instances matching the query.
        """
        records = self.client.query(f"SELECT * FROM `{table_name}` LIMIT 10000")
        return self._seatable_record_to_record(table_name, records)

    def create_table(self, name: str) -> Table:
        """Creates a new table in the database.

        Args:
            name (str): The name of the new table.

        Returns:
            Table: The newly created table instance.
        """
        self.client.add_table(name)
        return Table(name, self)

    def get_table(self, name: str) -> Table:
        """Retrieves a table from the database.

        Args:
            name (str): The name of the table to retrieve.

        Returns:
            Table: The table instance.
        """
        if name not in [table["name"] for table in self.metadata["tables"]]:
            raise ValueError(f"Table {name} does not exist.")
        return Table(name, self)

    def close(self) -> bool:
        """Closes the database connection.

        Returns:
            bool: True if the database was successfully closed, False otherwise.
        """
        # Seatable API doesn't require explicit closing, so we'll just return True
        return True
