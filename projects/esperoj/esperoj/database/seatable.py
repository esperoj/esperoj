"""Module contains SeatableDatabase class."""

import os
from typing import Any
import uuid
from seatable_api import Base

from esperoj.database.database import (
    Database,
    FieldKey,
    Fields,
    Record,
    ID,
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
        self.client = Base(config['api_token'], config['server_url'])
        self.client.auth()
        self.metadata = self.client.get_metadata()

    def _seatable_record_to_record(self, table_name: str, fields_list: list[dict[FieldKey, Any]]) -> list[Record]:
        """Converts dictionaries representing a record to a Record instance."""
        model_class = self.models.get(table_name, Record)
        return [model_class(id=fields["_id"], **{key: value for key, value in fields.items() if not key.startswith("_")}) for fields in fields_list]

    def _fields_to_seatable_record(self, table_name: str, fields_list: list[dict[FieldKey, Any]]) -> list[Record]:
        """Converts dictionaries representing a record to a Record instance.

        Args:
            table_name (str): The name of the table.
            fields_list (list[dict[FieldKey, Any]]): A list of dictionaries representing a record.

        Returns:
            Records (list[Record]): Records instance representing the records.
        """
        model_class = self.models.get(table_name, Record)
        results = []
        for fields in fields_list:
            record_id = fields["_id"]
            fields = {key: value for key, value in record_dict.items() if not key.startswith("_")}
            results.extend(model_class(id=record_id, **fields))
        return results

    def batch_create(self, table_name: str, fields_list: list[Fields]) -> list[Record]:
        """Creates multiple records in the table.

        Args:
            table_name (str): The name of the table.
            fields_list (list[Fields]): A list of dictionaries representing the fields for the new records.

        Returns:
            list[Record]: A list of Record instances representing the created records.
        """
        model_class = self.models.get(table_name, Record)
        records = []
        for chunk in [fields_list[i : i + 1000] for i in range(0, len(fields_list), 1000)]:
            chunk_fields = [
                {**{key: value for key, value in fields.items() if key != "id"}, "_id": fields.get("id", str(uuid.uuid4())[:22])}
                for fields in (model_class(raw_fields).model_dump() for raw_fields in chunk)
            ]
            chunk_records = self._seatable_record_to_record(chunk_fields)
            if self.client.batch_append_rows(table_name, chunk_fields)["inserted_row_count"] != len(chunk_fields):
                raise RuntimeError("Failed to create all rows")
            if not self._update_links(chunk_records):
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
        for chunk in [record_ids[i : i + 1000] for i in range(0, len(record_ids), 1000)]:
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
        query = f"""SELECT * from `table_name` WHERE `_id` IN ({','.join([f"'{record_id}'" for record_id in record_ids])})"""
        return self._seatable_record_to_record(self.client.query(query))

    def batch_update(self, table_name: str, records: list[Fields]) -> list[Record]:
        """Updates multiple records in the table.

        Args:
            table_name (str): The name of the table.
            records (list[tuple[ID, Fields]]): A list of tuples containing record IDs and fields to update.

        Returns:
            list[Record]: A list of Record instances representing the updated records.
        """
        results = []
        model_class = self.models.get(table_name, Record)
        records  = [model_class(fields).model_dump() for fields in records]
        for chunk in [records[i : i + 1000] for i in range(0, len(records), 1000)]:
            chunk_records = [self._seatable_record_to_record({"_id": fields["id"], **fields}) for fields in chunk]
            if not self._update_links(chunk_records):
                raise RuntimeError("Failed to link all records")
            if not self.client.batch_update_rows(
                table_name, [{"row_id": fields["id"], "row": fields} for fields in chunk]
            )["success"]:
                raise RuntimeError("Failed to update all records")
            results += chunk_records
        return results

    def batch_update_links(self, table_name: str, field_key: FieldKey, record_ids_map: dict[ID, list[ID]]) -> bool:
        """Updates links for multiple records in the table.

        Args:
            table_name (str): The name of the table.
            field_key (FieldKey): The key of the link field.
            record_ids_map (dict[ID, list[ID]]): A dictionary mapping record IDs to lists of linked record IDs.

        Returns:
            bool: True if the links were successfully updated, False otherwise.
        """
        return self.client.batch_update_rows(table_name, [
            {"row_id": record_id, field_key: linked_ids}
            for record_id, linked_ids in record_ids_map.items()
        ])

    def get_linked_records(self, table_name: str, field_key: FieldKey, record_ids: list[ID]) -> dict[ID, list[ID]]:
        """Retrieves linked records for the given record IDs.

        Args:
            table_name (str): The name of the table.
            field_key (FieldKey): The key of the link field.
            record_ids (list[ID]): A list of record IDs.

        Returns:
            dict[ID, list[ID]]: A dictionary mapping record IDs to lists of linked record IDs.
        """
        records = self.client.get_rows(table_name, row_ids=record_ids)
        return {
            record["_id"]: record.get(field_key, [])
            for record in records
        }

    def query(self, table_name: str, query: Query | None = None) -> list[Record]:
        """Queries the table with the given query.

        Args:
            table_name (str): The name of the table.
            query (Query | None): The query object to execute.

        Returns:
            list[Record]: A list of Record instances matching the query.
        """
        # Note: This is a simplified implementation. You may need to translate the Query object
        # into Seatable's query format for more complex queries.
        records = self.client.get_rows(table_name)
        return [self._seatable_record_to_record(table_name, record) for record in records]

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
        if name not in [table['name'] for table in self.metadata['tables']]:
            raise ValueError(f"Table {name} does not exist.")
        return Table(name, self)

    def close(self) -> bool:
        """Closes the database connection.

        Returns:
            bool: True if the database was successfully closed, False otherwise.
        """
        # Seatable API doesn't require explicit closing, so we'll just return True
        return True