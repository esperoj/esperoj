"""Module contains Airtable class."""

import os
from collections.abc import Iterable
from dataclasses import dataclass
from typing import Any

from pyairtable import Api
from pyairtable.formulas import match

from esperoj.database import Database, Record, Table
from esperoj.exceptions import InvalidRecordError, RecordDeletionError, RecordNotFoundError


@dataclass
class AirtableRecord(Record):
    """A class representing a record in an Airtable table.

    Attributes:
        table (AirtableTable): The table this record belongs to.
    """

    table: "AirtableTable"

    def update(self, fields: dict[str, Any]) -> "AirtableRecord":
        """Updates the record with the given fields.

        Args:
            fields (dict[str, Any]): The fields to update.

        Returns:
            Self: The updated record.
        """
        return self.table.update(self.record_id, fields)


class AirtableTable(Table):
    """A class representing a table in Airtable.

    Attributes:
        name (str): The name of the table.
        client (Api): The Airtable API client.
    """

    def __init__(self, name: str, client):
        """Initializes an AirtableTable instance.

        Args:
            name (str): The name of the table.
            client (Api): The Airtable API client.
        """
        self.name = name
        self.client = client

    def _record_from_dict(self, record_dict: dict[str, Any]) -> AirtableRecord:
        """Creates an AirtableRecord from a dictionary.

        Args:
            record_dict (dict[str, Any]): The dictionary to create the record from.

        Returns:
            AirtableRecord: The created record.
        """
        return AirtableRecord(record_dict["id"], record_dict["fields"], self)

    def create(self, fields: dict[str, Any]) -> AirtableRecord:
        """Creates a new record in the table.

        Args:
            fields (dict[str, Any]): The fields of the new record.

        Returns:
            dict[str, Any]: The created record.
        """
        if not isinstance(fields, dict):
            raise InvalidRecordError("fields must be a dictionary.")
        return self._record_from_dict(self.client.create(fields))

    def create_many(self, fields_list: Iterable[dict[str, Any]]) -> Iterable[AirtableRecord]:
        """Creates multiple new records in the table.

        Args:
            fields_list (Iterable[dict[str, Any]]): The records to create.

        Returns:
            Iterable[AirtableRecord]: The created records.
        """
        return (self._record_from_dict(record) for record in self.client.batch_create(fields_list))

    def delete(self, record_id: str) -> str:
        """Deletes a record from the table.

        Args:
            record_id (str): The ID of the record to delete.

        Returns:
            str: The ID of the deleted record.
        """
        if not isinstance(record_id, str):
            raise InvalidRecordError("record_id must be a string.")

        record = self.get(record_id)
        result = self.client.delete(record.record_id)
        if result["deleted"] is True:
            return record_id
        raise RecordDeletionError(f"Deletion failed for id: {result['id']}")

    def delete_many(self, record_ids: Iterable[str]) -> Iterable[str]:
        """Deletes multiple records from the table.

        Args:
            record_ids (Iterable[str]): The IDs of the records to delete.

        Returns:
            Iterable[str]: The IDs of the deleted records.
        """
        results = self.client.batch_delete(record_ids)
        for result in results:
            if result["deleted"] is False:
                raise ValueError(f"Deletion failed for id: {result['id']}")
        return record_ids

    def get(self, record_id: str) -> AirtableRecord:
        """Gets a record from the table.

        Args:
            record_id (str): The ID of the record to get.

        Returns:
            AirtableRecord: The retrieved record.
        """
        if not isinstance(record_id, str):
            raise InvalidRecordError("record_id must be a string.")

        record = self.client.get(record_id)
        if record is None:
            raise RecordNotFoundError(f"Record with id '{record_id}' not found.")

        return self._record_from_dict(record)

    def get_all(self, formulas: dict[str, Any] | None = None) -> Iterable[AirtableRecord]:
        """Gets all records from the table.

        Args:
            formulas (dict[str, Any] | None, optional): The formulas to filter the records. Defaults to None.

        Returns:
            Iterable[AirtableRecord]: The retrieved records.
        """
        formula = match(formulas) if formulas is not None else ""
        return (self._record_from_dict(record) for record in self.client.all(formula=formula))

    def get_many(self, record_ids: Iterable[str]) -> Iterable[AirtableRecord]:
        """Gets multiple records from the table.

        Args:
            record_ids (Iterable[str]): The IDs of the records to get.

        Returns:
            Iterable[AirtableRecord]: The retrieved records.
        """
        return (self.get(record_id) for record_id in record_ids)

    def update(self, record_id: str, fields: dict[str, Any]) -> AirtableRecord:
        """Updates a record in the table.

        Args:
            record_id (str): The ID of the record to update.
            fields (dict[str, Any]): The fields to update.

        Returns:
            AirtableRecord: The updated record.
        """
        return self._record_from_dict(self.client.update(record_id, fields))

    def update_many(self, records: Iterable[dict[str, Any]]) -> Iterable[AirtableRecord]:
        """Updates multiple records in the table.

        Args:
            records (Iterable[dict[str, Any]]): The records to update.

        Returns:
            Iterable[AirtableRecord]: The updated records.
        """
        return (
            self._record_from_dict(record)
            for record in self.client.batch_update(
                {"id": r["record_id"], "fields": r["fields"]} for r in records
            )
        )


class Airtable(Database):
    """A class representing an Airtable database.

    Attributes:
        name (str): The name of the database.
        api_key (str): The API key for the Airtable API.
        base_id (str): The base ID of the Airtable database.
        client (Api): The Airtable API client.
    """

    def __init__(self, name: str, api_key: str = "", base_id: str = ""):
        """Initializes an Airtable instance.

        Args:
            name (str): The name of the database.
            api_key (str, optional): The API key for the Airtable API. Defaults to "".
            base_id (str, optional): The base ID of the Airtable database. Defaults to "".
        """
        self.name = name
        self.api_key = api_key or str(os.getenv("AIRTABLE_API_KEY"))
        self.base_id = base_id or str(os.getenv("AIRTABLE_BASE_ID"))
        self.client = Api(self.api_key)

    def table(self, name: str) -> AirtableTable:
        """Gets a table from the Airtable database.

        Args:
            name (str): The name of the table.

        Returns:
            AirtableTable: The retrieved table.
        """
        return AirtableTable(name, self.client.table(self.base_id, name))

    def close(self) -> bool:
        """Closes the Airtable database connection.

        Returns:
            bool: True if the connection is closed successfully, False otherwise.
        """
        return True
