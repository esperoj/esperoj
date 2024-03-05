"""Module contains Seatable class."""

import os
from collections.abc import Iterable, Iterator
from dataclasses import dataclass
from typing import Any

from seatable_api import Base, context

from esperoj.database import Database, Record, Table
from esperoj.exceptions import InvalidRecordError, RecordDeletionError, RecordNotFoundError


@dataclass
class SeatableRecord(Record):
    """A class representing a record in an Seatable table.

    Attributes:
        table (SeatableTable): The table this record belongs to.
    """

    table: "SeatableTable"

    def update(self, fields: dict[str, Any]) -> "SeatableRecord":
        """Updates the record with the given fields.

        Args:
            fields (dict[str, Any]): The fields to update.

        Returns:
            Self: The updated record.
        """
        self.fields.update(fields)
        return self.table.update(self.record_id, fields)


class SeatableTable(Table):
    """A class representing a table in Seatable.

    Attributes:
        name (str): The name of the table.
        client: The Seatable API client.
    """

    def __init__(self, name: str, client):
        """Initializes an SeatableTable instance.

        Args:
            name (str): The name of the table.
            client: The Seatable API client.
        """
        self.name = name
        self.client = client

    def _record_from_dict(self, record_dict: dict[str, Any]) -> SeatableRecord:
        """Creates an SeatableRecord from a dictionary.

        Args:
            record_dict (dict[str, Any]): The dictionary to create the record from.

        Returns:
            SeatableRecord: The created record.
        """
        record_id = record_dict["_id"]
        record_dict.pop("_id", None)
        record_dict.pop("_ctime", None)
        record_dict.pop("_mtime", None)
        return SeatableRecord(record_id, record_dict, self)

    def create(self, fields: dict[str, Any]) -> SeatableRecord:
        """Creates a new record in the table.

        Args:
            fields (dict[str, Any]): The fields of the new record.

        Returns:
            dict[str, Any]: The created record.
        """
        if not isinstance(fields, dict):
            raise InvalidRecordError("fields must be a dictionary.")
        return self.get(self.client.append_row(self.name, fields)["_id"])

    def create_many(self, fields_list: Iterator[dict[str, Any]]) -> Iterable[SeatableRecord]:
        """Creates multiple new records in the table.

        Args:
            fields_list (Iterator[dict[str, Any]]): The records to create.

        Returns:
            Iterable[SeatableRecord]: The created records.
        """
        return self.client.batch_append_rows(self.name, fields_list)
        # return [self._record_from_dict(record) for record in self.client.batch_append_rows(self.name, fields_list)]

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

    def delete_many(self, record_ids: Iterator[str]) -> Iterable[str]:
        """Deletes multiple records from the table.

        Args:
            record_ids (Iterator[str]): The IDs of the records to delete.

        Returns:
            Iterable[str]: The IDs of the deleted records.
        """
        results = self.client.batch_delete(record_ids)
        for result in results:
            if result["deleted"] is False:
                raise ValueError(f"Deletion failed for id: {result['id']}")
        return record_ids

    def get(self, record_id: str) -> SeatableRecord:
        """Gets a record from the table.

        Args:
            record_id (str): The ID of the record to get.

        Returns:
            SeatableRecord: The retrieved record.
        """
        if not isinstance(record_id, str):
            raise InvalidRecordError("record_id must be a string.")

        record = self.client.get_row(self.name, record_id)
        if not record:
            raise RecordNotFoundError(f"Record with id '{record_id}' not found.")

        return self._record_from_dict(record)

    def get_all(
        self, formulas: dict[str, Any] | None = None, sort: Iterable[str] | None = None
    ) -> Iterator[SeatableRecord]:
        """Gets all records from the table.

        Args:
            formulas (dict[str, Any] | None, optional): The formulas to filter the records. Defaults to None.
            sort (Iterable[str], optional): A list of field names to sort by, with a minus sign prefix for descending order. Defaults to [].

        Returns:
            Iterator[SeatableRecord]: The retrieved records.
        """
        print(self.name)
        if sort is None:
            sort = []
        i = 0
        records = []
        while True:
            new_records = self.client.list_rows(self.name, start=i, limit=1000)
            records.extend(new_records)
            if new_records == []:
                return (self._record_from_dict(record) for record in records)
            i += 1000

    def get_many(self, record_ids: Iterator[str]) -> Iterator[SeatableRecord]:
        """Gets multiple records from the table.

        Args:
            record_ids (Iterator[str]): The IDs of the records to get.

        Returns:
            Iterator[SeatableRecord]: The retrieved records.
        """
        return (self.get(record_id) for record_id in record_ids)

    def update(self, record_id: str, fields: dict[str, Any]) -> SeatableRecord:
        """Updates a record in the table.

        Args:
            record_id (str): The ID of the record to update.
            fields (dict[str, Any]): The fields to update.

        Returns:
            SeatableRecord: The updated record.
        """
        return self._record_from_dict(self.client.update(record_id, fields))

    def update_many(self, records: Iterator[dict[str, Any]]) -> Iterable[SeatableRecord]:
        """Updates multiple records in the table.

        Args:
            records (Iterator[dict[str, Any]]): The records to update.

        Returns:
            Iterable[SeatableRecord]: The updated records.
        """
        return [
            self._record_from_dict(record)
            for record in self.client.batch_update(
                {"id": r["record_id"], "fields": r["fields"]} for r in records
            )
        ]


class Seatable(Database):
    """A class representing an Seatable database.

    Attributes:
        name (str): The name of the database.
        api_token (str): The API key for the Airtable API.
        client (Api): The Seatable API client.
        server_url (str): The url of the server.
    """

    def __init__(self, name: str, api_token: str = ""):
        """Initializes an Seatable instance.

        Args:
            name (str): The name of the database.
            api_token (str, optional): The API key for the Seatable API. Defaults to "".
        """
        self.name = name
        self.server_url = context.server_url or "https://cloud.seatable.io"
        self.api_token = api_token or context.api_token or os.getenv("SEATABLE_API_TOKEN")
        self.client = Base(self.api_token, self.server_url)
        self.client.auth()

    def table(self, name: str) -> SeatableTable:
        """Gets a table from the Seatable database.

        Args:
            name (str): The name of the table.

        Returns:
            SeatableTable: The retrieved table.
        """
        return SeatableTable(name, self.client)

    def close(self) -> bool:
        """Closes the Airtable database connection.

        Returns:
            bool: True if the connection is closed successfully, False otherwise.
        """
        return True
