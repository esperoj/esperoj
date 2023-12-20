"""Database module."""
import uuid
from abc import ABC, abstractmethod
from typing import Any

from esperoj.exceptions import InvalidRecordError, RecordNotFoundError


class BaseTable(ABC):
    """An abstract base class for table operations."""

    def __init__(self, name: str):
        self.name = name

    @abstractmethod
    def create(self, fields: dict[str, Any]) -> dict[str, Any]:
        """
        Create a new record in the table.

        Args:
            fields (dict[str, Any]): A dictionary containing field data for the new record.

        Returns
        -------
            dict[str, Any]: The created record.
        """

    @abstractmethod
    def delete(self, record_id: str) -> str:
        """
        Delete a record from the table.

        Args:
            record_id (str): The ID of the record to delete.

        Returns
        -------
            str: The deleted record ID.
        """

    @abstractmethod
    def get(self, record_id: str) -> dict[str, Any]:
        """
        Retrieve a record from the table.

        Args:
            record_id (str): The ID of the record to retrieve.

        Returns
        -------
            dict[str, Any]: The retrieved record.
        """

    @abstractmethod
    def update(self, record: dict[str, Any]) -> dict[str, Any]:
        """
        Update a record in the table.

        Args:
            record (dict[str, Any]): A dictionary containing the updated record data.

        Returns
        -------
            dict[str, Any]: The updated record.
        """

    def create_many(self, list_of_fields: list[dict[str, Any]]) -> list[dict[str, Any]]:
        """
        Create multiple records in the table.

        Args:
            list_of_fields (list[dict[str, Any]]): A list of dictionaries containing field data for each record.

        Returns
        -------
            list[dict[str, Any]]: A list of created records.
        """
        if not isinstance(list_of_fields, list):
            raise InvalidRecordError("list_of_fields must be a list of dictionaries.")
        return [self.create(fields) for fields in list_of_fields]

    def delete_many(self, record_ids: list[str]) -> list[str]:
        """
        Delete multiple records from the table.

        Args:
            record_ids (list[str]): A list of record IDs to delete.

        Returns
        -------
            list[str]: A list of deleted record IDs.
        """
        if not isinstance(record_ids, list):
            raise InvalidRecordError("record_ids must be a list of strings.")
        return [self.delete(record_id) for record_id in record_ids]

    def get_many(self, record_ids: list[str]) -> list[dict[str, Any]]:
        """
        Retrieve multiple records from the table.

        Args:
            record_ids (list[str]): A list of record IDs to retrieve.

        Returns
        -------
            list[dict[str, Any] | None]: A list of retrieved records or None if not found.
        """
        if not isinstance(record_ids, list):
            raise InvalidRecordError("record_ids must be a list of strings.")
        return [self.get(record_id) for record_id in record_ids]

    def update_many(self, records: list[dict[str, Any]]) -> list[dict[str, Any]]:
        """
        Update multiple records in the table.

        Args:
            records (list[dict[str, Any]]): A list of dictionaries containing updated record data.

        Returns
        -------
            list[dict[str, Any]]: A list of updated records.
        """
        if not isinstance(records, list):
            raise InvalidRecordError("records must be a list of dictionaries.")
        return [self.update(record) for record in records]


class Table(BaseTable):
    """A class representing a table with basic CRUD operations."""

    def __init__(self, name: str):
        super().__init__(name)
        self.records: list[dict[str, Any]] = []

    def create(self, fields: dict[str, Any]) -> dict[str, Any]:
        """
        Create a new record in the table.

        Args:
            fields (dict[str, Any]): A dictionary containing field data for the new record.

        Returns
        -------
            dict[str, Any]: The created record.
        """
        if not isinstance(fields, dict):
            raise InvalidRecordError("fields must be a dictionary.")
        record = {"id": str(uuid.uuid4()), "fields": fields}
        self.records.append(record)
        return record

    def delete(self, record_id: str) -> str:
        """
        Delete a record from the table.

        Args:
            record_id (str): The ID of the record to delete.

        Returns
        -------
            str: The deleted record ID.
        """
        if not isinstance(record_id, str):
            raise InvalidRecordError("record_id must be a string.")

        record = self.get(record_id)
        if record is None:
            raise RecordNotFoundError(f"Record with id '{record_id}' not found.")

        self.records = [record for record in self.records if record["id"] != record_id]
        return record_id

    def get(self, record_id: str) -> dict[str, Any]:
        """
        Retrieve a record from the table.

        Args:
            record_id (str): The ID of the record to retrieve.

        Returns
        -------
            dict[str, Any]: The retrieved record.
        """
        if not isinstance(record_id, str):
            raise InvalidRecordError("record_id must be a string.")

        record = next((record for record in self.get_all() if record["id"] == record_id), None)
        if record is None:
            raise RecordNotFoundError(f"Record with id '{record_id}' not found.")

        return record

    def get_all(self) -> list[dict[str, Any]]:
        """
        Retrieve all records from the table.

        Returns
        -------
            list[dict[str, Any]]: A list of all records in the table.
        """
        return self.records

    def match(self, pattern: dict[str, Any]) -> list[dict[str, Any]]:
        """
        Find records in the table that match a given pattern.

        Args:
            pattern (dict[str, Any]): A dictionary containing the pattern to match.

        Returns
        -------
            list[dict[str, Any]]: A list of records that match the pattern.
        """
        if not isinstance(pattern, dict):
            raise InvalidRecordError("pattern must be a dictionary.")
        return [
            record
            for record in self.get_all()
            if isinstance(record["fields"], type(pattern))
            and all(record["fields"].get(k) == v for k, v in pattern.items())
        ]

    def update(self, record: dict[str, Any]) -> dict[str, Any]:
        """
        Update a record in the table.

        Args:
            record (dict[str, Any]): A dictionary containing the updated record data.

        Returns
        -------
            dict[str, Any]: The updated record.
        """
        if not isinstance(record, dict):
            raise InvalidRecordError("record must be a dictionary.")
        if "id" not in record or "fields" not in record:
            raise InvalidRecordError("record must contain 'id' and 'fields' keys.")

        record_id = record["id"]
        updated_fields = record["fields"]

        existing_record = self.get(record_id)
        if existing_record is None:
            raise RecordNotFoundError(f"Record with id '{record_id}' not found.")

        existing_record["fields"].update(updated_fields)
        return existing_record


class Database:
    """A class representing a simple in-memory database."""

    def __init__(self, name: str = "Database"):
        self.name = name

    def table(self, name: str) -> Table:
        """
        Create a new table in the database.

        Args:
            name (str): The name of the table.

        Returns
        -------
            Table: The created table.
        """
        if not isinstance(name, str):
            raise ValueError("Table name must be a string.")
        return Table(name=name)

    def close(self) -> None:
        """
        Close the database.

        Returns
        -------
            None
        """
        return
