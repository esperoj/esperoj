"""Memory database module."""

from collections.abc import Iterator
from typing import Any, Self
from uuid import uuid4

from esperoj.database import Database, Record, Table
from esperoj.exceptions import RecordNotFoundError


class MemoryRecord(Record):
    """A class representing an in-memory record.

    Inherits from the Record class.
    """

    def update(self, fields: dict[str, Any]) -> Self:
        """Update the record with the given fields.

        Args:
            fields (dict[str, Any]): A dictionary of field names and their new values.

        Returns:
        -------
            Self: The updated record.
        """
        self.fields.update(fields)
        return self


class MemoryTable(Table):
    """A class representing an in-memory table.

    Inherits from the Table class.
    """

    def __init__(self, name: str):
        """Initialize a new in-memory table.

        Args:
            name (str): The name of the table.
        """
        self.records: dict[Any, dict[str, Any]] = {}
        self.name = name

    def create(self, fields: dict[str, Any]) -> MemoryRecord:
        """Create a new record in the table.

        Args:
            fields (dict[str, Any]): A dictionary of field names and their values.

        Returns:
        -------
            MemoryRecord: The created record.
        """
        record_id = uuid4()
        self.records[record_id] = fields
        return MemoryRecord(record_id, fields)

    def delete(self, record_id: Any) -> Any:
        """Delete a record from the table.

        Args:
            record_id (Any): The ID of the record to delete.

        Returns:
        -------
            Any: The deleted record ID.

        Raises:
        ------
            RecordNotFoundError: If the record is not found.
        """
        if record_id not in self.records:
            raise RecordNotFoundError
        self.records.pop(record_id)
        return record_id

    def get(self, record_id: Any) -> MemoryRecord:
        """Get a record from the table.

        Args:
            record_id (Any): The ID of the record to get.

        Returns:
        -------
            MemoryRecord: The requested record.

        Raises:
        ------
            RecordNotFoundError: If the record is not found.
        """
        if record_id not in self.records:
            raise RecordNotFoundError
        fields = self.records[record_id]
        return MemoryRecord(record_id, fields)

    def get_all(
        self, formulas: dict[str, Any] | None = None, sort: list[str] | None = None
    ) -> Iterator[MemoryRecord]:
        """Get all records from the table that match the given formulas and sort them.

        Args:
            formulas (dict[str, Any], optional): A dictionary of field names and their values to filter records. Defaults to None.
            sort (Iterable[str], optional): A list of field names to sort by, with a minus sign prefix for descending order. Defaults to [].

        Returns:
        -------
            Iterator[MemoryRecord]: An Iterator of matching records.

        Raises:
        ------
            ValueError: If the formulas argument is not a dictionary.
        """
        if sort is None:
            sort = []
        filtered_records = [
            MemoryRecord(record_id, fields)
            for record_id, fields in self.records.items()
            if not formulas or all(fields.get(key) == value for key, value in formulas.items())
        ]
        if sort:
            for spec in reversed(sort):
                reverse = spec.startswith("-")
                key = spec[1:] if reverse else spec
                filtered_records.sort(key=lambda record: record.fields[key], reverse=reverse)
        return iter(filtered_records)

    def update(self, record_id: Any, fields: dict[str, Any]) -> MemoryRecord:
        """Update a record in the table.

        Args:
            record_id (Any): The ID of the record to update.
            fields (dict[str, Any]): A dictionary of field names and their new values.

        Returns:
        -------
            MemoryRecord: The updated record.
        """
        record = self.get(record_id)
        record.fields.update(fields)
        return MemoryRecord(record_id, record.fields)


class MemoryDatabase(Database):
    """A class representing an in-memory database.

    Inherits from the Database class.
    """

    def __init__(self, name: str):
        """Initialize a new in-memory database.

        Args:
            name (str): The name of the database.
        """
        self.tables: dict[str, MemoryTable] = {}
        self.name = name

    def table(self, name: str) -> MemoryTable:
        """Get or create a table in the database.

        Args:
            name (str): The name of the table.

        Returns:
        -------
            MemoryTable: The requested table.
        """
        if name not in self.tables:
            self.tables[name] = MemoryTable(name)
        return self.tables[name]

    def close(self) -> bool:
        """Close the in-memory database and clear all tables.

        Returns:
        -------
            bool: True if the operation is successful.
        """
        self.tables.clear()
        return True
