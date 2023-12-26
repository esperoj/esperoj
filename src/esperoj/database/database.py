"""Database module."""

from abc import ABC, abstractmethod
from collections.abc import Iterable, Iterator
from dataclasses import dataclass
from typing import Any, Self


@dataclass
class Record(ABC):
    """A class representing a generic database record."""

    record_id: Any
    fields: dict[str, Any]

    @abstractmethod
    def update(self, fields: dict[str, Any]) -> Self:
        """Update the record with the given fields.

        Args:
            fields (dict[str, Any]): A dictionary of field names and their new values.

        Returns:
        -------
            Self: The updated record.
        """


class Table(ABC):
    """A class representing a generic database table."""

    @abstractmethod
    def create(self, fields: dict[str, Any]) -> Record:
        """Create a new record in the table.

        Args:
            fields (dict[str, Any]): A dictionary of field names and their values.

        Returns:
        -------
            Record: The created record.
        """

    @abstractmethod
    def delete(self, record_id: Any) -> Any:
        """Delete a record from the table and return its ID.

        Args:
            record_id (Any): The ID of the record to delete.

        Returns:
        -------
            Any: The deleted record ID.
        """

    @abstractmethod
    def get(self, record_id: Any) -> Record:
        """Get a record from the table.

        Args:
            record_id (Any): The ID of the record to get.

        Returns:
        -------
            Record: The requested record.
        """

    @abstractmethod
    def get_all(
        self, formulas: dict[str, Any] | None = None, sort: Iterable[str] | None = None
    ) -> Iterator[Record]:
        """Get all records from the table that match the given formulas.

        Example: get_all({"Name":"Long", "Age":9})

        Args:
            formulas (dict[str, Any], optional): A dictionary of field names and their values to filter records. Defaults to {}.
            sort (Iterable[str], optional): A list of field names to sort by, with a minus sign prefix for descending order. Defaults to [].

        Returns:
        -------
            Iterator[Record]: An Iterator of matching records.
        """

    @abstractmethod
    def update(self, record_id: Any, fields: dict[str, Any]) -> Record:
        """Update a record in the table.

        Args:
            record_id (Any): The ID of the record to update.
            fields (dict[str, Any]): A dictionary of field names and their new values.

        Returns:
        -------
            Record: The updated record.
        """

    def create_many(self, fields_list: Iterator[dict[str, Any]]) -> Iterator[Record]:
        """Create multiple records in the table.

        Args:
            fields_list (Iterator[dict[str, Any]]): An Iterator of dictionaries containing field names and their values.

        Returns:
        -------
            Iterator[Record]: An Iterator of created records.
        """
        return (self.create(fields) for fields in fields_list)

    def delete_many(self, record_ids: Iterator[Any]) -> Iterator[Any]:
        """Delete multiple records from the table.

        Args:
            record_ids (Iterator[Any]): An Iterator of record IDs to delete.

        Returns:
        -------
            Iterator[Any]: An Iterator of deleted record IDs.
        """
        return (self.delete(record_id) for record_id in record_ids)

    def get_many(self, record_ids: Iterator[Any]) -> Iterator[Record]:
        """Get multiple records from the table.

        Args:
            record_ids (Iterator[Any]): An Iterator of record IDs to get.

        Returns:
        -------
            Iterator[Record]: An Iterator of requested records.
        """
        return (self.get(record_id) for record_id in record_ids)

    def update_many(self, records: Iterator[dict[str, Any]]) -> Iterator[Record]:
        """Update multiple records in the table.

        Args:
            records (Iterator[dict[str, Any]]): An Iterator of dictionaries containing record IDs and field updates.

        Returns:
        -------
            Iterator[Record]: An Iterator of updated records.
        """
        return (self.update(**record) for record in records)


class Database(ABC):
    """A class representing a generic database."""

    @abstractmethod
    def table(self, name: str) -> Table:
        """Get or create a table in the database.

        Args:
            name (str): The name of the table.

        Returns:
        -------
            Table: The requested table.
        """

    @abstractmethod
    def close(self) -> bool:
        """Close the database connection.

        Returns:
        -------
            bool: True if the operation is successful.
        """
