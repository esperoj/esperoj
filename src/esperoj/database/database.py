"""Database module."""

from abc import ABC, abstractmethod
from collections.abc import Iterable
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
    def get_all(self, formulas: dict[str, Any] | None = None) -> Iterable[Record]:
        """Get all records from the table that match the given formulas.

        Example: get_all({"Name":"Long", "Age":9})

        Args:
            formulas (dict[str, Any], optional): A dictionary of field names and their values to filter records. Defaults to {}.

        Returns:
        -------
            Iterable[Record]: An iterable of matching records.
        """
        if formulas is None:
            formulas = {}

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

    def create_many(self, fields_list: Iterable[dict[str, Any]]) -> Iterable[Record]:
        """Create multiple records in the table.

        Args:
            fields_list (Iterable[dict[str, Any]]): An iterable of dictionaries containing field names and their values.

        Returns:
        -------
            Iterable[Record]: An iterable of created records.
        """
        return [self.create(fields) for fields in fields_list]

    def delete_many(self, record_ids: Iterable[Any]) -> Iterable[Any]:
        """Delete multiple records from the table.

        Args:
            record_ids (Iterable[Any]): An iterable of record IDs to delete.

        Returns:
        -------
            Iterable[Any]: An iterable of deleted record IDs.
        """
        return [self.delete(record_id) for record_id in record_ids]

    def get_many(self, record_ids: Iterable[Any]) -> Iterable[Record]:
        """Get multiple records from the table.

        Args:
            record_ids (Iterable[Any]): An iterable of record IDs to get.

        Returns:
        -------
            Iterable[Record]: An iterable of requested records.
        """
        return [self.get(record_id) for record_id in record_ids]

    def update_many(self, records: Iterable[dict[str, Any]]) -> Iterable[Record]:
        """Update multiple records in the table.

        Args:
            records (Iterable[dict[str, Any]]): An iterable of dictionaries containing record IDs and field updates.

        Returns:
        -------
            Iterable[Record]: An iterable of updated records.
        """
        return [self.update(**record) for record in records]


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
