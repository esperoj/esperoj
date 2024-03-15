"""Database module contain abstractions and types."""

from abc import ABC, abstractmethod
from dataclasses import dataclass
from typing import Any, Self

RecordId = str | int
FieldValue = Any
FieldKey = str
Fields = dict[FieldKey, FieldValue]


@dataclass
class Record(ABC):
    """Base class for all records."""

    record_id: RecordId
    fields: Fields
    table: "Table"

    def __getitem__(self, key: FieldKey) -> FieldValue:
        """Get the value of the field with the given key."""
        return self.fields[key]

    def __setitem__(self, key: FieldKey, value: FieldValue):
        """Set the value of the field with the given key."""
        self.update({key: value}).fields[key] = value

    def delete(self) -> RecordId:
        """Delete the record from the database."""
        return self.table.delete(self.record_id)

    def update(self, fields: Fields) -> Self:
        """Update the record with the given fields."""
        self.table.update(self.record_id, fields)
        return self


class Table(ABC):
    """Base class for all tables."""

    def add_link(
        self,
        field_key: FieldKey,
        record_id: RecordId,
        other_record_id: RecordId,
    ) -> bool:
        """Add a link between the record with the given record_id and the record with the given other_record_id in the other table."""
        return self.batch_add_link(field_key, {record_id: [other_record_id]})

    def batch_add_link(
        self,
        field_key: FieldKey,
        record_ids_map: dict[RecordId, list[RecordId]],
    ) -> bool:
        """Add links between the records with the given record_ids and the records with the given other_record_ids in the other table."""
        record_ids = list(record_ids_map.keys())
        current_other_record_ids_map = self.get_linked_records(field_key, record_ids)
        updated_record_ids_map = {}
        for record_id, other_record_ids in record_ids_map.items():
            current_other_record_ids = current_other_record_ids_map[record_id]
            updated_record_ids_map[record_id] = current_other_record_ids + other_record_ids
        return self.batch_update_links(field_key, updated_record_ids_map)

    def create(self, fields: Fields) -> Record:
        """Create a new record with the given fields."""
        return self.batch_create([fields])[0]

    def delete(self, record_id: RecordId) -> bool:
        """Delete the record with the given record_id."""
        return self.batch_delete([record_id])

    def get(self, record_id: RecordId) -> Record:
        """Get the record with the given record_id."""
        return self.batch_get([record_id])[0]

    def get_link_id(self, field_key: FieldKey) -> str:
        """Get the link id for the given field key."""
        return self.batch_get_link_id([field_key])[field_key]

    def update(self, record_id: RecordId, fields: Fields) -> bool:
        """Update the record with the given record_id with the given fields."""
        return self.batch_update([(record_id, fields)])

    def update_link(
        self,
        field_key: FieldKey,
        other_table_name: str,
        record_id: RecordId,
        other_record_ids: list[RecordId],
    ):
        """Update the link between the record with the given record_id and the records with the given other_record_ids in the other table."""
        return self.batch_update_links(field_key, {record_id: other_record_ids})

    @abstractmethod
    def batch_create(self, fields_list: list[Fields]) -> list[Record]:
        """Create new records with the given fields."""

    @abstractmethod
    def batch_delete(self, record_ids: list[RecordId]) -> bool:
        """Delete the records with the given record_ids."""

    @abstractmethod
    def batch_get(self, record_ids: list[RecordId]) -> list[Record]:
        """Get the records with the given record_ids."""

    @abstractmethod
    def batch_get_link_id(self, field_keys: list[FieldKey]) -> dict[FieldKey, str]:
        """Get the link ids for the given field keys."""

    @abstractmethod
    def batch_update(self, records: list[tuple[RecordId, Fields]]) -> bool:
        """Update the records with the given record_ids with the given fields."""

    @abstractmethod
    def batch_update_links(
        self,
        field_key: FieldKey,
        record_ids_map: dict[RecordId, list[RecordId]],
    ) -> bool:
        """Update the links between the records with the given record_ids and the records with the given other_record_ids in the other table."""

    @abstractmethod
    def get_linked_records(
        self, field_key: FieldKey, record_ids: list[RecordId]
    ) -> dict[RecordId, list[RecordId]]:
        """Get the linked records for the given record_ids."""

    @abstractmethod
    def query(self, query: str) -> list[Record]:
        """Query the table with the given query."""


class Database(ABC):
    """Base class for all databases."""

    def __enter__(self):
        """Enter the database context."""
        return self

    def __exit__(self, exc_type, exc_value, traceback):
        """Exit the database context."""
        self.close()

    @abstractmethod
    def close(self) -> bool:
        """Close the database."""

    @abstractmethod
    def create_table(self, name: str) -> Table:
        """Create a new table with the given name."""


class DatabaseFactory:
    """DatabaseFactory class."""

    @staticmethod
    def create(config: dict):
        """Method to create database.

        Args:
            config (dict): The configs of the database.
        """
        database_type = config["type"]
        match database_type:
            case "seatable":
                from esperoj.database.seatable import SeatableDatabase

                return SeatableDatabase(config)
        raise ValueError(f"Unknown storage type: {database_type}")
