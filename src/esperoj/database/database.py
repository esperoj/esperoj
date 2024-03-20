"""Database module contains abstractions and types."""

from abc import ABC, abstractmethod
from dataclasses import dataclass
from typing import Any, Self

RecordId = str
FieldValue = Any
FieldKey = str
Fields = dict[FieldKey, FieldValue]


@dataclass
class Record(ABC):
    """Base class for all records.

    Attributes:
        record_id (RecordId): The unique identifier of the record.
        fields (Fields): A dictionary of field keys and values representing the record's data.
        table (Table): The table to which this record belongs.
    """

    record_id: RecordId
    fields: Fields
    table: "Table"

    def __getitem__(self, key: FieldKey) -> FieldValue:
        """Get the value of the field with the given key.

        Args:
            key (FieldKey): The key of the field to retrieve.

        Returns:
            FieldValue: The value of the field.
        """
        return self.fields[key]

    def __setitem__(self, key: FieldKey, value: FieldValue):
        """Set the value of the field with the given key.

        Args:
            key (FieldKey): The key of the field to set.
            value (FieldValue): The value to set for the field.
        """
        self.update({key: value}).fields[key] = value

    def delete(self) -> bool:
        """Delete the record from the database.

        Returns:
            bool: True if the record was successfully deleted, False otherwise.
        """
        return self.table.delete(self.record_id)

    def to_dict(self) -> dict:
        """Convert the record to a dictionary.

        Returns:
            dict: A dictionary representation of the record, with the record ID as the "_id" key.
        """
        return {"_id": self.record_id} | self.fields

    def update(self, fields: Fields) -> Self:
        """Update the record with the given fields.

        Args:
            fields (Fields): A dictionary of field keys and values to update.

        Returns:
            Record: The updated record instance.
        """
        if self.table.update(self.record_id, fields):
            self.fields.update(fields)
        return self


class Table(ABC):
    """Base class for all tables."""

    def add_link(
        self,
        field_key: FieldKey,
        record_id: RecordId,
        other_record_id: RecordId,
    ) -> bool:
        """Add a link between the record with the given record_id and the record with the given other_record_id in the other table.

        Args:
            field_key (FieldKey): The key of the field representing the link.
            record_id (RecordId): The ID of the record to link.
            other_record_id (RecordId): The ID of the other record to link.

        Returns:
            bool: True if the link was successfully added, False otherwise.
        """
        return self.batch_add_link(field_key, {record_id: [other_record_id]})

    def batch_add_link(
        self,
        field_key: FieldKey,
        record_ids_map: dict[RecordId, list[RecordId]],
    ) -> bool:
        """Add links between the records with the given record_ids and the records with the given other_record_ids in the other table.

        Args:
            field_key (FieldKey): The key of the field representing the link.
            record_ids_map (dict[RecordId, list[RecordId]]): A dictionary mapping record IDs to a list of other record IDs to link.

        Returns:
            bool: True if the links were successfully added, False otherwise.
        """
        record_ids = list(record_ids_map.keys())
        current_other_record_ids_map = self.get_linked_records(field_key, record_ids)
        updated_record_ids_map = {}
        for record_id, other_record_ids in record_ids_map.items():
            current_other_record_ids = current_other_record_ids_map[record_id]
            updated_record_ids_map[record_id] = current_other_record_ids + other_record_ids
        return self.batch_update_links(field_key, updated_record_ids_map)

    def create(self, fields: Fields) -> Record:
        """Create a new record with the given fields.

        Args:
            fields (Fields): A dictionary of field keys and values representing the record's data.

        Returns:
            Record: The newly created record instance.
        """
        return self.batch_create([fields])[0]

    def delete(self, record_id: RecordId) -> bool:
        """Delete the record with the given record_id.

        Args:
            record_id (RecordId): The ID of the record to delete.

        Returns:
            bool: True if the record was successfully deleted, False otherwise.
        """
        return self.batch_delete([record_id])

    def get(self, record_id: RecordId) -> Record:
        """Get the record with the given record_id.

        Args:
            record_id (RecordId): The ID of the record to retrieve.

        Returns:
            Record: The record instance.
        """
        return self.batch_get([record_id])[0]

    def get_link_id(self, field_key: FieldKey) -> str:
        """Get the link id for the given field key.

        Args:
            field_key (FieldKey): The key of the field representing the link.

        Returns:
            str: The link ID for the given field key.
        """
        return self.batch_get_link_id([field_key])[field_key]

    def update(self, record_id: RecordId, fields: Fields) -> Record:
        """Update the record with the given record_id with the given fields.

        Args:
            record_id (RecordId): The ID of the record to update.
            fields (Fields): A dictionary of field keys and values to update.

        Returns:
            Record: The updated record instance.
        """
        return self.batch_update([(record_id, fields)])[0]

    def update_link(
        self,
        field_key: FieldKey,
        other_table_name: str,
        record_id: RecordId,
        other_record_ids: list[RecordId],
    ) -> bool:
        """Update the link between the record with the given record_id and the records with the given other_record_ids in the other table.

        Args:
            field_key (FieldKey): The key of the field representing the link.
            other_table_name (str): The name of the other table.
            record_id (RecordId): The ID of the record to update.
            other_record_ids (list[RecordId]): A list of IDs of the other records to link.

        Returns:
            bool: True if the link was successfully updated, False otherwise.
        """
        return self.batch_update_links(field_key, {record_id: other_record_ids})

    @abstractmethod
    def batch_create(self, fields_list: list[Fields]) -> list[Record]:
        """Create new records with the given fields.

        Args:
            fields_list (list[Fields]): A list of dictionaries, where each dictionary represents the fields of a record.

        Returns:
            list[Record]: A list of newly created record instances.
        """
        raise NotImplementedError

    @abstractmethod
    def batch_delete(self, record_ids: list[RecordId]) -> bool:
        """Delete the records with the given record_ids.

        Args:
            record_ids (list[RecordId]): A list of record IDs to delete.

        Returns:
            bool: True if the records were successfully deleted, False otherwise.
        """
        raise NotImplementedError

    @abstractmethod
    def batch_get(self, record_ids: list[RecordId]) -> list[Record]:
        """Get the records with the given record_ids.

        Args:
            record_ids (list[RecordId]): A list of record IDs to retrieve.

        Returns:
            list[Record]: A list of record instances.
        """
        raise NotImplementedError

    @abstractmethod
    def batch_get_link_id(self, field_keys: list[FieldKey]) -> dict[FieldKey, str]:
        """Get the link ids for the given field keys.

        Args:
            field_keys (list[FieldKey]): A list of field keys representing links.

        Returns:
            dict[FieldKey, str]: A dictionary mapping field keys to their corresponding link IDs.
        """
        raise NotImplementedError

    @abstractmethod
    def batch_update(self, records: list[tuple[RecordId, Fields]]) -> list[Record]:
        """Update the records with the given record_ids with the given fields.

        Args:
            records (list[tuple[RecordId, Fields]]): A list of tuples, where each tuple contains a record ID and a dictionary of fields to update.

        Returns:
            list[Record]: A list of updated record instances.
        """
        raise NotImplementedError

    @abstractmethod
    def batch_update_links(
        self,
        field_key: FieldKey,
        record_ids_map: dict[RecordId, list[RecordId]],
    ) -> bool:
        """Update the links between the records with the given record_ids and the records with the given other_record_ids in the other table.

        Args:
            field_key (FieldKey): The key of the field representing the link.
            record_ids_map (dict[RecordId, list[RecordId]]): A dictionary mapping record IDs to a list of other record IDs to link.

        Returns:
            bool: True if the links were successfully updated, False otherwise.
        """
        raise NotImplementedError

    @abstractmethod
    def get_linked_records(
        self, field_key: FieldKey, record_ids: list[RecordId]
    ) -> dict[RecordId, list[RecordId]]:
        """Get the linked records for the given record_ids.

        Args:
            field_key (FieldKey): The key of the field representing the link.
            record_ids (list[RecordId]): A list of record IDs.

        Returns:
            dict[RecordId, list[RecordId]]: A dictionary mapping record IDs to a list of linked record IDs.
        """
        raise NotImplementedError

    @abstractmethod
    def query(self, query: str) -> list[Record]:
        """Query the table with the given query.

        Args:
            query (str): The query string.

        Returns:
            list[Record]: A list of record instances matching the query.
        """
        raise NotImplementedError


class Database(ABC):
    """Base class for all databases.

    Attributes:
        config (dict[Any, Any]): The configuration for the database.
    """

    config: dict[Any, Any]

    def __enter__(self):
        """Enter the database context.

        Returns:
            Database: The database instance.
        """
        return self

    def __exit__(self, exc_type, exc_value, traceback):
        """Exit the database context.

        Args:
            exc_type (type): The type of exception raised, if any.
            exc_value (Exception): The exception instance raised, if any.
            traceback (traceback): The traceback object, if an exception was raised.
        """
        self.close()

    def close(self) -> bool:
        """Close the database.

        Returns:
            bool: True if the database was successfully closed, False otherwise.
        """
        return True

    @abstractmethod
    def create_table(self, name: str) -> Table:
        """Create a new table with the given name.

        Args:
            name (str): The name of the new table.

        Returns:
            Table: The newly created table instance.
        """
        raise NotImplementedError

    @abstractmethod
    def get_table(self, name: str) -> Table:
        """Get a table with the given name.

        Args:
            name (str): The name of the table to retrieve.

        Returns:
            Table: The table instance.
        """
        raise NotImplementedError


class DatabaseFactory:
    """DatabaseFactory class.

    This class provides a factory method for creating database instances based on the provided configuration.
    """

    @staticmethod
    def create(config: dict) -> Database:
        """Create a database instance based on the provided configuration.

        Args:
            config (dict): The configuration dictionary for the database.

        Returns:
            Database: The database instance.

        Raises:
            ValueError: If the database type in the configuration is unknown.
        """
        database_type = config["type"]
        match database_type:
            case "seatable":
                from esperoj.database.seatable import SeatableDatabase

                return SeatableDatabase(config)
        raise ValueError(f"Unknown database type: {database_type}")
