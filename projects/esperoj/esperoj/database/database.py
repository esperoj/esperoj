"""Database module contains abstractions and types."""

from abc import ABC, abstractmethod
from functools import partial
from typing import Annotated, Any

from nanoid import generate
from pydantic import BaseModel, ConfigDict, Field

from esperoj.database.query import Query

FieldValue = Any
FieldKey = str
Fields = dict[FieldKey, FieldValue]
ID = Annotated[str, Field(default_factory=lambda: generate(size=22), min_length=1, max_length=36)]


class Record(BaseModel):
    """Record class.

    This class represents a record in a database table.
    """

    id: ID

    model_config = ConfigDict(
        extra="allow",
    )


class Table:
    """Base class for all tables.

    This abstract class defines the structure and behavior common to all table classes.
    It provides a mechanism to dynamically access database operations specific to the associated table.
    """

    def __init__(self, name, db):
        """Initialize a Table instance.

        Args:
            name (str): The name of the table.
            db (Database): The database instance to which this table belongs.

        Initializes the table with a list of valid operations that can be performed on the table.
        """
        self.db = db
        self.name = name
        self.valid_operations = [
            "add_link",
            "batch_add_link",
            "update_link",
            "batch_update_links",
            "create",
            "batch_create",
            "get",
            "batch_get",
            "query",
            "update",
            "batch_update",
            "delete",
            "batch_delete",
        ]

    def __getattr__(self, name):
        """Dynamically access a valid database operation for this table.

        Args:
            name (str): The name of the operation to access.

        Returns:
            Callable: A partial function that binds the operation to this table's name if the operation is valid.

        Raises:
            AttributeError: If the requested operation is not valid for this table.
        """
        if name in self.valid_operations:
            return partial(getattr(self.db, name), self.name)
        raise AttributeError(f"'{self.__class__.__name__}' object has no attribute '{name}'")


class Database(ABC):
    """Base class for all databases.

    Attributes:
        config (dict[Any, Any]): The configuration for the database.
    """

    def __enter__(self) -> "Database":
        """Enter the database context.

        Returns:
            Database: The database instance.
        """
        return self

    def __init__(self, name: str, config: dict[Any, Any], models: dict[str, type[Record]] | None = None):
        if models is None:
            models = {}
        self.config = config
        self.name = name
        self.models = models

    def __exit__(self, exc_type, exc_value, traceback) -> None:
        """Exit the database context.

        Args:
            exc_type: The type of exception raised, if any.
            exc_value: The exception instance raised, if any.
            traceback: The traceback object, if an exception was raised.
        """
        self.close()

    def close(self) -> bool:
        """Close the database.

        Returns:
            bool: True if the database was successfully closed, False otherwise.
        """
        return True

    def add_link(
        self,
        table_name: str,
        field_key: FieldKey,
        record_id: ID,
        other_record_id: ID,
    ) -> bool:
        """Add a link between the record with the given record_id and the record with the given other_record_id in the other table.

        Args:
            table_name (str): The name of the table to which the link is being added.
            field_key (FieldKey): The key of the field representing the link.
            record_id (ID): The ID of the record to link.
            other_record_id (ID): The ID of the other record to link.

        Returns:
            bool: True if the link was successfully added, False otherwise.
        """
        return self.batch_add_link(table_name, field_key, {record_id: [other_record_id]})

    def batch_add_link(
        self,
        table_name: str,
        field_key: FieldKey,
        record_ids_map: dict[ID, list[ID]],
    ) -> bool:
        """Add links between the records with the given record_ids and the records with the given other_record_ids in the other table.

        Args:
            table_name (str): The name of the table to which the links are being added.
            field_key (FieldKey): The key of the field representing the link.
            record_ids_map (dict[ID, list[ID]]): A dictionary mapping record IDs to a list of other record IDs to link.

        Returns:
            bool: True if the links were successfully added, False otherwise.
        """
        record_ids = list(record_ids_map.keys())
        current_other_record_ids_map = self._get_linked_records(table_name, field_key, record_ids)
        updated_record_ids_map = {}
        for record_id, other_record_ids in record_ids_map.items():
            current_other_record_ids = current_other_record_ids_map[record_id]
            updated_record_ids_map[record_id] = current_other_record_ids + other_record_ids
        return self.batch_update_links(table_name, field_key, updated_record_ids_map)

    def create(self, table_name: str, fields: Fields) -> Record:
        """Create a new record with the given fields.

        Args:
            table_name (str): The name of the table where the record will be created.
            fields (Fields): A dictionary of field keys and values representing the record's data.

        Returns:
            Record: The newly created record instance.
        """
        return self.batch_create(table_name, [fields])[0]

    def delete(self, table_name: str, record_id: ID) -> bool:
        """Delete the record with the given record_id.

        Args:
            table_name (str): The name of the table from which the record will be deleted.
            record_id (ID): The ID of the record to delete.

        Returns:
            bool: True if the record was successfully deleted, False otherwise.
        """
        return self.batch_delete(table_name, [record_id])

    def get(self, table_name: str, record_id: ID) -> Record:
        """Get the record with the given record_id.

        Args:
            table_name (str): The name of the table from which the record will be retrieved.
            record_id (ID): The ID of the record to retrieve.

        Returns:
            Record: The record instance.
        """
        return self.batch_get(table_name, [record_id])[0]

    def update(self, table_name: str, fields: Fields) -> Record:
        """Update the record with the given record_id with the given fields.

        Args:
            table_name (str): The name of the table where the record will be updated.
            fields (Fields): A dictionary of field keys and values to update.

        Returns:
            Record: The updated record instance.
        """
        return self.batch_update(table_name, [fields])[0]

    def update_link(
        self,
        table_name: str,
        field_key: FieldKey,
        other_table_name: str,
        record_id: ID,
        other_record_ids: list[ID],
    ) -> bool:
        """Update the link between the record with the given record_id and the records with the given other_record_ids in the other table.

        Args:
            table_name (str): The name of the table where the link will be updated.
            field_key (FieldKey): The key of the field representing the link.
            other_table_name (str): The name of the other table.
            record_id (ID): The ID of the record to update.
            other_record_ids (list[ID]): A list of IDs of the other records to link.

        Returns:
            bool: True if the link was successfully updated, False otherwise.
        """
        return self.batch_update_links(table_name, field_key, {record_id: other_record_ids})

    @abstractmethod
    def batch_create(self, table_name: str, fields_list: list[Fields]) -> list[Record]:
        """Create new records with the given fields.

        Args:
            table_name (str): The name of the table where the records will be created.
            fields_list (list[Fields]): A list of dictionaries, where each dictionary represents the fields of a record.

        Returns:
            list[Record]: A list of newly created record instances.
        """
        raise NotImplementedError

    @abstractmethod
    def batch_delete(self, table_name: str, record_ids: list[ID]) -> bool:
        """Delete the records with the given record_ids.

        Args:
            table_name (str): The name of the table from which the records will be deleted.
            record_ids (list[ID]): A list of record IDs to delete.

        Returns:
            bool: True if the records were successfully deleted, False otherwise.
        """
        raise NotImplementedError

    @abstractmethod
    def batch_get(self, table_name: str, record_ids: list[ID]) -> list[Record]:
        """Get the records with the given record_ids.

        Args:
            table_name (str): The name of the table from which the records will be retrieved.
            record_ids (list[ID]): A list of record IDs to retrieve.

        Returns:
            list[Record]: A list of record instances.
        """
        raise NotImplementedError

    @abstractmethod
    def batch_update(self, table_name: str, fields_list: list[Fields]) -> list[Record]:
        """Update the records with the given record_ids with the given fields.

        Args:
            table_name (str): The name of the table where the records will be updated.
            fields_list (list[Fields]): A list of tuples, where each tuple contains a record ID and a dictionary of fields to update.

        Returns:
            list[Record]: A list of updated record instances.
        """
        raise NotImplementedError

    @abstractmethod
    def batch_update_links(
        self,
        table_name: str,
        field_key: FieldKey,
        record_ids_map: dict[ID, list[ID]],
    ) -> bool:
        """Update the links between the records with the given record_ids and the records with the given other_record_ids in the other table.

        Args:
            table_name (str): The name of the table where the links will be updated.
            field_key (FieldKey): The key of the field representing the link.
            record_ids_map (dict[ID, list[ID]]): A dictionary mapping record IDs to a list of other record IDs to link.

        Returns:
            bool: True if the links were successfully updated, False otherwise.
        """
        raise NotImplementedError

    @abstractmethod
    def _get_linked_records(self, table_name: str, field_key: FieldKey, record_ids: list[ID]) -> dict[ID, list[ID]]:
        """Get the linked records for the given record_ids.

        Args:
            table_name (str): The name of the table from which linked records will be retrieved.
            field_key (FieldKey): The key of the field representing the link.
            record_ids (list[ID]): A list of record IDs.

        Returns:
            dict[ID, list[ID]]: A dictionary mapping record IDs to a list of linked record IDs.
        """
        raise NotImplementedError

    @abstractmethod
    def query(self, table_name: str, query: Query | None = None) -> list[Record]:
        """Query the table with the given query.

        Args:
            table_name (str): The name of the table that will be queried.
            query (Query | None): The query object to execute.

        Returns:
            list[Record]: A list of record instances matching the query.
        """
        raise NotImplementedError

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
