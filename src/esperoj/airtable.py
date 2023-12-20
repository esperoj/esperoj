"""Module contains Airtable class."""

import os
from typing import Any

from pyairtable import Api
from pyairtable.formulas import match

from esperoj.database import BaseTable, Database
from esperoj.exceptions import InvalidRecordError, RecordNotFoundError


class AirtableTable(BaseTable):
    """
    A class to interact with a specific table in Airtable.

    This class provides methods to create, retrieve, update, and delete records in the table.

    Attributes
    ----------
        client: The client to interact with the Airtable API.
    """

    def __init__(self, name: str, client):
        """
        Initialize an AirtableTable instance.

        Args:
            name (str): The name of the table.
            client: The client to interact with the Airtable API.
        """
        super().__init__(name)
        self.client = client

    def create(self, fields: dict[str, Any]) -> dict[str, Any]:
        """
        Create a new record in the table.

        Args:
            fields (dict): The fields of the record to create.

        Returns
        -------
            dict: The created record.

        Raises
        ------
            InvalidRecordError: If fields is not a dictionary.
        """
        if not isinstance(fields, dict):
            raise InvalidRecordError("fields must be a dictionary.")
        return self.client.create(fields)

    def create_many(self, records: list[dict[str, Any]]) -> list[dict[str, Any]]:
        """
        Create multiple new records in the table.

        Args:
            records (list): A list of dictionaries, each representing a record to create.

        Returns
        -------
            list: The created records.

        Raises
        ------
            InvalidRecordError: If records is not a list of dictionaries.
        """
        if not isinstance(records, list):
            raise InvalidRecordError("records must be a list of dictionaries.")
        return self.client.batch_create(records)

    def delete(self, record_id: str) -> str:
        """
        Delete a record from the table.

        Args:
            record_id (str): The ID of the record to delete.

        Returns
        -------
            str: The ID of the deleted record.

        Raises
        ------
            InvalidRecordError: If record_id is not a string.
            RecordNotFoundError: If the record with the given ID does not exist.
            ValueError: If deletion failed.
        """
        if not isinstance(record_id, str):
            raise InvalidRecordError("record_id must be a string.")

        record = self.get(record_id)
        if record is None:
            raise RecordNotFoundError(f"Record with id '{record_id}' not found.")
        result = self.client.delete(record_id)
        if result["deleted"] is False:
            raise ValueError(f"Deletion failed for id: {result['id']}")
        return record_id

    def delete_many(self, record_ids: list[str]) -> list[str]:
        """
        Delete multiple records from the table.

        Args:
            record_ids (list): A list of record IDs to delete.

        Returns
        -------
            list: The IDs of the deleted records.

        Raises
        ------
            InvalidRecordError: If record_ids is not a list of strings.
            ValueError: If deletion of Any record failed.
        """
        if not isinstance(record_ids, list):
            raise InvalidRecordError("record_ids must be a list of strings.")
        results = self.client.batch_delete(record_ids)
        for result in results:
            if result["deleted"] is False:
                raise ValueError(f"Deletion failed for id: {result['id']}")
        return record_ids

    def get(self, record_id: str) -> dict[str, Any]:
        """
        Retrieve a record from the table.

        Args:
            record_id (str): The ID of the record to retrieve.

        Returns
        -------
            dict: The retrieved record.

        Raises
        ------
            InvalidRecordError: If record_id is not a string.
            RecordNotFoundError: If the record with the given ID does not exist.
        """
        if not isinstance(record_id, str):
            raise InvalidRecordError("record_id must be a string.")

        record = self.client.get(record_id)
        if record is None:
            raise RecordNotFoundError(f"Record with id '{record_id}' not found.")

        return record

    def get_all(self) -> list[dict[str, Any]]:
        """
        Retrieve all records from the table.

        Returns
        -------
            list: A list of all records in the table.
        """
        return self.client.all()

    def get_many(self, record_ids: list[str]) -> list[dict[str, Any]]:
        """
        Retrieve multiple records from the table.

        Args:
            record_ids (list): A list of record IDs to retrieve.

        Returns
        -------
            list: The retrieved records.

        Raises
        ------
            InvalidRecordError: If record_ids is not a list of strings.
        """
        if not isinstance(record_ids, list):
            raise InvalidRecordError("record_ids must be a list of strings.")
        records = []
        for record_id in record_ids:
            record = self.get(record_id)
            records.append(record)
        return records

    def match(self, pattern: dict[str, Any]) -> list[dict[str, Any]]:
        """
        Retrieve records from the table that match a certain pattern.

        Args:
            pattern (dict): The pattern to match records against.

        Returns
        -------
            list: The matching records.

        Raises
        ------
            InvalidRecordError: If pattern is not a dictionary.
        """
        if not isinstance(pattern, dict):
            raise InvalidRecordError("pattern must be a dictionary.")

        formula = match(pattern)
        return self.client.all(formula=formula)

    def update(self, record: dict[str, Any]) -> dict[str, Any]:
        """
        Update a record in the table.

        Args:
            record (dict): The record to update, must contain 'id' and 'fields' keys.

        Returns
        -------
            dict: The updated record.

        Raises
        ------
            InvalidRecordError: If record is not a dictionary or does not contain 'id' and 'fields' keys.
            RecordNotFoundError: If the record with the given ID does not exist.
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

        return self.client.update(record_id, updated_fields)

    def update_many(self, records: list[dict[str, Any]]) -> list[dict[str, Any]]:
        """
        Update multiple records in the table.

        Args:
            records (list): A list of records to update.

        Returns
        -------
            list: The updated records.

        Raises
        ------
            InvalidRecordError: If records is not a list of dictionaries.
        """
        if not isinstance(records, list):
            raise InvalidRecordError("records must be a list of dictionaries.")
        return self.client.batch_update(records)


class Airtable(Database):
    """
    A class to interact with the Airtable API.

    Attributes
    ----------
        api_key (str): The API key for the Airtable API.
        base_id (str): The base ID for the Airtable API.
        client (Api): The client to interact with the Airtable API.
    """

    def __init__(self, name: str = "Airtable", api_key: str = "", base_id: str = ""):
        """
        Initialize an Airtable instance.

        Args:
            name (str, optional): The name of the database. Defaults to "Airtable".
            api_key (str, optional): The API key for the Airtable API. Defaults to "".
            base_id (str, optional): The base ID for the Airtable API. Defaults to "".
        """
        super().__init__(name)
        self.api_key = api_key or str(os.getenv("AIRTABLE_API_KEY"))
        self.base_id = base_id or str(os.getenv("AIRTABLE_BASE_ID"))
        self.client = Api(self.api_key)

    def table(self, name: str):
        """
        Get an AirtableTable instance for the specified table name.

        Args:
            name (str): The name of the table.

        Returns
        -------
            AirtableTable: An instance of AirtableTable for the specified table name.

        Raises
        ------
            ValueError: If name is not a string.
        """
        if not isinstance(name, str):
            raise ValueError("Table id must be a string.")
        return AirtableTable(name, self.client.table(self.base_id, name))

    def close(self) -> None:
        """Close the connection to the Airtable API. This method does not do Anything as the pyairtable library does not."""
        return
