"""orm module."""

from typing import Any, Self

from pydantic import BaseModel

FieldValue = Any
FieldKey = str
Fields = dict[FieldKey, FieldValue]


class OrmRecord(BaseModel):
    _client: "OrmClient | None" = None
    _table_name: str | None = None
    _primary_key: str = "id"

    def get_client(self) -> "OrmClient | None":
        return self._client

    def set_client(self, client: "OrmClient | None") -> "OrmClient | None":
        self._client = client
        return self._client

    def get_table_name(self) -> str | None:
        return self._table_name

    def set_table_name(self, table_name: str | None) -> str | None:
        self._table_name = table_name
        return self._table_name

    def delete(self) -> bool:
        """Delete the record from the database.

        Returns:
            bool: True if the record was successfully deleted, False otherwise.
        """
        return self._client.delete(self._primary_key)

    def update(self, fields: Fields) -> Self:
        """Update the record with the given fields.

        Args:
            fields (Fields): A dictionary of field keys and values to update.

        Returns:
            OrmRecord: The updated record instance.
        """
        if self.update(self._primary_key, fields):
            self.fields.update(fields)
        return self


class OrmClient:
    pass
