"""Database module."""

from abc import ABC, abstractmethod
from collections.abc import Iterable
from dataclasses import dataclass
from typing import Any, Self


@dataclass
class Record(ABC):
    record_id: Any
    fields: dict[str, Any]

    @abstractmethod
    def update(self, fields: dict[str, Any]) -> Self:
        pass


class Table(ABC):
    @abstractmethod
    def create(self, fields: dict[str, Any]) -> Record:
        pass

    @abstractmethod
    def delete(self, record_id: Any) -> Any:
        """Delete a record and return id."""

    @abstractmethod
    def get(self, record_id: Any) -> Record:
        pass

    @abstractmethod
    def get_all(self, formulas: dict[str, Any] = {}) -> Iterable[Record]:
        """Get all records with formulas.

        Example: get_all({"Name":"Long", "Age":9})
        """

    @abstractmethod
    def update(self, record_id: Any, fields: dict[str, Any]) -> Record:
        pass

    def create_many(self, fields_list: Iterable[dict[str, Any]]) -> Iterable[Record]:
        return [self.create(fields) for fields in fields_list]

    def delete_many(self, record_ids: Iterable[Any]) -> Iterable[Any]:
        return [self.delete(record_id) for record_id in record_ids]

    def get_many(self, record_ids: Iterable[Any]) -> Iterable[Record]:
        return [self.get(record_id) for record_id in record_ids]

    def update_many(self, records: Iterable[dict[str, Any]]) -> Iterable[Record]:
        return [self.update(**record) for record in records]

class Database(ABC):
    @abstractmethod
    def table(self, name: str) -> Table:
        pass

    @abstractmethod
    def close(self) -> bool:
        pass
