from typing import Any

from esperoj.database.database import ID, Database, FieldKey, Fields, Query, Record, Table


class MemoryDatabase(Database):
    def __init__(self, name: str, config: dict[Any, Any], models: dict[str, type[Record]] | None = None):
        if models is None:
            models = {}
        super().__init__(name, config, models)
        self.tables = {}

    def batch_create(self, table_name: str, fields_list: list[Fields]) -> list[Record]:
        table = self.tables.get(table_name)
        if table is None:
            raise ValueError(f"Table {table_name} does not exist.")

        model_class = self.models.get(table_name, Record)
        records = [model_class(**fields) for fields in fields_list]
        table.extend(records)
        return records

    def batch_delete(self, table_name: str, record_ids: list[ID]) -> bool:
        table = self.tables.get(table_name)
        if table is None:
            raise ValueError(f"Table {table_name} does not exist.")
        initial_count = len(table)
        table[:] = [record for record in table if record.id not in record_ids]
        return len(table) < initial_count

    def batch_get(self, table_name: str, record_ids: list[ID]) -> list[Record]:
        table = self.tables.get(table_name)
        if table is None:
            raise ValueError(f"Table {table_name} does not exist.")
        return [record for record in table if record.id in record_ids]

    def batch_update(self, table_name: str, records: list[tuple[ID, Fields]]) -> list[Record]:
        table = self.tables.get(table_name)
        if table is None:
            raise ValueError(f"Table {table_name} does not exist.")
        updated_records = []
        for record_id, fields in records:
            for record in table:
                if record.id == record_id:
                    record.__dict__.update(fields)
                    updated_records.append(record)
        return updated_records

    def batch_update_links(self, table_name: str, field_key: FieldKey, record_ids_map: dict[ID, list[ID]]) -> bool:
        table = self.tables.get(table_name)
        if table is None:
            raise ValueError(f"Table {table_name} does not exist.")
        for record in table:
            if record.id in record_ids_map:
                setattr(record, field_key, record_ids_map[record.id])
        return True

    def get_linked_records(self, table_name: str, field_key: FieldKey, record_ids: list[ID]) -> dict[ID, list[ID]]:
        table = self.tables.get(table_name)
        if table is None:
            raise ValueError(f"Table {table_name} does not exist.")
        result = {}
        for record in table:
            if record.id in record_ids:
                result[record.id] = getattr(record, field_key, [])
        return result

    def query(self, table_name: str, query: Query | None = None) -> list[Record]:
        table = self.tables.get(table_name)
        if table is None:
            raise ValueError(f"Table {table_name} does not exist.")
        if query is None:
            return table
        return table

    def create_table(self, name: str) -> Table:
        if name in self.tables:
            raise ValueError(f"Table {name} already exists.")
        self.tables[name] = []
        return Table(name, self)

    def get_table(self, name: str) -> Table:
        if name not in self.tables:
            raise ValueError(f"Table {name} does not exist.")
        return Table(name, self)
