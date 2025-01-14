"""Database module."""

from esperoj.config import get_config
from esperoj.database.database import Database, Record
from esperoj.database.models import table_models

databases = {}


def create_database(config: dict, models: dict[str, type[Record]] | None = table_models) -> Database:
    """Create a database instance based on the provided configuration.

    Args:
        config (dict): The configuration dictionary for the database, which must include a 'type' key
                       specifying the database type.
        models: The list of models used by thr database.

    Returns:
        Database: The database instance corresponding to the specified type.

    Raises:
        ValueError: If the database type in the configuration is unknown.
    """
    database_type = config["type"]
    name = config["name"]
    match database_type:
        case "seatable":
            from esperoj.database.seatable import SeatableDatabase

            return SeatableDatabase(name, config, models=models)
        case "memory":
            from esperoj.database.memory import MemoryDatabase

            return MemoryDatabase(name, config, models=models)
    raise ValueError(f"Unknown database type: {database_type}")


def get_database(name, models: dict[str, type[Record]] | None = table_models):
    if not (database := databases.get(name)):
        for database_config in get_config()["databases"]:
            names = [database_config["name"], *database_config.get("aliases", [])]
            if name in names:
                database = create_database(database_config, models)
                for _name in names:
                    databases[_name] = database
    if database is None:
        raise ValueError(f"Can't find database '{name}'")
    return database


def get_all_databases():
    for database_config in get_config()["databases"]:
        get_database(database_config["name"])
    return databases
