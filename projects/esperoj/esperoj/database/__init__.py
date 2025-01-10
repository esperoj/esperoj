"""Database module."""
from esperoj.database.models import table_models
from esperoj.database.database import Record, Database
from esperoj.config import getConfig

databases = {}

def createDatabase(config: dict, models: dict[str, type[Record]] | None = table_models) -> Database:
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

def getDatabase(name):
    if not (database := databases.get(name)):
        for database_config in getConfig()["databases"]:
            if database_config["name"] == name:
                database = createDatabase(database_config)
        databases[name] = database
    return database

def getAllDatabases():
    for database_config in getConfig()["databases"]:
        getDatabase(database_config["name"])
    return databases