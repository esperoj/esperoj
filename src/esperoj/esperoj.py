"""Module that contains the Esperoj class, which can ingest and archive files."""

import logging

from esperoj.database.database import DatabaseFactory
from esperoj.storage.storage import StorageFactory


class Esperoj:
    def __init__(
        self,
        config: dict,
        databases,
        storages,
        loggers,
    ):
        self.config = config
        self.databases = databases
        self.loggers = loggers
        self.storages = storages

    def __getattr__(self, name):
        try:
            mod = __import__(f"esperoj.scripts.{name}", None, None, ["esperoj_method"])
        except ImportError:
            return

        def add_esperoj(func):
            def wrapper(*args, **kwargs):
                args = (self,) + args
                return func(*args, **kwargs)

            return wrapper

        return add_esperoj(mod.esperoj_method)


class EsperojFactory:
    """EsperojFactory class."""

    @staticmethod
    def create(config: dict):
        storages = {}
        databases = {}
        loggers = {}
        logger = logging.getLogger("Esperoj")
        logger.setLevel(logging.INFO)
        handler = logging.StreamHandler()
        formatter = logging.Formatter("%(asctime)s - %(name)s - %(levelname)s - %(message)s")
        handler.setFormatter(formatter)
        logger.addHandler(handler)

        loggers["Primary"] = logger
        for storage_config in config["storages"]:
            storage = StorageFactory.create(storage_config)
            for name in [storage.config["name"]] + storage.config["aliases"]:
                storages[name] = storage
        for database_config in config["databases"]:
            database = DatabaseFactory.create(database_config)
            for name in [database.config["name"]] + database.config["aliases"]:
                databases[name] = database
        return Esperoj(config=config, databases=databases, storages=storages, loggers=loggers)
